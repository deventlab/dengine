//! High-performance buffered Raft log — notify-then-spawn-fsync architecture.
//!
//! ## Durability guarantee (Level 3 — concurrent pipeline, #422)
//!
//! **Level 3 (current)**: `db.write()` → OS page cache → concurrent `fdatasync` via `spawn_blocking`
//! - Power-loss safe: `durable_index` advances only after physical fsync completes
//! - Process crash safe: RocksDB WAL replay on restart
//! - Performance: IO thread never blocks on fsync — batches pipeline at OS/RocksDB layer
//!
//! **Level 2 (pre-#407)**: `db.write()` → OS page cache only, no fdatasync
//! - Was the default; superseded by Level 3 for correctness
//!
//! ## Write path
//!
//! `append_entries` inserts entries into in-memory SkipMap, calls `write_notify.notify_one()`.
//! Multiple concurrent writers coalesce into a single IO thread wakeup.
//!
//! ## IO thread (notify-then-spawn-fsync)
//!
//! On wakeup from `write_notify` (`run_batch_turn`):
//! 1. **Read** — scan SkipMap range `(durable_index, memory_max_index]`
//! 2. **Persist** — write range to OS page cache via `persist_entries` (no fsync)
//! 3. **Dispatch fsync** — `FsyncCoordinator::submit()` hands the fdatasync to a
//!    `spawn_blocking` task and returns immediately
//! 4. **Loop** — back to `select!`; the prior fsync runs concurrently in the pool
//!
//! ## How `durable_index` advances (#446 single-owner)
//!
//! The blocking fsync task does **not** write `durable_index`. On completion it
//! calls `notify_fsync_completed(index, term)`, sending
//! `InternalEvent::FsyncCompleted { index, term }` to `raft.rs`'s event loop —
//! the sole owner of `durable_index`. That loop calls
//! `try_advance_durable_index(index, term)`, which content-validates the report
//! (`entry_term(index) == Some(term)`) and clamps to `memory_max_index` before
//! `fetch_max`. A stale report — for entries a concurrent truncation already
//! discarded — is rejected. `FsyncCoordinator`'s `generation` fence
//! (`fence_truncation` / `fence_reset`) is the first line of defence: a fsync
//! round whose generation changed mid-flight never sends its completion at all.
//!
//! ## Fsync triggers
//!
//! 1. **Notify-driven** (normal): `write_notify` → `run_batch_turn`
//! 2. **Explicit** (flush API): `flush()` → `IOTask::Flush(tx)` → `run_batch_turn` with a reply sender
//! 3. **Idle timer** (safety net): `idle_flush_interval_ms` elapsed →
//!    `persist_pending_range` + `FsyncCoordinator::submit()` directly (not via `run_batch_turn`)
//! 4. **Shutdown**: `IOTask::Shutdown` → `run_batch_turn`, then `close()` waits
//!    (bounded by `shutdown_timeout_ms`) for the IO thread's runtime to drain any in-flight fsync
//!
//! ## Durability contract
//!
//! `durable_index` advances only after a physical fdatasync completes — and only
//! on `raft.rs`'s event loop, after content validation. Concurrent fsyncs
//! coalesce at the storage layer: if batch B's fsync covers A's WAL position,
//! A's `flush_wal` returns fast with no extra disk IO — storage-layer group commit.

use super::fsync_coordinator::FsyncCoordinator;
use crate::Error;
use crate::FlushPolicy;
use crate::HardState;
use crate::LogStore;
use crate::MetaStore;
use crate::NetworkError;
use crate::PersistenceConfig;
use crate::RaftLog;
use crate::Result;
use crate::StorageEngine;
use crate::TypeConfig;
use crate::alias::SOF;
use crate::scoped_timer::ScopedTimer;
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tracing::debug;
use tracing::error;
use tracing::warn;

/// Maximum number of historical term segments (one per leader election).
/// 1024 is far more than any realistic cluster lifetime.
const MAX_TERM_SEGMENTS: usize = 1024;

/// Compact term boundary index for O(1) `entry_term()` lookups.
///
/// Hot path (99%+ of queries): two `Acquire` loads, no lock, no CAS.
/// Cold path (election recovery): atomic array reverse-scan, no lock, no unsafe.
///
/// When the array is full, new segments are silently dropped and `entry_term()`
/// falls back to the SkipMap (O(log n)) — correct but slower.
///
/// Memory: 2 AtomicU64 + 1 AtomicUsize + 2×1024 AtomicU64 = ~16KB, all inline.
pub(crate) struct TermSegments {
    /// Term of the most-recently appended segment.
    pub(crate) last_term: AtomicU64,
    /// First log index belonging to `last_term`.
    pub(crate) last_term_start: AtomicU64,
    /// Number of valid historical segments stored in `seg_starts`/`seg_terms`.
    seg_count: AtomicUsize,
    /// First index of each historical segment, in append order.
    seg_starts: [AtomicU64; MAX_TERM_SEGMENTS],
    /// Term of each historical segment, parallel to `seg_starts`.
    seg_terms: [AtomicU64; MAX_TERM_SEGMENTS],
}

impl TermSegments {
    pub(crate) fn new() -> Self {
        Self {
            last_term: AtomicU64::new(0),
            last_term_start: AtomicU64::new(0),
            seg_count: AtomicUsize::new(0),
            seg_starts: std::array::from_fn(|_| AtomicU64::new(0)),
            seg_terms: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    /// Return the term for `index`, or `None` if the log is empty or index is out of range.
    ///
    /// Hot path: two `Acquire` loads, no lock, no CAS — O(1).
    /// Cold path: reverse-scan atomic array, k = election count (typically < 10) — O(k).
    pub(crate) fn get(
        &self,
        index: u64,
    ) -> Option<u64> {
        let last_start = self.last_term_start.load(Ordering::Acquire);
        let last_term = self.last_term.load(Ordering::Acquire);
        if last_term == 0 {
            return None;
        }
        if index >= last_start {
            return Some(last_term);
        }
        // Cold path: reverse-scan historical segments.
        let count = self.seg_count.load(Ordering::Acquire);
        (0..count).rev().find_map(|i| {
            let start = self.seg_starts[i].load(Ordering::Acquire);
            if start <= index {
                Some(self.seg_terms[i].load(Ordering::Acquire))
            } else {
                None
            }
        })
    }

    /// Update after appending entries at the tail. Entries must be in ascending index order.
    ///
    /// Common case (same term): one `Acquire` load, no write — O(1).
    /// Term change: two atomic stores to next slot, no lock — O(1).
    pub(crate) fn on_append(
        &self,
        entries: &[Entry],
    ) {
        for entry in entries {
            let lt = self.last_term.load(Ordering::Acquire);
            if entry.term == lt {
                // Same term: pull segment start back if needed (truncation + re-insert).
                let ls = self.last_term_start.load(Ordering::Acquire);
                if entry.index < ls {
                    self.last_term_start.store(entry.index, Ordering::Release);
                }
                continue;
            }
            if lt == 0 {
                // First entries ever: initialise hot atomics.
                self.last_term_start.store(entry.index, Ordering::Release);
                self.last_term.store(entry.term, Ordering::Release);
                continue;
            }
            // New term boundary: archive current segment into the next slot.
            // When the array is full, skip the write — entry_term() falls back
            // to the SkipMap cold path for any overflow segments.
            let ls = self.last_term_start.load(Ordering::Acquire);
            let i = self.seg_count.fetch_add(1, Ordering::AcqRel);
            if i < MAX_TERM_SEGMENTS {
                self.seg_starts[i].store(ls, Ordering::Release);
                self.seg_terms[i].store(lt, Ordering::Release);
            }
            // Always update hot atomics so the current term remains O(1).
            self.last_term_start.store(entry.index, Ordering::Release);
            self.last_term.store(entry.term, Ordering::Release);
        }
    }

    /// Reset to empty. Called on log reset (snapshot install / full rewind).
    pub(crate) fn clear(&self) {
        self.seg_count.store(0, Ordering::Release);
        self.last_term.store(0, Ordering::Release);
        self.last_term_start.store(0, Ordering::Release);
    }
}

/// IO tasks for the dedicated raft-io thread.
///
/// All blocking storage operations route through this channel so they never run
/// on tokio worker threads or the inbound event loop.
#[derive(Debug)]
pub enum IOTask {
    /// Atomically truncate from `truncate_from` then persist `new_entries`.
    /// Conflict-resolution path: truncate + write are a single atomic IO unit.
    /// `done` is signalled after the IO thread finishes the replace so callers
    /// can flush() knowing the truncation is durable.
    ReplaceRange {
        truncate_from: u64,
        new_entries: Vec<Entry>,
        done: oneshot::Sender<Result<()>>,
    },
    /// Purge log entries up to `cutoff` from storage.
    Purge {
        cutoff: LogId,
        done: oneshot::Sender<()>,
    },
    /// Reset log storage (snapshot install). Routes through IO thread so reset
    /// never runs on the inbound event loop.
    Reset { done: oneshot::Sender<Result<()>> },
    /// Persist any pending entries then fsync. The IO thread sends the fsync
    /// result (Ok or Err) back to the caller via the oneshot channel.
    /// Replaces the former `FlushNow` + `WaitDurable` two-message dance.
    Flush(oneshot::Sender<Result<()>>),
    /// Shutdown the IO thread
    Shutdown,
}

/// High-performance buffered Raft log with event-driven architecture
///
/// This implementation provides in-memory first access with configurable
/// persistence strategies while ensuring thread safety and avoiding deadlocks.
///
/// Key design principles:
/// - Lock-free reads for 99% of operations
/// - Event-driven asynchronous processing
/// - Deadlock prevention through proper error handling
/// - Memory-efficient batch operations
pub struct BufferedRaftLog<T>
where
    T: TypeConfig,
{
    node_id: u32,

    pub(crate) log_store: Arc<<SOF<T> as StorageEngine>::LogStore>,
    pub(crate) meta_store: Arc<<SOF<T> as StorageEngine>::MetaStore>,

    /// Safety-net timer interval (ms). Normal-path latency is determined by fsync time.
    idle_flush_interval_ms: u64,

    /// Max time to wait, on shutdown, for an in-flight fsync task to finish before
    /// giving up. Bounds `close()` against a stuck/slow disk — the task itself is
    /// not cancelled, it keeps running in the background regardless.
    shutdown_timeout_ms: u64,

    // --- In-memory state ---
    // The Raft log itself: every entry this node currently holds in memory.
    // This is the single source of truth — every other field below is either
    // a boundary marker or a lookup accelerator derived from what's here.
    pub(crate) entries: RwLock<SkipMap<u64, Entry>>,
    // How far the log has actually been made crash-safe (fsynced to disk).
    // Raft must not tell a client or a peer a write is safe ahead of this point,
    // regardless of what's already visible in `entries`.
    pub(crate) durable_index: AtomicU64,

    // The next index to be allocated
    pub(crate) next_id: AtomicU64,

    // --- In-memory index ---
    // O(1) answer to "is this index currently held in memory" — lets callers
    // (e.g. entry_term()) reject an out-of-range index without touching `entries`.
    min_index: AtomicU64,        // Smallest log index (0 if empty)
    memory_max_index: AtomicU64, // Largest log index held in memory (0 if empty) — may be ahead of what's persisted/durable

    // The term of the last entry ever purged (compacted away after a snapshot).
    // Raft's AppendEntries consistency check needs the term at prev_log_index
    // even when that entry no longer physically exists in `entries` — without
    // this, a follower can't tell "purged, but we agree" apart from "conflict".
    //
    // Must be published in the same critical section as the entries removal
    // and the min_index/memory_max_index advance it corresponds to — a reader must
    // never be able to observe the entry gone but this boundary not yet set.
    last_purged_index: AtomicU64,
    last_purged_term: AtomicU64,

    // Where each term starts/ends. Lets a leader/follower jump straight to a
    // term boundary during post-election conflict backtracking, instead of
    // walking the log entry by entry to find where history diverged.
    term_first_index: SkipMap<u64, AtomicU64>,
    term_last_index: SkipMap<u64, AtomicU64>,

    // Same question as term_first/last_index ("what term is at index i"), but
    // optimized for the by-far most common case: a query near the current tail
    // of the log, asked on essentially every AppendEntries in normal replication.
    term_segments: TermSegments,

    // --- Flush coordination ---
    /// Coalesced write notification. `append_entries` calls `notify_one()` after
    /// inserting into the SkipMap. Multiple concurrent writers coalesce into a
    /// single IO thread wakeup, eliminating per-write kernel cond_signal overhead.
    pub(crate) write_notify: Arc<Notify>,
    pub(crate) command_sender: mpsc::UnboundedSender<IOTask>,

    // --- P0: LogFlushed event notification ---
    // Sends InternalEvent::LogFlushed(durable) to Raft loop after each fsync.
    // None in tests; Some(internal_event_tx) in production.
    log_flush_tx: Option<mpsc::UnboundedSender<crate::InternalEvent>>,

    // IO thread handle — set by start(), used by close() to join before returning.
    io_thread_handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,

    fsync_coordinator: Arc<FsyncCoordinator>,

    // set once on first fsync failure, never cleared for this instance's lifetime
    pub(super) poisoned: AtomicBool,
}

#[async_trait]
impl<T> RaftLog for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    ///TODO: not considered the order of configured storage rule
    /// also should we remove Result<>?
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>> {
        Ok(self.entries.read().get(&index).map(|e| e.value().clone()))
    }

    fn first_entry_id(&self) -> u64 {
        self.min_index.load(Ordering::Acquire)
    }

    fn last_entry_id(&self) -> u64 {
        self.memory_max_index.load(Ordering::Acquire)
    }

    fn durable_index(&self) -> u64 {
        // fsync is async; only entries confirmed by batch_processor are crash-safe.
        self.durable_index.load(Ordering::Acquire)
    }

    fn last_entry(&self) -> Option<Entry> {
        let last_index = self.last_entry_id();
        if last_index > 0 {
            self.entry(last_index).ok().flatten()
        } else {
            None
        }
    }

    // #446: this is what election-eligibility comparisons (is_target_log_more_recent)
    // read. It must keep reflecting the in-memory tail, never durable_index — a node
    // with an un-fsynced entry must still be able to reject a less-up-to-date candidate.
    fn last_log_id(&self) -> Option<LogId> {
        let last_index = self.last_entry_id();
        if last_index > 0 {
            self.entry(last_index).ok().flatten().map(|entry| LogId {
                term: entry.term,
                index: entry.index,
            })
        } else {
            // Log is empty (e.g. immediately after snapshot install).
            // Return the snapshot boundary so the leader sees the correct
            // last-log position and sends entries starting from index+1.
            let purged_index = self.last_purged_index.load(Ordering::Acquire);
            if purged_index > 0 {
                Some(LogId {
                    index: purged_index,
                    term: self.last_purged_term.load(Ordering::Acquire),
                })
            } else {
                None
            }
        }
    }

    fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64> {
        // Bounds check: skip TermSegments entirely for out-of-range queries.
        let max = self.memory_max_index.load(Ordering::Acquire);
        let min = self.min_index.load(Ordering::Acquire);
        if max == 0 || entry_id < min || entry_id > max {
            // Cold path: check purge boundary so that AppendEntries built with
            // prev_log_index == last_purged_index carries the correct term.
            let purged_index = self.last_purged_index.load(Ordering::Acquire);
            if purged_index > 0 && entry_id == purged_index {
                return Some(self.last_purged_term.load(Ordering::Acquire));
            }
            return None;
        }
        // Hot path: TermSegments — O(1) for current segment, O(log k) for history.
        // No SkipMap lookup, no epoch::pin(), no CAS.
        if let Some(term) = self.term_segments.get(entry_id) {
            return Some(term);
        }
        // Fallback: SkipMap — guards against rare transient inconsistency.
        self.entries.read().get(&entry_id).map(|e| e.value().term)
    }

    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.term_first_index.get(&term).map(|e| e.value().load(Ordering::Acquire))
    }

    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64> {
        self.term_last_index.get(&term).map(|e| e.value().load(Ordering::Acquire))
    }

    fn pre_allocate_raft_logs_next_index(&self) -> u64 {
        // self.get_raft_logs_length() + 1
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64> {
        match count {
            0 => u64::MAX..=u64::MAX, // Standard empty range
            _ => {
                // Overflow checking (enable on demand)
                let cur = self.next_id.load(Ordering::SeqCst);
                assert!(cur <= u64::MAX - count, "ID overflow");

                let start = self.next_id.fetch_add(count, Ordering::SeqCst);
                start..=(start + count - 1)
            }
        }
    }

    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>> {
        // OPTIMIZED: SkipMap range scan O(k + log n); pre-allocate to avoid realloc
        let capacity = (range.end().saturating_sub(*range.start()) + 1) as usize;
        let mut result = Vec::with_capacity(capacity);

        let entries = self.entries.read();

        result.extend(entries.range(range).map(|e| e.value().clone()));
        Ok(result)
    }

    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()> {
        // Fast-fail optimization, not a correctness gate — the real durability
        // boundary is persist_pending_range()/fsync_coordinator. Poisoned data
        // reaching memory here is harmless as long as it never gets marked durable.
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }

        let _timer = ScopedTimer::new("append_entries");
        if entries.is_empty() {
            return Ok(());
        }

        self.insert_to_memory(&entries);

        // Signal the IO thread to persist. Fire-and-forget: the entry is in the
        // in-memory log and quorum-visible now; the IO thread scans the SkipMap
        // and persists + fsyncs off this task. Concurrent notify_one() calls
        // coalesce into one wakeup. RPO=0 is enforced downstream (#446), not
        // here: commit quorum counts only `durable_index()`, and followers
        // withhold AppendEntries ACKs until their own `durable_index` catches up.
        self.write_notify.notify_one();

        Ok(())
    }

    async fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()> {
        self.append_entries(logs).await?;
        Ok(())
    }

    async fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>> {
        let _timer = ScopedTimer::new("filter_out_conflicts_and_append");
        // prev_log_index == 0 means the leader wants the follower to start from scratch
        // (e.g. new follower joining, or follower log fully diverged). Reset and replace.
        if prev_log_index == 0 && prev_log_term == 0 {
            self.reset().await?;
            self.append_entries(new_entries.clone()).await?;
            return Ok(new_entries.last().map(|e| LogId {
                term: e.term,
                index: e.index,
            }));
        }

        // Check log consistency: use entry_term() so purge-boundary entries
        // (entries removed from the SkipMap but recorded in last_purged_index/term)
        // are still recognised as valid prev_log positions after snapshot install.
        if self.entry_term(prev_log_index) != Some(prev_log_term) {
            return Ok(self.last_log_id());
        }

        let last_current_index = self.last_entry_id();

        // Step 1: partition_point — O(log n_batch), zero SkipMap lookups.
        // Locates the overlap boundary without touching entry_term() at all.
        let skip = new_entries.partition_point(|e| e.index <= last_current_index);
        let overlap = &new_entries[..skip];
        let tail = &new_entries[skip..];

        // Step 2: Overlap safety check — O(1), two atomic loads.
        //
        // Overlap is safe to skip entirely when all three conditions hold:
        //   (a) The entire overlap range falls within follower's current term segment
        //       (first.index >= last_term_start) — no older-term entries below overlap start.
        //   (b) Incoming overlap entries all carry follower's current term (first.term == last_term).
        //   (c) No term change within incoming overlap (last.term == last_term); guaranteed by
        //       Raft non-decreasing term property when combined with (b).
        //
        // Covers the steady-state pipeline path (99%+ of calls). Election recovery (new leader,
        // term conflict in overlap) is caught by condition (b) failing → slow path.
        let overlap_safe = match overlap.first() {
            None => true,
            Some(first) => {
                let ft = self.term_segments.last_term.load(Ordering::Acquire);
                let fs = self.term_segments.last_term_start.load(Ordering::Acquire);
                first.index >= fs
                    && first.term == ft
                    && overlap.last().is_none_or(|last| last.term == ft)
            }
        };

        let last_log_id = if overlap_safe {
            // Fast path: skip entire overlap, append tail only.
            if tail.is_empty() {
                new_entries.last().map(|e| LogId {
                    term: e.term,
                    index: e.index,
                })
            } else {
                self.append_entries(tail.to_vec()).await?;
                tail.last().map(|e| LogId {
                    term: e.term,
                    index: e.index,
                })
            }
        } else {
            // Slow path: term boundary detected in overlap — scan for first conflict.
            // entry_term() uses TermSegments (O(1)) with SkipMap fallback, so each
            // check is cheap even on this rare election-recovery path.
            let diverge_pos = new_entries.iter().position(|e| {
                e.index > last_current_index || self.entry_term(e.index) != Some(e.term)
            });

            match diverge_pos {
                None => {
                    // All terms matched — idempotent RPC, nothing to do.
                    new_entries.last().map(|e| LogId {
                        term: e.term,
                        index: e.index,
                    })
                }
                Some(pos) => {
                    let tail = &new_entries[pos..];
                    let diverge_index = new_entries[pos].index;

                    if diverge_index <= last_current_index {
                        // Real term conflict: truncate from diverge_index, replace with tail.
                        // Await the done channel so callers can flush() knowing the truncation
                        // is durable — durable_index may exceed memory_max_index after truncation,
                        // which would cause flush() to short-circuit before the replace lands.
                        self.remove_range(diverge_index..=u64::MAX);
                        self.insert_to_memory(tail);
                        let (done_tx, done_rx) = oneshot::channel();
                        self.command_sender
                            .send(IOTask::ReplaceRange {
                                truncate_from: diverge_index,
                                new_entries: tail.to_vec(),
                                done: done_tx,
                            })
                            .map_err(|e| {
                                NetworkError::SingalSendFailed(format!(
                                    "Failed to send ReplaceRange: {e:?}"
                                ))
                            })?;
                        done_rx.await.map_err(|_| {
                            NetworkError::SingalSendFailed(
                                "ReplaceRange done channel closed".into(),
                            )
                        })??;
                    } else {
                        // No conflict — pipeline overlap consumed, append only the new tail.
                        self.append_entries(tail.to_vec()).await?;
                    }

                    tail.last().map(|e| LogId {
                        term: e.term,
                        index: e.index,
                    })
                }
            }
        };

        Ok(last_log_id)
    }

    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        mut peer_matched_ids: Vec<u64>,
    ) -> Option<u64> {
        let _timer = ScopedTimer::new("calculate_majority_matched_index");
        // RPO=0 (#446): leader's own contribution must be its own durable (fsynced)
        // position, not the in-memory tail — otherwise a majority-looking commit can
        // still lose data on correlated power loss.
        peer_matched_ids.push(self.durable_index());

        // Sort in descending order
        peer_matched_ids.sort_unstable_by(|a, b| b.cmp(a));

        // Calculate median as majority index
        let majority_index = peer_matched_ids[peer_matched_ids.len() / 2];

        debug!(
            "Majority calculation: peers={:?}, majority_index={}",
            peer_matched_ids, majority_index,
        );

        // Verify commit conditions
        if majority_index < commit_index {
            return None;
        }

        // Check term consistency
        match self.entry(majority_index) {
            Ok(Some(entry)) if entry.term == current_term => Some(majority_index),
            _ => None,
        }
    }

    async fn purge_logs_up_to(
        &self,
        cutoff_index: LogId,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("purge_logs_up_to");
        debug!(?cutoff_index, "purge_logs_up_to");

        // Remove range + publish last_purged_* atomically in the same lock (#442).
        self.purge_prefix(cutoff_index);

        // Purged entries are backed by the snapshot; treat cutoff as durable.
        // Already running on the single owner (called from role_state.rs, same
        // thread as remove_range) — safe to apply directly, no message hop needed.
        if let Some(new_durable) =
            self.try_advance_durable_index(cutoff_index.index, cutoff_index.term)
            && let Some(ref tx) = self.log_flush_tx
        {
            let _ = tx.send(crate::InternalEvent::LogFlushed {
                durable_index: new_durable,
            });
        }

        // Route purge through the IO thread so it never blocks the inbound event loop.
        // Also writes the purge boundary to META_CF in the RocksDB implementation.
        let (done_tx, done_rx) = oneshot::channel();
        self.command_sender
            .send(IOTask::Purge {
                cutoff: cutoff_index,
                done: done_tx,
            })
            .map_err(|e| NetworkError::SingalSendFailed(format!("Failed to send Purge: {e:?}")))?;
        done_rx
            .await
            .map_err(|_| NetworkError::SingalSendFailed("Purge channel closed".into()))?;

        Ok(())
    }

    fn try_advance_durable_index(
        &self,
        index: u64,
        term: u64,
    ) -> Option<u64> {
        let prev = self.durable_index.load(Ordering::Acquire);
        if index <= prev {
            return None;
        }
        if self.entry_term(index) != Some(term) {
            return None;
        }
        let safe = index.min(
            self.memory_max_index
                .load(Ordering::Acquire)
                .max(self.last_purged_index.load(Ordering::Acquire)),
        );
        if safe <= prev {
            return None;
        }
        self.durable_index.fetch_max(safe, Ordering::AcqRel);
        Some(safe)
    }

    async fn flush(&self) -> Result<()> {
        let memory_max_index = self.memory_max_index.load(Ordering::Acquire);
        if memory_max_index == 0 {
            return Ok(());
        }
        if self.durable_index.load(Ordering::Acquire) >= memory_max_index {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.command_sender
            .send(IOTask::Flush(tx))
            .map_err(|e| NetworkError::SingalSendFailed(format!("flush send failed: {e:?}")))?;
        rx.await
            .map_err(|_| NetworkError::SingalSendFailed("flush channel closed".into()))?
    }

    async fn reset(&self) -> Result<()> {
        let _timer = ScopedTimer::new("buffered_raft_log::reset");
        self.reset_internal().await
    }

    fn load_hard_state(&self) -> Result<Option<HardState>> {
        self.meta_store.load_hard_state()
    }

    fn save_hard_state(
        &self,
        hard_state: &HardState,
    ) -> Result<()> {
        if self.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".into()));
        }
        self.meta_store.save_hard_state(hard_state).inspect_err(|e| {
            error!("save_hard_state failed (fatal): {e:?}");
            self.mark_poisoned_and_notify(format!("save_hard_state failed: {e:?}"));
        })
    }

    fn is_poisoned(&self) -> bool {
        self.is_poisoned()
    }

    async fn close(&self) {
        // Signal the IO thread to flush remaining data and exit.
        let _ = self.command_sender.send(IOTask::Shutdown);
        // Take the handle — idempotent; second call is a no-op.
        let handle = self.io_thread_handle.lock().unwrap().take();
        if let Some(handle) = handle {
            // Join on a spawn_blocking thread so we don't block a tokio worker.
            tokio::task::spawn_blocking(move || {
                let _ = handle.join();
            })
            .await
            .ok();
        }
    }
}

impl<T> BufferedRaftLog<T>
where
    T: TypeConfig,
{
    pub fn new(
        node_id: u32,
        persistence_config: PersistenceConfig,
        storage: Arc<SOF<T>>,
    ) -> (Self, mpsc::UnboundedReceiver<IOTask>) {
        let log_store = storage.log_store();
        let meta_store = storage.meta_store();
        let disk_len = log_store.last_index();

        let FlushPolicy::Batch {
            idle_flush_interval_ms,
        } = persistence_config.flush_policy;
        debug!(
            "Creating BufferedRaftLog with node_id: {}, idle_flush_interval_ms: {:?}, disk_len: {:?}",
            node_id, idle_flush_interval_ms, disk_len
        );

        let shutdown_timeout_ms = persistence_config.shutdown_timeout_ms;

        //TODO: if switch to UnboundedChannel?
        let (command_sender, command_receiver) = mpsc::unbounded_channel();
        let entries = SkipMap::new();

        // Initialize term indexes
        let term_first_index = SkipMap::new();
        let term_last_index = SkipMap::new();
        let term_segments = TermSegments::new();

        // Load all entries from disk to memory
        let mut loaded_count = 0;
        if disk_len > 0 {
            match log_store.get_entries(1..=disk_len) {
                Ok(all_entries) if !all_entries.is_empty() => {
                    loaded_count = all_entries.len();
                    debug!("Successfully loaded {} entries from disk", loaded_count);

                    for entry in &all_entries {
                        let index = entry.index;
                        entries.insert(index, entry.clone());

                        // MODIFIED: Initialize term indexes for each loaded entry
                        // Update first index for term
                        term_first_index
                            .get_or_insert(entry.term, AtomicU64::new(u64::MAX))
                            .value()
                            .fetch_min(index, Ordering::AcqRel);
                        // Update last index for term
                        term_last_index
                            .get_or_insert(entry.term, AtomicU64::new(0))
                            .value()
                            .fetch_max(index, Ordering::AcqRel);
                    }
                    term_segments.on_append(&all_entries);
                }
                Ok(_empty_entries) => {
                    warn!("Disk reported length {} but loaded 0 entries", disk_len);
                }
                Err(e) => {
                    error!("Failed to load entries from storage: {:?}", e);
                    // Handle critical error if needed
                }
            }
        }

        // Initialize atomic boundaries
        let min_index = entries.front().map(|e| *e.key()).unwrap_or(0);
        let memory_max_index = entries.back().map(|e| *e.key()).unwrap_or(0);

        if disk_len > 0 && loaded_count == 0 {
            warn!(
                "Inconsistent state: disk_len={} but loaded_count=0",
                disk_len
            );
        }

        // Restore purge boundary from storage so entry_term() returns the correct
        // term for the snapshot boundary entry even after a restart.
        let (last_purged_index_val, last_purged_term_val) = match log_store.load_purge_boundary() {
            Ok(Some(lid)) => (lid.index, lid.term),
            Ok(None) => (0, 0),
            Err(e) => {
                warn!("Failed to load purge boundary: {:?}, defaulting to 0", e);
                (0, 0)
            }
        };

        (
            Self {
                node_id,
                log_store,
                meta_store,
                idle_flush_interval_ms,
                shutdown_timeout_ms,
                entries: RwLock::new(entries),
                min_index: AtomicU64::new(min_index),
                memory_max_index: AtomicU64::new(memory_max_index),
                last_purged_index: AtomicU64::new(last_purged_index_val),
                last_purged_term: AtomicU64::new(last_purged_term_val),
                durable_index: AtomicU64::new(disk_len),
                next_id: AtomicU64::new(disk_len + 1),
                write_notify: Arc::new(Notify::new()),
                command_sender: command_sender.clone(),
                term_first_index,
                term_last_index,
                term_segments,
                log_flush_tx: None, // set in start()
                io_thread_handle: std::sync::Mutex::new(None),
                fsync_coordinator: Arc::new(FsyncCoordinator::new()),
                poisoned: AtomicBool::new(false),
            },
            command_receiver,
        )
    }

    /// Start the command processor and return an Arc-wrapped instance.
    ///
    /// `batch_processor` runs on a dedicated OS thread (not a tokio worker) so that
    /// synchronous RocksDB calls (`db.write`, `flush_wal`) never block the async runtime.
    /// The thread owns its own `new_current_thread` runtime — fully independent of the
    /// caller's runtime lifecycle.  When the caller's runtime drops (e.g. at test teardown),
    /// the IO thread's timers and channels are unaffected; it exits cleanly on `Shutdown`.
    pub fn start(
        mut self,
        receiver: mpsc::UnboundedReceiver<IOTask>,
        log_flush_tx: Option<mpsc::UnboundedSender<crate::InternalEvent>>,
    ) -> Arc<Self> {
        self.log_flush_tx = log_flush_tx;
        let arc_self = Arc::new(self);
        let weak_self = Arc::downgrade(&arc_self);

        let idle_flush_interval_ms = arc_self.idle_flush_interval_ms;
        let shutdown_timeout_ms = arc_self.shutdown_timeout_ms;
        let node_id = arc_self.node_id;
        let io_handle = std::thread::Builder::new()
            .name(format!("raft-io-{}", node_id))
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build raft-io runtime");
                rt.block_on(Self::batch_processor(
                    weak_self,
                    receiver,
                    idle_flush_interval_ms,
                ));
                // Explicit bounded shutdown instead of an implicit Drop — an
                // implicit Drop blocks this thread indefinitely on any still-running
                // spawn_blocking task (confirmed with the not_durable_gated_flush test).
                // Healthy path: a real fsync is millisecond-scale, so this virtually
                // always completes well inside the window — same behavior as before.
                // Stuck-disk path: this thread's (and therefore close()'s) wait is now
                // bounded by this timeout instead of hanging forever. The stuck task
                // itself is not killed — it keeps running in the background until
                // flush() actually returns, so durable_index advancement and reply
                // delivery are unaffected; nobody is just waiting for it anymore.
                rt.shutdown_timeout(Duration::from_millis(shutdown_timeout_ms));
            })
            .expect("failed to spawn raft-io thread");

        *arc_self.io_thread_handle.lock().unwrap() = Some(io_handle);

        arc_self
    }

    /// Notify-driven IO loop.
    ///
    /// Waits on `write_notify.notified()` for new entries in the SkipMap.
    /// Multiple `notify_one()` calls while the IO thread is busy (persisting or
    /// fsyncing) coalesce into a single wakeup, reducing kernel cond_signal overhead
    /// from one-per-write to one-per-burst.
    ///
    /// On each wakeup:
    ///   1. Read entries in `(durable_index, memory_max_index]` from SkipMap.
    ///   2. persist_entries to OS page cache (no fsync).
    ///   3. Drain any pending control commands from the mpsc channel.
    ///   4. fsync once — advance durable_index, wake WaitDurable callers.
    ///
    /// Safety-net timer fires after `idle_flush_interval_ms` of inactivity.
    async fn batch_processor(
        this: std::sync::Weak<Self>,
        mut receiver: mpsc::UnboundedReceiver<IOTask>,
        idle_flush_interval_ms: u64,
    ) {
        // Upgrade once at entry — Arc stays alive until Shutdown (the real exit signal).
        let Some(this) = this.upgrade() else { return };

        let start = tokio::time::Instant::now() + Duration::from_millis(idle_flush_interval_ms);
        let mut safety_timer =
            tokio::time::interval_at(start, Duration::from_millis(idle_flush_interval_ms));
        safety_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Highest index in OS page cache, awaiting fsync. Reset to 0 after each fsync.
        let mut pending_max: u64 = 0;

        loop {
            tokio::select! {
                _ = this.write_notify.notified() => {
                    if Self::run_batch_turn(&this, &mut receiver, &mut pending_max, Vec::new(), false).await {
                        break;
                    }
                }
                cmd = receiver.recv() => {
                    let Some(cmd) = cmd else { break };
                    let should_break = match cmd {
                        IOTask::Shutdown => Self::run_batch_turn(&this, &mut receiver, &mut pending_max, Vec::new(), true).await,
                        IOTask::Flush(reply) => Self::run_batch_turn(&this, &mut receiver, &mut pending_max, vec![reply], false).await,
                        cmd => {
                            if Self::run_storage_tasks(cmd, &this, &mut pending_max).await {
                                break;
                            }
                            continue;
                        }
                    };
                    if should_break { break; }
                }
                _ = safety_timer.tick() => {
                    let start = this.durable_index.load(Ordering::Acquire) + 1;
                    let end = this.memory_max_index.load(Ordering::Acquire);
                    let _ = Self::persist_pending_range(&this, start, end, &mut pending_max, "safety-net").await;

                    if pending_max > 0 {
                        this.fsync_coordinator.submit(&this, pending_max, vec![]);
                        pending_max = 0;
                    }
                }
            }
        }
    }

    /// Writes entries in `(from, to]` that haven't reached page cache yet (no
    /// fsync). Iterates the SkipMap for the range — entries removed by a
    /// concurrent truncation simply aren't returned, so a stale from/to pair is
    /// self-correcting and never writes wrong data. Advances `pending_max` on
    /// success; propagates the error as-is on failure.
    async fn persist_pending_range(
        this: &Arc<Self>,
        from: u64,
        to: u64,
        pending_max: &mut u64,
        ctx: &str,
    ) -> Result<()> {
        if this.is_poisoned() {
            return Err(Error::Fatal("raft log storage is poisoned".to_string()));
        }
        if from > to {
            return Ok(());
        }
        let entries = this.get_entries_range(from..=to)?;
        if entries.is_empty() {
            return Ok(());
        }
        this.log_store
            .persist_entries(entries)
            .await
            .inspect(|_| {
                *pending_max = (*pending_max).max(to);
            })
            .inspect_err(|e| {
                error!("{ctx} persist_entries failed: {e:?}");
                this.mark_poisoned_and_notify(format!("{ctx}: persist_entries failed: {e:?}"));
            })
    }

    async fn run_batch_turn(
        this: &Arc<Self>,
        receiver: &mut mpsc::UnboundedReceiver<IOTask>,
        pending_max: &mut u64,
        mut replies: Vec<oneshot::Sender<Result<()>>>,
        mut seen_shutdown: bool,
    ) -> bool {
        let start = this.durable_index.load(Ordering::Acquire) + 1;
        let end = this.memory_max_index.load(Ordering::Acquire);
        let mut persist_failed = false;
        if let Err(e) = Self::persist_pending_range(this, start, end, pending_max, "batch").await {
            for reply in replies.drain(..) {
                let _ = reply.send(Err(Error::Fatal(format!("persist_entries failed: {e:?}"))));
            }
            persist_failed = true;
        }

        // `seen_shutdown` is not a gate here — regardless of whether the
        // caller already knows shutdown is happening, any commands still
        // queued must be drained and replied to. Whether to drain the queue
        // and whether to eventually break the loop are separate concerns
        // and must not share one flag.
        while let Ok(cmd) = receiver.try_recv() {
            match cmd {
                IOTask::Shutdown => {
                    seen_shutdown = true;
                }
                IOTask::Flush(reply) => replies.push(reply),
                cmd => {
                    if Self::run_storage_tasks(cmd, this, pending_max).await {
                        for reply in replies {
                            let _ = reply
                                .send(Err(Error::Fatal("fatal IO error, batch aborted".into())));
                        }
                        return true;
                    }
                }
            }
        }

        // A Flush caller needs everything up to now durable; a command drained
        // above may have advanced memory_max_index past the leading persist.
        if !replies.is_empty() && !persist_failed {
            let start = *pending_max + 1;
            let end = this.memory_max_index.load(Ordering::Acquire);
            let _ =
                Self::persist_pending_range(this, start, end, pending_max, "batch catch-up").await;
        }

        this.fsync_coordinator.submit(this, *pending_max, replies);

        *pending_max = 0;
        if seen_shutdown {
            let _ = this.meta_store.flush();
        }

        // Observability — IO thread, per write burst (not per append). Where the
        // log pipeline stands; `rpo_window` = in memory but not yet fsynced.
        let mem = this.memory_max_index.load(Ordering::Relaxed);
        let dur = this.durable_index.load(Ordering::Relaxed);
        metrics::gauge!("core.raft.log.memory_max_index").set(mem as f64);
        metrics::gauge!("core.raft.log.durable_index").set(dur as f64);
        metrics::gauge!("core.raft.log.rpo_window").set(mem.saturating_sub(dur) as f64);

        seen_shutdown
    }

    /// Runs one storage-mutating IOTask (ReplaceRange/Purge/Reset)
    /// against log_store. Flush/Shutdown are intercepted by the caller
    /// (`batch_processor`) before this is called — unreachable here.
    ///
    /// Returns `true` if `batch_processor` must exit immediately (fatal IO error).
    async fn run_storage_tasks(
        cmd: IOTask,
        this: &Arc<Self>,
        pending_max: &mut u64,
    ) -> bool {
        match cmd {
            IOTask::Flush(_) => {
                unreachable!("Flush must be intercepted in the drain loop before run_storage_tasks")
            }
            IOTask::Shutdown => {
                unreachable!("Shutdown is always filtered out before reaching run_storage_tasks")
            }
            IOTask::ReplaceRange {
                truncate_from,
                new_entries,
                done,
            } => {
                // Result is relied on immediately for protocol answers
                // (entry_term(), AppendEntries consistency checks) — must not
                // proceed on an already-untrusted disk.
                if this.is_poisoned() {
                    let _ = done.send(Err(Error::Fatal("raft log storage is poisoned".into())));
                    return true;
                }

                let max_idx = new_entries.last().map(|e| e.index).unwrap_or(0);
                let result = this.log_store.replace_range(truncate_from, new_entries).await;
                if let Err(ref e) = result {
                    error!("IOTask::ReplaceRange failed (fatal): {e:?}");
                    this.mark_poisoned_and_notify(format!("ReplaceRange failed: {e:?}"));
                    let _ = done.send(result);
                    return true; // signal batch_processor to exit — disk state is corrupted
                }

                if max_idx > 0 {
                    *pending_max = (*pending_max).max(max_idx);
                    this.fsync_coordinator.submit(this, max_idx, vec![]);
                }
                let _ = done.send(result);
                false
            }
            IOTask::Purge { cutoff, done } => {
                // Same reasoning as ReplaceRange: purge changes what future
                // protocol queries see.
                if this.is_poisoned() {
                    let _ = done.send(());
                    return true;
                }

                if let Err(ref e) = this.log_store.purge(cutoff).await {
                    error!("IOTask::Purge failed (fatal): {e:?}");
                    this.mark_poisoned_and_notify(format!("Purge failed: {e:?}"));
                    let _ = done.send(());
                    return true; // signal batch_processor to exit — disk state is corrupted
                }
                let _ = done.send(());
                false
            }
            IOTask::Reset { done } => {
                // Deliberately NO is_poisoned() check: Reset makes no new
                // durability promise — it's a clean wipe, not a write the
                // cluster will rely on. Allowing it to run even when poisoned
                // lets the node reach a known-clean state before it exits.
                let result = this.log_store.reset().await;
                if let Err(ref e) = result {
                    error!("IOTask::Reset failed (fatal): {e:?}");
                    this.mark_poisoned_and_notify(format!("Reset failed: {e:?}"));
                    let _ = done.send(result);
                    return true; // signal batch_processor to exit — disk state is corrupted
                }
                *pending_max = 0; // disk wiped — pending page-cache watermark must be zeroed
                let _ = done.send(result);
                false
            }
        }
    }

    async fn reset_internal(&self) -> Result<()> {
        // Fence first: bump generation before clearing state, so any fsync task
        // still in flight will observe a mismatch and discard its stale result.
        self.fsync_coordinator.fence_reset();

        self.entries.write().clear();
        self.durable_index.store(0, Ordering::Release);
        self.next_id.store(1, Ordering::Release);

        // Reset boundaries
        self.min_index.store(0, Ordering::Release);
        self.memory_max_index.store(0, Ordering::Release);

        // Clear term indexes to ensure consistency after reset
        self.term_first_index.clear();
        self.term_last_index.clear();
        self.term_segments.clear();

        let (done_tx, done_rx) = oneshot::channel();
        self.command_sender
            .send(IOTask::Reset { done: done_tx })
            .map_err(|e| crate::Error::Fatal(format!("IOTask::Reset send failed: {e:?}")))?;
        done_rx
            .await
            .map_err(|e| crate::Error::Fatal(format!("IOTask::Reset recv failed: {e:?}")))??;

        Ok(())
    }

    /// Insert entries into the in-memory index (SkipMap + term indexes + atomics).
    fn insert_to_memory(
        &self,
        entries: &[Entry],
    ) {
        {
            let log = self.entries.write();
            for entry in entries {
                log.insert(entry.index, entry.clone());
            }
        }

        self.update_term_indexes(entries);
        self.term_segments.on_append(entries);

        let max_index = entries.iter().map(|e| e.index).max().unwrap_or(0);
        let current_next = self.next_id.load(Ordering::Acquire);
        if max_index >= current_next {
            self.next_id.store(max_index + 1, Ordering::Release);
        }

        if let Some(first_entry) = entries.first() {
            let mut current_min = self.min_index.load(Ordering::Relaxed);
            while first_entry.index < current_min || current_min == 0 {
                match self.min_index.compare_exchange_weak(
                    current_min,
                    first_entry.index,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(e) => current_min = e,
                }
            }
        }

        if let Some(last_entry) = entries.last() {
            let mut current_max = self.memory_max_index.load(Ordering::Relaxed);
            while last_entry.index > current_max {
                match self.memory_max_index.compare_exchange_weak(
                    current_max,
                    last_entry.index,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(e) => current_max = e,
                }
            }
        }
    }

    /// Fire-and-forget signal to the single owner (raft.rs's event loop).
    /// Called by fsync_coordinator (C) — never writes `durable_index` itself.
    pub(super) fn notify_fsync_completed(
        &self,
        index: u64,
        term: u64,
    ) {
        if let Some(ref tx) = self.log_flush_tx {
            let _ = tx.send(crate::InternalEvent::FsyncCompleted { index, term });
        }
    }

    /// Mark the log as permanently poisoned and notify the Raft driving loop.
    /// Single choke point for both failure surfaces (persist_entries write
    /// failure and fsync failure) — do not set `poisoned` or call `notify_fatal`
    /// directly from anywhere else.
    pub(super) fn mark_poisoned_and_notify(
        &self,
        error: String,
    ) {
        self.poisoned.store(true, Ordering::Release);
        self.notify_fatal(error);
    }

    /// Send `InternalEvent::FatalError` to the Raft driving loop, mirroring the
    /// pattern SM-worker failures already use (state_machine_handler/worker.rs).
    /// Idempotent in effect: even if called multiple times (e.g. both
    /// persist_entries and a later fsync fail), raft.rs::run() only needs to
    /// see it once to exit.
    pub(super) fn notify_fatal(
        &self,
        error: String,
    ) {
        if let Some(ref tx) = self.log_flush_tx
            && tx
                .send(crate::InternalEvent::FatalError {
                    source: "RaftLog".to_string(),
                    error,
                })
                .is_err()
        {
            error!(
                "FatalError delivery failed (channel closed) — node may be poisoned \
                     but still running; durability is no longer guaranteed"
            );
        }
    }

    /// Efficient range removal with targeted term index updates
    /// O(k + t) where k = number of entries removed, t = number of affected terms
    pub fn remove_range(
        &self,
        range: RangeInclusive<u64>,
    ) {
        let entries = self.entries.write();
        let (new_min, new_max) = self.remove_range_locked(&entries, range);
        self.min_index.store(new_min, Ordering::Release);
        self.memory_max_index.store(new_max, Ordering::Release);

        self.durable_index.fetch_min(new_max, Ordering::AcqRel);
        // Clamps pending_max and bumps generation, in that order — see
        // fence_truncation()'s doc comment for why the order matters.
        self.fsync_coordinator.fence_truncation(new_max);
        // `entries` guard drops here (end of scope) — write lock released.
    }

    /// Shared deletion logic for `remove_range`/`purge_prefix`. Caller already
    /// holds the write lock; this only touches entries + term_first/last_index
    /// and returns the new (min, max) without storing them, so a caller that
    /// needs to publish additional state under the same lock (purge_prefix's
    /// last_purged_*) can do so before the lock is released.
    fn remove_range_locked(
        &self,
        entries: &SkipMap<u64, Entry>,
        range: RangeInclusive<u64>,
    ) -> (u64, u64) {
        let (start, end) = range.into_inner();

        // Track affected terms and their min/max indexes in the removal range
        let mut affected_terms: HashMap<u64, (Option<u64>, Option<u64>)> = HashMap::new();

        // Remove entries in range and track affected terms
        let mut current = start;
        while current <= end {
            if let Some(entry) = entries.range(current..=end).next() {
                let key = *entry.key();
                let term = entry.value().term;

                let (min_idx, max_idx) = affected_terms.entry(term).or_insert((None, None));
                if min_idx.is_none() || key < min_idx.unwrap() {
                    *min_idx = Some(key);
                }
                if max_idx.is_none() || key > max_idx.unwrap() {
                    *max_idx = Some(key);
                }

                entries.remove(&key);
                current = key + 1;
            } else {
                break;
            }
        }

        // Update only affected term indexes
        for (term, (removed_min, removed_max)) in affected_terms {
            // Update first index if the removed entry was the first for this term
            if let Some(term_first) = self.term_first_index.get(&term) {
                let current_first = term_first.value().load(Ordering::Acquire);
                if removed_min.is_some() && current_first >= removed_min.unwrap() {
                    // Find new first index for this term
                    let new_first =
                        entries.iter().find(|e| e.value().term == term).map(|e| *e.key());

                    if let Some(idx) = new_first {
                        term_first.value().store(idx, Ordering::Release);
                    } else {
                        self.term_first_index.remove(&term);
                    }
                }
            }

            // Update last index if the removed entry was the last for this term
            if let Some(term_last) = self.term_last_index.get(&term) {
                let current_last = term_last.value().load(Ordering::Acquire);
                if removed_max.is_some() && current_last <= removed_max.unwrap() {
                    // Find new last index for this term
                    let new_last =
                        entries.iter().rev().find(|e| e.value().term == term).map(|e| *e.key());

                    if let Some(idx) = new_last {
                        term_last.value().store(idx, Ordering::Release);
                    } else {
                        self.term_last_index.remove(&term);
                    }
                }
            }
        }

        let new_min = entries.front().map(|e| *e.key()).unwrap_or(0);
        let new_max = entries.back().map(|e| *e.key()).unwrap_or(0);
        (new_min, new_max)
    }

    /// Purge entries at/below `cutoff.index`, publishing `last_purged_index`/
    /// `last_purged_term` in the SAME critical section as the entries removal
    /// and the min_index/memory_max_index advance. Only place that should ever write
    /// `last_purged_*` — a reader must never observe the entries gone but the
    /// boundary not yet recorded (#442).
    pub fn purge_prefix(
        &self,
        cutoff: LogId,
    ) {
        let entries = self.entries.write();
        let (new_min, new_max) = self.remove_range_locked(&entries, 0..=cutoff.index);

        self.min_index.store(new_min, Ordering::Release);
        self.memory_max_index.store(new_max, Ordering::Release);

        // Write term before index (Release) so readers that load index first
        // then term (Acquire) always observe a consistent pair.
        self.last_purged_term.store(cutoff.term, Ordering::Release);
        self.last_purged_index.store(cutoff.index, Ordering::Release);
    }

    // Update the term index (completely lock-free)
    fn update_term_indexes(
        &self,
        entries: &[Entry],
    ) {
        for entry in entries {
            let term = entry.term;

            // Update first index
            self.term_first_index
                .get_or_insert(term, AtomicU64::new(u64::MAX))
                .value()
                .fetch_min(entry.index, Ordering::AcqRel);

            // Update last index
            self.term_last_index
                .get_or_insert(term, AtomicU64::new(0))
                .value()
                .fetch_max(entry.index, Ordering::AcqRel);
        }
    }

    pub(super) fn is_poisoned(&self) -> bool {
        self.poisoned.load(Ordering::Relaxed)
    }

    /// Returns the number of entries in the buffer.
    ///
    /// # Visibility
    /// This method is only available in test builds.
    #[cfg(any(test, feature = "__test_support"))]
    pub fn len(&self) -> usize {
        self.entries.read().len()
    }

    /// Returns reference to next_id atomic for test verification (test-only).
    ///
    /// This accessor allows tests to verify ID allocation behavior.
    #[cfg(test)]
    pub fn next_id(&self) -> &std::sync::atomic::AtomicU64 {
        &self.next_id
    }

    /// Returns true if the buffer contains no entries.
    ///
    /// This complements the `len()` method to follow Rust API conventions.
    /// Clippy requires: Any struct with a public `len` method should also
    /// have a public `is_empty` method.
    #[cfg(any(test, feature = "__test_support"))]
    pub fn is_empty(&self) -> bool {
        self.entries.read().is_empty()
    }

    #[cfg(test)]
    pub(super) fn set_memory_max_index_for_test(
        &self,
        value: u64,
    ) {
        self.memory_max_index.store(value, Ordering::Release);
    }
}

impl<T> Drop for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        if let Err(e) = self.command_sender.clone().send(IOTask::Shutdown) {
            debug!(
                "Shutdown command send failed (receiver already closed): {:?}",
                e
            );
        }
    }
}

impl<T> std::fmt::Debug for BufferedRaftLog<T>
where
    T: TypeConfig,
{
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("BufferedRaftLog").finish()
    }
}

#[cfg(test)]
#[path = "buffered_raft_log_test/basic_operations_test.rs"]
mod basic_operations_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/concurrent_fsync_test.rs"]
mod concurrent_fsync_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/concurrent_operations_test.rs"]
mod concurrent_operations_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/drain_fsync_test.rs"]
mod drain_fsync_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/durable_index_test.rs"]
mod durable_index_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/edge_cases_test.rs"]
mod edge_cases_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/flush_strategy_test.rs"]
mod flush_strategy_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/id_allocation_test.rs"]
mod id_allocation_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/performance_test.rs"]
mod performance_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/durable_index_truncation_clamp_test.rs"]
mod durable_index_truncation_clamp_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/pipeline_overlap_test.rs"]
mod pipeline_overlap_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/quorum_durability_test.rs"]
mod quorum_durability_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/raft_properties_test.rs"]
mod raft_properties_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/remove_range_test.rs"]
mod remove_range_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/replace_range_fsync_test.rs"]
mod replace_range_fsync_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/shutdown_test.rs"]
mod shutdown_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/term_index_test.rs"]
mod term_index_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/term_segments_test.rs"]
mod term_segments_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/truncation_fsync_fence_test.rs"]
mod truncation_fsync_fence_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/content_validated_watermark_test.rs"]
mod content_validated_watermark_test;

#[cfg(test)]
#[path = "buffered_raft_log_test/worker_test.rs"]
mod worker_test;
