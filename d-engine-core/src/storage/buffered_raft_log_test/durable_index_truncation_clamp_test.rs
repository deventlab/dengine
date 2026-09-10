//! `durable_index` must never claim more of the log survived to disk than the
//! log actually holds right now. The danger case: a term-conflict truncation
//! shrinks the log while a persist / fsync for the old, longer log is still in
//! flight — the stale in-flight write must not push `durable_index` past the
//! truncation point. Two guards cover this: `try_advance_durable_index`'s term
//! check, and the `FsyncCoordinator` generation fence bumped by `remove_range`.

use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{
    BufferedRaftLog, FlushPolicy, MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig,
    PersistenceConfig,
};

fn entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: None,
    }
}

/// After a drastic truncate-then-regrow, `durable_index` must land exactly on
/// the new tail — never above it (would claim durability for discarded
/// entries), never stuck below it (the new tail must actually become durable).
#[tokio::test]
async fn test_durable_index_lands_on_new_tail_after_truncate_and_resync() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000, // isolate from the safety-net timer
        },
        "durable_index_lands_on_new_tail_after_truncate_and_resync",
    );

    // Old leader (term 1) replicates 1..=10. append_entries inserts them into
    // memory and notifies the IO thread; nothing is fsync-confirmed until the
    // FsyncCompleted events are drained below.
    ctx.append_entries(1, 10, 1).await;
    assert_eq!(ctx.raft_log.last_entry_id(), 10);
    assert_eq!(
        ctx.raft_log.durable_index(),
        0,
        "no fsync report drained yet"
    );

    // New leader (term 2): index 2 conflicts, so the log is truncated from 2
    // and replaced with a single new entry — real log becomes [1, 2]. Slow
    // path: remove_range(2..) drops memory_max_index to 1 and clamps
    // durable_index down, then the new index 2 is inserted.
    ctx.raft_log
        .filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)])
        .await
        .unwrap();
    assert_eq!(ctx.raft_log.last_entry_id(), 2, "log is now [1, 2]");

    // flush() only returns after its own fsync-completion event is enqueued,
    // so draining right here is deterministic — no sleep needed.
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // The stale report for index 10 must be rejected (index 10 no longer
    // exists); the report for index 2 must be accepted.
    assert!(
        ctx.raft_log.durable_index() <= ctx.raft_log.last_entry_id(),
        "durable_index ({}) must not exceed last_entry_id ({})",
        ctx.raft_log.durable_index(),
        ctx.raft_log.last_entry_id()
    );
    assert_eq!(
        ctx.raft_log.durable_index(),
        2,
        "durable_index must reach the true tail (2), not a stale pre-truncation watermark"
    );
}

/// A persist whose entry set was captured *before* a truncation but finishes
/// *after* it must not let a stale fsync-completion report advance
/// `durable_index` into the range the truncation discarded.
///
/// Timeline (deterministic via the persist gate):
/// 1. Old leader (term 1) replicates 1..=10. append_entries returns at once;
///    the IO thread starts persisting the range and blocks on the gate — its
///    captured entry set is [1..=10].
/// 2. New leader (term 2): index 2 conflicts. filter_out_conflicts_and_append
///    runs remove_range(2..) synchronously (log is now [1]), inserts the new
///    index 2, and queues IOTask::ReplaceRange — which can't run yet, the IO
///    thread is still on the gate.
/// 3. Release the gate: the stale persist writes [1..=10], then the queued
///    ReplaceRange fixes the disk, then a flush() drives one legitimate
///    fsync-completion for index 2.
///
/// Expected: draining the fsync completions advances `durable_index` to 2 and
/// rejects the stale report for index 10.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_stale_persist_after_truncation_does_not_advance_durable_index() {
    let (storage, persist_gate) =
        MockStorageEngine::not_durable_gated_persist("stale_persist_after_truncation".into());
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    let (log_flush_tx, mut log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));
    std::thread::sleep(Duration::from_millis(10));

    // Step 1: replicate 1..=10; the IO thread blocks persisting this range.
    let entries: Vec<Entry> = (1..=10).map(|i| entry(i, 1)).collect();
    raft_log.append_entries(entries).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Step 2: term conflict at index 2 — on a task, since its ReplaceRange
    // await is stuck behind the gated persist.
    let truncate = {
        let raft_log = raft_log.clone();
        tokio::spawn(async move {
            raft_log.filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)]).await
        })
    };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(
        raft_log.last_entry_id(),
        2,
        "in-memory truncation is synchronous — visible without waiting on the IO thread"
    );

    // Step 3: release the stale persist, let ReplaceRange land, then flush.
    persist_gate.send(()).expect("IO thread should still be waiting on the gate");
    truncate.await.unwrap().unwrap();
    raft_log.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Drain fsync completions the way raft.rs's event loop would.
    while let Ok(event) = log_flush_rx.try_recv() {
        if let crate::InternalEvent::FsyncCompleted { index, term } = event {
            raft_log.try_advance_durable_index(index, term);
        }
    }

    assert!(
        raft_log.durable_index() <= raft_log.last_entry_id(),
        "durable_index ({}) must never exceed last_entry_id ({}) — the stale persist \
         for 1..=10 must not be reported durable after truncation shrank the log to [1, 2]",
        raft_log.durable_index(),
        raft_log.last_entry_id()
    );
    assert_eq!(
        raft_log.durable_index(),
        2,
        "durable_index must land on the post-truncation tail (2), not the stale 10"
    );
}

/// `persist_pending_range` must report the highest index it *actually wrote*,
/// not the upper scan bound it was handed. The two differ during a truncation
/// race: the IO thread latched `memory_max_index` = 10 (an old leader had sent
/// 8, 9, 10), then a term-conflict truncation removed everything above 7 before
/// the SkipMap scan ran. Asking to persist `(4, 10]` then writes only 5, 6, 7.
///
/// Returning the bound (10) would push the caller's `persisted_index` and the
/// fsync target past entries that never reached disk — a redundant fdatasync
/// plus a spurious `FsyncCompleted{10}` that the term check then has to reject.
/// Reporting 7 keeps every downstream watermark on real data.
#[tokio::test]
async fn test_persist_pending_range_reports_written_max_not_scan_bound() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "persist_pending_range_reports_written_max".into(),
    ));
    // No `.start()` — drive `persist_pending_range` directly, no IO thread.
    let (raft_log, _receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );
    let raft_log = Arc::new(raft_log);

    raft_log
        .append_entries((1..=7).map(|i| entry(i, 1)).collect())
        .await
        .unwrap();

    // Scan bound is 10 (stale latch); the SkipMap holds only 1..=7.
    let written = BufferedRaftLog::persist_pending_range(&raft_log, 5, 10, "test")
        .await
        .unwrap();

    assert_eq!(
        written,
        Some(7),
        "must report the highest index actually written (7), not the scan bound (10)"
    );
}

/// After a term-conflict truncation, the IO thread's persist frontier must land
/// *past* the new tail — not on it. `IOTask::ReplaceRange` already wrote (and
/// fsynced) the new tail via `replace_range`; the next write's persist scan must
/// start at `new_tail + 1`. If the frontier is left *at* `new_tail`, every
/// subsequent write re-scans and re-`persist_entries` that one boundary entry
/// (and re-submits a redundant fsync for it) — the exact waste #446 removes.
///
/// Guards the "highest-persisted" watermark semantics: `ReplaceRange` sets the
/// watermark to `new_tail`, and scans start at `watermark + 1`.
#[tokio::test]
async fn test_persist_frontier_skips_new_tail_after_truncation() {
    // Records the index list of every persist_entries() call.
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    {
        let calls = persist_calls.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls
                .lock()
                .unwrap()
                .push(entries.iter().map(|e| e.index).collect());
            Ok(())
        });
    }
    log_store
        .expect_replace_range()
        .returning(|from, new_entries| {
            Ok(new_entries
                .last()
                .map(|e| e.index)
                .unwrap_or(from.saturating_sub(1)))
        });
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_is_write_durable().returning(|| false);
    log_store.expect_flush().returning(|| Ok(()));
    log_store.expect_flush_async().returning(|| Ok(()));

    let mut meta_store = MockMetaStore::new();
    meta_store.expect_save_hard_state().returning(|_| Ok(()));
    meta_store.expect_load_hard_state().returning(|| Ok(None));
    meta_store.expect_flush().returning(|| Ok(()));
    meta_store.expect_flush_async().returning(|| Ok(()));

    let storage = Arc::new(MockStorageEngine::from(log_store, meta_store));
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10));

    // Old leader (term 1): entries 1..=10, persisted.
    raft_log
        .append_entries((1..=10).map(|i| entry(i, 1)).collect())
        .await
        .unwrap();
    raft_log.flush().await.unwrap();

    // New leader (term 2): conflict at index 6 → truncate [6..], replace with
    // [6, 7] (term 2). `filter_out_conflicts_and_append` awaits the
    // `IOTask::ReplaceRange` reply, so the frontier is at new-tail 7 on return.
    raft_log
        .filter_out_conflicts_and_append(5, 1, vec![entry(6, 2), entry(7, 2)])
        .await
        .unwrap();

    // Only care about persist calls from here on — no flush() in between, so the
    // next append's `IOTask::Persist` is the first thing to touch the frontier.
    persist_calls.lock().unwrap().clear();

    // Next write extends the log. Its persist scan must start at 8, not 7.
    raft_log
        .append_entries((8..=10).map(|i| entry(i, 2)).collect())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let calls = persist_calls.lock().unwrap().clone();
    let re_persisted_tail = calls.iter().flatten().any(|&idx| idx <= 7);
    assert!(
        !re_persisted_tail,
        "after ReplaceRange set the frontier at new-tail 7, the next persist must \
         start at 8 — entry 7 (or below) must not be handed to persist_entries again. \
         Got calls: {calls:?}"
    );
}
