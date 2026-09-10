//! Tests for the drain-then-fsync IO architecture (Method C).
//!
//! These tests verify three core properties:
//!
//! 1. **Write/flush separation**: `durable_index` advances only after fsync, not
//!    after the write-only phase. This ensures crash-safety semantics are preserved.
//!
//! 2. **Batch efficiency**: rapid concurrent writes are covered by far fewer fsyncs
//!    than individual writes. The fsync execution time itself acts as the batch window.
//!
//! 3. **Explicit flush barrier**: `flush()` waits until all entries written before
//!    the call are durable, regardless of how many internal fsyncs occurred.

use crate::BufferedRaftLog;
use crate::Error;
use crate::HardState;
use crate::IOTask;
use crate::MockLogStore;
use crate::MockMetaStore;
use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::PersistenceConfig;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio::time::{Duration, sleep};

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, RaftLog};

/// The IO thread auto-fsyncs on each write_notify wakeup without any timer.
///
/// After `append_entries`, the IO thread is notified via `write_notify`, reads
/// pending entries from the SkipMap, and calls fsync. `durable_index` advances
/// automatically — no explicit `flush()` required.
#[tokio::test]
async fn test_writes_become_durable_via_io_thread() {
    let (mut ctx, flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "writes_become_durable_via_io_thread",
    );

    // Append 5 entries — each calls write_notify.notify_one().
    for i in 1u64..=5 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }

    // Entries must be readable from SkipMap immediately (MemFirst invariant).
    assert_eq!(ctx.raft_log.last_entry_id(), 5);
    for i in 1u64..=5 {
        assert!(
            ctx.raft_log.entry(i).unwrap().is_some(),
            "entry {i} must be in memory"
        );
    }

    // Give IO thread time to process write_notify wakeup and fsync.
    sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();

    // durable_index must have advanced via IO thread auto-fsync (no explicit flush).
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "durable_index must advance via IO thread drain-then-fsync"
    );

    // IO thread called log_store.flush() at least once.
    assert!(
        flush_count.load(Ordering::Relaxed) >= 1,
        "IO thread must have called flush at least once"
    );
}

/// A single `append_entries` call with 100 entries batches into far fewer fsyncs
/// than individual writes.
///
/// `append_entries` calls `write_notify.notify_one()` once regardless of how many
/// entries are in the batch. The IO thread wakes once, persists all entries to page
/// cache, then dispatches fsync via `FsyncCoordinator`. The explicit `flush()` call
/// may race with the spawned fsync task: if it observes `durable_index` before the
/// first task completes, it submits a second round (coalesced by the coordinator).
/// N entries in one call → ≤2 fsyncs (not N), regardless of storage speed.
#[tokio::test]
async fn test_batch_append_produces_one_flush() {
    let (mut ctx, flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "batch_append_one_flush",
    );

    // All 100 entries in one append_entries call.
    let entries: Vec<Entry> = (1u64..=100)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    ctx.raft_log.append_entries(entries).await.unwrap();

    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    assert_eq!(ctx.raft_log.durable_index(), 100);

    // One notify_one() → IO thread wakes once → far fewer fsyncs than entries.
    // With FsyncCoordinator the explicit flush() may add one extra round if it
    // races with the in-flight spawned task; the invariant is "not N flushes".
    let flushes = flush_count.load(Ordering::Relaxed);
    assert!(
        (1..=2).contains(&flushes),
        "one append_entries batch must produce ≤2 flushes, not {flushes}"
    );
}

/// `IOTask::Reset` must zero `pending_max`; stale value corrupts `durable_index`.
///
/// ## Background
/// `batch_processor` tracks `pending_max`: the highest log index written to the OS
/// page cache but not yet fsynced. After a successful `fsync_and_advance`, it is
/// zeroed (`pending_max = 0`). After a **failed** fsync, it is NOT zeroed — the
/// `else { pending_max = 0 }` branch is skipped.
///
/// ## Original bug (fixed pre-#422)
/// `run_storage_tasks(IOTask::Reset)` wiped the on-disk log but did NOT zero
/// `pending_max`. On the next `write_notify` wakeup the IO thread would compute:
/// ```
/// pending_max = pending_max.max(new_end)   // stale 10 wins over new 3
/// fsync_and_advance(10)                    // advances durable_index to 10 — WRONG
/// ```
/// `durable_index (10) >= max_index (3)` would then make every subsequent `flush()`
/// return immediately without syncing the new entries — silently lost on a crash.
///
/// ## Update (2026-07-19): now structurally unreachable
/// A failed fsync now poisons the log permanently, so writes after Phase 2
/// are rejected outright — the corruption repro path no longer exists.
/// Assertions updated to check for that rejection instead.
#[tokio::test]
async fn test_pending_max_zeroed_on_reset_preventing_durable_index_corruption() {
    let storage = Arc::new(MockStorageEngine::not_durable_first_flush_fails(
        "pending_max_zeroed_on_reset".into(),
    ));
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            // Safety-net disabled: only write_notify triggers fsync.
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Phase 1: append 10 entries.
    // IO thread wakes on write_notify: persist succeeds, fsync FAILS (first call).
    // Failed fsync leaves pending_max = 10 (not zeroed — only success path zeros
    // it) AND now poisons the log permanently (see FsyncCoordinator).
    let entries: Vec<Entry> = (1u64..=10)
        .map(|i| Entry {
            index: i,
            term: 1,
            payload: None,
        })
        .collect();
    raft_log.append_entries(entries).await.unwrap();
    // Wait for IO thread to process write_notify (persist ok, fsync fails).
    sleep(Duration::from_millis(20)).await;
    assert!(
        raft_log.is_poisoned(),
        "the fsync failure above must have poisoned the log"
    );

    // Phase 2: reset — disk wiped. Still permitted (reset doesn't promise
    // durability), but does NOT clear poisoned (see test_poisoned_survives_reset).
    raft_log.reset().await.unwrap();
    assert!(
        raft_log.is_poisoned(),
        "poisoned must survive reset — this is what closes off the original bug"
    );

    // Phase 3: attempt to append 3 new entries starting from index 1 — this is
    // the exact sequence that used to reproduce the durable_index corruption.
    // It must now be rejected outright, never reaching the IO thread at all.
    let new_entries: Vec<Entry> = (1u64..=3)
        .map(|i| Entry {
            index: i,
            term: 2,
            payload: None,
        })
        .collect();
    let result = raft_log.append_entries(new_entries).await;
    assert!(
        result.is_err(),
        "a poisoned log must reject writes even after reset — the original \
         'stale pending_max causes silent data loss' bug can no longer be \
         reached because there is no post-poisoning write path left to corrupt"
    );

    raft_log.close().await;
}

/// flush() acts as a strict durability barrier: all entries appended before the
/// flush() call must be durable when flush() returns, regardless of internal batching.
#[tokio::test]
async fn test_flush_is_strict_durability_barrier() {
    let (mut ctx, _flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "flush_durability_barrier",
    );

    // First batch.
    for i in 1u64..=20 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        20,
        "first batch must be fully durable"
    );

    // Second batch after the barrier.
    for i in 21u64..=50 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        50,
        "second batch must be fully durable"
    );
}

/// flush() must return Err when the underlying fsync fails — not hang indefinitely.
///
/// ## Bug (pre-fix)
/// `flush()` sends `IOTask::FlushNow` (fire-and-forget), then calls `wait_durable()`
/// which registers a `WaitDurable` waiter. If the IO thread's fsync fails, it logs the
/// error and moves on — `durable_index` never advances, the waiter is never notified,
/// and `flush()` blocks forever.
///
/// ## Fix (#331)
/// Replace `FlushNow` + `WaitDurable` with a single `IOTask::Flush(oneshot::Sender<Result<()>>)`.
/// The IO thread performs fsync and sends the result (Ok or Err) directly back to the
/// caller via the oneshot channel, so `flush()` always returns within bounded time.
#[tokio::test]
async fn test_flush_propagates_io_error() {
    // Every flush() call on the underlying log store returns an error.
    // This covers both the auto-fsync triggered by write_notify and the explicit
    // flush() call, so timing between the two does not affect the outcome.
    let storage = Arc::new(MockStorageEngine::not_durable_always_failing_flush(
        "flush_propagates_io_error".into(),
    ));
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

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // flush() must return Err, not hang.
    // A 2 s timeout distinguishes the fixed path (Err returned quickly) from
    // the pre-fix hang (WaitDurable waiter never notified).
    let result = timeout(Duration::from_secs(2), raft_log.flush()).await;

    match result {
        Err(_elapsed) => {
            panic!(
                "flush() hung: IO error was not propagated back to the caller (pre-fix behaviour)"
            );
        }
        Ok(Ok(())) => {
            panic!("flush() returned Ok but the fsync mock always fails");
        }
        Ok(Err(_e)) => {
            // Expected: flush() surfaces the fsync failure to the caller.
        }
    }

    raft_log.close().await;
}

/// Test: a real fsync failure poisons the log end-to-end (via the actual
/// `FsyncCoordinator` failure path, not by forcing the flag directly like
/// `test_poisoned_survives_reset` does), and poisoning survives a
/// subsequent `reset()` — writes afterward are rejected.
///
/// ## History
/// Originally written (pre-#422 fatal-poisoning fix) to prove a `flush()`
/// reply queued after one failed fsync round still resolves once a later
/// round succeeds — i.e. the log recovers from a transient failure. That
/// property no longer exists: one fsync failure is now permanent. Rewritten
/// to check for the new (correct) behavior instead.
#[tokio::test]
async fn test_fsync_failure_poisons_and_rejects_writes_after_reset() {
    let storage = Arc::new(MockStorageEngine::not_durable_first_flush_fails(
        "fsync_failure_poisons_and_rejects_writes_after_reset".into(),
    ));
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
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Trigger a real fsync failure via the actual FsyncCoordinator path.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;
    assert!(
        raft_log.is_poisoned(),
        "a real fsync failure must poison the log"
    );

    raft_log.reset().await.unwrap();
    assert!(raft_log.is_poisoned(), "poisoned must survive reset");

    let result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 2,
            payload: None,
        }])
        .await;
    assert!(
        result.is_err(),
        "writes after reset must still be rejected while poisoned"
    );

    raft_log.close().await;
}

/// A `replace_range()` failure (conflict-resolution truncate+write) poisons
/// the log, same as persist_entries/fsync failures — disk state is uncertain
/// either way. Exercised end-to-end via `filter_out_conflicts_and_append`'s
/// real conflict-truncation path, not by forcing the flag directly.
#[tokio::test]
async fn test_replace_range_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_replace_range_fails(
        "replace_range_failure_poisons".into(),
    ));
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
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Base log: 3 entries at term 1.
    raft_log
        .append_entries(vec![
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
            Entry {
                index: 2,
                term: 1,
                payload: None,
            },
            Entry {
                index: 3,
                term: 1,
                payload: None,
            },
        ])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    // A new leader (term 2) overwrites from index 2 onward — a real conflict
    // that must truncate + replace, routing through IOTask::ReplaceRange.
    let result = raft_log
        .filter_out_conflicts_and_append(
            1,
            1,
            vec![
                Entry {
                    index: 2,
                    term: 2,
                    payload: None,
                },
                Entry {
                    index: 3,
                    term: 2,
                    payload: None,
                },
            ],
        )
        .await;
    assert!(
        result.is_err(),
        "the failed replace_range() must surface as an error"
    );

    assert!(
        raft_log.is_poisoned(),
        "a replace_range() failure must poison the log — disk state is uncertain"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 4,
            term: 2,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// A `purge()` failure poisons the log, same as the other storage-layer
/// failures. `purge_logs_up_to()`'s own `done` channel is `oneshot::Sender<()>`
/// (no `Result`), so it always returns `Ok` regardless of the underlying
/// outcome — poisoning is the only way this failure becomes observable.
#[tokio::test]
async fn test_purge_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_purge_fails(
        "purge_failure_poisons".into(),
    ));
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

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    raft_log.purge_logs_up_to(LogId { term: 1, index: 1 }).await.unwrap(); // always Ok — see doc comment above
    sleep(Duration::from_millis(20)).await;

    assert!(
        raft_log.is_poisoned(),
        "a purge() failure must poison the log"
    );

    let result = raft_log
        .append_entries(vec![Entry {
            index: 2,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(result.is_err(), "writes must be rejected once poisoned");
}

/// A `reset()` failure poisons the log, same as the other storage-layer
/// failures.
#[tokio::test]
async fn test_reset_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_reset_fails(
        "reset_failure_poisons".into(),
    ));
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

    let result = raft_log.reset().await;
    assert!(
        result.is_err(),
        "a failed reset() must surface as an error to the caller"
    );

    assert!(
        raft_log.is_poisoned(),
        "a reset() failure must poison the log"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// A `save_hard_state()` failure (persisting current_term/voted_for) poisons
/// the log — this is the Election Safety gap the expert flagged as highest
/// priority: a silently-failed vote write, if not caught, lets a restarted
/// node believe it never voted this term and cast a second vote, breaking
/// "at most one leader per term."
#[tokio::test]
async fn test_save_hard_state_failure_poisons() {
    let storage = Arc::new(MockStorageEngine::not_durable_save_hard_state_fails(
        "save_hard_state_failure_poisons".into(),
    ));
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

    let result = raft_log.save_hard_state(&HardState {
        current_term: 1,
        voted_for: None,
    });
    assert!(
        result.is_err(),
        "a failed save_hard_state() must surface as an error to the caller"
    );

    assert!(
        raft_log.is_poisoned(),
        "a save_hard_state() failure must poison the log"
    );

    let append_result = raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}

/// Once already poisoned, `save_hard_state()` must be rejected immediately
/// without ever calling `meta_store.save_hard_state()`.
#[tokio::test]
async fn test_poisoned_rejects_save_hard_state() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "poisoned_rejects_save_hard_state".into(),
    ));
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

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.save_hard_state(&HardState {
        current_term: 1,
        voted_for: None,
    });
    match result {
        Err(Error::Fatal(msg)) => assert!(
            msg.contains("poisoned"),
            "expected the poisoned short-circuit to fire before save_hard_state() \
             was ever called, got: {msg}"
        ),
        other => panic!("expected Err(Fatal(\"...poisoned...\")), got: {other:?}"),
    }
}

// ============================================================================
// Gap fix: run_storage_tasks now checks is_poisoned() before executing
// ReplaceRange/Purge/Reset, instead of only checking it in run_batch_turn's
// drain loop (which missed the direct-dispatch path in batch_processor's
// top-level select, and the "just poisoned mid-turn" race).
// ============================================================================

/// Once already poisoned, `ReplaceRange` must be rejected immediately
/// without ever calling `log_store.replace_range()`. Proven by checking the
/// error message: if the underlying mock's own failure ("simulated
/// replace_range failure") had been reached, the message would differ from
/// the immediate "raft log storage is poisoned" rejection.
#[tokio::test]
async fn test_poisoned_skips_replace_range() {
    let storage = Arc::new(MockStorageEngine::not_durable_replace_range_fails(
        "poisoned_skips_replace_range".into(),
    ));
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

    // Base log, written while still healthy.
    raft_log
        .append_entries(vec![
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
            Entry {
                index: 2,
                term: 1,
                payload: None,
            },
            Entry {
                index: 3,
                term: 1,
                payload: None,
            },
        ])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await;

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log
        .filter_out_conflicts_and_append(
            1,
            1,
            vec![
                Entry {
                    index: 2,
                    term: 2,
                    payload: None,
                },
                Entry {
                    index: 3,
                    term: 2,
                    payload: None,
                },
            ],
        )
        .await;

    match result {
        Err(Error::Fatal(msg)) => assert!(
            msg.contains("poisoned"),
            "expected the poisoned short-circuit to fire before replace_range() \
             was ever called, got: {msg}"
        ),
        other => panic!("expected Err(Fatal(\"...poisoned...\")), got: {other:?}"),
    }
}

/// Once already poisoned, `Reset` is deliberately NOT short-circuited —
/// unlike `ReplaceRange`/`Purge`, it makes no new durability promise (it's a
/// clean wipe, not a write the cluster will rely on), so it's allowed to run
/// even when poisoned, letting the node reach a known-clean state before it
/// exits. This test proves `reset()` actually executes (reaches
/// `log_store.reset()`) instead of being rejected — using a mock whose
/// `reset()` always succeeds, so the call completing with `Ok(())` is proof
/// it wasn't skipped.
#[tokio::test]
async fn test_poisoned_does_not_skip_reset() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "poisoned_does_not_skip_reset".into(),
    ));
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

    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.reset().await;
    assert!(
        result.is_ok(),
        "Reset must not be short-circuited by poisoned — got: {result:?}"
    );
    assert!(
        raft_log.is_poisoned(),
        "poisoned must remain true — Reset succeeding doesn't clear it"
    );
}

/// Once already poisoned, `Purge` should be rejected immediately without
/// calling `log_store.purge()` — but this can only be checked weakly.
/// Unlike ReplaceRange/Reset, `Purge`'s `done` channel is
/// `oneshot::Sender<()>` and cannot carry error text, so
/// `purge_logs_up_to()` always returns `Ok` whether the underlying purge
/// ran or was skipped. This is a pre-existing API limitation, not something
/// this test can close — it only confirms poisoned stays true and the call
/// doesn't hang.
#[tokio::test]
async fn test_poisoned_skips_purge() {
    let storage = Arc::new(MockStorageEngine::not_durable_purge_fails(
        "poisoned_skips_purge".into(),
    ));
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

    // advance_durable_and_notify() clamps against max_index — simulate a log
    // that already has the entry this test purges up to.
    raft_log.set_memory_max_index_for_test(1);
    raft_log.poisoned.store(true, Ordering::SeqCst);

    let result = raft_log.purge_logs_up_to(LogId { term: 1, index: 1 }).await;
    assert!(
        result.is_ok(),
        "purge_logs_up_to() always returns Ok regardless of the underlying outcome"
    );
    assert!(raft_log.is_poisoned(), "poisoned must remain true");
}

/// Test: a failed `IOTask::ReplaceRange` mid-batch-turn causes an explicit
/// `Err` reply to any `Flush` already queued in that same turn (fixed
/// 2026-07-19 — `run_batch_turn`'s drain loop now replies before returning,
/// instead of silently dropping the oneshot sender).
///
/// Ordering is made deterministic (not timing-sensitive) by gating the base
/// entries' `persist_entries()` call — `append_entries()` now calls it
/// synchronously, so the base append is spawned as its own task and blocks
/// there instead of returning immediately. While it's blocked, an
/// `IOTask::Flush` is sent directly (guaranteed FIFO-first) followed by a
/// conflict-triggering `filter_out_conflicts_and_append` call (sends
/// `IOTask::ReplaceRange` second). Releasing the gate lets the base append
/// finish and `run_batch_turn` drain both queued commands in one pass, in
/// that order.
///
/// Needs `flavor = "multi_thread"`: the gate blocks on a synchronous
/// `std::sync::mpsc::Receiver::recv()`, which would otherwise freeze the
/// single default executor thread that the spawned base-append task, the
/// conflict task, and this test body all need to share.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_run_batch_turn_replace_range_failure_replies_err_to_queued_flush() {
    let (gate_tx, gate_rx) = std::sync::mpsc::channel::<()>();
    let gate_rx = std::sync::Mutex::new(Some(gate_rx));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(move |_| {
        // Only the very first call (the base-entries append) blocks.
        if let Some(gate) = gate_rx.lock().unwrap().take() {
            let _ = gate.recv();
        }
        Ok(())
    });
    log_store.expect_replace_range().returning(|_, _| {
        Err(crate::Error::Fatal(
            "simulated replace_range failure".into(),
        ))
    });
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_is_write_durable().returning(|| true);
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

    // Base entries land in memory synchronously (before the gate), then
    // append_entries() blocks inside its own gated persist_entries() call —
    // spawned so the rest of this test can proceed while it's stuck there.
    let base_append_task = tokio::spawn({
        let raft_log = raft_log.clone();
        async move {
            raft_log
                .append_entries(vec![
                    Entry {
                        index: 1,
                        term: 1,
                        payload: None,
                    },
                    Entry {
                        index: 2,
                        term: 1,
                        payload: None,
                    },
                    Entry {
                        index: 3,
                        term: 1,
                        payload: None,
                    },
                ])
                .await
        }
    });
    sleep(Duration::from_millis(20)).await; // let it reach the gate

    // Send Flush directly — guarantees it's enqueued before the ReplaceRange
    // sent below, so it's the one already sitting in `replies` when the
    // ReplaceRange failure triggers the early return.
    let (flush_tx, flush_rx) = tokio::sync::oneshot::channel();
    raft_log.command_sender.send(IOTask::Flush(flush_tx)).unwrap();

    let conflict_raft_log = raft_log.clone();
    let conflict_task = tokio::spawn(async move {
        conflict_raft_log
            .filter_out_conflicts_and_append(
                1,
                1,
                vec![
                    Entry {
                        index: 2,
                        term: 2,
                        payload: None,
                    },
                    Entry {
                        index: 3,
                        term: 2,
                        payload: None,
                    },
                ],
            )
            .await
    });
    sleep(Duration::from_millis(50)).await; // let the ReplaceRange send land
    gate_tx.send(()).unwrap();

    timeout(Duration::from_secs(2), base_append_task)
        .await
        .expect("base append task must not hang")
        .expect("base append task must not panic")
        .expect("base append must succeed once the gate releases");

    let flush_result = timeout(Duration::from_secs(2), flush_rx)
        .await
        .expect("flush reply must not hang");
    match flush_result {
        Ok(Err(e)) => {
            // Expected: explicit Err reply, not a dropped-sender RecvError.
            debug_assert!(!format!("{e:?}").is_empty());
        }
        Ok(Ok(())) => panic!("a Flush queued alongside a fatal ReplaceRange must not succeed"),
        Err(_recv_err) => panic!(
            "flush reply sender was dropped without a reply — the fix should have \
             sent an explicit Err instead of silently closing the channel"
        ),
    }

    let _ = timeout(Duration::from_secs(2), conflict_task).await;
}

// ============================================================================
// Poisoned-state tests (#422 follow-up: fsync/persist failure must be fatal,
// not silently retried — see decision discussion 2026-07-19)
// ============================================================================

/// A freshly constructed log is never born poisoned.
///
/// Guards against a constructor regression (e.g. a copy-paste default flip)
/// that would make every node refuse writes from the very first call.
#[tokio::test]
async fn test_new_buffered_raft_log_starts_unpoisoned() {
    let (storage, _flush_call_count) =
        MockStorageEngine::not_durable("new_buffered_raft_log_starts_unpoisoned".into());
    let (raft_log, _receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            // Safety-net disabled: only write_notify triggers fsync.
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    assert!(
        !raft_log.is_poisoned(),
        "A freshly constructed log is never born poisoned"
    );
}

/// Once poisoned, the state survives `reset()` — it must NOT be cleared by
/// `reset_internal()` / `FsyncCoordinator::fence_reset()`.
///
/// Why this matters: `reset()` is also invoked mid-flight for legitimate
/// reasons (snapshot install, log conflict rewind). If poisoned were treated
/// as just another piece of "current generation" state and wiped on reset,
/// a node with an unconfirmed/corrupted WAL could silently resume accepting
/// writes right after a snapshot install — exactly the silent-continue bug
/// this whole fix exists to close.
#[tokio::test]
async fn test_poisoned_survives_reset() {
    let storage =
        MockStorageEngine::not_durable_first_flush_fails("poisoned_survives_reset".into());
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            // Safety-net disabled: only write_notify triggers fsync.
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );

    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    raft_log.poisoned.store(true, Ordering::SeqCst);
    assert!(raft_log.reset().await.is_ok());
    assert!(
        raft_log.is_poisoned(),
        "reset shoud not cleaned poisoned flag"
    );
}

/// A `persist_entries()` (page-cache write) failure poisons the log, exactly
/// like an fsync failure does — these are two independent failure surfaces
/// (`persist_pending_range` vs `FsyncCoordinator::run_until_caught_up`) and
/// both must reach the same fatal outcome.
///
/// The persist runs on the IO thread off `append_entries`'s task, so the
/// poison lands asynchronously — the black-box guarantee is that the *next*
/// write is rejected, not that this one fails.
///
/// Without this test, a bug that only wires up ONE of the two poisoning
/// paths (e.g. fsync failures poison correctly, but persist_entries
/// failures are still silently swallowed) would go unnoticed —
/// `test_poisoned_survives_reset` alone only exercises the fsync-failure
/// surface via `not_durable_first_flush_fails`.
#[tokio::test]
async fn test_persist_entries_failure_poisons() {
    let storage = MockStorageEngine::not_durable_first_persist_fails(
        "persist_entries_failure_poisons".into(),
    );
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
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Notifies the IO thread, which runs persist_pending_range and hits the
    // mock's first (failing) persist_entries() — the persist_pending_range
    // poisoning path, not FsyncCoordinator's.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await; // let the IO thread process it

    assert!(
        raft_log.is_poisoned(),
        "a persist_entries() failure must poison the log, same as an fsync failure"
    );

    // Black-box confirmation from the caller's point of view: the log now
    // refuses further writes, not just an internal flag flip.
    let result = raft_log
        .append_entries(vec![Entry {
            index: 2,
            term: 1,
            payload: None,
        }])
        .await;
    assert!(
        result.is_err(),
        "append_entries() must reject writes once poisoned"
    );
}

/// If `notify_fatal`'s underlying channel is already closed when a failure
/// happens, the node must not fail *silently* — poisoned must still end up
/// `true`, and the failure must be visible somewhere (log line), even though
/// nothing can drive `raft.rs::run()` to exit via this specific event.
///
/// This test intentionally does not assert an exit or a state transition —
/// there isn't one to observe from here. It only pins down: (1) poisoning
/// itself is independent of whether the notification channel is alive, and
/// (2) the failure-to-deliver path is not a silent no-op.
#[tokio::test]
async fn test_notify_fatal_channel_closed_still_poisons_and_logs() {
    // tracing-test's #[traced_test] only captures events on this test's own
    // thread — the fsync failure and its log line happen on BufferedRaftLog's
    // dedicated IO thread, so a cross-thread-capable global subscriber is
    // needed instead (see test_utils::log_capture).
    let logs = crate::test_utils::capture_logs_globally();

    let storage = MockStorageEngine::not_durable_first_flush_fails(
        "notify_fatal_channel_closed_still_poisons_and_logs".into(),
    );
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

    // Build the InternalEvent channel but drop the receiver immediately —
    // by the time notify_fatal() runs, log_flush_tx.send() hits a closed
    // channel and returns Err.
    let (tx, rx) = mpsc::unbounded_channel();
    drop(rx);

    let raft_log = raft_log.start(receiver, Some(tx));
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(20)).await; // let the IO thread hit the fsync failure

    assert!(
        raft_log.is_poisoned(),
        "poisoning must not depend on whether the FatalError channel is still alive"
    );
    assert!(
        crate::test_utils::logs_contain_globally(&logs, "FatalError delivery failed"),
        "a channel-closed failure to notify must still be observable via logs, \
         not a silent no-op — see notify_fatal()'s error! call"
    );
}

/// Efficiency: the IO thread's persist scan must start from its own page-cache
/// frontier, not from `durable_index`. Since #446 `durable_index` only advances
/// after an `FsyncCompleted` round-trips through raft.rs's event loop; under
/// load it lags far behind what the IO thread has already written. If the scan
/// restarted from `durable_index + 1` on every wakeup, each of N appends would
/// re-scan and re-`persist_entries` the whole not-yet-durable window — O(N^2)
/// total work.
///
/// This test pins `durable_index` at 0 (no `log_flush_tx`, so no
/// `FsyncCompleted` is ever consumed) and appends N entries one at a time. The
/// total number of entries handed to `persist_entries` across all calls must
/// stay ~N, not ~N^2/2.
#[tokio::test]
async fn test_persist_scan_tracks_frontier_not_stuck_durable_index() {
    let persisted_total = Arc::new(AtomicU64::new(0));
    let persisted_total_c = persisted_total.clone();

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(move |entries| {
        persisted_total_c.fetch_add(entries.len() as u64, Ordering::Relaxed);
        Ok(())
    });
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_replace_range().returning(|from, new_entries| {
        Ok(new_entries.last().map(|e| e.index).unwrap_or(from.saturating_sub(1)))
    });
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
    // No log_flush_tx: FsyncCompleted is never consumed, so durable_index
    // stays pinned at 0 for the whole test.
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10));

    const N: u64 = 100;
    for i in 1..=N {
        raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
        // flush() forces the IO thread to persist up to memory_max_index right
        // now, so the scan boundary is exercised once per append — deterministic,
        // no sleeps.
        raft_log.flush().await.unwrap();
    }

    assert_eq!(
        raft_log.durable_index(),
        0,
        "durable_index must stay stuck for this test to be meaningful"
    );
    let total = persisted_total.load(Ordering::Relaxed);
    assert!(
        total < 3 * N,
        "persist_entries received {total} entries for {N} appends; a frontier-tracking \
         scan is ~{N}, a durable_index-relative scan would be ~{} (O(N^2))",
        N * (N + 1) / 2
    );
}

/// Cold start: after a restart, `durable_index` starts at the disk length and
/// the IO thread's persist frontier must start *past* it. The first write's
/// persist scan begins at `durable_index + 1` — an already-durable entry on
/// disk must never be handed back to `persist_entries`.
///
/// Guards the frontier initialization (`= durable_index`, scans use `+ 1`).
#[tokio::test]
async fn test_cold_start_persist_frontier_starts_past_durable_index() {
    let persist_calls: Arc<Mutex<Vec<Vec<u64>>>> = Arc::new(Mutex::new(Vec::new()));

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 5); // disk already holds 1..=5
    log_store.expect_get_entries().returning(|range| {
        Ok(range
            .map(|i| Entry {
                index: i,
                term: 1,
                payload: None,
            })
            .collect())
    });
    {
        let calls = persist_calls.clone();
        log_store.expect_persist_entries().returning(move |entries| {
            calls.lock().unwrap().push(entries.iter().map(|e| e.index).collect());
            Ok(())
        });
    }
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store
        .expect_replace_range()
        .returning(|from, e| Ok(e.last().map(|x| x.index).unwrap_or(from.saturating_sub(1))));
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

    assert_eq!(
        raft_log.durable_index(),
        5,
        "restart: disk length 5 is treated as durable"
    );

    // First write after restart. Its persist scan must start at 6.
    raft_log
        .append_entries(vec![Entry {
            index: 6,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    sleep(Duration::from_millis(50)).await;

    let calls = persist_calls.lock().unwrap().clone();
    assert!(!calls.is_empty(), "entry 6 must have been persisted");
    assert!(
        calls.iter().flatten().all(|&idx| idx >= 6),
        "cold start: the first persist must scan from durable_index+1 (6), never \
         re-scan already-durable entry 5. Got: {calls:?}"
    );
}

/// The flush turn's unconditional tail re-scan must persist writes that were
/// coalesced into the turn — a write whose `IOTask::Persist` is dropped in the
/// drain loop still becomes durable, because the catch-up re-reads
/// `memory_max_index` and persists everything past the frontier.
///
/// Deterministic via a per-call persist gate: every `persist_entries` announces
/// its indices and blocks until released.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn test_flush_turn_catch_up_persists_writes_coalesced_during_the_turn() {
    let (entered_tx, entered_rx) = std::sync::mpsc::channel::<Vec<u64>>();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let release_rx = Mutex::new(release_rx);

    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(move |entries| {
        entered_tx.send(entries.iter().map(|e| e.index).collect()).ok();
        release_rx.lock().unwrap().recv().ok();
        Ok(())
    });
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store
        .expect_replace_range()
        .returning(|from, e| Ok(e.last().map(|x| x.index).unwrap_or(from.saturating_sub(1))));
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
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));
    std::thread::sleep(Duration::from_millis(10));

    let e = |i: u64| Entry {
        index: i,
        term: 1,
        payload: None,
    };

    // 1..=2 persisted (frontier → 2).
    raft_log.append_entries(vec![e(1), e(2)]).await.unwrap();
    assert_eq!(entered_rx.recv().unwrap(), vec![1, 2]);
    release_tx.send(()).unwrap();

    // 3..=4: their Persist is in progress (blocked in persist_entries).
    raft_log.append_entries(vec![e(3), e(4)]).await.unwrap();
    assert_eq!(entered_rx.recv().unwrap(), vec![3, 4]);

    // flush() enqueues IOTask::Flush behind the in-progress Persist(3,4).
    let flush_task = {
        let rl = raft_log.clone();
        tokio::spawn(async move { rl.flush().await })
    };
    tokio::time::sleep(Duration::from_millis(30)).await;

    // 5..=6 land while Persist(3,4) is still blocked → their Persist queues
    // behind Flush.
    raft_log.append_entries(vec![e(5), e(6)]).await.unwrap();
    release_tx.send(()).unwrap(); // release Persist(3,4) → frontier → 4

    // IO thread moves to Flush → run_flush_turn. Leading persist covers 5,6.
    assert_eq!(entered_rx.recv().unwrap(), vec![5, 6]);

    // 7..=8 land now — after run_flush_turn read memory_max for its leading
    // persist, before its drain loop. Their Persist queues behind Flush and
    // will be dropped in the drain loop.
    raft_log.append_entries(vec![e(7), e(8)]).await.unwrap();
    release_tx.send(()).unwrap(); // release leading persist(5,6) → frontier → 6

    // The drain loop drops Persist(5,6) and Persist(7,8); the unconditional
    // tail re-scan then persists 7,8.
    assert_eq!(
        entered_rx.recv().unwrap(),
        vec![7, 8],
        "flush turn's tail re-scan must persist 7,8 whose Persist was dropped"
    );
    release_tx.send(()).unwrap();

    flush_task.await.unwrap().unwrap();
    while let Ok(ev) = log_flush_rx.try_recv() {
        if let crate::InternalEvent::FsyncCompleted(mark) = ev {
            raft_log.try_advance_durable_index(mark);
        }
    }
    assert_eq!(
        raft_log.durable_index(),
        8,
        "7,8 (coalesced into the flush turn) must be durable via the catch-up"
    );
}
