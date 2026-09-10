use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{
    BufferedRaftLog, FlushPolicy, MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig,
    PersistenceConfig,
};
use d_engine_proto::common::{Entry, EntryPayload};

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

/// Verifies that the IO thread exits cleanly when the outer tokio runtime is dropped before
/// `close()` is called — the root scenario that caused SIGABRT in CI.
///
/// Root cause: the previous implementation used `Handle::current()` to borrow the outer
/// runtime's timer wheel. When the outer runtime dropped, `interval.tick().await` accessed a
/// destroyed timer → panic while holding a pthread mutex → SIGABRT (uncatchable).
///
/// After the fix the IO thread owns its own `new_current_thread` runtime; dropping the outer
/// runtime has no effect on the IO thread's timers.
#[test]
fn test_io_thread_survives_runtime_drop() {
    // Build an outer runtime — simulates the tokio test runtime that is dropped at test end.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build outer runtime");

    let storage = Arc::new(MockStorageEngine::with_id(
        "test_io_thread_runtime_drop".to_string(),
    ));

    let raft_log = rt.block_on(async {
        let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
            1,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 50,
                },
                shutdown_timeout_ms: 5000,
            },
            storage,
        );
        let log = log.start(receiver, None);
        for i in 1..=10 {
            log.append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
        }
        log
    });

    // Drop the outer runtime BEFORE dropping raft_log.
    // Old code: SIGABRT (process abort).
    // Fixed code: IO thread's own runtime is unaffected; it exits cleanly on Drop.
    drop(rt);

    // Allow the IO thread time to process the Shutdown signal sent by Drop.
    std::thread::sleep(Duration::from_millis(200));

    // If we reach here the process did not abort — test passes.
    drop(raft_log);
}

#[tokio::test]
async fn test_shutdown_closes_channel_properly() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 100,
        },
        "test_shutdown_channel",
    );

    // Add some entries
    for i in 1..=10 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }

    // Drop raft_log to trigger shutdown
    drop(ctx.raft_log);

    // Verify shutdown completes without hanging
    tokio::time::sleep(Duration::from_millis(50)).await;
}

#[tokio::test]
async fn test_shutdown_awaits_worker_completion() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 5000,
        },
        "test_shutdown_await_workers",
    );

    // Append entries to trigger worker activity
    for i in 1..=50 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 100]))),
            }])
            .await
            .unwrap();
    }

    // Force flush to ensure workers have work
    ctx.raft_log.flush().await.unwrap();

    // Give workers time to start processing
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Trigger shutdown via Drop
    let shutdown_start = std::time::Instant::now();
    drop(ctx.raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    // Verify shutdown completed in reasonable time
    assert!(
        shutdown_duration < Duration::from_millis(500),
        "Shutdown took too long: {shutdown_duration:?}",
    );
}

#[tokio::test]
async fn test_shutdown_handles_slow_workers() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "test_shutdown_slow_workers".to_string(),
    ));

    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 100,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );

    let raft_log = raft_log.start(receiver, None);

    // Append entries
    for i in 1..=10 {
        raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 50]))),
            }])
            .await
            .unwrap();
    }

    // Trigger flush
    raft_log.flush().await.unwrap();

    // Give workers time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Shutdown should wait for workers
    let shutdown_start = std::time::Instant::now();
    drop(raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    assert!(
        shutdown_duration < Duration::from_millis(1000),
        "Shutdown with slow workers took too long"
    );
}

#[tokio::test]
async fn test_shutdown_with_multiple_flushes() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 100,
        },
        "test_shutdown_multiple_flushes",
    );

    // Create multiple flush operations
    for batch in 0..5 {
        for i in 1..=10 {
            let index = batch * 10 + i;
            ctx.raft_log
                .append_entries(vec![Entry {
                    index,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0u8; 100]))),
                }])
                .await
                .unwrap();
        }
        ctx.raft_log.flush().await.unwrap();
    }

    // Give workers time to process
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Shutdown should handle all pending work
    let shutdown_start = std::time::Instant::now();
    drop(ctx.raft_log);
    let shutdown_duration = shutdown_start.elapsed();

    assert!(
        shutdown_duration < Duration::from_millis(500),
        "Shutdown with multiple flushes took too long"
    );
}

/// Verifies that a fatal IOTask::ReplaceRange failure:
/// 1. Propagates the error synchronously to the caller via the done channel.
/// 2. Shuts down the IO thread.
/// 3. Poisons the log — writes after the failure are rejected outright, not
///    silently accepted into memory with no IO thread left to persist them.
///
/// Root issue: without `return` after done.send(Err), batch_processor continues
/// running on a storage whose on-disk state is now inconsistent with memory.
/// With the fix, it exits immediately so no further IO is attempted.
///
/// ## Update (2026-07-19)
/// Point 3 and the poisoning check are new. The old version of this test
/// asserted the opposite of point 3 — that a write after the fatal error
/// "succeeds (in-memory only)". That was accurate but was itself a bug this
/// session closed: a write silently accepted with no live IO thread to ever
/// persist it. The trailing `flush()` check is removed — with writes now
/// rejected, `max_index` never gets ahead of `durable_index`, so `flush()`
/// short-circuits to `Ok(())` and no longer exercises the dead-channel path.
#[tokio::test]
async fn test_replace_range_failure_propagates_error_and_shuts_down_io_thread() {
    let mut log_store = MockLogStore::new();

    // replace_range always fails — simulates an unrecoverable disk error.
    log_store
        .expect_replace_range()
        .returning(|_, _| Err(crate::Error::Fatal("simulated disk failure".into())));

    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(|_| Ok(()));
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
                idle_flush_interval_ms: 60_000, // no auto-flush
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10));

    // Append [1..4] term=1 and let the IO thread persist them (durable_index → 4).
    raft_log
        .append_entries(vec![entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)])
        .await
        .unwrap();
    std::thread::sleep(Duration::from_millis(30));

    // Trigger IOTask::ReplaceRange: term conflict at index 3 (term 1 → 2).
    let result = raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 2), entry(4, 2)])
        .await;

    // Error must be propagated back to the caller via the done channel.
    assert!(
        result.is_err(),
        "expected ReplaceRange failure to be propagated to caller"
    );

    // Allow IO thread time to fully exit after the fatal error.
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        raft_log.is_poisoned(),
        "a replace_range() failure must poison the log"
    );

    // Writes after the failure must be rejected outright, not silently
    // accepted into memory with no live IO thread to ever persist them.
    let append_result = raft_log.append_entries(vec![entry(5, 2)]).await;
    assert!(
        append_result.is_err(),
        "writes must be rejected once poisoned"
    );
}
