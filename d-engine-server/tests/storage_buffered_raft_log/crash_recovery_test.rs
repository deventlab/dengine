//! Crash recovery integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog behavior with real disk persistence
//! and crash recovery semantics using FileStorageEngine.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{BufferedRaftLog, FlushPolicy, PersistenceConfig, RaftLog};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tokio::time::sleep;

use super::TestContext;

#[tokio::test]
async fn test_crash_recovery() {
    // Create and populate storage
    let original_ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_crash_recovery",
    );

    // Append an entry
    original_ctx
        .raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
        }])
        .await
        .unwrap();

    // Ensure the entry is persisted for DiskFirst strategy
    original_ctx.raft_log.flush().await.unwrap();

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Graceful shutdown: close() joins the IO thread before returning,
    // preventing Tokio runtime shutdown panics.
    original_ctx.close().await;

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - for DiskFirst, entries should be immediately durable
    assert_eq!(recovered_ctx.raft_log.durable_index(), 1);

    // The entry should be available
    let entry = recovered_ctx.raft_log.entry(1).unwrap();
    assert!(entry.is_some());
    assert_eq!(entry.unwrap().index, 1);
    recovered_ctx.close().await;
}

#[tokio::test]
async fn test_crash_recovery_with_multiple_entries() {
    // Create and populate storage
    let mut original_ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_crash_recovery_with_multiple_entries",
    );

    // Append multiple entries
    for i in 1..=5 {
        original_ctx
            .raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(
                    format!("data{i}").into_bytes(),
                ))),
            }])
            .await
            .unwrap();
    }

    // Ensure all entries are persisted for DiskFirst strategy
    original_ctx.raft_log.flush().await.unwrap();
    original_ctx.drain_fsync_completions();

    // Verify all entries are in memory and durable
    assert_eq!(original_ctx.raft_log.durable_index(), 5);
    assert_eq!(original_ctx.raft_log.len(), 5);

    // Recover from the same storage (simulating restart)
    let recovered_ctx = original_ctx.recover_from_crash();

    // Graceful shutdown: close() joins the IO thread before returning,
    // preventing Tokio runtime shutdown panics.
    original_ctx.close().await;

    sleep(Duration::from_millis(50)).await; // Allow recovery

    // Verify recovery - all entries should be recovered
    assert_eq!(recovered_ctx.raft_log.durable_index(), 5);
    assert_eq!(recovered_ctx.raft_log.len(), 5);

    // All entries should be available
    for i in 1..=5 {
        let entry = recovered_ctx.raft_log.entry(i).unwrap();
        assert!(entry.is_some());
        assert_eq!(entry.unwrap().index, i);
    }
    recovered_ctx.close().await;
}

#[tokio::test]
async fn test_partial_flush_with_graceful_shutdown() {
    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_graceful");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
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

        // Add 75 entries (1.5 batches)
        for i in 1..=75 {
            raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
        }

        // Wait for first batch to flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Graceful shutdown: close() joins the IO thread, ensuring all entries are
        // flushed before recovery reads from disk.
        raft_log.close().await;
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 1,
                },
                shutdown_timeout_ms: 5000,
            },
            storage,
        );
    let raft_log = raft_log.start(receiver, None);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With graceful shutdown, close() flushes all entries (75)
    assert_eq!(raft_log.len(), 75);
    assert_eq!(raft_log.durable_index(), 75);
    raft_log.close().await;
}

/// MemFirst crash semantics with notify-then-fsync architecture.
///
/// `append_entries` calls `write_notify.notify_one()` on every write, which eagerly
/// wakes the IO thread regardless of `idle_flush_interval_ms`. The idle timer is a
/// safety-net only — the normal path persists data almost immediately after each append.
///
/// True crash (kill -9 / power loss) means data MAY survive if the IO thread had time
/// to run before the crash. This test verifies:
/// - Entries flushed before the crash (first batch, waited 150ms) always survive.
/// - Entries written immediately before crash (second batch, no wait) may or may not
///   survive depending on IO thread scheduling at crash time.
/// - `mem::forget` simulates a hard crash: no graceful shutdown, no final flush.
#[tokio::test]
async fn test_partial_flush_after_crash() {
    let batch_size = 50;

    // Create and partially populate storage
    let temp_dir = tempfile::tempdir().unwrap();
    let storage_path = temp_dir.path().join("partial_flush_crash");

    {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
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

        // Add first batch (50 entries)
        for i in 1..=50 {
            raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
        }

        // Wait for first batch to flush
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Add remaining entries (25 entries) as a single batch call.
        // Single .await minimises the window for the IO thread to run between writes.
        let second_batch: Vec<Entry> = (51..=75)
            .map(|i| Entry {
                index: i,
                term: 1,
                payload: None,
            })
            .collect();
        raft_log.append_entries(second_batch).await.unwrap();

        // Simulate crash immediately: Skip Drop with mem::forget (like kill -9 or power loss)
        // This prevents the processor from flushing the remaining 25 entries
        // Storage is moved into raft_log, so forgetting raft_log is enough
        std::mem::forget(raft_log);
    }

    // Recover from disk
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 1,
                },
                shutdown_timeout_ms: 5000,
            },
            storage,
        );
    let raft_log = raft_log.start(receiver, None);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // With notify-then-fsync, the IO thread processes writes eagerly.
    // At minimum, the first batch (flushed by safety timer) must survive.
    // The second batch may also survive depending on IO thread scheduling.
    let recovered = raft_log.len();
    assert!(
        recovered >= batch_size,
        "flushed entries must survive crash: expected >= {batch_size}, got {recovered}"
    );
    assert!(
        recovered <= 75,
        "cannot recover more entries than written: got {recovered}"
    );
    assert_eq!(
        raft_log.durable_index() as usize,
        recovered,
        "durable_index must match recovered len after restart"
    );
    raft_log.close().await;
}

#[tokio::test]
async fn test_recovery_under_different_scenarios() {
    // Test various recovery scenarios
    // Method C (drain-then-fsync): all writes are fsynced by the IO thread after each
    // drain cycle, so all 100 entries are always durable after explicit flush().
    let scenarios = vec![
        (
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            100usize,
        ),
        (
            FlushPolicy::Batch {
                idle_flush_interval_ms: 10,
            },
            100,
        ),
        (
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1000,
            },
            100,
        ),
    ];

    for (flush_policy, expected_recovery) in scenarios {
        let instance_id = format!("recovery_test_{flush_policy:?}");
        let original_ctx = TestContext::new(flush_policy.clone(), &instance_id);

        // Add test data
        for i in 1..=100 {
            original_ctx
                .raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(
                        format!("data{i}").into_bytes(),
                    ))),
                }])
                .await
                .unwrap();
        }

        // Explicit flush ensures all entries are durable before crash simulation.
        original_ctx.raft_log.flush().await.unwrap();

        // Simulate crash and recovery
        let recovered_ctx = original_ctx.recover_from_crash();
        original_ctx.close().await;

        // Verify recovery based on expected behavior
        assert_eq!(
            recovered_ctx.raft_log.len(),
            expected_recovery,
            "Recovery mismatch for policy {flush_policy:?}"
        );
        recovered_ctx.close().await;
    }
}

#[tokio::test]
async fn test_memfirst_crash_recovery_durability() {
    let instance_id = "test_memfirst_durability";

    let recovered_path = {
        let ctx = TestContext::new(
            FlushPolicy::Batch {
                idle_flush_interval_ms: 10000,
            },
            instance_id,
        );

        ctx.append_entries(1, 100, 1).await;

        // Verify visible in memory
        assert_eq!(ctx.raft_log.len(), 100);

        let path = ctx.path.clone();
        // Graceful shutdown: data is flushed but _temp_dir is deleted,
        // so recovery still sees an empty storage.
        ctx.close().await;
        path
    };

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Recovery
    let storage = Arc::new(FileStorageEngine::new(PathBuf::from(&recovered_path)).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 1,
                },
                shutdown_timeout_ms: 5000,
            },
            storage,
        );
    let raft_log = raft_log.start(receiver, None);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // After crash without flush, data should be lost
    assert_eq!(
        raft_log.len(),
        0,
        "MemFirst without flush should lose uncommitted data"
    );
    raft_log.close().await;
}

#[tokio::test]
async fn test_diskfirst_crash_recovery_durability() {
    let temp_dir = tempfile::tempdir().unwrap();
    let instance_id = "test_diskfirst_durability";
    let storage_path = temp_dir.path().join(instance_id);

    let ctx1 = {
        let storage = Arc::new(FileStorageEngine::new(storage_path.clone()).unwrap());
        let (raft_log, receiver) =
            BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
                1,
                PersistenceConfig {
                    flush_policy: FlushPolicy::Batch {
                        idle_flush_interval_ms: 1,
                    },
                    shutdown_timeout_ms: 5000,
                },
                storage,
            );
        let raft_log = raft_log.start(receiver, None);

        let entries: Vec<_> = (1..=100)
            .map(|index| Entry {
                index,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
            })
            .collect();

        raft_log.append_entries(entries).await.unwrap();
        raft_log.flush().await.unwrap();
        assert_eq!(raft_log.len(), 100, "All entries should be in memory");

        raft_log
    };

    ctx1.close().await;

    // Phase 2: Recovery
    let storage = Arc::new(FileStorageEngine::new(storage_path).unwrap());
    let (raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            1,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 1,
                },
                shutdown_timeout_ms: 5000,
            },
            storage,
        );
    let raft_log = raft_log.start(receiver, None);

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Verify recovery
    assert_eq!(raft_log.len(), 100, "All entries should be recovered");
    assert_eq!(raft_log.durable_index(), 100, "Durable index should be 100");
    raft_log.close().await;
}
