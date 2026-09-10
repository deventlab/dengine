//! Performance tests for BufferedRaftLog with controllable delays
//!
//! These tests verify BufferedRaftLog performance behavior during concurrent
//! operations like flush, using MockStorageEngine with controllable delays.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::Barrier;
use tokio::time::Instant;

use crate::{
    BufferedRaftLog, FlushPolicy, MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig,
    PersistenceConfig, RaftLog,
};
use d_engine_proto::common::{Entry, EntryPayload};

// Test helper: Creates storage with controllable delay
fn create_delayed_storage(delay_ms: u64) -> Arc<MockStorageEngine> {
    let mut log_store = MockLogStore::new();
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_truncate().returning(|_| Ok(()));
    log_store.expect_reset().returning(|| Ok(()));
    log_store.expect_is_write_durable().returning(|| true);
    log_store.expect_flush().returning(|| Ok(()));

    // Add controllable delay to persist_entries
    log_store.expect_persist_entries().returning(move |_| {
        let delay = Duration::from_millis(delay_ms);
        std::thread::sleep(delay);
        Ok(())
    });

    Arc::new(MockStorageEngine::from(log_store, MockMetaStore::new()))
}

// Tests reset performance during active flush
#[tokio::test]
async fn test_reset_performance_during_active_flush() {
    // persist_entries mock sleeps for FLUSH_DELAY_MS.
    // reset() waits for the IO thread to finish its current in-flight operation before processing
    // Reset — this is correct behavior. The test verifies reset completes within a bounded time
    // (3x the flush delay) and does not block indefinitely.
    const FLUSH_DELAY_MS: u64 = 200;
    let max_reset_duration_ms = FLUSH_DELAY_MS * 3; // 600ms: accounts for IO thread overhead

    let test_cases = vec![
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1000,
        },
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
    ];

    for flush_policy in test_cases {
        let storage = create_delayed_storage(FLUSH_DELAY_MS);
        let config = PersistenceConfig {
            flush_policy: flush_policy.clone(),
            shutdown_timeout_ms: 5000,
        };

        let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(1, config, storage);
        let log = log.start(receiver, None);
        let barrier = Arc::new(Barrier::new(2));

        // Start long-running append+flush in background (slow due to persist_entries delay)
        let flush_log = log.clone();
        let flush_barrier = barrier.clone();
        tokio::spawn(async move {
            flush_barrier.wait().await; // Sync point
            let entries: Vec<Entry> = (1..=10)
                .map(|i| Entry {
                    index: i,
                    term: 1,
                    payload: None,
                })
                .collect();
            let _ = flush_log.append_entries(entries).await;
            let _ = flush_log.flush().await;
        });

        // Wait for flush to start
        barrier.wait().await;

        // Measure reset performance during active flush
        let start = Instant::now();
        log.reset().await.unwrap();
        let duration = start.elapsed();

        assert!(
            duration.as_millis() < max_reset_duration_ms as u128,
            "Reset took {}ms during active flush ({:?})",
            duration.as_millis(),
            flush_policy
        );
    }
}

// Tests filter_out_conflicts performance with active flush
#[tokio::test]
async fn test_filter_conflicts_performance_during_flush() {
    let is_ci = std::env::var("CI").is_ok();
    // Relax time limit in CI environment
    let test_cases = if is_ci {
        vec![(10, 500), (100, 500), (1000, 500)]
    } else {
        vec![(10, 50), (100, 50), (1000, 50)]
    };

    const FLUSH_DELAY_MS: u64 = 300;

    for (idle_flush_interval_ms, max_duration_ms) in test_cases {
        let storage = create_delayed_storage(FLUSH_DELAY_MS);
        let config = PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms,
            },
            shutdown_timeout_ms: 5000,
        };

        let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(1, config, storage);
        let log = log.start(receiver, None);
        let barrier = Arc::new(Barrier::new(2));

        // Populate with test data
        let mut entries = vec![];
        for i in 1..=1000 {
            entries.push(Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))),
            });
        }
        log.append_entries(entries).await.unwrap();

        // Start long flush in background (slow due to persist_entries delay)
        let flush_log = log.clone();
        let flush_barrier = barrier.clone();
        tokio::spawn(async move {
            flush_barrier.wait().await;
            let _ = flush_log.flush().await;
        });

        // Wait for flush to start
        barrier.wait().await;

        // Measure performance during active flush
        let start = Instant::now();
        log.filter_out_conflicts_and_append(
            500,
            1,
            vec![Entry {
                index: 501,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
            }],
        )
        .await
        .unwrap();

        let duration = start.elapsed();
        assert!(
            duration.as_millis() < max_duration_ms as u128,
            "Operation took {}ms with {}ms interval during flush",
            duration.as_millis(),
            idle_flush_interval_ms
        );
    }
}

// Tests fresh cluster performance consistency
#[tokio::test]
async fn test_fresh_cluster_performance_consistency() {
    let is_ci = std::env::var("CI").is_ok();
    // Relax time limit in CI environment
    let max_duration_ms = if is_ci { 50 } else { 5 };

    let test_cases = vec![
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1000,
        },
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
    ];

    for flush_policy in test_cases {
        let mut log_store = MockLogStore::new();
        log_store.expect_is_write_durable().returning(|| true);
        log_store.expect_flush().return_once(|| Ok(()));
        log_store.expect_last_index().returning(|| 0);
        log_store.expect_load_purge_boundary().returning(|| Ok(None));
        log_store.expect_truncate().returning(|_| Ok(()));
        log_store.expect_persist_entries().returning(|_| Ok(()));
        log_store.expect_reset().returning(|| Ok(()));

        let config = PersistenceConfig {
            flush_policy: flush_policy.clone(),
            shutdown_timeout_ms: 5000,
        };

        let (log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
            1,
            config,
            Arc::new(MockStorageEngine::from(log_store, MockMetaStore::new())),
        );
        let log = log.start(receiver, None);

        // Measure reset performance in fresh cluster
        let start = Instant::now();
        log.reset().await.unwrap();
        let duration = start.elapsed();

        assert!(
            duration.as_millis() < max_duration_ms as u128,
            "Fresh cluster reset took {}ms ({:?})",
            duration.as_millis(),
            flush_policy
        );
    }
}
