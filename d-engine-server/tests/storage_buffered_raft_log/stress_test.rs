//! High concurrency stress tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog behavior under high load with
//! real FileStorageEngine, testing race conditions and resource limits.

use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{FlushPolicy, LogStore, RaftLog, StorageEngine};
use d_engine_proto::common::{Entry, EntryPayload};
use futures::future::join_all;
use tokio::time::Instant;
use tracing_test::traced_test;

use super::TestContext;

// TODO: Extract 4 high concurrency stress tests from legacy_all_tests.rs:
// - test_high_concurrency
// - test_high_concurrency_memory_only
// - test_high_concurrency_mixed_operations
// - test_term_index_correctness_under_load

#[tokio::test]
async fn test_high_concurrency() {
    let mut ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_high_concurrency",
    );
    let mut handles = vec![];

    for i in 0..10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for j in 1..=100 {
                let index = i * 100 + j;
                log.append_entries(vec![Entry {
                    index,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // With MemFirst, entries are buffered; wait for all to be durable before asserting.
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // Verify all entries persisted
    assert_eq!(ctx.raft_log.durable_index(), 1000);
    assert_eq!(ctx.raft_log.len(), 1000);
}

#[tokio::test]
#[traced_test]
async fn test_high_concurrency_mixed_operations() {
    let ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 100,
        },
        "test_high_concurrency_mixed_operations",
    );

    let mut handles = vec![];
    let start_time = Instant::now();

    // Spawn concurrent writers
    for i in 0..10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for j in 1..=1000 {
                let entry = Entry {
                    index: (i * 1000) + j,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0; 1024]))), // 1KB payload
                };
                log.append_entries(vec![entry]).await.unwrap();
            }
        }));
    }

    // Spawn concurrent readers
    for _ in 0..5 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..500 {
                let last_index = log.last_entry_id();
                if last_index > 0 {
                    let _ = log.get_entries_range(1..=last_index.min(100));
                }
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        }));
    }

    // Spawn concurrent term queries
    for _ in 0..3 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..300 {
                let _ = log.first_index_for_term(1);
                let _ = log.last_index_for_term(1);
                tokio::time::sleep(Duration::from_micros(15)).await;
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    let duration = start_time.elapsed();
    println!("Mixed operations completed in: {duration:?}");

    // Verify data integrity
    assert_eq!(ctx.raft_log.len(), 10000);
    // append_entries() now round-trips through a dedicated IO thread via
    // oneshot (see #444: leader's own write must reach the storage engine
    // before counting toward quorum) — an intentional correctness/speed
    // tradeoff, not a regression. The old 10s bound predates that fix;
    // observed wall-clock for this test's 10k concurrent writes is now
    // 13-18s depending on machine load. New bound leaves real headroom
    // above that range rather than chasing the exact number.
    assert!(
        duration < Duration::from_secs(30),
        "Operations took too long: {duration:?}"
    );
}

mod mem_first_tests {
    use super::*;

    #[tokio::test]
    async fn test_basic_write_before_persist() {
        let ctx = TestContext::new(
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            "test_basic_write_before_persist",
        );
        ctx.append_entries(1, 5, 1).await;

        // Verify in memory but not yet durable
        assert_eq!(ctx.raft_log.len(), 5);
        assert_eq!(ctx.raft_log.durable_index(), 0);
    }

    #[tokio::test]
    async fn test_async_persistence() {
        let mut ctx = TestContext::new(
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            "test_async_persistence",
        );
        ctx.append_entries(1, 100, 1).await;

        // Trigger flush
        ctx.raft_log.flush().await.unwrap();
        ctx.drain_fsync_completions();

        // Verify persistence
        assert_eq!(ctx.raft_log.durable_index(), 100);
        assert_eq!(ctx.storage.as_ref().log_store().last_index(), 100);
    }

    #[tokio::test]
    async fn test_power_loss_data_loss() {
        let ctx = TestContext::new(
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            "test_power_loss_data_loss",
        );
        ctx.append_entries(1, 100, 1).await;

        // Simulate power loss before flush
        // storage.reset() simulates data loss after crash

        // In real scenario, durable_index would be 0 after restart
        assert_eq!(ctx.raft_log.durable_index(), 0);
    }

    #[tokio::test]
    async fn test_high_concurrency_memory_only() {
        let ctx = TestContext::new(
            FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            "test_high_concurrency_memory_only",
        );
        let mut handles = vec![];

        for i in 0..10 {
            let log = ctx.raft_log.clone();
            handles.push(tokio::spawn(async move {
                for j in 1..=100 {
                    let index = i * 100 + j;
                    log.append_entries(vec![Entry {
                        index,
                        term: 1,
                        payload: None,
                    }])
                    .await
                    .unwrap();
                }
            }));
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify all entries in memory
        assert_eq!(ctx.raft_log.len(), 1000);
        assert_eq!(ctx.raft_log.last_entry_id(), 1000);
    }
}

#[tokio::test]
async fn test_term_index_correctness_under_load() {
    let ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_term_index_under_load",
    );

    // Concurrent writes with different terms
    let mut handles = vec![];
    for term in 1..=10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for i in 1..=100 {
                log.append_entries(vec![Entry {
                    index: (term - 1) * 100 + i,
                    term,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    join_all(handles).await;

    // Verify term indexes are correct
    for term in 1..=10 {
        let first = ctx.raft_log.first_index_for_term(term);
        let last = ctx.raft_log.last_index_for_term(term);

        assert_eq!(first, Some((term - 1) * 100 + 1));
        assert_eq!(last, Some(term * 100));
    }
}
