//! Performance benchmark integration tests for BufferedRaftLog
//!
//! These tests measure real I/O performance with FileStorageEngine.
//! Most tests are marked with #[ignore] and can be run explicitly or
//! skipped in CI environments.

use super::TestContext;
use bytes::Bytes;
use d_engine_core::{BufferedRaftLog, FlushPolicy, PersistenceConfig, RaftLog};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tempfile::tempdir;
use tokio::time::Instant;

mod filter_out_conflicts_and_append_performance_tests {
    use super::*;

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals_fresh_cluster()
    {
        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (idle_flush_interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms,
                },
                shutdown_timeout_ms: 5000,
            };

            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().to_path_buf();

            let (log, receiver) = BufferedRaftLog::<
                RaftTypeConfig<FileStorageEngine, FileStateMachine>,
            >::new(
                1, config, Arc::new(FileStorageEngine::new(path).unwrap())
            );
            let log = log.start(receiver, None);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                0, // prev_log_index
                0, // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {idle_flush_interval_ms}ms: Took {duration}ms");

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {duration}ms exceeds max {max_duration_ms}ms for {idle_flush_interval_ms}ms interval"
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_none());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_none());
        }
    }

    #[tokio::test]
    async fn test_filter_out_conflicts_performance_consistent_across_flush_intervals() {
        // Test configuration
        let test_cases = vec![
            (10, 50),   // 10ms interval, 50ms max duration
            (100, 50),  // 100ms interval, 50ms max duration
            (1000, 50), // 1000ms interval, 50ms max duration
        ];

        for (idle_flush_interval_ms, max_duration_ms) in test_cases {
            // Create MemFirst storage with batch policy
            let config = PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms,
                },
                shutdown_timeout_ms: 5000,
            };

            let temp_dir = tempdir().unwrap();
            let path = temp_dir.path().to_path_buf();

            let (log, receiver) = BufferedRaftLog::<
                RaftTypeConfig<FileStorageEngine, FileStateMachine>,
            >::new(
                1, config, Arc::new(FileStorageEngine::new(path).unwrap())
            );
            let log = log.start(receiver, None);

            // Populate with test data (1000 entries)
            let mut entries = vec![];
            for i in 1..=1000 {
                entries.push(Entry {
                    index: i,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))), // 256B payload
                });
            }
            log.append_entries(entries.clone()).await.unwrap();
            // Wait for IO to complete so the measurement is pure in-memory performance,
            // not affected by background flush contention from the raft-io thread.
            log.flush().await.unwrap();

            // Measure conflict resolution performance
            let start = Instant::now();
            log.filter_out_conflicts_and_append(
                500, // prev_log_index
                1,   // prev_log_term
                vec![Entry {
                    index: 501,
                    term: 1,
                    payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
                }],
            )
            .await
            .unwrap();

            let duration = start.elapsed().as_millis() as u64;
            println!("Interval {idle_flush_interval_ms}ms: Took {duration}ms");

            // Verify performance consistency
            assert!(
                duration <= max_duration_ms,
                "Duration {duration}ms exceeds max {max_duration_ms}ms for {idle_flush_interval_ms}ms interval"
            );

            // Verify correctness
            assert!(log.entry(500).unwrap().is_some());
            assert!(log.entry(501).unwrap().is_some());
            assert!(log.entry(502).unwrap().is_some()); // No conflict — pipeline overlap, entry retained per Raft semantics
        }
    }
}

#[tokio::test]
async fn test_last_entry_id_performance() {
    // Set up test context
    let test_context = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 360_000,
        },
        "test_last_entry_id_performance",
    );

    // Create a large number of entries
    const ENTRY_COUNT: usize = 1_000_000;
    let entries: Vec<_> = (0..ENTRY_COUNT)
        .map(|index| Entry {
            index: index as u64,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(vec![1; 256]))),
        })
        .collect();

    // Insert entries into the log
    let insert_start = Instant::now();
    test_context.raft_log.append_entries(entries).await.unwrap();
    let insert_duration = insert_start.elapsed();
    println!("Insert duration: {insert_duration:?}");

    // Measure last_entry_id performance
    let mut durations = Vec::with_capacity(10);
    let mut last_id = 0;
    for _ in 1..1_000 {
        let start = Instant::now();
        last_id = test_context.raft_log.last_entry_id();
        let duration = start.elapsed();
        durations.push(duration);
    }

    // Calculate average duration
    let avg_duration = durations.iter().sum::<Duration>() / durations.len() as u32;
    println!("Average last_entry_id duration: {avg_duration:?}");
    assert!(avg_duration < Duration::from_millis(1));
    // Assert that the last entry ID is correct
    assert_eq!(last_id, ENTRY_COUNT as u64 - 1);
}

#[tokio::test]
async fn test_performance_benchmarks() {
    // Skip performance tests during coverage runs due to instrumentation overhead
    if std::env::var("CARGO_LLVM_COV").is_ok() || std::env::var("CARGO_TARPAULIN").is_ok() {
        eprintln!("Skipping performance test during coverage run");
        return;
    }

    let is_ci = std::env::var("CI").is_ok();

    // Adjust test parameters according to the environment
    let operations = if is_ci {
        // CI environment uses a more relaxed threshold
        //
        // append_entries now round-trips through a dedicated IO thread via
        // oneshot (see #444: leader's own write must reach the storage
        // engine before counting toward quorum) — this is an intentional
        // correctness/speed tradeoff, not a regression. The old threshold
        // (500) predates that fix. New floor leaves ~2x headroom below the
        // observed ~318-328 ops/sec on a modern dev machine, keeping the
        // 2:1 local:CI ratio from before.
        [
            ("append_entries", 500, 100.0),
            ("get_entries_range", 2500, 25000.0),
            ("entry_lookup", 5000, 100000.0),
            ("term_queries", 4000, 25000.0),
        ]
    } else {
        // Local environment uses a stricter threshold
        //
        // See CI-branch comment above — same #444 rationale.
        [
            ("append_entries", 1000, 200.0),
            ("get_entries_range", 5000, 50000.0),
            ("entry_lookup", 10000, 200000.0),
            ("term_queries", 8000, 50000.0),
        ]
    };

    let ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 100,
        },
        "performance_benchmark",
    );

    // Pre-populate with data
    let mut entries = Vec::new();
    // Reduce pre-population data in a CI environment
    let pre_populate_count = if is_ci { 5000 } else { 10000 };

    for i in 1..=pre_populate_count {
        entries.push(Entry {
            index: i,
            term: i / 100 + 1, // Vary terms
            payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))), // 256B payload
        });
    }
    ctx.raft_log.append_entries(entries).await.unwrap();

    let mut results = HashMap::new();

    for (op_name, count, min_ops_per_sec) in operations {
        let start = Instant::now();

        match op_name {
            "append_entries" => {
                for i in 0..count {
                    let entry = Entry {
                        index: 10000 + i as u64 + 1,
                        term: 101,
                        payload: Some(EntryPayload::command(Bytes::from(vec![0; 256]))),
                    };
                    ctx.raft_log.append_entries(vec![entry]).await.unwrap();
                }
            }
            "get_entries_range" => {
                for i in 0..count {
                    let start_idx = (i % 9000) as u64 + 1;
                    let end_idx = start_idx + 100;
                    let _ = ctx.raft_log.get_entries_range(start_idx..=end_idx);
                }
            }
            "entry_lookup" => {
                for i in 0..count {
                    let index = (i % 10000) as u64 + 1;
                    let _ = ctx.raft_log.entry(index);
                }
            }
            "term_queries" => {
                for i in 0..count {
                    let term = (i % 100) as u64 + 1;
                    let _ = ctx.raft_log.first_index_for_term(term);
                    let _ = ctx.raft_log.last_index_for_term(term);
                }
            }
            _ => {}
        }

        let duration = start.elapsed();
        let ops_per_sec = count as f64 / duration.as_secs_f64();
        results.insert(op_name, (duration, ops_per_sec));

        println!("{op_name}: {count} operations in {duration:?} ({ops_per_sec:.2} ops/sec)");

        // Performance assertions with more realistic thresholds
        assert!(
            ops_per_sec > min_ops_per_sec,
            "{op_name} operations too slow: {ops_per_sec:.2} ops/sec (expected > {min_ops_per_sec:.2})"
        );
    }
}

#[tokio::test]
async fn test_read_performance_under_concurrent_write_load() {
    // Use environment variable to control performance validation mode
    // Set CI=1 to use relaxed threshold for CI environment (10 reads/sec)
    // Without it, use strict performance validation (10K reads/sec)
    //
    // `entry()` and `append_entries()` both go through the same `entries: RwLock<SkipMap>`
    // (see #439) — this asserts the read side stays fast enough under concurrent writes,
    // it does not assert the absence of a lock.
    let expected_min = if std::env::var("CI").is_ok() {
        10.0
    } else {
        10_000.0
    };

    let ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 100,
        },
        "test_read_performance_under_concurrent_write_load",
    );

    // Pre-populate
    ctx.append_entries(1, 10000, 1).await;

    // Measure read performance under write load
    let log = ctx.raft_log.clone();
    let write_handle = tokio::spawn(async move {
        for i in 1..=1000 {
            log.append_entries(vec![Entry {
                index: 10000 + i,
                term: 2,
                payload: Some(EntryPayload::command(Bytes::from(vec![0; 1024]))),
            }])
            .await
            .unwrap();
            tokio::time::sleep(Duration::from_micros(100)).await;
        }
    });

    // Use saturating_add to prevent overflow and add timeout
    let start = Instant::now();
    let mut read_count: u64 = 0;
    let timeout = Duration::from_millis(200);

    while !write_handle.is_finished() && start.elapsed() < timeout {
        for i in 1..=100 {
            let index = (i * 100).min(10000); // Ensure index is valid
            let _ = ctx.raft_log.entry(index);
            read_count = read_count.saturating_add(1);
        }

        // Add small yield to prevent tight loop
        tokio::task::yield_now().await;
    }

    write_handle.await.unwrap();
    let duration = start.elapsed();
    let reads_per_sec = read_count as f64 / duration.as_secs_f64();

    println!("Performed {read_count} reads in {duration:?} ({reads_per_sec:.2} reads/sec)",);

    // More reasonable performance expectation
    assert!(
        reads_per_sec > expected_min,
        "Read performance degraded: {reads_per_sec:.2} reads/sec (expected > {expected_min:.2})",
    );
}
