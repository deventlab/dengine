use std::time::Duration;

use futures::future::join_all;
use tokio;

use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, simulate_insert_command};
use d_engine_proto::common::{Entry, LogId};

#[tokio::test]
async fn test_remove_range_with_concurrent_reads() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_range_with_concurrent_reads",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 1000 entries
    simulate_insert_command(&ctx.raft_log, (1..=1000).collect(), 1).await;

    // Spawn concurrent readers
    let readers: Vec<_> = (0..10)
        .map(|_| {
            let log = ctx.raft_log.clone();
            tokio::spawn(async move {
                for i in 1..=1000 {
                    // This shouldn't panic even during removal
                    let _ = log.entry(i);
                }
            })
        })
        .collect();

    // Remove a large range while reads are happening
    ctx.raft_log.remove_range(300..=700);

    // Wait for readers to finish
    for handle in readers {
        handle.await.expect("reader task failed");
    }

    // Verify final state
    assert_eq!(ctx.raft_log.len(), 599); // 299 before + 300 after
    assert!(ctx.raft_log.entry(299).unwrap().is_some());
    assert!(ctx.raft_log.entry(300).unwrap().is_none());
    assert!(ctx.raft_log.entry(700).unwrap().is_none());
    assert!(ctx.raft_log.entry(701).unwrap().is_some());
}

#[tokio::test]
async fn test_concurrent_append_and_purge() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 50,
        },
        "test_concurrent_append_purge",
    );

    // Pre-populate
    simulate_insert_command(&ctx.raft_log, (1..=1000).collect(), 1).await;

    let mut handles = vec![];

    // Concurrent appends
    for i in 0..5 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            let start = 1000 + i * 100 + 1;
            for j in 0..100 {
                log.append_entries(vec![Entry {
                    index: start + j,
                    term: 2,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    // Concurrent purges
    for cutoff in [100, 200, 300, 400, 500] {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            log.purge_logs_up_to(LogId {
                index: cutoff,
                term: 1,
            })
            .await
            .unwrap();
        }));
    }

    join_all(handles).await;

    // Verify consistency
    assert!(ctx.raft_log.first_entry_id() > 500);
    assert_eq!(ctx.raft_log.last_entry_id(), 1500);
}

/// #436/#6 (CorruptGap): `remove_range` deletes keys from the SkipMap one at a
/// time in a plain loop — no lock spans the whole removal. A concurrent
/// `get_entries_range` scan running on a genuinely different OS thread (hence
/// `flavor = "multi_thread"` below — a single-threaded runtime can never
/// preempt `remove_range`'s non-yielding loop, so it would never be able to
/// reproduce this) can therefore observe some keys already removed and others
/// not yet, in the middle of a still-open range.
///
/// This does NOT test the leader's own replication path (`retrieve_to_be_synced_logs_for_peers`
/// only ever runs on the single-threaded raft core loop, serialized with its own
/// purge calls — see 436 design docs). It tests the lower-level primitive any
/// concurrent reader (e.g. `DefaultCommitHandler`, which runs as its own spawned
/// task and also calls `get_entries_range` on the same shared log) depends on.
///
/// Non-deterministic by nature: a torn read is a real, reproducible bug (fails
/// this test if hit), but a clean pass does not prove the race is absent — only
/// that this run didn't hit the window. Run with `--release` or increase
/// `ITERATIONS` if this needs to be more convincing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_get_entries_range_never_returns_torn_result_during_concurrent_purge() {
    const TOTAL: u64 = 2000;
    const ITERATIONS: usize = 500;

    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_get_entries_range_never_returns_torn_result",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");
    simulate_insert_command(&ctx.raft_log, (1..=TOTAL).collect(), 1).await;

    // Reader: repeatedly scans the full still-live range and checks that
    // whatever comes back is an internally contiguous run. A legitimate purge
    // only ever truncates the *prefix* — the surviving suffix is always
    // contiguous by construction. A gap strictly between two returned entries
    // (index jump > 1) can only mean the scan observed a torn mid-removal state.
    let reader_log = ctx.raft_log.clone();
    let reader = tokio::spawn(async move {
        for _ in 0..ITERATIONS {
            let Ok(entries) = reader_log.get_entries_range(1..=TOTAL) else {
                continue;
            };
            for pair in entries.windows(2) {
                if pair[1].index != pair[0].index + 1 {
                    return Some(format!(
                        "torn range read: entries jumped from index {} to {} \
                         (a gap strictly inside the returned range, not a prefix truncation)",
                        pair[0].index, pair[1].index
                    ));
                }
            }
        }
        None
    });

    // Purger: repeatedly advances the purge boundary concurrently with the reader.
    let purge_log = ctx.raft_log.clone();
    let purger = tokio::spawn(async move {
        let step = (TOTAL as usize / ITERATIONS).max(1) as u64;
        let mut cutoff = step;
        while cutoff < TOTAL {
            let _ = purge_log
                .purge_logs_up_to(LogId {
                    index: cutoff,
                    term: 1,
                })
                .await;
            cutoff += step;
        }
    });

    let (reader_result, _) = tokio::join!(reader, purger);
    if let Some(msg) = reader_result.expect("reader task panicked") {
        panic!("{msg}");
    }
}
