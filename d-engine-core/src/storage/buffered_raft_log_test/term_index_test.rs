//! Term index calculation and tracking tests
//!
//! Tests verify term boundary tracking:
//! - First/last index for term queries
//! - Updates after purge/compaction
//! - Correctness under concurrent writes

use d_engine_proto::common::Entry;

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, RaftLog};

#[tokio::test]
async fn test_first_index_for_term() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_first_index_for_term",
    );
    ctx.raft_log.reset().await.unwrap();

    let entries = vec![
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
            term: 2,
            payload: None,
        },
        Entry {
            index: 4,
            term: 2,
            payload: None,
        },
        Entry {
            index: 5,
            term: 3,
            payload: None,
        },
        Entry {
            index: 6,
            term: 3,
            payload: None,
        },
        Entry {
            index: 7,
            term: 3,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();

    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(5));
    assert_eq!(ctx.raft_log.first_index_for_term(4), None);

    let entries = vec![
        Entry {
            index: 8,
            term: 4,
            payload: None,
        },
        Entry {
            index: 9,
            term: 5,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();
    assert_eq!(ctx.raft_log.first_index_for_term(4), Some(8));
    assert_eq!(ctx.raft_log.first_index_for_term(5), Some(9));

    ctx.raft_log.reset().await.unwrap();
    assert_eq!(ctx.raft_log.first_index_for_term(1), None);
}

#[tokio::test]
async fn test_last_index_for_term() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_index_for_term",
    );
    ctx.raft_log.reset().await.unwrap();

    let entries = vec![
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
            term: 2,
            payload: None,
        },
        Entry {
            index: 4,
            term: 2,
            payload: None,
        },
        Entry {
            index: 5,
            term: 3,
            payload: None,
        },
        Entry {
            index: 6,
            term: 3,
            payload: None,
        },
        Entry {
            index: 7,
            term: 3,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();

    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(2));
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(4));
    assert_eq!(ctx.raft_log.last_index_for_term(3), Some(7));
    assert_eq!(ctx.raft_log.last_index_for_term(4), None);

    let entries = vec![
        Entry {
            index: 8,
            term: 5,
            payload: None,
        },
        Entry {
            index: 9,
            term: 5,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();
    assert_eq!(ctx.raft_log.last_index_for_term(5), Some(9));

    ctx.raft_log.reset().await.unwrap();
    assert_eq!(ctx.raft_log.last_index_for_term(1), None);
}

#[tokio::test]
async fn test_term_index_functions_with_purged_logs() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_term_index_with_purged",
    );
    ctx.raft_log.reset().await.unwrap();

    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: None,
        },
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
        Entry {
            index: 4,
            term: 3,
            payload: None,
        },
    ];
    ctx.raft_log.insert_batch(entries).await.unwrap();

    ctx.raft_log
        .purge_logs_up_to(d_engine_proto::common::LogId { index: 2, term: 2 })
        .await
        .unwrap();

    assert_eq!(ctx.raft_log.first_index_for_term(1), None);
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(4));
}

/// Sequential multi-term insertion correctness test.
///
/// Production invariant: all writes to BufferedRaftLog go through the single
/// inbound event-loop task; there are no concurrent writers. The previous version
/// of this test spawned multiple tasks writing concurrently, which is not a
/// production scenario and masked the real invariant. This test verifies
/// first/last index tracking across five consecutive term segments.
#[tokio::test]
async fn test_term_index_sequential_multi_term_insertion() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1000,
        },
        "test_term_index_sequential_multi_term",
    );
    ctx.raft_log.reset().await.unwrap();

    // Insert five consecutive term segments sequentially (as the Raft loop would)
    for term in 1u64..=5 {
        for i in 1u64..=10 {
            let index = (term - 1) * 10 + i;
            ctx.raft_log
                .insert_batch(vec![Entry {
                    index,
                    term,
                    payload: None,
                }])
                .await
                .unwrap();
        }
    }

    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(10));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(21));
    assert_eq!(ctx.raft_log.last_index_for_term(3), Some(30));
    assert_eq!(ctx.raft_log.first_index_for_term(5), Some(41));
    assert_eq!(ctx.raft_log.last_index_for_term(5), Some(50));
}

/// Verify that term indexes (term_first_index, term_last_index) and TermSegments
/// are correctly rebuilt from disk after a restart.
///
/// # Why this matters
/// `BufferedRaftLog::new()` loads all entries from disk and rebuilds three
/// in-memory term indexes from scratch. If any of them are incorrectly populated,
/// queries like `first_index_for_term()` silently return `None` instead of the
/// correct boundary — causing the #346 conflict-skip optimization to fall back
/// to one-step-at-a-time backtracking without any error or warning.
///
/// # Scenario
/// - Write 9 entries across 3 terms (term 1: 1-3, term 2: 4-6, term 3: 7-9)
/// - Flush to disk so entries survive restart
/// - Simulate restart via `recover_from_crash()`
/// - Assert all three indexes return correct values on the recovered instance
#[tokio::test]
async fn test_term_indexes_rebuilt_correctly_after_restart() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_term_indexes_rebuilt_after_restart",
    );

    // term 1: indices 1-3, term 2: indices 4-6, term 3: indices 7-9
    let entries: Vec<Entry> = (1u64..=9)
        .map(|i| Entry {
            index: i,
            term: ((i - 1) / 3) + 1, // 1,1,1,2,2,2,3,3,3
            payload: None,
        })
        .collect();
    ctx.raft_log.insert_batch(entries).await.unwrap();
    ctx.raft_log.flush().await.unwrap();

    // Simulate process restart: new BufferedRaftLog loads from same storage.
    let recovered = ctx.recover_from_crash();

    // first_index_for_term — used by #346 conflict-skip optimization
    assert_eq!(recovered.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(recovered.raft_log.first_index_for_term(2), Some(4));
    assert_eq!(recovered.raft_log.first_index_for_term(3), Some(7));
    assert_eq!(recovered.raft_log.first_index_for_term(4), None); // non-existent term

    // last_index_for_term — used by Leader conflict backtracking
    assert_eq!(recovered.raft_log.last_index_for_term(1), Some(3));
    assert_eq!(recovered.raft_log.last_index_for_term(2), Some(6));
    assert_eq!(recovered.raft_log.last_index_for_term(3), Some(9));

    // entry_term via TermSegments — used by AppendEntries log matching check
    assert_eq!(recovered.raft_log.entry_term(1), Some(1));
    assert_eq!(recovered.raft_log.entry_term(5), Some(2));
    assert_eq!(recovered.raft_log.entry_term(9), Some(3));
}

#[tokio::test]
async fn test_term_index_performance_large_dataset() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 5000,
        },
        "test_term_index_performance",
    );
    ctx.raft_log.reset().await.unwrap();

    let mut entries = vec![];
    for i in 1..=1000 {
        entries.push(Entry {
            index: i,
            term: (i / 100) + 1,
            payload: None,
        });
    }
    ctx.raft_log.insert_batch(entries).await.unwrap();

    let start = tokio::time::Instant::now();
    for term in 1..=10 {
        ctx.raft_log.first_index_for_term(term);
        ctx.raft_log.last_index_for_term(term);
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed.as_millis() < 100,
        "Term index lookup should be fast (took {elapsed:?})"
    );
}
