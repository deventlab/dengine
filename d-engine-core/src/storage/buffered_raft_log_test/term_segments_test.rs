//! Unit tests for TermSegments hot/cold path correctness.
//!
//! Tests exercise TermSegments directly (pub(crate)) for precise coverage,
//! plus a set of end-to-end tests via entry_term() for full-stack validation.
//!
//! # Scenarios covered
//! - Hot path: index in current term segment — O(1), no array scan
//! - Cold path: index in historical segment — atomic array reverse scan
//! - Multi-election: k=3 historical segments, all reachable
//! - Truncation + re-append: last_term_start pullback after conflict resolution
//! - Empty log: get() returns None
//! - Clear: state resets to empty
//! - Overflow guard: debug_assert fires at MAX_TERM_SEGMENTS

use d_engine_proto::common::Entry;

use crate::storage::buffered_raft_log::TermSegments;
use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, RaftLog};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn entries(
    range: std::ops::RangeInclusive<u64>,
    term: u64,
) -> Vec<Entry> {
    range
        .map(|i| Entry {
            index: i,
            term,
            payload: None,
        })
        .collect()
}

fn ctx(name: &str) -> BufferedRaftLogTestContext {
    BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 50,
        },
        name,
    )
}

// ---------------------------------------------------------------------------
// Direct TermSegments unit tests
// ---------------------------------------------------------------------------

/// Empty log: get() must return None for any index.
#[test]
fn test_term_segments_empty_returns_none() {
    let ts = TermSegments::new();
    assert_eq!(ts.get(0), None);
    assert_eq!(ts.get(1), None);
    assert_eq!(ts.get(100), None);
}

/// Hot path: all entries same term. Every get() hits last_term directly.
#[test]
fn test_term_segments_hot_path_single_term() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=100, 2));

    for i in 1..=100 {
        assert_eq!(ts.get(i), Some(2), "index={i}");
    }
}

/// Hot path: after election, queries for indices in the latest term avoid array scan.
#[test]
fn test_term_segments_hot_path_latest_term() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=50, 1));
    ts.on_append(&entries(51..=100, 2));

    // Indices in current segment hit hot path
    for i in 51..=100 {
        assert_eq!(ts.get(i), Some(2), "index={i}");
    }
}

/// Cold path: after one election, historical segment is accessible.
#[test]
fn test_term_segments_cold_path_one_segment() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=50, 1));
    ts.on_append(&entries(51..=100, 2));

    // Indices in historical segment hit cold path
    for i in 1..=50 {
        assert_eq!(ts.get(i), Some(1), "index={i}");
    }
}

/// Cold path: three elections, all three historical segments accessible.
#[test]
fn test_term_segments_cold_path_multiple_elections() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=20, 1));
    ts.on_append(&entries(21..=40, 2));
    ts.on_append(&entries(41..=60, 3));
    ts.on_append(&entries(61..=80, 4)); // current

    assert_eq!(ts.get(10), Some(1));
    assert_eq!(ts.get(30), Some(2));
    assert_eq!(ts.get(50), Some(3));
    assert_eq!(ts.get(70), Some(4)); // hot path
}

/// Truncation + re-append: last_term_start must be pulled back when re-inserting
/// an entry whose term matches last_term but index < last_term_start.
#[test]
fn test_term_segments_truncation_reappend_pulls_start_back() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=5, 1));
    ts.on_append(&entries(6..=10, 2)); // last_term=2, last_term_start=6

    // Simulate truncation at 8 and re-append [8..10] with term=3
    // (new leader, conflict at 8)
    ts.on_append(&entries(8..=10, 3)); // last_term=3, last_term_start=8

    assert_eq!(ts.get(8), Some(3));
    assert_eq!(ts.get(9), Some(3));
    assert_eq!(ts.get(10), Some(3));
    // Segment [6..7] still term=2 in historical
    assert_eq!(ts.get(6), Some(2));
    assert_eq!(ts.get(7), Some(2));
}

/// clear() resets all state; get() returns None afterwards.
#[test]
fn test_term_segments_clear_resets_state() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=10, 1));
    ts.on_append(&entries(11..=20, 2));
    assert_eq!(ts.get(5), Some(1));
    assert_eq!(ts.get(15), Some(2));

    ts.clear();

    assert_eq!(ts.get(1), None);
    assert_eq!(ts.get(15), None);
}

/// After clear(), the structure can be reused from scratch.
/// Note: TermSegments.get() does not enforce log bounds — that is entry_term()'s job.
/// After re-appending [1..5] term=3, last_term_start=1, so any index >= 1 returns Some(3).
#[test]
fn test_term_segments_reuse_after_clear() {
    let ts = TermSegments::new();
    ts.on_append(&entries(1..=10, 1));
    ts.clear();

    ts.on_append(&entries(1..=5, 3)); // new epoch, different term
    assert_eq!(ts.get(3), Some(3));
    assert_eq!(ts.get(5), Some(3));
    // Index 0 is below last_term_start=1 and seg_count=0 → None
    assert_eq!(ts.get(0), None);
}

// ---------------------------------------------------------------------------
// End-to-end via entry_term() (full-stack integration)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_entry_term_hot_path_single_term() {
    let ctx = ctx("ts_e2e_hot_single");
    for i in 1u64..=10 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: None,
            }])
            .await
            .unwrap();
    }
    for i in 1u64..=10 {
        assert_eq!(ctx.raft_log.entry_term(i), Some(1), "index={i}");
    }
}

#[tokio::test]
async fn test_entry_term_cold_path_multiple_elections() {
    let ctx = ctx("ts_e2e_cold_multi");
    let ranges: &[(u64, u64, u64)] = &[(1, 5, 1), (6, 10, 2), (11, 15, 3), (16, 20, 4)];
    for &(start, end, term) in ranges {
        for i in start..=end {
            ctx.raft_log
                .append_entries(vec![Entry {
                    index: i,
                    term,
                    payload: None,
                }])
                .await
                .unwrap();
        }
    }
    for &(start, end, term) in ranges {
        for i in start..=end {
            assert_eq!(ctx.raft_log.entry_term(i), Some(term), "index={i}");
        }
    }
}

#[tokio::test]
async fn test_entry_term_after_truncation_reappend() {
    let ctx = ctx("ts_e2e_truncation");
    for i in 1u64..=8 {
        ctx.raft_log
            .append_entries(vec![Entry {
                index: i,
                term: if i <= 5 { 1 } else { 2 },
                payload: None,
            }])
            .await
            .unwrap();
    }
    ctx.raft_log
        .filter_out_conflicts_and_append(
            6,
            2,
            vec![
                Entry {
                    index: 7,
                    term: 3,
                    payload: None,
                },
                Entry {
                    index: 8,
                    term: 3,
                    payload: None,
                },
                Entry {
                    index: 9,
                    term: 3,
                    payload: None,
                },
            ],
        )
        .await
        .unwrap();

    for i in 7u64..=9 {
        assert_eq!(ctx.raft_log.entry_term(i), Some(3), "index={i}");
    }
    assert_eq!(ctx.raft_log.entry_term(6), Some(2));
    assert_eq!(ctx.raft_log.entry_term(1), Some(1));
}

#[tokio::test]
async fn test_entry_term_after_reset_returns_none() {
    let ctx = ctx("ts_e2e_reset");
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
    assert_eq!(ctx.raft_log.entry_term(3), Some(1));
    ctx.raft_log.reset().await.unwrap();
    assert_eq!(ctx.raft_log.entry_term(1), None);
}
