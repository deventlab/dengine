use d_engine_proto::common::{Entry, LogId};

use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, simulate_insert_command};

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

#[tokio::test]
async fn test_remove_middle_range() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_middle_range",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;
    assert_eq!(ctx.raft_log.len(), 100);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 100);

    // Remove middle range
    ctx.raft_log.remove_range(40..=60);

    // Verify removal
    assert_eq!(ctx.raft_log.len(), 79);
    assert_eq!(ctx.raft_log.first_entry_id(), 1); // Min unchanged
    assert_eq!(ctx.raft_log.last_entry_id(), 100); // Max unchanged

    // Verify specific entries
    assert!(ctx.raft_log.entry(39).unwrap().is_some());
    assert!(ctx.raft_log.entry(40).unwrap().is_none());
    assert!(ctx.raft_log.entry(60).unwrap().is_none());
    assert!(ctx.raft_log.entry(61).unwrap().is_some());
}

#[tokio::test]
async fn test_remove_from_start() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_from_start",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove first 50 entries
    ctx.raft_log.remove_range(1..=50);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 50);
    assert_eq!(ctx.raft_log.first_entry_id(), 51); // Min updated
    assert_eq!(ctx.raft_log.last_entry_id(), 100); // Max unchanged

    // Boundary checks
    assert!(ctx.raft_log.entry(50).unwrap().is_none());
    assert!(ctx.raft_log.entry(51).unwrap().is_some());
}

#[tokio::test]
async fn test_remove_to_end() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_to_end",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove from 90 to end
    ctx.raft_log.remove_range(90..=u64::MAX);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 89);
    assert_eq!(ctx.raft_log.first_entry_id(), 1); // Min unchanged
    assert_eq!(ctx.raft_log.last_entry_id(), 89); // Max updated

    // Boundary checks
    assert!(ctx.raft_log.entry(89).unwrap().is_some());
    assert!(ctx.raft_log.entry(90).unwrap().is_none());
    assert!(ctx.raft_log.entry(100).unwrap().is_none());
}

#[tokio::test]
async fn test_remove_empty_range() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_empty_range",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    simulate_insert_command(&ctx.raft_log, vec![1, 2, 3], 1).await;

    // Remove nothing
    ctx.raft_log.remove_range(5..=10);

    assert_eq!(ctx.raft_log.len(), 3);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 3);
}

#[tokio::test]
async fn test_remove_entire_log() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_entire_log",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Insert 100 entries
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Remove entire log
    ctx.raft_log.remove_range(1..=u64::MAX);

    // Verify state
    assert_eq!(ctx.raft_log.len(), 0);
    assert_eq!(ctx.raft_log.first_entry_id(), 0);
    assert_eq!(ctx.raft_log.last_entry_id(), 0);
    assert!(ctx.raft_log.is_empty());
}

#[tokio::test]
async fn test_remove_single_entry() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_single_entry",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    simulate_insert_command(&ctx.raft_log, vec![1, 2, 3], 1).await;

    // Remove middle entry
    ctx.raft_log.remove_range(2..=2);

    assert_eq!(ctx.raft_log.len(), 2);
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 3);
    assert!(ctx.raft_log.entry(2).unwrap().is_none());
}

/// remove_range must update term_first_index and term_last_index when all entries
/// for a term are removed.
///
/// # Why this matters
/// The #346 conflict-skip optimization uses first_index_for_term() to jump to the
/// correct backtrack position. If term_first_index retains a stale entry after
/// remove_range, the optimization silently returns a wrong index — causing the
/// leader to backtrack to an incorrect position with no error or warning.
///
/// # Scenario
/// Log: term1=[1-3], term2=[4-6], term3=[7-9]
/// remove_range(4..=6) removes all term=2 entries
/// Expected: first/last_index_for_term(2) = None; term=1 and term=3 unchanged
#[tokio::test]
async fn test_remove_range_clears_term_indexes_for_removed_entries() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_remove_range_clears_term_indexes",
    );
    ctx.raft_log.reset().await.unwrap();

    // Arrange: term1=[1-3], term2=[4-6], term3=[7-9]
    for i in 1u64..=3 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    for i in 4u64..=6 {
        ctx.raft_log.append_entries(vec![entry(i, 2)]).await.unwrap();
    }
    for i in 7u64..=9 {
        ctx.raft_log.append_entries(vec![entry(i, 3)]).await.unwrap();
    }

    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(4));
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(6));

    // Act: remove all term=2 entries
    ctx.raft_log.remove_range(4..=6);

    // Assert: term=2 indexes cleared
    assert_eq!(
        ctx.raft_log.first_index_for_term(2),
        None,
        "all term=2 entries removed: first_index_for_term(2) must be None"
    );
    assert_eq!(
        ctx.raft_log.last_index_for_term(2),
        None,
        "all term=2 entries removed: last_index_for_term(2) must be None"
    );

    // Assert: term=1 and term=3 indexes unchanged
    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(3));
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(7));
    assert_eq!(ctx.raft_log.last_index_for_term(3), Some(9));
}

/// #442: `purge_logs_up_to()` must remove entries and publish the purge boundary
/// (`last_purged_index`/`last_purged_term`) as a single atomic unit via
/// `purge_prefix()`, not as two separate steps (SkipMap removal, then later,
/// unlocked, the boundary stores). The old two-step version left a window where
/// a concurrent `entry_term(cutoff)` call could observe entries already removed
/// but the boundary not yet recorded, wrongly returning `None`.
///
/// # Why this matters
/// `entry_term()` falls back to `last_purged_index`/`last_purged_term` whenever
/// the queried index has already fallen below `min_index` (i.e. it was purged).
/// A `prev_log_index` lookup for the just-purged boundary must never observe a
/// state where the entry is gone but its term isn't recorded yet — that would
/// make `filter_out_conflicts_and_append()` wrongly reject an AppendEntries that
/// should have succeeded (needless conflict/backtrack on every purge).
///
/// # Expected
/// The moment `purge_prefix(cutoff)` returns, both effects must already be
/// visible together: entries at/below the cutoff are gone, and
/// `entry_term(cutoff.index)` resolves to `cutoff.term` — never a transient
/// `None` in between.
#[tokio::test]
async fn test_purge_prefix_removes_entries_and_records_boundary_together() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_purge_prefix_boundary_atomicity",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Arrange: entries 1..=100, all term 1.
    simulate_insert_command(&ctx.raft_log, (1..=100).collect(), 1).await;

    // Act: purge up to index 50 (term 1) as one call.
    ctx.raft_log.purge_prefix(LogId { index: 50, term: 1 });

    // Assert: removal and boundary publish are both visible, together.
    assert!(
        ctx.raft_log.entry(50).unwrap().is_none(),
        "index 50 must be removed"
    );
    assert!(
        ctx.raft_log.entry(51).unwrap().is_some(),
        "index 51 must survive the purge"
    );
    assert_eq!(
        ctx.raft_log.entry_term(50),
        Some(1),
        "entry_term(50) must resolve to the purged boundary's term the instant \
         purge_prefix() returns; returning None here means an in-flight \
         AppendEntries with prev_log_index=50 would be wrongly rejected as a \
         log conflict"
    );
}

/// #442: `purge_prefix()` must correctly maintain `term_first_index`/`term_last_index`
/// (via the shared `remove_range_locked` helper) when the purge cutoff spans more
/// than one term, not just the single-term case above.
///
/// # Scenario
/// term1=[1-30], term2=[31-60], term3=[61-90]. Purge up to index 45 (term 2) —
/// removes all of term1 and part of term2.
///
/// # Expected
/// - term1's index is fully gone: `first/last_index_for_term(1) == None`.
/// - term2's surviving range starts at 46: `first_index_for_term(2) == Some(46)`.
/// - The purge boundary itself resolves to term 2 (the cutoff's own term), and
///   the surviving term3 range is untouched.
#[tokio::test]
async fn test_purge_prefix_multi_term_cutoff_updates_term_indexes_and_boundary() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_purge_prefix_multi_term_cutoff",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Arrange: term1=[1-30], term2=[31-60], term3=[61-90].
    for i in 1u64..=30 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    for i in 31u64..=60 {
        ctx.raft_log.append_entries(vec![entry(i, 2)]).await.unwrap();
    }
    for i in 61u64..=90 {
        ctx.raft_log.append_entries(vec![entry(i, 3)]).await.unwrap();
    }

    // Act: purge up to index 45 (term 2) — all of term1, half of term2.
    ctx.raft_log.purge_prefix(LogId { index: 45, term: 2 });

    // Assert: term1 fully purged out of the term index.
    assert_eq!(
        ctx.raft_log.first_index_for_term(1),
        None,
        "term1 no longer has any surviving entries"
    );
    assert_eq!(ctx.raft_log.last_index_for_term(1), None);

    // Assert: term2's surviving range starts right after the cutoff.
    assert_eq!(
        ctx.raft_log.first_index_for_term(2),
        Some(46),
        "term2's first surviving index must move to 46 after purging through 45"
    );
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(60));

    // Assert: term3 untouched.
    assert_eq!(ctx.raft_log.first_index_for_term(3), Some(61));
    assert_eq!(ctx.raft_log.last_index_for_term(3), Some(90));

    // Assert: the purge boundary itself resolves correctly.
    assert!(
        ctx.raft_log.entry(45).unwrap().is_none(),
        "index 45 must be removed"
    );
    assert_eq!(
        ctx.raft_log.entry_term(45),
        Some(2),
        "entry_term(45) must resolve to the cutoff's own term (2), not None or term1"
    );
}
