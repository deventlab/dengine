//! Tests for filter_out_conflicts_and_append pipeline overlap handling.
//!
//! These tests cover the three scenarios that were previously untested:
//! - Pipeline overlap with matching terms (no truncation)
//! - Full overlap idempotency (no-op)
//! - Partial overlap with real term conflict (truncate only from conflict point)

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use d_engine_proto::common::Entry;

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{
    BufferedRaftLog, FlushPolicy, MockLogStore, MockMetaStore, MockStorageEngine, MockTypeConfig,
    PersistenceConfig, RaftLog,
};

fn ctx(name: &str) -> BufferedRaftLogTestContext {
    BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 50,
        },
        name,
    )
}

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

/// Pipeline overlap: follower already has index=4 (term=1), leader sends [4,5] (both term=1).
/// Expected: no truncation, only index=5 appended.
#[tokio::test]
async fn test_filter_conflicts_pipeline_overlap_no_truncation() {
    let ctx = ctx("pipeline_overlap_no_truncation");

    // Arrange: follower log [1,2,3,4], all term=1
    for i in 1u64..=4 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 4);

    // Act: leader sends prev_log_index=3, entries=[4,5] (pipeline overlap at 4)
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(3, 1, vec![entry(4, 1), entry(5, 1)])
        .await
        .unwrap();

    // Assert: last log id = 5, index=4 untouched (term still 1), index=5 appended
    assert_eq!(result.unwrap().index, 5);
    assert_eq!(ctx.raft_log.last_entry_id(), 5);
    assert_eq!(
        ctx.raft_log.entry(4).unwrap().unwrap().term,
        1,
        "index=4 must not be truncated"
    );
    assert_eq!(ctx.raft_log.entry(5).unwrap().unwrap().term, 1);
}

/// Full overlap: leader sends [3,4] and follower already has both with matching terms.
/// Expected: no-op, log unchanged.
#[tokio::test]
async fn test_filter_conflicts_full_overlap_is_idempotent() {
    let ctx = ctx("full_overlap_idempotent");

    // Arrange: follower log [1,2,3,4], all term=1
    for i in 1u64..=4 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }

    // Act: leader resends prev_log_index=2, entries=[3,4] — already present, same term
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 1), entry(4, 1)])
        .await
        .unwrap();

    // Assert: log still ends at 4, both entries unchanged
    assert_eq!(result.unwrap().index, 4);
    assert_eq!(ctx.raft_log.last_entry_id(), 4);
    assert_eq!(ctx.raft_log.entry(3).unwrap().unwrap().term, 1);
    assert_eq!(ctx.raft_log.entry(4).unwrap().unwrap().term, 1);
}

/// Partial overlap with real conflict: follower has [1,2,3,4] term=1.
/// Leader sends [3(term=1), 4(term=2), 5(term=2)] — index=3 matches, index=4 conflicts.
/// Expected: truncate only from index=4, append [4(term=2), 5(term=2)].
#[tokio::test]
async fn test_filter_conflicts_partial_overlap_truncates_only_from_conflict() {
    let ctx = ctx("partial_overlap_truncate_from_conflict");

    // Arrange: follower log [1,2,3,4], all term=1
    for i in 1u64..=4 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }

    // Act: leader sends prev_log_index=2, entries=[3(t1), 4(t2), 5(t2)]
    // index=3 term matches → skip; index=4 term conflicts → truncate from 4
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 1), entry(4, 2), entry(5, 2)])
        .await
        .unwrap();

    // Assert
    assert_eq!(result.unwrap().index, 5);
    assert_eq!(ctx.raft_log.last_entry_id(), 5);
    assert_eq!(
        ctx.raft_log.entry(3).unwrap().unwrap().term,
        1,
        "index=3 must not be touched"
    );
    assert_eq!(
        ctx.raft_log.entry(4).unwrap().unwrap().term,
        2,
        "index=4 replaced with term=2"
    );
    assert_eq!(ctx.raft_log.entry(5).unwrap().unwrap().term, 2);
}

/// Large pipeline overlap (50 entries): leader re-sends 50 already-known entries followed by
/// 50 new ones, all same term. Validates correctness of the hot path seen in profiling —
/// no truncation, only the new tail appended.
#[tokio::test]
async fn test_filter_conflicts_large_overlap_no_conflict() {
    let ctx = ctx("large_overlap_no_conflict");

    // Arrange: follower log [1..100], all term=1
    for i in 1u64..=100 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 100);

    // Act: leader sends prev=50, entries=[51..150] all term=1
    // overlap = [51..100] (50 entries), new = [101..150] (50 entries)
    let new_entries: Vec<_> = (51u64..=150).map(|i| entry(i, 1)).collect();
    let result = ctx.raft_log.filter_out_conflicts_and_append(50, 1, new_entries).await.unwrap();

    // Assert: log ends at 150, overlap range [51..100] untouched
    assert_eq!(result.unwrap().index, 150);
    assert_eq!(ctx.raft_log.last_entry_id(), 150);
    for i in 51u64..=100 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            1,
            "overlap entry index={i} must not be truncated"
        );
    }
    assert_eq!(ctx.raft_log.entry(101).unwrap().unwrap().term, 1);
    assert_eq!(ctx.raft_log.entry(150).unwrap().unwrap().term, 1);
}

/// Election recovery: new leader (term=2) sends entries that conflict at the head of
/// the overlap range. Follower has [1..80] term=1 (uncommitted tail from old leader).
/// New leader sends [51..90] all term=2 — conflict starts at index=51 (first overlap entry).
/// Expected: truncate from 51, replace with new leader's entries.
#[tokio::test]
async fn test_filter_conflicts_election_recovery_conflict_at_overlap_head() {
    let ctx = ctx("election_recovery_conflict_head");

    // Arrange: follower has [1..80] all term=1
    for i in 1u64..=80 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 80);

    // Act: new leader (term=2), prev=50 term=1 (still exists in follower log),
    // sends [51..90] all term=2 — immediately conflicts at index=51
    let new_entries: Vec<_> = (51u64..=90).map(|i| entry(i, 2)).collect();
    let result = ctx.raft_log.filter_out_conflicts_and_append(50, 1, new_entries).await.unwrap();

    // Assert: log ends at 90, [51..90] all term=2 (replaced)
    assert_eq!(result.unwrap().index, 90);
    assert_eq!(ctx.raft_log.last_entry_id(), 90);
    for i in 51u64..=90 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            2,
            "index={i} must be term=2 after election recovery"
        );
    }
    // Entries before conflict point are untouched
    assert_eq!(ctx.raft_log.entry(50).unwrap().unwrap().term, 1);
}

/// Election recovery: conflict appears in the middle of the overlap range.
/// Follower has [1..80]: indices [1..60] term=1 (stable), [61..80] term=1 (uncommitted).
/// New leader sends prev=50 term=1, entries=[51..90]:
///   [51..60] term=1 (match), [61..90] term=2 (conflict at index=61).
/// Expected: truncate from 61, append new leader's entries from 61 onward.
#[tokio::test]
async fn test_filter_conflicts_election_recovery_conflict_in_overlap_middle() {
    let ctx = ctx("election_recovery_conflict_middle");

    // Arrange: follower has [1..80] all term=1
    for i in 1u64..=80 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 80);

    // Act: new leader sends prev=50 term=1,
    // entries=[51..60 term=1, 61..90 term=2] — conflict at index=61
    let mut new_entries: Vec<_> = (51u64..=60).map(|i| entry(i, 1)).collect();
    new_entries.extend((61u64..=90).map(|i| entry(i, 2)));
    let result = ctx.raft_log.filter_out_conflicts_and_append(50, 1, new_entries).await.unwrap();

    // Assert: log ends at 90
    assert_eq!(result.unwrap().index, 90);
    assert_eq!(ctx.raft_log.last_entry_id(), 90);
    // [51..60] untouched, still term=1
    for i in 51u64..=60 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            1,
            "index={i} matched and must remain term=1"
        );
    }
    // [61..90] replaced with term=2
    for i in 61u64..=90 {
        assert_eq!(
            ctx.raft_log.entry(i).unwrap().unwrap().term,
            2,
            "index={i} must be term=2 after truncation"
        );
    }
}

/// Pure append: zero overlap. First entry in the batch is exactly last_current_index + 1.
/// This is the baseline case — no overlap scan needed at all.
/// Expected: all entries appended, nothing truncated.
#[tokio::test]
async fn test_filter_conflicts_pure_append_no_overlap() {
    let ctx = ctx("pure_append_no_overlap");

    // Arrange: follower has [1..50] all term=1
    for i in 1u64..=50 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 50);

    // Act: leader sends prev=50 term=1, entries=[51..60] — zero overlap
    let new_entries: Vec<_> = (51u64..=60).map(|i| entry(i, 1)).collect();
    let result = ctx.raft_log.filter_out_conflicts_and_append(50, 1, new_entries).await.unwrap();

    // Assert: all 10 entries appended cleanly
    assert_eq!(result.unwrap().index, 60);
    assert_eq!(ctx.raft_log.last_entry_id(), 60);
    for i in 51u64..=60 {
        assert_eq!(ctx.raft_log.entry(i).unwrap().unwrap().term, 1);
    }
}

/// Bench pattern: leader sends [N, N+1] but follower already has N (same term).
/// RPC carries 1-entry overlap. Expected: only N+1 appended, N untouched, no truncation.
///
/// This is the exact pattern observed in bench output:
///   prev_log_index=N-1, last_current_index=N, new_entries=[N, N+1], first_term_conflict=Some(N+1)
#[tokio::test]
async fn test_filter_conflicts_bench_pattern_no_truncation() {
    let ctx = ctx("bench_pattern_no_truncation");

    // Arrange: follower log [1..5], all term=1
    for i in 1u64..=5 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    assert_eq!(ctx.raft_log.last_entry_id(), 5);

    // Act: leader sends prev=4, entries=[5(t1), 6(t1)]
    // follower has 5 already (term matches) — overlap at index=5
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(4, 1, vec![entry(5, 1), entry(6, 1)])
        .await
        .unwrap();

    // Assert: log grows to 6, index=5 NOT truncated
    assert_eq!(result.unwrap().index, 6);
    assert_eq!(ctx.raft_log.last_entry_id(), 6);
    assert_eq!(
        ctx.raft_log.entry(5).unwrap().unwrap().term,
        1,
        "index=5 must not be truncated"
    );
    assert_eq!(ctx.raft_log.entry(6).unwrap().unwrap().term, 1);
}

/// ReplaceRange durability: after conflict-triggered truncate + re-append, a crash+recovery
/// must restore the post-conflict entries, not the original pre-conflict ones.
///
/// # Why this matters
/// A real term conflict routes through IOTask::ReplaceRange in the IO thread
/// (truncate old entries from disk, persist new ones). If this path is not correctly
/// flushed, crash recovery would replay the original WAL and restore the stale entries —
/// violating the Raft log matching property (§5.3).
///
/// # Scenario
/// Follower: [1,2,3,4] all term=1 (flushed to disk)
/// Leader sends: prev=2, entries=[3(t1), 4(t2), 5(t2)] — conflict at index=4
/// After recovery: [1,2,3(t1), 4(t2), 5(t2)] — index=4 must be term=2
#[tokio::test]
async fn test_replace_range_conflict_persists_correctly_after_crash() {
    use tokio::time::{Duration, sleep};

    let ctx = ctx("replace_range_crash_recovery");

    // Arrange: follower log [1,2,3,4] all term=1, flushed to disk
    for i in 1u64..=4 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    assert_eq!(ctx.raft_log.entry(4).unwrap().unwrap().term, 1);

    // Act: leader sends conflict — index=4 was term=1, leader has term=2
    // filter_out_conflicts_and_append detects conflict at index=4,
    // sends IOTask::ReplaceRange(truncate_from=4, new_entries=[4(t2), 5(t2)])
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 1), entry(4, 2), entry(5, 2)])
        .await
        .unwrap();
    assert_eq!(result.unwrap().index, 5);

    // Flush ensures IOTask::ReplaceRange is fully processed and persisted by IO thread
    ctx.raft_log.flush().await.unwrap();

    // Verify memory state before crash
    assert_eq!(
        ctx.raft_log.entry(4).unwrap().unwrap().term,
        2,
        "pre-crash: index=4 must be term=2"
    );
    assert_eq!(ctx.raft_log.last_entry_id(), 5);

    // Simulate crash and recovery
    let recovered = ctx.recover_from_crash();
    sleep(Duration::from_millis(50)).await;

    // Assert: crash recovery restores post-conflict entries, not original term=1 at index=4
    assert_eq!(
        recovered.raft_log.entry(4).unwrap().unwrap().term,
        2,
        "crash recovery: index=4 must be term=2 (post-conflict), not term=1 (pre-conflict)"
    );
    assert_eq!(recovered.raft_log.entry(5).unwrap().unwrap().term, 2);
    assert_eq!(
        recovered.raft_log.entry(3).unwrap().unwrap().term,
        1,
        "index=3 was not in conflict range and must remain term=1"
    );
    assert_eq!(recovered.raft_log.last_entry_id(), 5);
}

/// ReplaceRange with fewer new entries than the truncated range must remove trailing old entries.
///
/// # Why this matters
/// When a leader sends a shorter suffix than the follower had (e.g. follower had [1-5]
/// but new entries only cover [3,4] after truncation), the follower must not retain
/// stale entry 5. A no-op truncate would leave entry 5 in storage, violating
/// the log-matching property (§5.3).
///
/// # Scenario
/// Follower: [1,2,3,4,5] all term=1 (flushed)
/// Leader sends: prev_index=2, prev_term=1, entries=[3(t2), 4(t2)] — conflict at index=3, no entry 5
/// After: log must be [1,2,3(t2),4(t2)] — entry 5 must not exist
#[tokio::test]
async fn test_replace_range_removes_entries_beyond_new_tail() {
    use tokio::time::{Duration, sleep};

    let ctx = ctx("replace_range_removes_trailing");

    // Arrange: follower log [1..5] all term=1, flushed to disk
    for i in 1u64..=5 {
        ctx.raft_log.append_entries(vec![entry(i, 1)]).await.unwrap();
    }
    ctx.raft_log.flush().await.unwrap();
    assert_eq!(ctx.raft_log.last_entry_id(), 5);

    // Act: leader conflict at index=3 (term mismatch); new suffix is only [3(t2), 4(t2)]
    // IOTask::ReplaceRange(truncate_from=3, new_entries=[3(t2), 4(t2)]) — entry 5 must vanish
    ctx.raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 2), entry(4, 2)])
        .await
        .unwrap();
    ctx.raft_log.flush().await.unwrap();

    // Entry 5 must be gone; last_entry_id must reflect the new tail
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        4,
        "last_entry_id must be 4 after truncation"
    );
    assert!(
        ctx.raft_log.entry(5).unwrap().is_none(),
        "entry 5 must be removed by truncate — stale tail must not survive ReplaceRange"
    );
    assert_eq!(ctx.raft_log.entry(3).unwrap().unwrap().term, 2);
    assert_eq!(ctx.raft_log.entry(4).unwrap().unwrap().term, 2);

    // Verify crash recovery also restores correct state
    let recovered = ctx.recover_from_crash();
    sleep(Duration::from_millis(50)).await;

    assert_eq!(recovered.raft_log.last_entry_id(), 4);
    assert!(
        recovered.raft_log.entry(5).unwrap().is_none(),
        "after crash recovery: entry 5 must still be absent"
    );
    assert_eq!(recovered.raft_log.entry(3).unwrap().unwrap().term, 2);
}

/// IOTask::ReplaceRange must call LogStore::replace_range() as a single operation,
/// not truncate() + persist_entries() separately.
///
/// # Why
/// Delegating to replace_range() lets storage backends provide atomic crash semantics
/// (e.g. single RocksDB WriteBatch). Calling truncate + persist_entries separately
/// breaks this contract regardless of what the backend implements.
///
/// # Red/Green
/// This test FAILS before implementation (truncate IS called separately).
/// It PASSES after IOTask::ReplaceRange is updated to call replace_range().
#[tokio::test]
async fn test_io_task_replace_range_delegates_to_replace_range_not_truncate() {
    let replace_range_count = Arc::new(AtomicU64::new(0));
    let truncate_count = Arc::new(AtomicU64::new(0));

    let mut log_store = MockLogStore::new();

    // replace_range() must be called exactly once for one conflict resolution
    let rr_counter = replace_range_count.clone();
    log_store.expect_replace_range().returning(move |_from, new_entries| {
        rr_counter.fetch_add(1, Ordering::Relaxed);
        Ok(new_entries.last().map(|e| e.index).unwrap_or(0))
    });

    // truncate() must NOT be called — IOTask::ReplaceRange owns the full operation
    let tr_counter = truncate_count.clone();
    log_store.expect_truncate().returning(move |_| {
        tr_counter.fetch_add(1, Ordering::Relaxed);
        Ok(())
    });

    // Standard expectations for other operations
    log_store.expect_last_index().returning(|| 0);
    log_store.expect_persist_entries().returning(|_| Ok(()));
    log_store.expect_entry().returning(|_| Ok(None));
    log_store.expect_get_entries().returning(|_| Ok(vec![]));
    log_store.expect_purge().returning(|_| Ok(()));
    log_store.expect_load_purge_boundary().returning(|| Ok(None));
    log_store.expect_reset().returning(|| Ok(()));
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
    std::thread::sleep(std::time::Duration::from_millis(10));

    // Populate in-memory SkipMap with [1,2,3,4] term=1 so conflict detection fires
    raft_log
        .append_entries(vec![entry(1, 1), entry(2, 1), entry(3, 1), entry(4, 1)])
        .await
        .unwrap();

    // Trigger IOTask::ReplaceRange: conflict at index=3 (term mismatch 1 vs 2)
    raft_log
        .filter_out_conflicts_and_append(2, 1, vec![entry(3, 2), entry(4, 2)])
        .await
        .unwrap();
    raft_log.flush().await.unwrap();

    assert_eq!(
        replace_range_count.load(Ordering::Relaxed),
        1,
        "replace_range() must be called exactly once"
    );
    assert_eq!(
        truncate_count.load(Ordering::Relaxed),
        0,
        "truncate() must not be called separately — replace_range() owns the full operation"
    );
}
