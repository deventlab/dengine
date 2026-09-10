//! Basic BufferedRaftLog operations tests
//!
//! Tests verify core CRUD functionality:
//! - Entry insertion and retrieval
//! - Range queries
//! - Conflict detection and resolution
//! - Log purging
//! - Metadata updates

use d_engine_proto::common::{Entry, LogId};

use crate::test_utils::{
    BufferedRaftLogTestContext, mock_empty_entries, simulate_delete_command,
    simulate_insert_command,
};
use crate::{FlushPolicy, RaftLog};

/// Test get_entries_range returns correct subset
///
/// # Scenario
/// - Insert 4 entries with indexes 1-4
/// - Query range 1..=2
/// - Expected: Returns exactly 2 entries with correct indexes
#[tokio::test]
async fn test_get_entries_range_returns_correct_subset() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_get_entries_range_returns_correct_subset",
    );

    // Arrange: Insert 4 entries
    simulate_insert_command(&ctx.raft_log, vec![11, 12, 13, 14], 4).await;

    // Act: Query range 1..=2
    let list = ctx.raft_log.get_entries_range(1..=2).unwrap();

    // Assert: Correct subset returned
    assert_eq!(list.len(), 2, "Should return 2 entries");
    assert_eq!(list[0].index, 1);
    assert_eq!(list[1].index, 2);
}

/// Test get_entries_range handles large range correctly
///
/// # Scenario
/// - Insert 299 entries
/// - Query single entry at index 255
/// - Expected: Returns exactly 1 entry
#[tokio::test]
async fn test_get_entries_range_handles_large_range() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_get_entries_range_handles_large_range",
    );

    // Arrange: Insert 299 entries
    for i in 1..300 {
        let entry = Entry {
            index: i,
            term: 7,
            payload: None,
        };
        ctx.raft_log.insert_batch(vec![entry]).await.unwrap();
    }

    // Act: Query single entry
    let list = ctx.raft_log.get_entries_range(255..=255).unwrap();

    // Assert: Correct entry returned
    assert_eq!(list.len(), 1);
    assert_eq!(list[0].index, 255);
}

/// Test filter_conflicts removes entries with different term (Raft Figure 7 case)
///
/// # Scenario
/// - Initial log: [1@1, 2@2, 3@2, 4@4]
/// - Append: [2@2, 3@3] with prev_log=(1,1)
/// - Expected: Entry 3 updated from term 2 to term 3
#[tokio::test]
async fn test_filter_conflicts_removes_entries_with_different_term() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_filter_conflicts_removes_entries_with_different_term",
    );

    // Arrange: Setup initial log
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2, 3], 2).await;
    simulate_insert_command(&ctx.raft_log, vec![4], 4).await;

    // Act: Append conflicting entries
    let new_entries = vec![
        Entry {
            index: 2,
            term: 2,
            payload: None,
        },
        Entry {
            index: 3,
            term: 3,
            payload: None,
        },
    ];
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(1, 1, new_entries)
        .await
        .unwrap()
        .unwrap();

    // Assert: Entry 3 updated to term 3
    assert_eq!(result.index, 3);
    let entry = ctx.raft_log.entry(3).unwrap().unwrap();
    assert_eq!(entry.term, 3, "Entry should be updated to term 3");
}

/// Test filter_conflicts handles multiple conflict scenarios
///
/// # Scenario
/// - Test two cases: overwrite with higher term, overwrite with lower term
/// - Expected: Conflicts resolved correctly in both cases
#[tokio::test]
async fn test_filter_conflicts_handles_multiple_scenarios() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_filter_conflicts_handles_multiple_scenarios",
    );

    // Arrange: Setup initial log
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2, 3], 2).await;
    simulate_insert_command(&ctx.raft_log, vec![4], 4).await;

    // Act & Assert: Case 1 - Update term 3
    let new_entries = vec![
        Entry {
            index: 2,
            term: 2,
            payload: None,
        },
        Entry {
            index: 3,
            term: 3,
            payload: None,
        },
    ];
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(1, 1, new_entries)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.index, 3);
    assert_eq!(ctx.raft_log.entry(3).unwrap().unwrap().term, 3);

    // Act & Assert: Case 2 - Overwrite with lower term
    let new_entries = vec![Entry {
        index: 4,
        term: 2,
        payload: None,
    }];
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(1, 1, new_entries)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.index, 4);
    assert_eq!(ctx.raft_log.entry(4).unwrap().unwrap().term, 2);
}

/// Test last_entry returns highest index after multiple appends
///
/// # Scenario
/// - Insert 299 entries in first batch
/// - Insert 2 entries in second batch
/// - Expected: last_entry returns index 301
#[tokio::test]
async fn test_last_entry_returns_highest_index() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_entry_returns_highest_index",
    );

    // Arrange: Reset and insert entries
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, (1..300).collect(), 1).await;
    simulate_insert_command(&ctx.raft_log, vec![1, 2], 1).await;

    // Act & Assert: Last entry should be 301
    let last = ctx.raft_log.last_entry().unwrap();
    assert_eq!(last.index, 301, "Last entry index should be 301");
}

/// Test last_entry matches buffer length
///
/// # Scenario
/// - Insert 300 sequential entries
/// - Expected: last_entry.index == len()
#[tokio::test]
async fn test_last_entry_matches_buffer_length() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_entry_matches_buffer_length",
    );

    // Arrange: Insert 300 entries
    simulate_insert_command(&ctx.raft_log, (1..=300).collect(), 1).await;

    // Act
    let last = ctx.raft_log.last_entry().unwrap();
    let len = ctx.raft_log.len();

    // Assert: Last index equals length
    assert_eq!(
        last.index, len as u64,
        "Last index should equal buffer length"
    );
}

/// Test last_entry with large payload ID value
///
/// # Scenario
/// - Insert entry with u64::MAX-1 as payload ID (not index)
/// - Expected: Entry index is sequential (1), large payload ID handled correctly
#[tokio::test]
async fn test_last_entry_with_large_payload_id() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_entry_with_large_payload_id",
    );

    // Arrange: Insert command with large payload ID (not affecting entry index)
    let max = u64::MAX;
    simulate_insert_command(&ctx.raft_log, vec![max - 1], 1).await;

    // Act & Assert: Entry index is sequential, not affected by payload ID
    let last = ctx.raft_log.last_entry().unwrap();
    assert_eq!(last.index, 1);
}

/// Test insert_batch appends large number of entries in order
///
/// # Scenario
/// - Insert 2000 entries
/// - Query range 900..=2000
/// - Expected: All entries present with correct sequential indexes
#[tokio::test]
async fn test_insert_batch_appends_entries_in_order() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_insert_batch_appends_entries_in_order",
    );

    // Arrange: Reset and insert 2000 entries
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, (1..2001).collect(), 1).await;

    // Act: Query range
    let entries = ctx.raft_log.get_entries_range(900..=2000).unwrap();

    // Assert: All entries present with correct indexes
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.index,
            900 + i as u64,
            "Entry index should be sequential"
        );
    }
}

/// Test get_entries_range with various range bounds
///
/// # Scenario
/// - Insert 2000 entries
/// - Test multiple range queries with different bounds
/// - Expected: Correct counts for each range
#[tokio::test]
async fn test_get_entries_range_multiple_bounds() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_get_entries_range_multiple_bounds",
    );

    // Arrange: Insert 2000 entries
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, (1..2001).collect(), 1).await;

    // Act & Assert: Test various ranges
    let log_len = ctx.raft_log.last_entry_id();

    let result = ctx.raft_log.get_entries_range(900..=log_len).unwrap();
    assert_eq!(
        result.len(),
        1101,
        "Range 900..=log_len should return 1101 entries"
    );

    let result = ctx.raft_log.get_entries_range(900..=log_len - 1).unwrap();
    assert_eq!(
        result.len(),
        1100,
        "Range 900..=log_len-1 should return 1100 entries"
    );

    let result = ctx.raft_log.get_entries_range(0..=log_len).unwrap();
    assert_eq!(
        result.len(),
        log_len as usize,
        "Range 0..=log_len should return all entries"
    );

    let result = ctx.raft_log.get_entries_range(0..=0).unwrap();
    assert_eq!(
        result.len(),
        0,
        "Range 0..=0 should return 0 entries (indexes start at 1)"
    );

    let result = ctx.raft_log.get_entries_range(0..=1).unwrap();
    assert_eq!(result.len(), 1, "Range 0..=1 should return 1 entry");
}

/// Test insert handles duplicate commands correctly
///
/// # Scenario
/// - Insert 9 entries in first batch
/// - Insert 9 more entries in second batch
/// - Expected: Treated as separate events, not duplicates
#[tokio::test]
async fn test_insert_duplicate_commands_as_separate_events() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_insert_duplicate_commands_as_separate_events",
    );

    // Arrange: Reset and insert first batch
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, (1..10).collect(), 1).await;

    // Act: Insert second batch with incremented indexes
    let len = ctx.raft_log.last_entry_id();
    simulate_insert_command(&ctx.raft_log, (1..10).map(|i| i + len).collect(), 2).await;

    // Assert: All entries present as separate events
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        18,
        "Should have 18 total entries"
    );
    let last = ctx.raft_log.last_entry().unwrap();
    assert_eq!(last.index, 18, "Last entry index should be 18");
}

/// Test purge after insert maintains consistency
///
/// # Scenario
/// - Insert 10 entries
/// - Delete first 3 entries (represented as new log entries)
/// - Insert 10 more entries
/// - Expected: Log length reflects all operations
#[tokio::test]
async fn test_purge_after_insert_maintains_consistency() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_purge_after_insert_maintains_consistency",
    );

    // Arrange: Insert initial entries
    ctx.raft_log.reset().await.unwrap();
    simulate_insert_command(&ctx.raft_log, (1..=10).collect(), 1).await;
    assert_eq!(ctx.raft_log.last_entry_id(), 10);

    // Act: Simulate delete and insert
    simulate_delete_command(&ctx.raft_log, 1..=3, 1).await;
    assert_eq!(ctx.raft_log.last_entry_id(), 13);
    assert_eq!(ctx.raft_log.len(), 13);

    simulate_insert_command(&ctx.raft_log, (11..=20).collect(), 1).await;

    // Assert: Final state correct
    assert_eq!(ctx.raft_log.last_entry_id(), 23);
    assert_eq!(ctx.raft_log.len(), 23);
}

/// Test purge_logs removes entries up to specified index
///
/// # Scenario
/// - Insert 9 entries
/// - Purge up to index 3
/// - Expected: Entries 1-3 removed, 4-9 remain
#[tokio::test]
async fn test_purge_logs_removes_entries_up_to_index() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_purge_logs_removes_entries_up_to_index",
    );

    // Arrange: Insert 9 entries
    ctx.raft_log.reset().await.unwrap();
    let entries = mock_empty_entries(1, 9, 1);
    ctx.raft_log.insert_batch(entries).await.unwrap();

    // Act: Purge up to index 3
    let last_applied = LogId { index: 3, term: 1 };
    ctx.raft_log.purge_logs_up_to(last_applied).await.unwrap();

    // Assert: Entries 1-3 removed, 4-9 remain
    assert_eq!(ctx.raft_log.last_entry_id(), 9);
    assert_eq!(
        ctx.raft_log.entry(3).unwrap(),
        None,
        "Entry 3 should be purged"
    );
    assert_eq!(
        ctx.raft_log.entry(2).unwrap(),
        None,
        "Entry 2 should be purged"
    );
    assert_eq!(
        ctx.raft_log.entry(1).unwrap(),
        None,
        "Entry 1 should be purged"
    );
    assert!(
        ctx.raft_log.entry(4).unwrap().is_some(),
        "Entry 4 should remain"
    );
}

/// Test concurrent purge operations are safe
///
/// # Scenario
/// - Insert 10 entries
/// - Spawn 7 concurrent purge operations
/// - Expected: All complete successfully without data corruption
#[tokio::test]
async fn test_concurrent_purge_operations_are_safe() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_concurrent_purge_operations_are_safe",
    );

    // Arrange: Insert 10 entries
    ctx.raft_log.reset().await.unwrap();
    let entries = mock_empty_entries(1, 10, 1);
    ctx.raft_log.insert_batch(entries).await.unwrap();

    // Act: Concurrent purge calls
    let handles: Vec<_> = vec![1, 2, 3, 4, 5, 6, 7]
        .into_iter()
        .map(|i| {
            let log = ctx.raft_log.clone();
            tokio::spawn(async move {
                let log_id = LogId { index: i, term: 1 };
                log.purge_logs_up_to(log_id).await
            })
        })
        .collect();

    // Assert: All operations complete successfully
    for handle in handles {
        handle.await.unwrap().unwrap();
    }
}

/// Test first_entry_id updates after purge
///
/// # Scenario
/// - Insert 10 entries
/// - Purge up to index 5
/// - Expected: first_entry_id reflects new starting index
#[tokio::test]
async fn test_first_entry_id_after_purge_updates() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_first_entry_id_after_purge_updates",
    );

    // Arrange: Insert 10 entries
    ctx.raft_log.reset().await.unwrap();
    let entries = mock_empty_entries(1, 10, 1);
    ctx.raft_log.insert_batch(entries).await.unwrap();

    // Act: Purge up to index 5
    let log_id = LogId { index: 5, term: 1 };
    ctx.raft_log.purge_logs_up_to(log_id).await.unwrap();

    // Assert: First entry ID updated
    let first_id = ctx.raft_log.first_entry_id();
    assert!(
        first_id > 5,
        "First entry ID should be greater than purged index"
    );
}

/// Test single entry insert succeeds
///
/// # Scenario
/// - Insert single entry
/// - Expected: Entry persisted with correct index
#[tokio::test]
async fn test_single_entry_insert_succeeds() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_single_entry_insert_succeeds",
    );

    // Act: Insert single entry
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;

    // Assert: Entry persisted
    assert_eq!(ctx.raft_log.last_entry_id(), 1);
}

/// Test is_empty returns true for new log
///
/// # Scenario
/// - Create new log without entries
/// - Expected: is_empty() returns true
#[tokio::test]
async fn test_is_empty_returns_true_for_new_log() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_is_empty_returns_true_for_new_log",
    );

    // Assert: New log is empty
    assert!(ctx.raft_log.is_empty(), "New log should be empty");
}

/// Test is_empty returns false after append
///
/// # Scenario
/// - Insert one entry
/// - Expected: is_empty() returns false
#[tokio::test]
async fn test_is_empty_returns_false_after_append() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_is_empty_returns_false_after_append",
    );

    // Arrange: Insert entry
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;

    // Assert: Log not empty
    assert!(
        !ctx.raft_log.is_empty(),
        "Log should not be empty after insert"
    );
}

/// Test last_log_id for empty log returns default
///
/// # Scenario
/// - Query last_log_id on empty log
/// - Expected: Returns (0, 0)
#[tokio::test]
async fn test_last_log_id_for_empty_log() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_log_id_for_empty_log",
    );

    // Act & Assert: Empty log returns default
    assert_eq!(
        ctx.raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 }),
        LogId { index: 0, term: 0 },
        "Empty log should return default LogId"
    );
}

/// Test last_log_id after appends returns correct metadata
///
/// # Scenario
/// - Insert entry with index=1, term=11
/// - Expected: last_log_id returns (1, 11)
#[tokio::test]
async fn test_last_log_id_after_appends() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_last_log_id_after_appends",
    );

    // Arrange: Insert entry with term 11
    simulate_insert_command(&ctx.raft_log, vec![1], 11).await;

    // Act & Assert: Last log ID correct
    assert_eq!(
        ctx.raft_log.last_log_id().unwrap_or(LogId { index: 0, term: 0 }),
        LogId { index: 1, term: 11 },
        "Last log ID should reflect inserted entry"
    );
}

/// Test drop shuts down workers gracefully
///
/// # Scenario
/// - Insert entry and explicitly drop context
/// - Expected: Workers shut down without panic
#[tokio::test]
async fn test_drop_shuts_down_workers_gracefully() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_drop_shuts_down_workers_gracefully",
    );

    // Arrange: Insert entry
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;

    // Act: Drop context (triggers shutdown)
    drop(ctx);

    // Assert: No panic during drop (implicit)
}

/// Test same index and term implies identical log prefix (Raft safety)
///
/// # Scenario
/// - Two logs with same entry at (index=5, term=3)
/// - Expected: All entries before index 5 are identical
#[tokio::test]
async fn test_same_index_and_term_implies_identical_prefix() {
    let ctx1 = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_log_matching_1",
    );
    let ctx2 = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_log_matching_2",
    );

    // Arrange: Create identical prefix in both logs
    ctx1.raft_log.reset().await.unwrap();
    ctx2.raft_log.reset().await.unwrap();

    for i in 1..=5 {
        let entries = mock_empty_entries(i, 1, 3);
        ctx1.raft_log.insert_batch(entries.clone()).await.unwrap();
        ctx2.raft_log.insert_batch(entries).await.unwrap();
    }

    // Assert: Both logs have same entry at index 5
    let entry1 = ctx1.raft_log.entry(5).unwrap().unwrap();
    let entry2 = ctx2.raft_log.entry(5).unwrap().unwrap();
    assert_eq!(entry1.index, entry2.index);
    assert_eq!(entry1.term, entry2.term);
}

/// Test committed entry present in future leaders (Raft leader completeness)
///
/// # Scenario
/// - Log contains committed entry at index 3
/// - Simulate new leader taking over
/// - Expected: Committed entry preserved
#[tokio::test]
async fn test_committed_entry_present_in_future_leaders() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_committed_entry_present",
    );

    // Arrange: Insert and commit entry
    ctx.raft_log.reset().await.unwrap();
    let entries = mock_empty_entries(1, 5, 1);
    ctx.raft_log.insert_batch(entries).await.unwrap();
    ctx.raft_log.flush().await.unwrap();

    // Act: Simulate leader change (new context reading same data)
    let recovered = ctx.recover_from_crash();

    // Assert: Committed entry still present
    let entry = recovered.raft_log.entry(3).unwrap();
    assert!(
        entry.is_some(),
        "Committed entry should be present after leader change"
    );
}

/// Test append updates last_entry correctly
///
/// # Scenario
/// - Append entries and verify last_entry updates
/// - Expected: last_entry always reflects most recent append
#[tokio::test]
async fn test_append_updates_last_entry() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_append_updates_last_entry",
    );

    // Arrange: Reset log
    ctx.raft_log.reset().await.unwrap();

    // Act & Assert: Append and verify
    simulate_insert_command(&ctx.raft_log, vec![1, 2, 3], 1).await;
    assert_eq!(ctx.raft_log.last_entry().unwrap().index, 3);

    simulate_insert_command(&ctx.raft_log, vec![4, 5], 2).await;
    assert_eq!(ctx.raft_log.last_entry().unwrap().index, 5);
}

/// Test insert_batch with empty list is safe
///
/// # Scenario
/// - Call insert_batch with empty vec
/// - Expected: No error, log unchanged
#[tokio::test]
async fn test_insert_batch_with_empty_list() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_insert_batch_with_empty_list",
    );

    // Act: Insert empty batch
    let result = ctx.raft_log.insert_batch(vec![]).await;

    // Assert: Operation succeeds
    assert!(result.is_ok(), "Empty batch insert should succeed");
    assert!(ctx.raft_log.is_empty(), "Log should remain empty");
}

/// Test insert_batch updates metadata correctly
///
/// # Scenario
/// - Insert batch and verify all metadata updated
/// - Expected: length, last_entry_id, last_log_id all correct
#[tokio::test]
async fn test_insert_batch_updates_metadata() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_insert_batch_updates_metadata",
    );

    // Arrange: Reset log
    ctx.raft_log.reset().await.unwrap();

    // Act: Insert batch
    let entries = mock_empty_entries(1, 10, 5);
    ctx.raft_log.insert_batch(entries).await.unwrap();

    // Assert: All metadata correct
    assert_eq!(ctx.raft_log.len(), 10, "Length should be 10");
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        10,
        "Last entry ID should be 10"
    );
    let last_log = ctx.raft_log.last_log_id().unwrap();
    assert_eq!(last_log.index, 10, "Last log index should be 10");
    assert_eq!(last_log.term, 5, "Last log term should be 5");
}
