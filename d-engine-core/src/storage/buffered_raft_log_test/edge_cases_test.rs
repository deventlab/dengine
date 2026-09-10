use bytes::Bytes;

use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use d_engine_proto::common::{Entry, EntryPayload, LogId};

#[tokio::test]
async fn test_empty_log_operations() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_empty_log",
    );

    assert!(ctx.raft_log.is_empty());
    assert_eq!(ctx.raft_log.first_entry_id(), 0);
    assert_eq!(ctx.raft_log.last_entry_id(), 0);
    assert!(ctx.raft_log.last_entry().is_none());
    assert_eq!(ctx.raft_log.get_entries_range(1..=10).unwrap().len(), 0);
    assert!(ctx.raft_log.first_index_for_term(1).is_none());
    assert!(ctx.raft_log.last_index_for_term(1).is_none());
}

#[tokio::test]
async fn test_single_entry_operations() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_single_entry",
    );

    // Append single entry
    let entry = Entry {
        index: 1,
        term: 1,
        payload: None,
    };
    ctx.raft_log.append_entries(vec![entry]).await.unwrap();

    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 1);
    assert_eq!(ctx.raft_log.first_index_for_term(1), Some(1));
    assert_eq!(ctx.raft_log.last_index_for_term(1), Some(1));

    // Purge should result in empty log
    ctx.raft_log.purge_logs_up_to(LogId { index: 1, term: 1 }).await.unwrap();

    assert!(ctx.raft_log.is_empty());
}

#[tokio::test]
async fn test_gap_handling_in_indexes() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_gap_handling",
    );

    // Create entries 1..=100
    for i in 1..=100 {
        let entry = Entry {
            index: i,
            term: 1,
            payload: None,
        };
        ctx.raft_log.append_entries(vec![entry]).await.unwrap();
    }

    // Create gaps by selective removal
    ctx.raft_log.remove_range(25..=75);

    // Verify boundaries are correct
    assert_eq!(ctx.raft_log.first_entry_id(), 1);
    assert_eq!(ctx.raft_log.last_entry_id(), 100);

    // Verify gaps are handled
    assert!(ctx.raft_log.entry(24).unwrap().is_some());
    assert!(ctx.raft_log.entry(25).unwrap().is_none());
    assert!(ctx.raft_log.entry(75).unwrap().is_none());
    assert!(ctx.raft_log.entry(76).unwrap().is_some());

    // Range queries should skip gaps
    let range = ctx.raft_log.get_entries_range(20..=80).unwrap();
    // 20-24 (5 entries) + 76-80 (5 entries) = 10 entries total
    assert_eq!(range.len(), 10, "Should have 10 entries: 20-24 and 76-80");

    // Verify content
    assert_eq!(range.first().unwrap().index, 20);
    assert_eq!(range.last().unwrap().index, 80);
}

#[tokio::test]
async fn test_extreme_boundary_conditions() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_extreme_boundary_conditions",
    );

    // Test with maximum index values
    let max_index = u64::MAX - 10;
    let entries = vec![
        Entry {
            index: max_index,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(b"max_data".to_vec()))),
        },
        Entry {
            index: max_index + 1,
            term: 1,
            payload: Some(EntryPayload::command(Bytes::from(b"max_data+1".to_vec()))),
        },
    ];

    ctx.raft_log.append_entries(entries).await.unwrap();

    // Verify extreme values are handled correctly
    assert_eq!(ctx.raft_log.last_entry_id(), max_index + 1);
    assert!(ctx.raft_log.entry(max_index).unwrap().is_some());
    assert!(ctx.raft_log.entry(max_index + 1).unwrap().is_some());
}
