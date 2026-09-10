use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, simulate_insert_command};
use d_engine_proto::common::Entry;

#[tokio::test]
async fn test_log_matching_property() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_log_matching",
    );

    // Build log: [1,1] [2,1] [3,2] [4,2] [5,3]
    ctx.append_entries(1, 2, 1).await;
    ctx.append_entries(3, 2, 2).await;
    ctx.append_entries(5, 1, 3).await;

    // Verify consistency
    assert_eq!(ctx.raft_log.entry_term(3), Some(2));
    assert_eq!(ctx.raft_log.first_index_for_term(2), Some(3));
    assert_eq!(ctx.raft_log.last_index_for_term(2), Some(4));

    // Conflict resolution: new leader with [1,1] [2,1] [3,2] [4,3]
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(
            3,
            2,
            vec![Entry {
                index: 4,
                term: 3,
                payload: None,
            }],
        )
        .await
        .unwrap();

    assert_eq!(result.unwrap().index, 4);
    assert_eq!(ctx.raft_log.entry_term(4), Some(3));
    assert!(ctx.raft_log.entry(5).unwrap().is_none());
}

#[tokio::test]
async fn test_leader_completeness_property() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_leader_completeness",
    );

    // Leader writes entries and flushes so durable_index = 10
    ctx.append_entries(1, 10, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // Simulate majority replication scenario:
    // Leader has: [1,2,3,4,5,6,7,8,9,10], durable_index=10
    // Peer1 matched: 7, Peer2 matched: 6, Peer3 matched: 5
    // With leader's durable_index (10), the sorted array is: [10, 7, 6, 5]
    // Majority index (median) at position 2 is: 6
    let commit_index = ctx.raft_log.calculate_majority_matched_index(
        1,             // current_term
        0,             // commit_index (previous)
        vec![7, 6, 5], // peer match indexes
    );

    // Expected result is 6, not 7
    // Because: sorted [10, 7, 6, 5], majority_index = peers[len/2] = peers[2] = 6
    assert_eq!(commit_index, Some(6), "Majority commit index should be 6");

    // New leader must have all committed entries
    for i in 1..=6 {
        assert!(
            ctx.raft_log.entry(i).unwrap().is_some(),
            "Entry {i} should exist",
        );
    }
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case0() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case0",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    let current_term = 2;
    let commit_index = 2;

    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2, 3], 2).await;
    ctx.drain_fsync_completions();

    assert_eq!(
        Some(3),
        ctx.raft_log
            .calculate_majority_matched_index(current_term, commit_index, vec![2, 12])
    );
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case1() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case1",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Case 1: majority matched index is 2, commit_index: 4, current_term is 3,
    // while log(2) term is 2, return None
    let ct = 3;
    let ci = 4;

    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2], 2).await;
    assert_eq!(
        None,
        ctx.raft_log.calculate_majority_matched_index(ct, ci, vec![1, 2])
    );
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case2() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case2",
    );
    ctx.raft_log.reset().await.expect("reset successfully!");

    // Case 2: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 3, return Some(3)
    let ct = 3;
    let ci = 2;

    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2], 2).await;
    simulate_insert_command(&ctx.raft_log, vec![3], 3).await;
    ctx.drain_fsync_completions();
    assert_eq!(
        Some(3),
        ctx.raft_log.calculate_majority_matched_index(ct, ci, vec![4, 2])
    );
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case3() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case3",
    );

    // Case 3: majority matched index is 3, commit_index: 2, current_term is 3,
    // while log(3) term is 2, return None
    let ct = 3;
    let ci = 2;
    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2], 2).await;

    assert_eq!(
        None,
        ctx.raft_log.calculate_majority_matched_index(ct, ci, vec![3, 2])
    );
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case4() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case4",
    );

    // Case 4: majority matched index is 2, commit_index: 2, current_term is 3,
    // while log(2) term is 2, return None
    let ct = 3;
    let ci = 2;

    simulate_insert_command(&ctx.raft_log, vec![1], 1).await;
    simulate_insert_command(&ctx.raft_log, vec![2], 2).await;

    assert_eq!(
        None,
        ctx.raft_log.calculate_majority_matched_index(ct, ci, vec![2, 2])
    );
}

#[tokio::test]
async fn test_calculate_majority_matched_index_case5() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_calculate_majority_matched_index_case5",
    );

    // Case 5: stress testing with 100,000 entries
    // Simulate 100,000 local log entries
    // peer1's match: 97,000, peer2's match 98,000
    let term = 1;
    let raft_log_length = 100000;
    let commit = 90000;

    let peer1_match = 97000;
    let peer2_match = 98000;

    let raft_log_entry_ids: Vec<u64> = (1..=raft_log_length).collect();
    simulate_insert_command(&ctx.raft_log, raft_log_entry_ids, 1).await;
    ctx.drain_fsync_completions();

    assert_eq!(
        Some(peer2_match),
        ctx.raft_log
            .calculate_majority_matched_index(term, commit, vec![peer1_match, peer2_match])
    );
}
