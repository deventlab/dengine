//! Single-Voter Commit Path Tests
//!
//! RPO=0 (#446): `handle_log_flushed` single-voter branch must commit to `durable`, not
//! `last_entry_id()` — a single-voter cluster has no majority to fall back on, so if the
//! leader itself hasn't fsynced an entry, there is no copy anywhere safe from power loss.
//!
//! ## Superseded design (kept as history, do not resurrect)
//! `fix #329` changed this branch to commit to `durable` instead of `last_entry_id()`,
//! then reverted it after measuring a ~3x latency regression in 3-node embedded bench
//! (1731µs vs ~566µs) — IO thread latency landed on the commit critical path. That
//! regression is real and will resurface here. RPO=0 makes paying it mandatory for
//! single-voter clusters — there is no majority to absorb the risk the old design
//! accepted.

use crate::MockMembership;
use crate::MockRaftLog;
use crate::MockReplicationCore;
use crate::raft_role::leader_state::LeaderState;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::MockBuilder;
use crate::test_utils::mock::MockTypeConfig;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::mpsc;
use tokio::sync::watch;

async fn setup_single_voter(
    last_entry_id_val: u64
) -> (
    LeaderState<MockTypeConfig>,
    crate::RaftContext<MockTypeConfig>,
    Arc<AtomicU64>,
) {
    let (_shutdown_tx, shutdown_rx) = watch::channel(());

    let last_entry_id = Arc::new(AtomicU64::new(last_entry_id_val));
    let last_entry_id_clone = last_entry_id.clone();

    let mut raft_log = MockRaftLog::new();
    raft_log
        .expect_last_entry_id()
        .returning(move || last_entry_id_clone.load(Ordering::Relaxed));

    let replication = MockReplicationCore::new();

    let ctx = MockBuilder::new(shutdown_rx)
        .with_raft_log(raft_log)
        .with_replication_handler(replication)
        .build_context();

    let mut state = LeaderState::<MockTypeConfig>::new(1, ctx.node_config.clone());

    let mut membership = MockMembership::new();
    membership.expect_voters().returning(Vec::new);
    membership.expect_replication_peers().returning(Vec::new);
    state.init_cluster_metadata(&Arc::new(membership)).await.unwrap();

    assert!(state.cluster_metadata.single_voter);

    (state, ctx, last_entry_id)
}

/// RPO=0: `handle_log_flushed` must commit to `durable`, not `last_entry_id`.
///
/// Simulates: entries 4-5 arrived in memory (`last_entry_id=5`) but the IO batch has
/// only flushed entries 1-3 so far (`durable=3`). Commit must stay at 3 — entries 4-5
/// aren't crash-safe yet, and a single-voter cluster has no other copy to fall back on.
///
/// This test FAILS if `handle_log_flushed` still uses `last_entry_id` for commit (the
/// old MemFirst behavior, since revoked — RPO=0 makes single-voter durability mandatory).
#[tokio::test]
async fn test_single_voter_commit_uses_durable_not_last_entry_id() {
    // last_entry_id=5: entries 4-5 arrived in memory during the IO flush of 1-3
    let (mut state, ctx, _last_entry_id) = setup_single_voter(5).await;
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // IO flushed only entries 1-3 (durable=3), but log has entries 1-5 in memory
    state.handle_log_flushed(3, &ctx, &internal_event_tx).await;

    assert_eq!(
        state.commit_index(),
        3,
        "RPO=0: commit must use durable=3, not last_entry_id=5 — entries 4-5 aren't \
         fsynced yet, and single-voter has no majority to fall back on"
    );
}

/// After IO catches up (durable == last_entry_id), commit equals durable.
#[tokio::test]
async fn test_single_voter_commit_when_durable_equals_last_entry_id() {
    let (mut state, ctx, _last_entry_id) = setup_single_voter(5).await;
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    state.handle_log_flushed(5, &ctx, &internal_event_tx).await;

    assert_eq!(state.commit_index(), 5, "commit must advance to durable=5");
}

/// Commit tracks `durable` across IO batches, not the in-memory tail.
///
/// Simulates rapid writes where the in-memory log runs ahead of what's fsynced:
/// - Flush 1: IO flushed 1-3 (durable=3), memory has 1-7 → commit=3, not 7
/// - Flush 2: IO flushed 4-7 (durable=7), memory now has 1-10 → commit=7, not 10
#[tokio::test]
async fn test_single_voter_commit_tracks_durable_not_memory_tail() {
    let (mut state, ctx, last_entry_id) = setup_single_voter(7).await;
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // IO batch 1: flushed 1-3, memory has 1-7
    state.handle_log_flushed(3, &ctx, &internal_event_tx).await;
    assert_eq!(
        state.commit_index(),
        3,
        "commit must stay at durable=3 — entries 4-7 aren't fsynced yet"
    );

    // IO batch 2: flushed 4-7, memory now has 1-10
    last_entry_id.store(10, Ordering::Relaxed);
    state.handle_log_flushed(7, &ctx, &internal_event_tx).await;
    assert_eq!(
        state.commit_index(),
        7,
        "commit must advance to durable=7, not the in-memory tail (10)"
    );
}

/// No-op flush: durable == commit_index means nothing new is safe to commit yet.
#[tokio::test]
async fn test_single_voter_no_commit_when_nothing_new_durable() {
    let (mut state, ctx, _last_entry_id) = setup_single_voter(3).await;
    let (internal_event_tx, _internal_event_rx) = mpsc::unbounded_channel();

    // First flush advances commit to 3
    state.handle_log_flushed(3, &ctx, &internal_event_tx).await;
    assert_eq!(state.commit_index(), 3);

    // Second flush with same durable=3: nothing new is fsynced → no commit advance
    state.handle_log_flushed(3, &ctx, &internal_event_tx).await;
    assert_eq!(
        state.commit_index(),
        3,
        "commit must not advance when durable == commit_index"
    );
}
