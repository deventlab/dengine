//! Tests for #446's withheld-AppendEntries-ACK primitives:
//!
//!   - `RaftRoleState::resolve_pending_acks` — the release / reject decision;
//!   - `RaftRole::{take,restore}_pending_acks` — carrying the queue across a role
//!     transition instead of dropping it.
//!
//! These drive bare role structs (no `RaftContext`, no mocks, no fs), so they
//! stay cheap to run and cheap to change.

use std::sync::Arc;

use d_engine_proto::common::LogId;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;
use tonic::Status;

use super::RaftRole;
use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::learner_state::LearnerState;
use crate::MaybeCloneOneshot;
use crate::MaybeCloneOneshotReceiver;
use crate::RaftNodeConfig;
use crate::RaftOneshot;
use crate::raft_role::role_state::PendingAck;
use crate::raft_role::role_state::RaftRoleState;
use crate::test_utils::mock::MockTypeConfig;

type Rx = MaybeCloneOneshotReceiver<std::result::Result<AppendEntriesResponse, Status>>;

fn config() -> Arc<RaftNodeConfig> {
    Arc::new(
        RaftNodeConfig::new()
            .expect("RaftNodeConfig::new")
            .validate()
            .expect("RaftNodeConfig::validate"),
    )
}

fn follower(term: u64) -> RaftRole<MockTypeConfig> {
    let mut s = FollowerState::new(1, config(), None, None);
    s.shared_state_mut().update_current_term(term);
    RaftRole::Follower(Box::new(s))
}

fn learner(term: u64) -> RaftRole<MockTypeConfig> {
    let mut s = LearnerState::new(1, config());
    s.shared_state_mut().update_current_term(term);
    RaftRole::Learner(Box::new(s))
}

fn candidate() -> RaftRole<MockTypeConfig> {
    RaftRole::Candidate(Box::new(CandidateState::new(1, config())))
}

/// Put a withheld success ACK for `index` straight into `role`'s queue, bypassing
/// the AppendEntries workflow. Returns the receiver a caller would be blocked on.
fn withhold(
    role: &mut RaftRole<MockTypeConfig>,
    index: u64,
    claimed_term: u64,
    term_when_withheld: u64,
) -> Rx {
    let (tx, rx) = MaybeCloneOneshot::new();
    role.state_mut()
        .pending_append_acks_mut()
        .expect("role keeps a pending-ack queue")
        .insert(
            index,
            PendingAck {
                claimed_term,
                term_when_withheld,
                senders: vec![tx],
            },
        );
    rx
}

fn queue_len(role: &mut RaftRole<MockTypeConfig>) -> usize {
    role.state_mut().pending_append_acks_mut().map_or(0, |q| q.len())
}

// -- resolve_pending_acks -----------------------------------------------------

/// A withheld ACK is released as a success once its claimed index is durable and
/// the node is still on the term it withheld under.
#[test]
fn test_resolve_releases_success_when_durable() {
    let mut role = follower(5);
    let mut rx = withhold(&mut role, 8, 5, 5);

    role.state_mut().resolve_pending_acks(8);

    let resp = rx.try_recv().expect("released").unwrap();
    assert!(matches!(
        resp.result,
        Some(append_entries_response::Result::Success(SuccessResult {
            last_match: Some(LogId { index: 8, term: 5 }),
        })),
    ));
    assert_eq!(queue_len(&mut role), 0);
}

/// A withheld ACK stays queued while its claimed index is still beyond durable.
#[test]
fn test_resolve_keeps_waiting_until_durable() {
    let mut role = follower(5);
    let mut rx = withhold(&mut role, 8, 5, 5);

    role.state_mut().resolve_pending_acks(7);

    assert!(rx.try_recv().is_err());
    assert_eq!(queue_len(&mut role), 1);
}

/// A withheld ACK is rejected with a conflict if the node moved to a newer term
/// since withholding: a higher-term leader may have overwritten the log at that
/// index, so the durability claim can no longer be trusted (#446).
#[test]
fn test_resolve_rejects_stale_term_ack() {
    let mut role = follower(6); // node is now on term 6
    let mut rx = withhold(&mut role, 8, 5, 5); // ACK was withheld under term 5

    role.state_mut().resolve_pending_acks(8);

    let resp = rx.try_recv().expect("resolved").unwrap();
    assert!(
        !resp.is_success(),
        "stale-term ACK must resolve to a conflict"
    );
    assert_eq!(queue_len(&mut role), 0);
}

/// Every sender queued on one index is answered — a leader retry or a heartbeat
/// can leave more than one waiter on the same index.
#[test]
fn test_resolve_answers_every_sender_on_an_index() {
    let mut role = follower(5);
    let (tx1, mut rx1) = MaybeCloneOneshot::new();
    let (tx2, mut rx2) = MaybeCloneOneshot::new();
    role.state_mut().pending_append_acks_mut().unwrap().insert(
        8,
        PendingAck {
            claimed_term: 5,
            term_when_withheld: 5,
            senders: vec![tx1, tx2],
        },
    );

    role.state_mut().resolve_pending_acks(8);

    assert!(rx1.try_recv().unwrap().unwrap().is_success());
    assert!(rx2.try_recv().unwrap().unwrap().is_success());
}

/// `resolve_pending_acks` on a role that keeps no queue (Candidate) is a no-op.
#[test]
fn test_resolve_is_noop_without_a_queue() {
    let mut role = candidate();
    role.state_mut().resolve_pending_acks(10); // must not panic
}

// -- take / restore across a role transition ---------------------------------

/// A withheld ACK survives a Learner -> Follower promotion: `take_pending_acks`
/// moves the queue out of the old role and `restore_pending_acks` installs it in
/// the new one. Dropping it here would strand the leader on a response that never
/// arrives — the bug #446 fixed.
#[test]
fn test_pending_ack_survives_learner_promotion() {
    let mut old = learner(5);
    let mut rx = withhold(&mut old, 8, 5, 5);

    let carried = old.take_pending_acks();
    assert_eq!(carried.len(), 1);
    assert_eq!(
        queue_len(&mut old),
        0,
        "take must move the queue, not copy it"
    );

    let mut new = follower(5);
    new.restore_pending_acks(carried);

    new.state_mut().resolve_pending_acks(8);
    assert!(rx.try_recv().expect("released after promotion").unwrap().is_success());
}

/// The symmetric Follower -> Learner demotion also carries the queue.
#[test]
fn test_pending_ack_survives_follower_demotion() {
    let mut old = follower(5);
    let mut rx = withhold(&mut old, 8, 5, 5);

    let carried = old.take_pending_acks();
    let mut new = learner(5);
    new.restore_pending_acks(carried);

    new.state_mut().resolve_pending_acks(8);
    assert!(rx.try_recv().expect("released after demotion").unwrap().is_success());
}

/// Restoring the queue into a role that cannot hold one (Candidate) fails every
/// withheld ACK with a conflict, so the leader retries rather than timing out.
#[test]
fn test_restore_into_candidate_fails_pending_acks() {
    let mut old = follower(5);
    let mut rx = withhold(&mut old, 8, 5, 5);
    let carried = old.take_pending_acks();

    let mut candidate = candidate();
    candidate.restore_pending_acks(carried);

    let resp = rx.try_recv().expect("failed, not dropped").unwrap();
    assert!(!resp.is_success());
}

/// `take_pending_acks` on a role with no queue yields an empty map, never panics.
#[test]
fn test_take_from_queueless_role_is_empty() {
    assert!(candidate().take_pending_acks().is_empty());
}
