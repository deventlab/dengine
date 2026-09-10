pub mod buffers;
pub mod candidate_state;
pub mod follower_state;
pub mod leader_state;
pub mod learner_state;
pub mod read_lease;
pub mod role_state;

#[cfg(test)]
mod raft_role_test;

#[cfg(test)]
mod candidate_state_test;
#[cfg(test)]
mod follower_state_test;
#[cfg(test)]
mod learner_state_test;
#[cfg(test)]
mod pending_ack_test;
#[cfg(test)]
mod role_state_test;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use candidate_state::CandidateState;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::storage::SnapshotMetadata;
use follower_state::FollowerState;
pub use leader_state::ClusterMetadata;
use leader_state::LeaderState;
use learner_state::LearnerState;
pub use read_lease::{ReadLease, init_clock, now_ms};
use role_state::RaftRoleState;
use serde::Deserialize;
use serde::Deserializer;
use serde::Serialize;
use serde::Serializer;
use serde::ser::SerializeStruct;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::debug;
use tracing::trace;

use super::InboundEvent;
use super::InternalEvent;
use super::RaftContext;
use crate::Result;
use crate::TypeConfig;
use crate::role_state::PeerReplicationState;

/// The role state focuses solely on its own logic
/// and does not directly manipulate the underlying storage or network.
#[repr(i32)]
pub enum RaftRole<T: TypeConfig> {
    Follower(Box<FollowerState<T>>),
    Candidate(Box<CandidateState<T>>),
    Leader(Box<LeaderState<T>>),
    Learner(Box<LearnerState<T>>),
}

#[derive(Clone, Debug, Copy)]
pub struct HardState {
    /// Persistent state on all servers(Updated on stable storage before
    /// responding to RPCs): latest term server has seen (initialized to 0
    /// on first boot, increases monotonically) Terms act as a logical clock
    /// in Raft, and they allow servers to detect obsolete information such as
    /// stale leaders. Each server stores a current term number, which increases
    /// monotonically over time.
    pub current_term: u64,
    /// Persistent state on all servers(Updated on stable storage before
    /// responding to RPCs): candidateId that received vote in current term
    /// (or null if none)
    pub voted_for: Option<VotedFor>,
}

/// Outcome of a `set_hard_state()` call — two independent signals with
/// different purposes, do not conflate them.
pub(crate) struct HardStateChange {
    /// True if the persisted HardState actually differs from before this
    /// call. Gates whether `save_hard_state` needs to run at all.
    changed: bool,
    /// True only if `voted_for`'s transition represents a NEW leader
    /// commitment (committed:false->true, leader/term change while
    /// committed, or node restart). Gates `LeaderDiscovered`. A term-only
    /// change has `changed=true` but this stays `false`.
    is_new_leader_commitment: bool,
}

pub struct SharedState {
    pub node_id: u32,

    /// === Persistent State (MUST be on disk)
    hard_state: HardState,

    /// === Volatile state on all servers:
    /// index of highest log entry known to be committed (initialized to 0,
    /// increases monotonically)
    pub commit_index: u64,

    /// In-memory leader ID for hot-path reads (0 = no leader)
    /// Performance optimization: avoid RwLock on AppendEntries path
    current_leader_id: AtomicU32,

    /// Shared lease state between Raft loop (writer) and EmbeddedClient (reader).
    /// Arc ensures the same allocation is shared across role transitions via clone().
    pub lease: Arc<ReadLease>,
}

impl Clone for SharedState {
    fn clone(&self) -> Self {
        Self {
            node_id: self.node_id,
            hard_state: self.hard_state,
            commit_index: self.commit_index,
            current_leader_id: AtomicU32::new(self.current_leader_id.load(Ordering::Acquire)),
            lease: Arc::clone(&self.lease),
        }
    }
}

impl std::fmt::Debug for SharedState {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("SharedState")
            .field("node_id", &self.node_id)
            .field("hard_state", &self.hard_state)
            .field("commit_index", &self.commit_index)
            .field("current_leader_id", &self.current_leader())
            .finish()
    }
}

#[derive(Clone, Debug)]
pub struct StateSnapshot {
    pub role: i32,
    pub current_term: u64,
    pub voted_for: Option<VotedFor>,
    pub commit_index: u64,
}
/// This structure will be used to retrieve Leader's current state snapshot
/// e.g. used inside replication handler
#[derive(Clone, Debug)]
pub struct LeaderStateSnapshot {
    pub next_index: HashMap<u32, u64>,
    pub match_index: HashMap<u32, u64>,
    pub noop_log_id: Option<u64>,
}

impl SharedState {
    fn new(
        node_id: u32,
        hard_state_from_db: Option<HardState>,
        last_applied_index_option: Option<u64>,
    ) -> Self {
        let hard_state = if let Some(s) = hard_state_from_db {
            s
        } else {
            HardState {
                current_term: 1,
                voted_for: None,
            }
        };
        debug!(
            "New Shared State wtih, hard_state_from_db:{:?}, last_applied_index_option:{:?} ",
            &hard_state_from_db, &last_applied_index_option
        );
        Self {
            node_id,
            hard_state,
            commit_index: last_applied_index_option.unwrap_or(0),
            current_leader_id: AtomicU32::new(0),
            lease: Arc::new(ReadLease::new()),
        }
    }

    /// Get current leader ID (0 = no leader)
    /// Hot-path optimized: ~5ns atomic load vs ~50ns RwLock read
    pub fn current_leader(&self) -> Option<u32> {
        match self.current_leader_id.load(Ordering::Acquire) {
            0 => None,
            id => Some(id),
        }
    }

    /// Set current leader ID (0 to clear)
    /// Hot-path optimized: ~5ns atomic store vs ~50ns RwLock write
    pub fn set_current_leader(
        &self,
        leader_id: u32,
    ) {
        self.current_leader_id.store(leader_id, Ordering::Release);
    }

    /// Clear current leader (same as set_current_leader(0))
    pub fn clear_current_leader(&self) {
        self.current_leader_id.store(0, Ordering::Release);
    }
    pub fn current_term(&self) -> u64 {
        self.hard_state.current_term
    }

    /// Applies term/vote changes to in-memory state and reports whether this
    /// represents a NEW leader commitment — used by callers to decide whether to
    /// fire a `LeaderDiscovered` notification. Must NOT fire on every heartbeat
    /// that merely reconfirms an already-known leader, only on a genuine
    /// transition (see match arms below).
    ///
    /// Private by design: this is the only method allowed to touch `hard_state`
    /// directly. Its one and only caller is `commit_hard_state()` on
    /// `RaftRoleState`, which always follows it with `save_hard_state()` —
    /// nothing may split "mutate" from "persist" into two separately-callable
    /// steps.
    fn set_hard_state(
        &mut self,
        term: Option<u64>,
        voted_for: Option<VotedFor>,
    ) -> HardStateChange {
        let mut changed = false;

        if let Some(t) = term
            && t != self.hard_state.current_term
        {
            self.hard_state.current_term = t;
            changed = true;
        }

        let Some(new_vote) = voted_for else {
            return HardStateChange {
                changed,
                is_new_leader_commitment: false,
            };
        };

        if self.hard_state.voted_for != Some(new_vote) {
            changed = true;
        }

        let is_new_leader_commitment = match self.hard_state.voted_for {
            Some(old) => {
                new_vote.committed
                    && (old.voted_for_id != new_vote.voted_for_id
                        || old.voted_for_term != new_vote.voted_for_term
                        || !old.committed
                        || self.current_leader().is_none())
            }
            None => new_vote.committed,
        };

        self.hard_state.voted_for = Some(new_vote);
        HardStateChange {
            changed,
            is_new_leader_commitment,
        }
    }

    /// Clears voted_for to None (e.g. entering a fresh term with no vote cast
    /// yet). Private — only `commit_vote_reset()` may call this.
    fn clear_voted_for(&mut self) {
        self.hard_state.voted_for = None;
    }

    fn voted_for(&self) -> Result<Option<VotedFor>> {
        Ok(self.hard_state.voted_for)
    }

    #[cfg(test)]
    fn reset_voted_for(&mut self) -> Result<()> {
        self.hard_state.voted_for = None;
        Ok(())
    }

    #[cfg(test)]
    fn update_current_term(
        &mut self,
        term: u64,
    ) {
        self.hard_state.current_term = term;
    }

    /// Update voted_for and return true if this represents a new leader commitment
    ///
    /// Returns true when:
    /// - committed transitions from false to true, OR
    /// - leader/term changes with committed=true, OR
    /// - current_leader is None (node restart scenario)
    ///
    /// This enables event-driven leader discovery notifications without hot-path overhead.
    #[cfg(test)]
    fn update_voted_for(
        &mut self,
        new_vote: VotedFor,
    ) -> Result<bool> {
        let is_new_commit = match self.hard_state.voted_for {
            Some(old) => {
                new_vote.committed
                    && (old.voted_for_id != new_vote.voted_for_id
                        || old.voted_for_term != new_vote.voted_for_term
                        || !old.committed
                        || self.current_leader().is_none())
            }
            None => new_vote.committed,
        };

        self.hard_state.voted_for = Some(new_vote);
        Ok(is_new_commit)
    }

    pub(crate) fn hard_state(&self) -> HardState {
        self.hard_state
    }
}

impl<T: TypeConfig> RaftRole<T> {
    pub(crate) fn state(&self) -> &dyn RaftRoleState<T = T> {
        match self {
            RaftRole::Follower(state) => state.as_ref(),
            RaftRole::Candidate(state) => state.as_ref(),
            RaftRole::Leader(state) => state.as_ref(),
            RaftRole::Learner(state) => state.as_ref(),
        }
    }

    pub(crate) fn state_mut(&mut self) -> &mut dyn RaftRoleState<T = T> {
        match self {
            RaftRole::Follower(state) => state.as_mut(),
            RaftRole::Candidate(state) => state.as_mut(),
            RaftRole::Leader(state) => state.as_mut(),
            RaftRole::Learner(state) => state.as_mut(),
        }
    }

    pub(crate) fn is_timer_expired(&self) -> bool {
        self.state().is_timer_expired()
    }

    pub(crate) fn reset_timer(&mut self) {
        self.state_mut().reset_timer()
    }

    pub(crate) async fn join_cluster(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        self.state().join_cluster(ctx).await
    }

    pub(crate) fn next_deadline(&self) -> Instant {
        self.state().next_deadline()
    }

    #[inline]
    pub fn as_i32(&self) -> i32 {
        match self {
            RaftRole::Follower(_) => d_engine_proto::common::NodeRole::Follower as i32,
            RaftRole::Candidate(_) => d_engine_proto::common::NodeRole::Candidate as i32,
            RaftRole::Leader(_) => d_engine_proto::common::NodeRole::Leader as i32,
            RaftRole::Learner(_) => d_engine_proto::common::NodeRole::Learner as i32,
        }
    }

    pub(crate) fn become_leader(&self) -> Result<RaftRole<T>> {
        self.state().become_leader()
    }
    pub(crate) fn become_candidate(&mut self) -> Result<RaftRole<T>> {
        self.state_mut().become_candidate()
    }
    pub(crate) fn become_follower(&self) -> Result<RaftRole<T>> {
        self.state().become_follower()
    }
    pub(crate) fn become_learner(&self) -> Result<RaftRole<T>> {
        self.state().become_learner()
    }
    /// Move the withheld-ACK queue out of the current role before a transition.
    /// Only Follower and Learner keep one; every other role yields an empty map.
    ///
    /// A withheld ACK describes this node's durable log, not its role. Dropping it
    /// on a `Learner -> Follower` promotion would strand the leader waiting on a
    /// response that never arrives (#446).
    pub(crate) fn take_pending_acks(
        &mut self
    ) -> std::collections::BTreeMap<u64, role_state::PendingAck> {
        self.state_mut()
            .pending_append_acks_mut()
            .map(std::mem::take)
            .unwrap_or_default()
    }

    /// Install a carried withheld-ACK queue into the role a transition produced.
    /// Follower and Learner adopt it; any other role cannot hold it, so its
    /// entries are failed with a conflict response.
    pub(crate) fn restore_pending_acks(
        &mut self,
        acks: std::collections::BTreeMap<u64, role_state::PendingAck>,
    ) {
        let node_id = self.state().node_id();
        let current_term = self.state().current_term();
        match self.state_mut().pending_append_acks_mut() {
            Some(queue) => *queue = acks,
            None => role_state::reject_pending_acks(acks, node_id, current_term),
        }
    }

    pub fn current_term(&self) -> u64 {
        self.state().current_term()
    }

    pub(crate) fn init_peers_next_index_and_match_index(
        &mut self,
        last_entry_id: u64,
        peer_ids: Vec<u32>,
    ) -> Result<()> {
        self.state_mut().init_peers_next_index_and_match_index(last_entry_id, peer_ids)
    }

    /// Reset `next_index[peer] = match_index[peer] + 1` after a bidi stream disconnect.
    /// Ensures the next heartbeat re-sends any unACKed in-flight entries.
    pub(crate) fn handle_peer_stream_error(
        &mut self,
        peer_id: u32,
    ) {
        // The bidi stream only carries AppendEntries. While this peer is in Snapshot
        // state, an error on this stream says nothing about the independent
        // connection the snapshot transfer runs on, so it has no authority to act
        // (mirrors etcd raft.go MsgUnreachable: only BecomeProbe() when StateReplicate).
        if self.state().peer_replication_state(peer_id) == PeerReplicationState::Snapshot {
            return;
        }
        let match_idx = self.state().match_index(peer_id).unwrap_or(0);
        let _ = self.state_mut().update_next_index(peer_id, match_idx + 1);

        // #436: stream is down, we don't know what (if anything) the peer received —
        // stop trusting speculative advance (etcd: BecomeProbe on MsgUnreachable).
        self.state_mut()
            .set_peer_replication_state(peer_id, PeerReplicationState::Probe);
    }

    pub(crate) async fn handle_zombie_detected(
        &mut self,
        node_id: u32,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        self.state_mut().handle_zombie_detected(node_id, internal_event_tx, ctx).await
    }

    pub(crate) fn handle_snapshot_push_completed(
        &mut self,
        peer_id: u32,
        success: bool,
        policy: &crate::InstallSnapshotBackoffPolicy,
        node_id: u32,
    ) {
        self.state_mut()
            .handle_snapshot_push_completed(peer_id, success, policy, node_id)
    }

    pub(crate) async fn tick(
        &mut self,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        event_tx: &mpsc::Sender<InboundEvent>,
        ctx: &RaftContext<T>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        trace!("raft_role:tick");
        self.state_mut().tick(internal_event_tx, event_tx, ctx).await
    }

    pub(crate) async fn handle_inbound_event(
        &mut self,
        inbound_event: InboundEvent,
        ctx: &RaftContext<T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()>
    where
        T: TypeConfig,
    {
        self.state_mut()
            .handle_inbound_event(inbound_event, ctx, internal_event_tx)
            .await
    }

    /// Fire-and-forget noop to confirm quorum; delegates to LeaderState only.
    /// Called from the BecomeLeader handler; no-op for non-leader roles.
    pub(crate) async fn initiate_noop_commit(
        &mut self,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        if let RaftRole::Leader(state) = self {
            state.initiate_noop_commit(ctx, internal_event_tx).await
        } else {
            Ok(())
        }
    }

    /// Drain pending read buffer when stepping down from Leader.
    /// Only Leader implements this; other roles are no-op.
    pub(crate) fn drain_read_buffer(&mut self) -> Result<()> {
        self.state_mut().drain_read_buffer()
    }

    /// Push client command directly to role's internal buffer (zero-copy).
    /// For Leader: push to batch_buffer or read_buffer.
    /// For non-Leader: immediately reject with NOT_LEADER error.
    pub(crate) fn push_client_cmd(
        &mut self,
        cmd: crate::event::ClientCmd,
        ctx: &crate::RaftContext<T>,
    ) {
        self.state_mut().push_client_cmd(cmd, ctx)
    }

    /// Flush command buffers if size or timeout thresholds are reached.
    /// For Leader: processes batches if FlushReason indicates need.
    /// For non-Leader: no-op (buffers are empty).
    pub(crate) async fn flush_cmd_buffers(
        &mut self,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().flush_cmd_buffers(ctx, internal_event_tx).await
    }

    /// Dispatch ApplyCompleted to the current role state.
    pub(crate) async fn handle_apply_completed(
        &mut self,
        last_index: u64,
        results: Vec<crate::ApplyResult>,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> crate::Result<()>
    where
        T: TypeConfig,
    {
        self.state_mut()
            .handle_apply_completed(last_index, results, ctx, internal_event_tx)
            .await
    }

    /// Dispatch LogFlushed(durable) to the current role state.
    /// Follower/Learner: sends deferred ACK. Leader: recalculates commit.
    pub(crate) async fn handle_log_flushed(
        &mut self,
        durable: u64,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) {
        self.state_mut().handle_log_flushed(durable, ctx, internal_event_tx).await
    }

    /// Dispatch AppendResult from a per-follower worker back to the current role state.
    pub(crate) async fn handle_append_result(
        &mut self,
        follower_id: u32,
        result: crate::Result<d_engine_proto::server::replication::AppendEntriesResponse>,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> crate::Result<()> {
        self.state_mut()
            .handle_append_result(follower_id, result, ctx, internal_event_tx)
            .await
    }

    /// Trigger an independent snapshot on this role (Raft §7 — each server snapshots
    /// independently). Called when `should_snapshot()` returns true after SM apply.
    /// Candidate: returns `RoleViolation` — transient state, defer to next stable role.
    pub(crate) async fn handle_create_snapshot(
        &mut self,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().handle_create_snapshot(ctx, internal_event_tx).await
    }

    /// Process the completed snapshot result (success or error).
    /// Leader: schedules log purge up to `last_included`.
    /// Follower/Learner: updates local snapshot path.
    /// Candidate: returns `RoleViolation`.
    pub(crate) async fn handle_snapshot_created(
        &mut self,
        result: crate::Result<(SnapshotMetadata, std::path::PathBuf)>,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().handle_snapshot_created(result, ctx, internal_event_tx).await
    }

    /// Advance the purge boundary after log entries up to `purged_id` are removed.
    /// Leader only: updates `last_purged_index`.
    /// Non-leader: no-op (unexpected; logs a warning).
    pub(crate) fn handle_log_purge_completed(
        &mut self,
        purged_id: LogId,
    ) -> Result<()> {
        self.state_mut().handle_log_purge_completed(purged_id)
    }

    /// Check pending learners for promotion eligibility after a membership change.
    /// Leader only: evaluates `pending_promotions`, proposes config change if ready.
    /// Non-leader: no-op (logs a warning — unexpected in steady state).
    pub(crate) async fn handle_promote_ready_learners(
        &mut self,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().handle_promote_ready_learners(ctx, internal_event_tx).await
    }

    /// Node was removed from cluster membership; step down immediately per Raft protocol.
    /// Leader: emits `BecomeFollower`. Non-leader: unreachable (only Leader proposes
    /// self-removal via config change).
    pub(crate) fn handle_self_removed(
        &mut self,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().handle_self_removed(internal_event_tx)
    }

    /// Membership config change applied to state — refresh any role-local derived state.
    /// Leader: invalidates `cluster_metadata` cache for hot-path reads.
    /// Learner: checks if it was promoted to Voter; emits `BecomeFollower` if so.
    /// Follower/Candidate: no-op.
    pub(crate) async fn handle_membership_applied(
        &mut self,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.state_mut().handle_membership_applied(ctx, internal_event_tx).await
    }
}

impl Serialize for HardState {
    fn serialize<S>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HardState", 2)?;
        state.serialize_field("current_term", &self.current_term)?;
        state.serialize_field("voted_for", &self.voted_for)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for HardState {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct HardStateDe {
            current_term: u64,
            voted_for: Option<VotedFor>,
        }

        let hard_state_de = HardStateDe::deserialize(deserializer)?;

        Ok(HardState {
            current_term: hard_state_de.current_term,
            voted_for: hard_state_de.voted_for,
        })
    }
}
