use async_trait::async_trait;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole::Candidate;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::storage::SnapshotMetadata;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::HardState;
use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::candidate_state::CandidateState;
use super::leader_state::LeaderState;
use super::learner_state::LearnerState;
use super::role_state::RaftRoleState;
use super::role_state::check_and_trigger_snapshot;
use crate::ConsensusError;
use crate::ElectionCore;
use crate::ElectionTimer;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::Membership;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::role_state::PendingAck;
use crate::role_state::schedule_and_execute_purge;
use crate::utils::cluster::error;
use crate::utils::cluster_printer::print_role_transition_line;

/// Follower node's state in Raft consensus.
///
/// Maintains state required for responding to leader heartbeats and log replication.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct FollowerState<T: TypeConfig> {
    // -- Core State --
    /// Shared cluster state
    pub shared_state: SharedState,

    // -- Log Compaction & Purge --
    /// === Volatile State ===
    /// The upper bound (exclusive) of log entries scheduled for asynchronous physical deletion.
    ///
    /// This value is set immediately after a new snapshot is successfully created.
    /// It represents the next log position that will trigger compaction.
    ///
    /// The actual log purge is performed by a background task, which may be delayed
    /// due to resource constraints or retry mechanisms.
    pub pending_purge_upto: Option<LogId>,

    /// === Persistent State ===
    /// Last physically purged log index (inclusive)
    pub last_purged_index: Option<LogId>,

    /// AppendEntries responses withheld pending this node's own durable_index.
    /// See `role_state::PendingAck`.
    pending_append_acks: BTreeMap<u64, PendingAck>,

    // -- Snapshot Management --
    /// Prevents concurrent snapshot creation
    ///
    /// Per industry best practices:
    /// - Protects against concurrent snapshot requests
    /// - Ensures snapshot consistency at Raft layer
    /// - Reduces unnecessary tokio::spawn overhead
    pub(crate) snapshot_in_progress: AtomicBool,

    // -- Cluster Configuration --
    /// Node configuration (shared immutable reference)
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Election Timing --
    /// Leader heartbeat detection timer
    ///
    /// Manages:
    /// - Heartbeat timeout tracking
    /// - Transition to candidate state when timeout occurs
    pub(super) timer: ElectionTimer,

    // -- Type System Marker --
    /// Phantom data for type parameterization
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for FollowerState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        error!("become_leader Illegal. I am Follower");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Candidate",
            self.node_id(),
            self.current_term(),
        );
        print_role_transition_line(Follower as i32, Candidate as i32, self.current_term());
        Ok(RaftRole::Candidate(Box::new(self.into())))
    }
    fn become_follower(&self) -> Result<RaftRole<T>> {
        warn!("I am follower already");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_learner(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Learner",
            self.node_id(),
            self.current_term(),
        );
        print_role_transition_line(Follower as i32, Learner as i32, self.current_term());
        Ok(RaftRole::Learner(Box::new(self.into())))
    }

    //--- Timer releated ---
    fn is_timer_expired(&self) -> bool {
        self.timer.is_expired()
    }
    fn reset_timer(&mut self) {
        self.timer.reset()
    }
    fn next_deadline(&self) -> Instant {
        self.timer.next_deadline()
    }

    /// Election Timeout
    /// As follower,
    ///  step as Candidate
    async fn tick(
        &mut self,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        _event_tx: &mpsc::Sender<InboundEvent>,
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
        if Instant::now() < self.timer.next_deadline() {
            return Ok(());
        }

        debug!("reset timer");
        self.timer.reset();

        debug!("follower::start_election...");

        internal_event_tx.send(InternalEvent::BecomeCandidate).map_err(|e| {
            error!("Failed to send: {:?}", e);
            NetworkError::SingalSendFailed(format!("{:?}", e))
        })?;

        Ok(())
    }

    async fn handle_inbound_event(
        &mut self,
        inbound_event: InboundEvent,
        ctx: &RaftContext<T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        let state_snapshot = self.state_snapshot();
        let my_term = self.current_term();

        match inbound_event {
            InboundEvent::ReceiveVoteRequest(vote_request, sender) => {
                let candidate_id = vote_request.candidate_id;

                let LogId {
                    index: last_log_index,
                    term: last_log_term,
                } = ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });

                match ctx
                    .election_handler()
                    .handle_vote_request(
                        vote_request,
                        my_term,
                        self.voted_for().unwrap(),
                        ctx.raft_log(),
                    )
                    .await
                {
                    Ok(state_update) => {
                        let vote_granted = state_update.new_voted_for.is_some();
                        if vote_granted {
                            self.reset_timer();
                        }

                        debug!(
                            "handle_vote_request success with state_update: {:?}",
                            &state_update
                        );

                        let new_voted_for = state_update.new_voted_for;
                        self.commit_hard_state(ctx, state_update.term_update, new_voted_for)?;

                        let response = VoteResponse {
                            term: my_term,
                            vote_granted: new_voted_for.is_some(),
                            last_log_index,
                            last_log_term,
                        };
                        debug!(
                            "Response candidate_{:?} with response: {:?}",
                            candidate_id, response
                        );

                        sender.send(Ok(response)).map_err(|e| {
                            error!("Failed to send: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{:?}", e))
                        })?;
                    }
                    Err(e) => {
                        let response = VoteResponse {
                            term: my_term,
                            vote_granted: false,
                            last_log_index,
                            last_log_term,
                        };
                        sender.send(Ok(response)).map_err(|e| {
                            error!("Failed to send: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{:?}", e))
                        })?;
                        error("handle_inbound_event::InboundEvent::ReceiveVoteRequest", &e);
                        return Err(e);
                    }
                }
            }

            InboundEvent::ClusterConf(_metadata_request, sender) => {
                let cluster_conf = ctx
                    .membership()
                    .retrieve_cluster_membership_config(self.shared_state().current_leader());
                debug!("Follower receive ClusterConf: {:?}", &cluster_conf);

                sender.send(Ok(cluster_conf)).map_err(|e| {
                    error!("Failed to send: {:?}", e);
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;
            }

            InboundEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version();

                let current_leader_id = self.shared_state().current_leader();

                debug!(?current_leader_id, %current_conf_version, ?cluste_conf_change_request,
                    "Follower receive ClusterConfUpdate"
                );

                let my_id = self.node_id();
                let response = match ctx
                    .membership()
                    .update_cluster_conf_from_leader(
                        my_id,
                        my_term,
                        current_conf_version,
                        current_leader_id,
                        &cluste_conf_change_request,
                    )
                    .await
                {
                    Ok(res) => res,
                    Err(e) => {
                        error!(?e, "update_cluster_conf_from_leader");
                        ClusterConfUpdateResponse::internal_error(
                            my_id,
                            my_term,
                            current_conf_version,
                        )
                    }
                };

                debug!(
                    "[peer-{}] update_cluster_conf_from_leader response: {:?}",
                    my_id, &response
                );
                sender.send(Ok(response)).map_err(|e| {
                    error!("Failed to send: {:?}", e);
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;
            }
            InboundEvent::AppendEntries(append_entries_request, sender) => {
                self.handle_append_entries_request_workflow(
                    append_entries_request,
                    sender,
                    ctx,
                    internal_event_tx,
                    &state_snapshot,
                )
                .await?;
            }

            InboundEvent::InstallSnapshotChunk(stream, sender) => {
                self.handle_install_snapshot_chunk_workflow(
                    stream,
                    sender,
                    ctx,
                    internal_event_tx,
                    &state_snapshot,
                )
                .await?;
            }

            InboundEvent::JoinCluster(_join_request, sender) => {
                sender
                    .send(Err(Status::permission_denied(
                        "Follower should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {:?}", e);
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Follower",
                    required_role: "Leader",
                    context: format!(
                        "Follower node {} receives InboundEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            InboundEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Follower::InboundEvent::DiscoverLeader");
                let response = match self.shared_state().current_leader() {
                    Some(leader_id) => match ctx.membership().retrieve_node_meta(leader_id) {
                        Some(meta) => Ok(LeaderDiscoveryResponse {
                            leader_id,
                            leader_address: meta.address,
                            term: self.current_term(),
                        }),
                        None => Err(Status::not_found("Leader metadata not found")),
                    },
                    None => Err(Status::unavailable("Leader not discovered")),
                };

                sender.send(response).map_err(|e| {
                    error!("Failed to send: {:?}", e);
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;

                return Ok(());
            }

            InboundEvent::FatalError { source, error } => {
                error!("[Follower] Fatal error from {}: {}", source, error);
                return Err(crate::Error::Fatal(format!(
                    "Fatal error from {source}: {error}"
                )));
            }
        }

        Ok(())
    }

    async fn handle_apply_completed(
        &mut self,
        last_index: u64,
        _results: Vec<crate::ApplyResult>,
        ctx: &RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> crate::Result<()> {
        // Per Raft §7: each server takes snapshots independently.
        check_and_trigger_snapshot(
            last_index,
            Follower as i32,
            self.current_term(),
            ctx,
            internal_event_tx,
        )
    }

    fn snapshot_in_progress(&self) -> Option<&AtomicBool> {
        Some(&self.snapshot_in_progress)
    }

    async fn handle_snapshot_created(
        &mut self,
        result: crate::Result<(SnapshotMetadata, std::path::PathBuf)>,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        self.snapshot_in_progress.store(false, Ordering::SeqCst);
        match result {
            Err(e) => error!(?e, "Follower snapshot creation failed"),
            Ok((metadata, _)) => {
                if let Some(last_included) = metadata.last_included {
                    schedule_and_execute_purge(
                        last_included,
                        ctx,
                        self.commit_index(),
                        self.last_purged_index,
                        &mut self.pending_purge_upto,
                        internal_event_tx,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    // Stale in-flight events — these are Leader-only operations that may arrive
    // after a step-down due to internal_event_tx (unbounded, P2) race with BecomeFollower.
    // Both orderings are valid; Follower silently ignores them rather than erroring.
    fn handle_log_purge_completed(
        &mut self,
        purged_id: d_engine_proto::common::LogId,
    ) -> Result<()> {
        if self.last_purged_index.is_none_or(|cur| purged_id.index > cur.index) {
            self.last_purged_index = Some(purged_id);

            // purge completed, clear to prevent re-execution
            self.pending_purge_upto = None;
        }
        Ok(())
    }

    async fn handle_promote_ready_learners(
        &mut self,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        Ok(())
    }

    async fn handle_membership_applied(
        &mut self,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        Ok(())
    }

    fn pending_purge_upto_mut(&mut self) -> Option<&mut Option<LogId>> {
        Some(&mut self.pending_purge_upto)
    }

    fn pending_append_acks_mut(
        &mut self
    ) -> Option<&mut std::collections::BTreeMap<u64, super::role_state::PendingAck>> {
        Some(&mut self.pending_append_acks)
    }
}

impl<T: TypeConfig> FollowerState<T> {
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
        hard_state_from_db: Option<HardState>,
        last_applied_index_option: Option<u64>,
    ) -> Self {
        trace!(
            node_config.raft.election.election_timeout_min,
            "FollowerState::new"
        );

        Self {
            shared_state: SharedState::new(node_id, hard_state_from_db, last_applied_index_option),
            timer: ElectionTimer::new((
                node_config.raft.election.election_timeout_min,
                node_config.raft.election.election_timeout_max,
            )),
            node_config,
            pending_append_acks: BTreeMap::new(),
            snapshot_in_progress: AtomicBool::new(false),
            _marker: PhantomData,
            last_purged_index: None,
            pending_purge_upto: None,
        }
    }

    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            role: Follower as i32,
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for FollowerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            timer: ElectionTimer::new((
                candidate_state.node_config.raft.election.election_timeout_min,
                candidate_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: candidate_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            pending_append_acks: BTreeMap::new(),
            last_purged_index: candidate_state.last_purged_index,
            // scheduled_purge_upto: None,
            _marker: PhantomData,
            pending_purge_upto: None,
        }
    }
}
impl<T: TypeConfig> From<&LeaderState<T>> for FollowerState<T> {
    fn from(leader_state: &LeaderState<T>) -> Self {
        Self {
            shared_state: leader_state.shared_state.clone(),
            timer: ElectionTimer::new((
                leader_state.node_config.raft.election.election_timeout_min,
                leader_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: leader_state.node_config.clone(),
            pending_append_acks: BTreeMap::new(),
            snapshot_in_progress: AtomicBool::new(
                leader_state.snapshot_in_progress.load(Ordering::SeqCst),
            ),
            last_purged_index: leader_state.last_purged_index,
            // scheduled_purge_upto: None,
            pending_purge_upto: leader_state.scheduled_purge_upto,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&LearnerState<T>> for FollowerState<T> {
    fn from(learner_state: &LearnerState<T>) -> Self {
        Self {
            //TODO: should we copy or new?
            shared_state: learner_state.shared_state.clone(),
            timer: ElectionTimer::new((
                learner_state.node_config.raft.election.election_timeout_min,
                learner_state.node_config.raft.election.election_timeout_max,
            )),
            node_config: learner_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            pending_append_acks: BTreeMap::new(),
            last_purged_index: learner_state.last_purged_index,
            pending_purge_upto: learner_state.pending_purge_upto,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> Drop for FollowerState<T> {
    fn drop(&mut self) {}
}

impl<T: TypeConfig> Debug for FollowerState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("FollowerState")
            .field("shared_state", &self.shared_state)
            .finish()
    }
}
