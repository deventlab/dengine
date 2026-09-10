use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use super::candidate_state::CandidateState;
use super::follower_state::FollowerState;
use super::role_state::RaftRoleState;
use super::role_state::check_and_trigger_snapshot;
use crate::ConsensusError;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::RaftContext;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::StateTransitionError;
use crate::Transport;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::cluster_printer::print_learner_join_success;
use crate::cluster_printer::print_learner_promoted_to_voter;
use crate::cluster_printer::print_role_transition_line;
use crate::role_state::PendingAck;
use crate::role_state::schedule_and_execute_purge;
use async_trait::async_trait;
use d_engine_proto::common::LogId;
use d_engine_proto::common::NodeRole;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeRole::Learner;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::storage::SnapshotMetadata;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::mpsc::{self};
use tokio::time::Instant;
use tonic::Status;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

/// Learner node's state in Raft cluster.
///
/// This state contains both:
/// - **Persistent State**: Should be written to stable storage before responding to RPCs
/// - **Volatile State**: Reinitialized after node restarts
///
/// Learners are non-voting members participating in log replication but not in leader election.
/// This state tracks the minimal required information for log synchronization.
///
/// # Type Parameters
/// - `T`: Application-specific Raft type configuration
pub struct LearnerState<T: TypeConfig> {
    // -- Core State --
    /// Shared cluster state with concurrency control
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

    /// === Persistent State (MUST be on disk) ===
    /// The last log position that has been **physically removed** from stable storage.
    ///
    /// This value is atomically updated when:
    /// 1. A new snapshot is persisted (marking logs up to `last_included_index` as purgeable)
    /// 2. The background purge task completes successfully
    ///
    /// Raft safety invariant:
    /// Any log entry with index ≤ `last_purged_index` is guaranteed to be
    /// reflected in the latest snapshot.
    pub last_purged_index: Option<LogId>,

    /// AppendEntries responses withheld pending this node's own durable_index.
    /// See `role_state::PendingAck`.
    pending_append_acks: BTreeMap<u64, PendingAck>,

    // -- Snapshot Management --
    /// Prevents concurrent snapshot creation
    ///
    /// Per industry best practices :
    /// - Protects against concurrent snapshot requests
    /// - Ensures snapshot consistency at Raft layer
    /// - Reduces unnecessary tokio::spawn overhead
    pub(crate) snapshot_in_progress: AtomicBool,

    // -- Cluster Configuration --
    /// Cached Raft node configuration (immutable shared reference)
    ///
    /// Contains:
    /// - Cluster membership topology
    /// - Timeout parameters
    /// - Performance tuning parameters
    pub(super) node_config: Arc<RaftNodeConfig>,

    // -- Type System Marker --
    /// Phantom type marker for compile-time validation
    _marker: PhantomData<T>,
}

#[async_trait]
impl<T: TypeConfig> RaftRoleState for LearnerState<T> {
    type T = T;

    fn shared_state(&self) -> &SharedState {
        &self.shared_state
    }

    fn shared_state_mut(&mut self) -> &mut SharedState {
        &mut self.shared_state
    }

    fn become_leader(&self) -> Result<RaftRole<T>> {
        error!("become_leader Illegal. I am Learner");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<T>> {
        warn!("become_candidate Illegal. I am Learner");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_follower(&self) -> Result<RaftRole<T>> {
        info!(
            "Node {} term {} transitioning to Follower",
            self.node_id(),
            self.current_term(),
        );
        print_role_transition_line(Learner as i32, Follower as i32, self.current_term());

        // Print promotion message (Plan B)
        print_learner_promoted_to_voter(self.node_id());

        Ok(RaftRole::Follower(Box::new(self.into())))
    }
    fn become_learner(&self) -> Result<RaftRole<T>> {
        warn!("I am Learner already");

        Err(StateTransitionError::InvalidTransition.into())
    }

    /// As Leader should not vote any more
    fn voted_for(&self) -> Result<Option<VotedFor>> {
        warn!("voted_for - As Learner should not vote any more.");

        Err(StateTransitionError::InvalidTransition.into())
    }
    //--- None state behaviors
    fn is_timer_expired(&self) -> bool {
        false
    }
    fn reset_timer(&mut self) {
        // Nothing to do for Learner
    }

    fn next_deadline(&self) -> Instant {
        Instant::now() + Duration::from_secs(24 * 60 * 60) //1 day
    }

    async fn tick(
        &mut self,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        _raft_tx: &mpsc::Sender<InboundEvent>,
        _ctx: &RaftContext<T>,
    ) -> Result<()> {
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
                info!("handle_inbound_event::ReceiveVoteRequest. Learner cannot vote.");
                // 1. Update term FIRST if needed
                if vote_request.term > my_term {
                    self.commit_hard_state(ctx, Some(vote_request.term), None)?;
                }

                // 2. Response sender with vote_granted=false
                let last_log_id =
                    ctx.raft_log().last_log_id().unwrap_or(LogId { index: 0, term: 0 });
                let response = VoteResponse {
                    term: my_term,
                    vote_granted: false,
                    last_log_index: last_log_id.index,
                    last_log_term: last_log_id.term,
                };
                debug!(
                    "Response candidate_{:?} with response: {:?}",
                    vote_request.candidate_id, response
                );

                sender.send(Ok(response)).map_err(|e| {
                    error!("Failed to send: {e:?}");
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;
            }

            InboundEvent::ClusterConf(_metadata_request, sender) => {
                debug!("Learner receive ClusterConf request...");
                sender
                    .send(Err(Status::permission_denied(
                        "Not able to respond to cluster conf request as node is Learner",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {e:?}");
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;
            }

            InboundEvent::ClusterConfUpdate(cluste_conf_change_request, sender) => {
                let current_conf_version = ctx.membership().get_cluster_conf_version();

                let current_leader_id = self.shared_state().current_leader();

                debug!(?current_leader_id, %current_conf_version, ?cluste_conf_change_request,
                    "Learner receive ClusterConfUpdate"
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
                    error!("Failed to send: {e:?}");
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
                        "Learner should not receive JoinCluster event.",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {e:?}");
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Err(ConsensusError::RoleViolation {
                    current_role: "Learner",
                    required_role: "Leader",
                    context: format!(
                        "Learner node {} receives InboundEvent::JoinCluster",
                        ctx.node_id
                    ),
                }
                .into());
            }

            InboundEvent::DiscoverLeader(request, sender) => {
                debug!(?request, "Learner::InboundEvent::DiscoverLeader");
                sender
                    .send(Err(Status::permission_denied(
                        "Learner should not response DiscoverLeader event.",
                    )))
                    .map_err(|e| {
                        error!("Failed to send: {e:?}");
                        NetworkError::SingalSendFailed(format!("{:?}", e))
                    })?;

                return Ok(());
            }

            InboundEvent::FatalError { source, error } => {
                error!("[Learner] Fatal error from {}: {}", source, error);
                return Err(crate::Error::Fatal(format!(
                    "Fatal error from {source}: {error}"
                )));
            }
        }

        Ok(())
    }

    async fn join_cluster(
        &self,
        ctx: &RaftContext<T>,
    ) -> Result<()> {
        // 1. Check if there is a Leader address (as specified in the configuration)
        let membership = ctx.membership();
        let leader_id = match self.shared_state().current_leader() {
            None => {
                // 2. Trigger broadcast discovery
                self.broadcast_discovery(membership.clone(), ctx).await?
            }
            Some(leader_id) => leader_id,
        };

        debug!(%leader_id, "join_cluster, leadder_id");

        // 3. Continue the original Join process
        let node_config = ctx.node_config();

        // Get this node's configured status from initial_cluster
        // Node MUST be defined in initial_cluster with explicit status
        let node_status = node_config
            .cluster
            .initial_cluster
            .iter()
            .find(|n| n.id == node_config.cluster.node_id)
            .map(|n| n.status)
            .ok_or_else(|| MembershipError::JoinClusterFailed(node_config.cluster.node_id))?;

        let response = ctx
            .transport()
            .join_cluster(
                leader_id,
                JoinRequest {
                    node_id: node_config.cluster.node_id,
                    node_role: Learner as i32,
                    address: node_config.cluster.listen_address.to_string(),
                    status: node_status,
                },
                node_config.retry.join_cluster,
                membership.clone(),
            )
            .await?;

        debug!(?response, "transport::join_cluster");
        if !response.success {
            return Err(MembershipError::JoinClusterFailed(self.shared_state.node_id).into());
        }

        // 4. mark leader_id (hot-path: ~5ns atomic store)
        self.shared_state().set_current_leader(leader_id);

        // Print join success message (Plan B)
        print_learner_join_success(self.shared_state.node_id, leader_id);

        Ok(())
    }

    async fn handle_apply_completed(
        &mut self,
        last_index: u64,
        _results: Vec<crate::ApplyResult>,
        ctx: &crate::RaftContext<T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> crate::Result<()> {
        // Per Raft §7: each server takes snapshots independently.
        check_and_trigger_snapshot(
            last_index,
            Learner as i32,
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
            Err(e) => error!(?e, "Learner snapshot creation failed"),
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

    /// Membership config change applied to state — refresh any role-local derived state.
    /// Leader: invalidates `cluster_metadata` cache.
    /// Learner: checks if promoted to Voter; emits `BecomeFollower` if so.
    /// Default: no-op (Follower/Candidate have no derived state to refresh).
    async fn handle_membership_applied(
        &mut self,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        // Check if this learner has been promoted to Voter
        let my_id = self.node_id();
        let node_meta = ctx.membership().retrieve_node_meta(my_id);

        if let Some(meta) = node_meta {
            // Check if role is Voter (any role except LEARNER)
            // FOLLOWER=0, CANDIDATE=1, LEADER=2, LEARNER=3
            let is_voter = meta.role != NodeRole::Learner as i32;

            if is_voter {
                info!(
                    "Learner {} detected promotion to Voter (role={}), transitioning to Follower",
                    my_id, meta.role
                );

                // Transition to Follower role
                internal_event_tx.send(InternalEvent::BecomeFollower(None)).map_err(|e| {
                    error!("Failed to send BecomeFollower event: {e:?}");
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;
            } else {
                debug!(
                    "Learner {} still in Learner role (role={}) after MembershipApplied",
                    my_id, meta.role
                );
            }
        } else {
            warn!(
                "Learner {} not found in membership after MembershipApplied",
                my_id
            );
        }
        Ok(())
    }

    // Stale in-flight events — these are Leader-only operations that may arrive
    // after a step-down due to internal_event_tx (unbounded, P2) race with BecomeFollower.
    // Both orderings are valid; Learner silently ignores them rather than erroring.
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

    fn pending_purge_upto_mut(&mut self) -> Option<&mut Option<LogId>> {
        Some(&mut self.pending_purge_upto)
    }

    fn pending_append_acks_mut(&mut self) -> Option<&mut BTreeMap<u64, PendingAck>> {
        Some(&mut self.pending_append_acks)
    }
}

impl<T: TypeConfig> LearnerState<T> {
    /// The fun will retrieve current state snapshot
    pub fn state_snapshot(&self) -> StateSnapshot {
        StateSnapshot {
            current_term: self.current_term(),
            voted_for: None,
            commit_index: self.commit_index(),
            role: Learner as i32,
        }
    }
}

impl<T: TypeConfig> LearnerState<T> {
    pub fn new(
        node_id: u32,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        LearnerState {
            shared_state: SharedState::new(node_id, None, None),
            last_purged_index: None,
            snapshot_in_progress: AtomicBool::new(false),
            pending_append_acks: BTreeMap::new(),
            node_config,
            _marker: PhantomData,
            pending_purge_upto: None,
        }
    }

    pub async fn broadcast_discovery(
        &self,
        membership: Arc<MOF<T>>,
        ctx: &RaftContext<T>,
    ) -> Result<u32> {
        let retry_policy = ctx.node_config.retry.auto_discovery;
        let mut retry_count = 0;
        let mut current_delay = Duration::from_millis(retry_policy.base_delay_ms);

        let request = LeaderDiscoveryRequest {
            node_id: ctx.node_id,
            requester_address: ctx.node_config.cluster.listen_address.to_string(),
        };

        let rpc_enable_compression = ctx.node_config.raft.auto_join.rpc_enable_compression;
        loop {
            // Execute discovery attempt with timeout
            let discovery_result = tokio::time::timeout(
                Duration::from_millis(retry_policy.timeout_ms),
                ctx.transport.discover_leader(
                    request.clone(),
                    rpc_enable_compression,
                    membership.clone(),
                ),
            )
            .await;

            debug!(?discovery_result);

            match discovery_result {
                Ok(responses) => {
                    if let Some(leader_id) = self.select_valid_leader(responses?).await {
                        debug!(%leader_id, "find valid leader");
                        return Ok(leader_id);
                    }
                }
                Err(_) => {
                    warn!(
                        "Discovery request timed out after {}ms",
                        retry_policy.timeout_ms
                    );
                }
            }

            // Check retry limits
            retry_count += 1;
            if retry_policy.max_retries > 0 && retry_count >= retry_policy.max_retries {
                error!("Discovery failed after {} retries", retry_count);
                return Err(NetworkError::RetryTimeoutError(Duration::from_millis(
                    retry_policy.timeout_ms,
                ))
                .into());
            }

            // Calculate next backoff delay
            current_delay = Duration::from_millis(
                (current_delay.as_millis() as u64)
                    .saturating_mul(2) // Exponential backoff
                    .min(retry_policy.max_delay_ms), // Cap at maximum delay
            );

            debug!(
                "Retrying discovery in {}ms (attempt {}/{})",
                current_delay.as_millis(),
                retry_count,
                retry_policy.max_retries
            );

            tokio::time::sleep(current_delay).await;
        }
    }

    /// @return Option<(leader_id, Channel)>
    pub async fn select_valid_leader(
        &self,
        responses: Vec<LeaderDiscoveryResponse>,
    ) -> Option<u32> {
        // Filter invalid responses
        let mut valid_responses: Vec<_> =
            responses.into_iter().filter(|r| r.leader_id != 0 && r.term > 0).collect();

        if valid_responses.is_empty() {
            return None;
        }

        // Sort by term in descending order, node_id in descending order
        valid_responses
            .sort_by(|a, b| b.term.cmp(&a.term).then_with(|| b.leader_id.cmp(&a.leader_id)));

        trace!(?valid_responses);

        // Select the response with the highest term
        let resp = valid_responses.first().unwrap();

        Some(resp.leader_id)
    }
}
impl<T: TypeConfig> From<&FollowerState<T>> for LearnerState<T> {
    fn from(follower_state: &FollowerState<T>) -> Self {
        Self {
            shared_state: follower_state.shared_state.clone(),
            node_config: follower_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            pending_append_acks: BTreeMap::new(),
            last_purged_index: follower_state.last_purged_index,
            pending_purge_upto: follower_state.pending_purge_upto,
            _marker: PhantomData,
        }
    }
}
impl<T: TypeConfig> From<&CandidateState<T>> for LearnerState<T> {
    fn from(candidate_state: &CandidateState<T>) -> Self {
        Self {
            shared_state: candidate_state.shared_state.clone(),
            node_config: candidate_state.node_config.clone(),
            snapshot_in_progress: AtomicBool::new(false),
            pending_append_acks: BTreeMap::new(),
            last_purged_index: candidate_state.last_purged_index,
            pending_purge_upto: None,
            _marker: PhantomData,
        }
    }
}

impl<T: TypeConfig> Debug for LearnerState<T> {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("LearnerState")
            .field("shared_state", &self.shared_state)
            .finish()
    }
}
