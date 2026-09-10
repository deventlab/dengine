use super::RaftRole;
use super::SharedState;
use super::StateSnapshot;
use crate::AppendResponseWithUpdates;
use crate::ConsensusError;
use crate::InboundEvent;
use crate::InternalEvent;
use crate::MaybeCloneOneshotSender;
use crate::Membership;
use crate::MembershipError;
use crate::NetworkError;
use crate::NewCommitData;
use crate::PurgeExecutor;
use crate::RaftContext;
use crate::RaftLog;
use crate::ReplicationCore;
use crate::Result;
use crate::SnapshotApplyResult;
use crate::StateMachineHandler;
use crate::StateTransitionError;
use crate::TypeConfig;
use crate::client::ClientReadRequest;
use crate::client::ClientResponse;
use crate::client::KvEntry;
use crate::client::LeaderHint;
use crate::event::ClientCmd;
use crate::scoped_timer::ScopedTimer;
use crate::utils::cluster::error;
use async_trait::async_trait;
use d_engine_proto::common::LogId;
use d_engine_proto::server::election::VotedFor;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::replication::SuccessResult;
use d_engine_proto::server::replication::append_entries_response;
use d_engine_proto::server::storage::SnapshotAck;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::SnapshotResponse;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;
use tracing::error;
use tracing::warn;
use tracing::{debug, info};

/// Per-peer replication trust state (#436): gates optimistic `next_index` advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PeerReplicationState {
    /// Unconfirmed: at most one AppendEntries in flight, no speculative advance.
    Probe,
    /// Confirmed caught up by a real ACK: safe to speculatively advance next_index.
    Replicate,
    /// A full snapshot push to this peer is in flight. Leader must not generate or
    /// dispatch any AppendEntries for this peer until SnapshotPushCompleted resolves
    /// it back to Probe — regardless of success or failure.
    Snapshot,
}

/// A success `AppendEntriesResponse` that has been computed but not yet sent,
/// because this node's own `durable_index` had not reached the index the response
/// claims. Held until fsync catches up, so an ACK never asserts durability the
/// node cannot yet guarantee (RPO=0, #446).
///
/// Keyed by the claimed index. `senders` accumulates when more than one request
/// claims the same index (a leader retry, or a heartbeat landing on the tail).
///
/// The response body is not stored: it is rebuilt on release, after re-checking
/// the term it was withheld under. A response frozen under a term the node has
/// since left must never be sent.
pub(crate) struct PendingAck {
    pub(crate) claimed_term: u64,
    pub(crate) term_when_withheld: u64,
    pub(crate) senders:
        Vec<MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>>,
}

/// Send the terminal response for one withheld ACK — a rebuilt success if
/// `confirm`, a conflict otherwise — to every accumulated sender.
fn resolve_pending_ack(
    node_id: u32,
    index: u64,
    ack: PendingAck,
    confirm: bool,
    current_term: u64,
) {
    let response = if confirm {
        AppendEntriesResponse::success(
            node_id,
            current_term,
            Some(LogId {
                index,
                term: ack.claimed_term,
            }),
        )
    } else {
        AppendEntriesResponse::conflict(node_id, current_term, None, None)
    };
    for sender in ack.senders {
        if let Err(e) = sender.send(Ok(response)) {
            error!("withheld AppendEntries ACK (index {index}): send failed: {e:?}");
        }
    }
}

/// Fail every withheld ACK with a conflict response. Used when the queue passes to
/// a role that cannot hold it (Candidate or Leader): the node no longer recognises
/// the leader those ACKs were owed to, so that leader's replication worker should
/// retry now rather than wait out an RPC timeout.
pub(crate) fn reject_pending_acks(
    acks: BTreeMap<u64, PendingAck>,
    node_id: u32,
    current_term: u64,
) {
    for (index, ack) in acks {
        resolve_pending_ack(node_id, index, ack, false, current_term);
    }
}

#[async_trait]
pub(crate) trait RaftRoleState: Send + Sync + 'static {
    type T: TypeConfig;

    //--- For sharing state behaviors
    fn shared_state(&self) -> &SharedState;
    fn shared_state_mut(&mut self) -> &mut SharedState;
    fn node_id(&self) -> u32 {
        self.shared_state().node_id
    }

    // Leader states
    #[allow(dead_code)]
    fn next_index(
        &self,
        _node_id: u32,
    ) -> Option<u64> {
        warn!("next_index NotLeader error");
        None
    }
    fn update_next_index(
        &mut self,
        _node_id: u32,
        _new_next_id: u64,
    ) -> Result<()> {
        warn!("update_next_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    fn match_index(
        &self,
        _node_id: u32,
    ) -> Option<u64> {
        warn!("match_index NotLeader error");
        None
    }
    fn update_match_index(
        &mut self,
        _node_id: u32,
        _new_match_id: u64,
    ) -> Result<()> {
        warn!("update_match_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }
    fn init_peers_next_index_and_match_index(
        &mut self,
        _last_log_id: u64,
        _node_ids: Vec<u32>,
    ) -> Result<()> {
        warn!("init_peers_next_index_and_match_index NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    /// Handle a ZombieDetected signal. No-op for non-leader roles.
    async fn handle_zombie_detected(
        &mut self,
        _node_id: u32,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        _ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        // Default: no-op for non-leader roles
        Ok(())
    }

    /// Update per-peer snapshot push backoff state and emit an alert when persistent
    /// failures exceed the configured threshold. No-op for non-leader roles.
    fn handle_snapshot_push_completed(
        &mut self,
        _peer_id: u32,
        _success: bool,
        _policy: &crate::InstallSnapshotBackoffPolicy,
        _node_id: u32,
    ) {
        // Default: no-op for non-leader roles
    }

    /// Initialize cluster metadata cache (only relevant for Leader)
    async fn init_cluster_metadata(
        &mut self,
        _membership: &std::sync::Arc<<Self::T as crate::TypeConfig>::M>,
    ) -> Result<()> {
        // Default: no-op for non-leader roles
        Ok(())
    }

    #[allow(dead_code)]
    fn noop_log_id(&self) -> Result<Option<u64>> {
        warn!("noop_log_id NotLeader error");
        Err(MembershipError::NotLeader.into())
    }

    async fn join_cluster(
        &self,
        _ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        warn!("join_cluster NotLearner error");
        Err(MembershipError::NotLearner.into())
    }

    fn become_leader(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_leader Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_candidate(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_candidate Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_follower(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_follower Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }
    fn become_learner(&self) -> Result<RaftRole<Self::T>> {
        warn!("become_learner Illegal");

        Err(StateTransitionError::InvalidTransition.into())
    }

    //--- Shared States
    fn current_term(&self) -> u64 {
        self.shared_state().current_term()
    }
    #[cfg(test)]
    fn update_current_term(
        &mut self,
        term: u64,
    ) {
        self.shared_state_mut().update_current_term(term)
    }

    fn commit_index(&self) -> u64 {
        self.shared_state().commit_index
    }

    fn update_commit_index(
        &mut self,
        new_commit_index: u64,
    ) -> Result<()> {
        if self.commit_index() != new_commit_index {
            debug!("update_commit_index to: {:?}", new_commit_index);
            self.shared_state_mut().commit_index = new_commit_index;
        }
        Ok(())
    }

    fn update_commit_index_with_signal(
        &mut self,
        role: i32,
        current_term: u64,
        new_commit_index: u64,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        if let Err(e) = self.update_commit_index(new_commit_index) {
            error!("Follower::update_commit_index: {:?}", e);
            return Err(e);
        }
        debug!(
            "[Node-{}] update_commit_index_with_signal, new_commit_index: {:?}",
            self.node_id(),
            new_commit_index
        );

        internal_event_tx
            .send(InternalEvent::NotifyNewCommitIndex(NewCommitData {
                new_commit_index,
                role,
                current_term,
            }))
            .map_err(|e| {
                error!("Failed to send NotifyNewCommitIndex: {e:?}");
                NetworkError::SingalSendFailed(format!("{:?}", e))
            })?;

        Ok(())
    }

    fn voted_for(&self) -> Result<Option<VotedFor>> {
        self.shared_state().voted_for()
    }

    /// Only sanctioned way to change current_term/voted_for: mutate + persist in
    /// one call. Persist failure propagates via `?`, no rollback.
    fn commit_hard_state(
        &mut self,
        ctx: &RaftContext<Self::T>,
        term: Option<u64>,
        voted_for: Option<VotedFor>,
    ) -> Result<bool> {
        if term.is_none() && voted_for.is_none() {
            return Ok(false);
        }
        let result = self.shared_state_mut().set_hard_state(term, voted_for);
        if result.changed {
            ctx.raft_log().save_hard_state(&self.shared_state().hard_state())?;
        }
        Ok(result.is_new_leader_commitment)
    }

    /// Reset counterpart to `commit_hard_state()` — clears voted_for and
    /// persists. Needed because `commit_hard_state`'s `Option<VotedFor>` can't
    /// distinguish "don't touch" from "clear to None".
    fn commit_vote_reset(
        &mut self,
        ctx: &RaftContext<Self::T>,
    ) -> Result<()> {
        self.shared_state_mut().clear_voted_for();
        ctx.raft_log().save_hard_state(&self.shared_state().hard_state())?;
        Ok(())
    }

    //--- Timer related ---
    fn next_deadline(&self) -> Instant;
    fn is_timer_expired(&self) -> bool;

    fn reset_timer(&mut self);

    async fn tick(
        &mut self,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
        event_tx: &mpsc::Sender<InboundEvent>,
        ctx: &RaftContext<Self::T>,
    ) -> Result<()>;

    async fn handle_inbound_event(
        &mut self,
        inbound_event: InboundEvent,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()>;

    /// Push single client command directly to role's internal buffer (zero-copy).
    ///
    /// For Leader: push to batch_buffer (writes) or read_buffer (reads)
    /// For non-Leader:
    ///   - Writes: immediately reject with NOT_LEADER error
    ///   - Eventual reads: serve locally from state machine
    ///   - Linear/Lease reads: immediately reject with NOT_LEADER error
    fn push_client_cmd(
        &mut self,
        cmd: ClientCmd,
        ctx: &RaftContext<Self::T>,
    ) {
        use crate::ReadConsistencyPolicy as ServerReadConsistencyPolicy;
        use crate::config::ReadConsistencyPolicy as ClientReadConsistencyPolicy;

        match cmd {
            ClientCmd::Propose(_, sender) => {
                // Writes always require leader - reject immediately
                let _ = sender.send(Ok(self.create_not_leader_response(ctx)));
            }
            ClientCmd::Scan(_, sender) => {
                // Scan requires leader (linearizable by default); reply with
                // NotLeader + best-effort leader hint for redirect, same as Propose/Read.
                let _ = sender.send(Ok(self.create_not_leader_response(ctx)));
            }
            ClientCmd::Read(req, sender) => {
                // Determine effective read policy, mirroring leader_state logic:
                // 1. If client specified a policy AND server allows override, use client policy.
                // 2. Otherwise (no policy, or override disabled), use server default.
                let effective_policy = if let Some(ref policy) = req.consistency_policy
                    && ctx.node_config().raft.read_consistency.allow_client_override
                {
                    match policy {
                        ClientReadConsistencyPolicy::EventualConsistency => {
                            ServerReadConsistencyPolicy::EventualConsistency
                        }
                        _ => {
                            // Linear/Lease requires leader
                            let _ = sender.send(Ok(self.create_not_leader_response(ctx)));
                            return;
                        }
                    }
                } else {
                    // No client policy, or client override not allowed — use server default
                    ctx.node_config().raft.read_consistency.default_policy.clone()
                };

                match effective_policy {
                    ServerReadConsistencyPolicy::EventualConsistency => {
                        self.process_eventual_read_local(req, sender, ctx);
                    }
                    _ => {
                        // Linear/Lease requires leader
                        let _ = sender.send(Ok(self.create_not_leader_response(ctx)));
                    }
                }
            }
        }
    }

    /// Process eventual consistency read locally (available on all nodes)
    ///
    /// Eventual consistency reads can be served by any node (leader, follower, candidate, learner)
    /// directly from the local state machine without additional consistency checks.
    /// May return stale data but provides best read performance and availability.
    fn process_eventual_read_local(
        &self,
        req: ClientReadRequest,
        sender: crate::MaybeCloneOneshotSender<std::result::Result<ClientResponse, tonic::Status>>,
        ctx: &RaftContext<Self::T>,
    ) {
        // Read directly from local state machine without any consistency checks
        let results: Vec<KvEntry> = ctx
            .state_machine_handler()
            .read_from_state_machine(req.keys)
            .unwrap_or_default();

        let response = ClientResponse::read_results(results);
        let _ = sender.send(Ok(response));
    }

    /// Flush command buffers if size or timeout thresholds are reached.
    ///
    /// Checks both write and read buffers using BatchBuffer::should_flush().
    /// For Leader: processes batches if FlushReason::SizeThreshold or FlushReason::Timeout
    /// For non-Leader: no-op (buffers are empty)
    async fn flush_cmd_buffers(
        &mut self,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        // Default implementation for non-leader: nothing to flush
        Ok(())
    }

    /// Handle ApplyCompleted: state machine has applied entries up to `last_index`.
    /// Leader: sends client responses + serves pending linearizable reads + checks snapshot.
    /// Follower/Learner: checks snapshot trigger.
    /// Default: no-op for Candidate (transient state; snapshot deferred to next stable role).
    async fn handle_apply_completed(
        &mut self,
        _last_index: u64,
        _results: Vec<crate::ApplyResult>,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        Ok(())
    }

    /// Release withheld AppendEntries ACKs now that the log is durable through
    /// `durable`. For each withheld ACK:
    ///
    /// - withheld under a term this node has since left → fail it with a conflict.
    ///   A higher-term leader may have overwritten the log at that index; within a
    ///   single term a follower's entries are never replaced, so the term check
    ///   alone is a sufficient content guard and no log lookup is needed.
    /// - claimed index now `<= durable` → rebuild and send the success.
    /// - otherwise → keep waiting.
    ///
    /// No-op for Candidate/Leader (no queue). Runs on every fsync completion, so it
    /// stays limited to integer comparisons — no log lookup. (#446)
    fn resolve_pending_acks(
        &mut self,
        durable: u64,
    ) {
        let node_id = self.node_id();
        let current_term = self.current_term();
        let Some(pending) = self.pending_append_acks_mut() else {
            return;
        };
        if pending.is_empty() {
            return;
        }
        let resolved: Vec<(u64, bool)> = pending
            .iter()
            .filter_map(|(&index, ack)| {
                if ack.term_when_withheld != current_term {
                    Some((index, false)) // stale term -> conflict
                } else if index <= durable {
                    Some((index, true)) // durable -> success
                } else {
                    None // keep waiting
                }
            })
            .collect();
        for (index, confirm) in resolved {
            if let Some(ack) = pending.remove(&index) {
                resolve_pending_ack(node_id, index, ack, confirm, current_term);
            }
        }
    }

    /// A batch of log entries reached `durable` on disk (fsync complete).
    ///
    /// Follower/Learner: release any withheld AppendEntries ACKs this now covers.
    /// Leader: overridden to recalculate `commit_index`. Candidate: no-op.
    async fn handle_log_flushed(
        &mut self,
        durable: u64,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) {
        self.resolve_pending_acks(durable);
    }

    /// Handle AppendEntries result from a per-follower ReplicationWorker.
    /// Leader: updates match_index, recalculates commit, drains pending_client_writes.
    /// Default: no-op for all non-leader roles (stale results arriving after step-down).
    async fn handle_append_result(
        &mut self,
        _follower_id: u32,
        _result: crate::Result<d_engine_proto::server::replication::AppendEntriesResponse>,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> crate::Result<()> {
        // Non-leader: stale result arrived after step-down — ignore safely.
        Ok(())
    }

    /// Create NOT_LEADER response with leader metadata for client redirection
    ///
    /// This method queries the cluster membership to get the current leader's
    /// ID and address, then returns a ClientResponse with ErrorMetadata populated.
    /// If no leader information is available, returns a basic NOT_LEADER error.
    ///
    /// Default implementation can be overridden by specific role states if needed.
    ///
    /// # Returns
    /// ClientResponse with NOT_LEADER error code and optional leader metadata
    fn create_not_leader_response(
        &self,
        ctx: &RaftContext<Self::T>,
    ) -> ClientResponse {
        let leader_id = self.shared_state().current_leader();

        if let Some(lid) = leader_id {
            // Get leader address from membership
            let leader_node = ctx.membership().get_address(lid);

            if let Some(address) = leader_node {
                return ClientResponse::not_leader(Some(LeaderHint {
                    leader_id: lid,
                    address,
                }));
            }
        }

        // No leader info available, return basic NOT_LEADER error
        ClientResponse::not_leader(None)
    }

    /// When a Follower receives an AppendEntries request, it performs the following logic (as
    /// described in Section 5.1 of the Raft paper):
    ///
    /// - If the term T in the request is greater than
    /// the Followers current term, the Follower updates its term and reverts to the Follower state.
    ///
    /// - If the term T in the request is less than the Followers current term, the Follower
    ///   responds with a HigherTerm reply
    async fn handle_append_entries_request_workflow(
        &mut self,
        append_entries_request: AppendEntriesRequest,
        senders: Vec<MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>>,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
        state_snapshot: &StateSnapshot,
    ) -> Result<()> {
        let _timer = ScopedTimer::new("handle_append_entries_request_workflow");
        debug!(
            "handle_inbound_event::InboundEvent::AppendEntries: {:?}",
            &append_entries_request
        );

        // Init response with success is false
        let raft_log_last_index = ctx.storage.raft_log.last_entry_id();

        let my_term = self.current_term();
        let term_update = match check_incoming_term(my_term, append_entries_request.term) {
            TermDecision::RejectStale => {
                let response = AppendEntriesResponse::higher_term(self.node_id(), my_term);
                debug!("AppendEntriesResponse: {:?}", response);
                for sender in senders {
                    if let Err(e) = sender.send(Ok(response)) {
                        error!("Failed to send client: {:?}", e);
                    }
                }
                return Ok(());
            }
            TermDecision::Accept => None,
            TermDecision::AcceptAndUpdateTerm(new_term) => Some(new_term),
        };

        // Legitimate leader (term >= ours) confirmed — reset now, not earlier.
        self.reset_timer();

        // Important to confirm heartbeat from Leader immediatelly
        let new_leader_id = append_entries_request.leader_id;
        let request_term = append_entries_request.term;

        // CRITICAL: capture is_new_leader BEFORE setting current_leader — after a
        // restart, current_leader is None (memory cleared), so commit_hard_state's
        // transition check (committed: false->true, leader/term change, or no
        // current leader) correctly re-fires LeaderDiscovered for wait_ready().
        let is_new_leader = self.commit_hard_state(
            ctx,
            term_update,
            Some(VotedFor {
                voted_for_id: new_leader_id,
                voted_for_term: request_term,
                committed: true,
            }),
        )?;

        // Keep syncing leader_id (hot-path: ~5ns atomic store vs ~50ns RwLock)
        self.shared_state().set_current_leader(new_leader_id);

        // Trigger leader discovery notification only on state transition —
        // now safely AFTER hard_state is durably persisted.
        if is_new_leader {
            internal_event_tx
                .send(InternalEvent::LeaderDiscovered(new_leader_id, request_term))
                .map_err(|e| {
                    error!("Failed to send LeaderDiscovered: {e:?}");
                    NetworkError::SingalSendFailed(format!("{:?}", e))
                })?;
        }

        // My term might be updated, has to fetch it again
        let my_term = self.current_term();

        // Handle replication request
        match ctx
            .replication_handler()
            .handle_append_entries(append_entries_request, state_snapshot, ctx.raft_log())
            .await
        {
            Ok(AppendResponseWithUpdates {
                response,
                commit_index_update,
            }) => {
                if let Some(commit) = commit_index_update
                    && let Err(e) = self.update_commit_index_with_signal(
                        state_snapshot.role,
                        state_snapshot.current_term,
                        commit,
                        &internal_event_tx,
                    )
                {
                    error!(
                        "update_commit_index_with_signal,commit={}, error: {:?}",
                        commit, e
                    );
                    return Err(e);
                }
                debug!("AppendEntriesResponse: {:?}", response);

                // RPO=0 (#446): a success response asserts the claimed entry is
                // fsync-durable on this node. If this node's own `durable_index`
                // has not reached that index, withhold the response until it does
                // (released by `resolve_pending_acks`). Conflict and higher-term
                // responses assert nothing about durability and are sent at once.
                let claim = match &response.result {
                    Some(append_entries_response::Result::Success(SuccessResult {
                        last_match: Some(log_id),
                    })) => Some((log_id.index, log_id.term)),
                    _ => None,
                };

                match claim {
                    Some((index, claimed_term)) if ctx.storage.raft_log.durable_index() < index => {
                        let term_when_withheld = self.current_term();
                        match self.pending_append_acks_mut() {
                            Some(pending) => {
                                pending
                                    .entry(index)
                                    .or_insert_with(|| PendingAck {
                                        claimed_term,
                                        term_when_withheld,
                                        senders: Vec::new(),
                                    })
                                    .senders
                                    .extend(senders);
                            }
                            None => {
                                // Only Follower and Learner produce a success
                                // response here, and both carry the queue. Reaching
                                // this arm means a role invariant broke — send the
                                // ACK now rather than strand the leader.
                                error!(
                                    "withheld a success ACK on a role with no pending-ACK queue"
                                );
                                for sender in senders {
                                    let _ = sender.send(Ok(response));
                                }
                            }
                        }
                    }
                    _ => {
                        for sender in senders {
                            if let Err(e) = sender.send(Ok(response)) {
                                error!("failed to send AppendEntries response: {e:?}");
                            }
                        }
                    }
                }
            }
            Err(e) => {
                // Conservatively fallback to a safe position, forcing the leader to retry or
                // trigger a snapshot. Return a Conflict response (conflict index =
                // current log length + 1)
                error!(
                    "Replication failed. Conservatively fallback to a safe position, forcing the leader to retry"
                );
                let response = AppendEntriesResponse::conflict(
                    self.node_id(),
                    my_term,
                    None,
                    Some(raft_log_last_index + 1),
                );
                debug!("AppendEntriesResponse: {:?}", response);

                for sender in senders {
                    if let Err(e) = sender.send(Ok(response)) {
                        error!("Failed to send: {:?}", e);
                    }
                }

                error("handle_inbound_event", &e);
                return Err(e);
            }
        }
        return Ok(());
    }

    /// Returns the purge watermark for roles that track it after an installed snapshot
    /// (Follower, Learner). Returns `None` for roles that don't call
    /// `handle_install_snapshot_chunk_workflow` (Candidate, Leader).
    fn pending_purge_upto_mut(&mut self) -> Option<&mut Option<LogId>> {
        None
    }

    /// Shared handling for a leader-pushed `InstallSnapshotChunk` stream. Only Follower
    /// and Learner call this — Candidate and Leader reject the event outright before
    /// ever reaching here. Mirrors `handle_append_entries_request_workflow`'s shape:
    /// validate leader term on the first chunk, hand the stream to the Worker, reply,
    /// then (on success) advance commit_index and schedule a purge.
    async fn handle_install_snapshot_chunk_workflow(
        &mut self,
        mut stream: mpsc::Receiver<SnapshotChunk>,
        sender: MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
        state_snapshot: &StateSnapshot,
    ) -> Result<()> {
        let my_term = self.current_term();

        // Validate leader identity/term on the first chunk, before reading the
        // rest of the stream — don't burn bandwidth on a stale/illegitimate leader.
        let first_chunk = match stream.recv().await {
            Some(chunk) => chunk,
            None => {
                warn!("InstallSnapshotChunk stream closed before any chunk arrived");
                let _ = sender.send(Ok(SnapshotResponse {
                    term: my_term,
                    success: false,
                    next_chunk: 0,
                }));
                return Ok(());
            }
        };

        let term_update = match check_incoming_term(my_term, first_chunk.leader_term) {
            TermDecision::RejectStale => {
                warn!(
                    my_term,
                    leader_term = first_chunk.leader_term,
                    leader_id = first_chunk.leader_id,
                    "Rejecting InstallSnapshotChunk from stale leader"
                );
                let _ = sender.send(Ok(SnapshotResponse {
                    term: my_term,
                    success: false,
                    next_chunk: 0,
                }));
                return Ok(());
            }
            TermDecision::Accept => None,
            TermDecision::AcceptAndUpdateTerm(new_term) => Some(new_term),
        };

        // Legitimate leader (term >= ours) confirmed — reset now, matching AppendEntries.
        self.reset_timer();

        self.commit_hard_state(
            ctx,
            term_update,
            Some(VotedFor {
                voted_for_id: first_chunk.leader_id,
                voted_for_term: first_chunk.leader_term,
                committed: true,
            }),
        )?;
        self.shared_state().set_current_leader(first_chunk.leader_id);
        let my_term = first_chunk.leader_term.max(my_term);

        // ack_tx drained in background — push mode never reads it, avoids backpressure (#308).
        let (ack_tx, mut ack_rx) = mpsc::channel::<SnapshotAck>(32);
        tokio::spawn(async move {
            let mut drained = 0u32;
            while ack_rx.recv().await.is_some() {
                drained += 1;
            }
            debug!(drained, "Snapshot ack channel drained and closed");
        });

        let prepared = match ctx
            .state_machine_handler()
            .prepare_snapshot_stream(first_chunk, stream, ack_tx, &ctx.node_config.raft.snapshot)
            .await
        {
            Ok(prepared) => prepared,
            Err(e) => {
                warn!(
                    ?e,
                    "Failed to receive/prepare snapshot stream from leader, node continues"
                );
                let _ = sender.send(Ok(SnapshotResponse {
                    term: my_term,
                    success: false,
                    next_chunk: 0,
                }));
                return Ok(());
            }
        };

        // Sole write path: hand the received snapshot to the Worker, await the result.
        let install_result = ctx.state_machine_commands().install_snapshot(prepared).await;

        // Raft §7: reply success only once the Worker has confirmed install (or a safe no-op).
        let _ = sender.send(Ok(SnapshotResponse {
            term: my_term,
            success: install_result.is_ok(),
            next_chunk: 0,
        }));

        match install_result {
            Err(e) => {
                if e.is_fatal() {
                    return Err(e);
                }
                warn!(?e, "Snapshot install failed, node continues");
            }
            Ok(result) => {
                info!("Snapshot stream successfully received and applied");

                let boundary = match result {
                    SnapshotApplyResult::Applied { last_included } => {
                        if last_included.index > self.commit_index()
                            && let Err(e) = self.update_commit_index_with_signal(
                                state_snapshot.role,
                                my_term,
                                last_included.index,
                                &internal_event_tx,
                            )
                        {
                            error!(?e, "Failed to advance commit_index after snapshot install");
                        }
                        last_included
                    }
                    SnapshotApplyResult::IgnoredStale { current } => current,
                    SnapshotApplyResult::IgnoredDuplicate { current } => current,
                };

                let Some(pending_purge_upto) = self.pending_purge_upto_mut() else {
                    return Err(ConsensusError::RoleViolation {
                        current_role: "unknown",
                        required_role: "Follower or Learner",
                        context: "Role without purge tracking attempted to install a snapshot."
                            .to_string(),
                    }
                    .into());
                };

                // Purge intent submitted after replying — must not delay or ride on the
                // leader response.
                if let Err(e) = schedule_installed_snapshot_purge(
                    boundary,
                    ctx,
                    pending_purge_upto,
                    &internal_event_tx,
                )
                .await
                {
                    error!(?e, "Failed to schedule purge after installed snapshot");
                }
            }
        }

        Ok(())
    }

    fn drain_read_buffer(&mut self) -> Result<()> {
        // No-op for non-leader roles (only Leader has read buffer to drain)
        Err(MembershipError::NotLeader.into())
    }

    /// Trigger an independent snapshot on this role (Raft §7 — each server snapshots
    /// independently). Called when `should_snapshot()` returns true after SM apply.
    /// Candidate short-circuits via `snapshot_in_progress()` returning `None`.
    async fn handle_create_snapshot(
        &mut self,
        ctx: &RaftContext<Self::T>,
        internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        let Some(flag) = self.snapshot_in_progress() else {
            return Err(ConsensusError::RoleViolation {
                current_role: "Candidate",
                required_role: "Follower/Leader/Learner",
                context: "Candidate node attempted to create snapshot.".to_string(),
            }
            .into());
        };

        if flag.load(Ordering::Acquire) {
            info!("Snapshot creation already in progress. Skipping duplicate request.");
            return Ok(());
        }

        let state_machine_handler = ctx.state_machine_handler().clone();

        // Second, handler-level guard for the same critical section: `flag` above lives
        // on this role instance and resets on role transition (e.g. Follower -> Candidate),
        // but the capture (via Worker round-trip) + build (background spawn) below is
        // `'static` and can outlive that transition. The handler is shared via `Arc` and
        // survives role changes, so this is the guard that's actually load-bearing across
        // a transition; `flag` just avoids redundant spawns within the same role.
        if state_machine_handler.try_begin_local_snapshot_capture().is_err() {
            info!("Snapshot capture already in progress on handler. Skipping duplicate request.");
            return Ok(());
        }

        flag.store(true, Ordering::Release);
        let sm_command_tx = ctx.state_machine_commands().clone();

        // Use spawn to perform snapshot creation in the background
        let internal_event_tx = internal_event_tx.clone();
        tokio::spawn(async move {
            let result = match sm_command_tx.capture_local_snapshot().await {
                Ok(captured) => state_machine_handler.build_local_snapshot(captured).await,
                Err(e) => Err(e),
            };
            state_machine_handler.end_local_snapshot_capture();

            info!("SnapshotCreated event will be processed in another event thread");
            if let Err(e) = internal_event_tx.send(InternalEvent::SnapshotCreated(result)) {
                error!("Failed to send snapshot creation result: {e:?}");
            }
        });

        Ok(())
    }

    /// Returns the snapshot deduplication flag for roles that support independent snapshot creation.
    ///
    /// Returns `None` for roles without snapshot capability (Candidate). The default
    /// `handle_create_snapshot` uses this to short-circuit with `RoleViolation` instead of
    /// silently operating on a dummy value.
    ///
    /// Leader, Follower, and Learner override this to return `Some(&self.snapshot_in_progress)`.
    fn snapshot_in_progress(&self) -> Option<&AtomicBool> {
        None
    }

    /// Process the completed snapshot result (success or error).
    /// Leader: schedules log purge up to `last_included`.
    /// Follower/Learner: updates local snapshot path.
    /// Default: no-op. Candidate overrides to return `RoleViolation`.
    async fn handle_snapshot_created(
        &mut self,
        _result: crate::Result<(SnapshotMetadata, std::path::PathBuf)>,
        _ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        Err(ConsensusError::RoleViolation {
            current_role: "Candidate",
            required_role: "Follower/Leader/Learner",
            context: "Candidate node attempted to handle created snapshot.".to_string(),
        }
        .into())
    }

    /// Advance the purge boundary after log entries up to `purged_id` are removed.
    /// Leader only: updates `last_purged_index`.
    /// Default: no-op + warn (unexpected on non-leader).
    fn handle_log_purge_completed(
        &mut self,
        _purged_id: LogId,
    ) -> Result<()> {
        Err(ConsensusError::RoleViolation {
            current_role: "non-Leader",
            required_role: "Leader",
            context: "LogPurgeCompleted is a Leader-only event.".to_string(),
        }
        .into())
    }

    /// Check pending learners for promotion eligibility after a membership change.
    /// Leader only: evaluates `pending_promotions`, proposes config change if ready.
    /// Default: no-op + warn (unexpected on non-leader).
    async fn handle_promote_ready_learners(
        &mut self,
        ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        Err(ConsensusError::RoleViolation {
            current_role: "None Leader",
            required_role: "Leader",
            context: format!(
                "None Leader node {} receives InternalEvent::PromoteReadyLearners",
                ctx.node_id
            ),
        }
        .into())
    }

    /// Node was removed from cluster membership; step down immediately per Raft protocol.
    /// Leader: emits `BecomeFollower`. Non-leader: unreachable in practice.
    /// Default: warn + Ok(()) (defensive; only Leader should receive this).
    fn handle_self_removed(
        &mut self,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        warn!(
            "Node {} received StepDownSelfRemoved in non-leader role — ignoring",
            self.node_id()
        );
        Ok(())
    }

    /// Membership config change applied to state — refresh any role-local derived state.
    /// Leader: invalidates `cluster_metadata` cache.
    /// Learner: checks if promoted to Voter; emits `BecomeFollower` if so.
    /// Default: no-op (Follower/Candidate have no derived state to refresh).
    async fn handle_membership_applied(
        &mut self,
        ctx: &RaftContext<Self::T>,
        _internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    ) -> Result<()> {
        // Followers don't maintain cluster metadata cache
        // This event is only relevant for leaders
        info!("Follower/Candidate node ignoring MembershipApplied event");
        Err(ConsensusError::RoleViolation {
            current_role: "None Leader/Learner",
            required_role: "Leader/Learner",
            context: format!(
                "None Leader/Learner node {} receives InternalEvent::PromoteReadyLearners",
                ctx.node_id
            ),
        }
        .into())
    }

    fn peer_replication_state(
        &self,
        _node_id: u32,
    ) -> PeerReplicationState {
        // Default: unknown peer, be conservative. Also the default for non-leader roles.
        PeerReplicationState::Probe
    }

    fn set_peer_replication_state(
        &mut self,
        _node_id: u32,
        _state: PeerReplicationState,
    ) {
    }

    /// The withheld-ACK queue, for the roles that keep one (Follower, Learner).
    /// `None` for Candidate and Leader. Carried across a Follower<->Learner
    /// transition by `RaftRole::take_pending_acks` / `restore_pending_acks` (#446).
    fn pending_append_acks_mut(&mut self) -> Option<&mut BTreeMap<u64, PendingAck>> {
        None
    }
}

/// Attempts to execute whatever purge target is currently pending, if any. Shared by both
/// purge paths (local snapshot creation and installed-snapshot cleanup) — on success sends
/// `LogPurgeCompleted` (cleared by the role's `handle_log_purge_completed`); on failure the
/// watermark stays set for the next caller to retry.
async fn execute_pending_purge<T: TypeConfig>(
    pending_purge_upto: &Option<LogId>,
    ctx: &RaftContext<T>,
    internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
) -> Result<()> {
    if let Some(scheduled) = *pending_purge_upto {
        match ctx.purge_executor().execute_purge(scheduled).await {
            Ok(_) => {
                if let Err(e) = internal_event_tx.send(InternalEvent::LogPurgeCompleted(scheduled))
                {
                    error!(%e, "Failed to notify purge completion");
                }
            }
            Err(e) => {
                error!(?e, ?scheduled, "Log purge execution failed");
                metrics::counter!(
                    "core.raft.log.purge_failures",
                    "node_id" => ctx.node_id.to_string()
                )
                .increment(1);
            }
        }
    }
    Ok(())
}

pub(super) async fn schedule_and_execute_purge<T: TypeConfig>(
    last_included: LogId,
    ctx: &RaftContext<T>,
    commit_index: u64,
    last_purged_index: Option<LogId>,
    pending_purge_upto: &mut Option<LogId>,
    internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
) -> Result<()> {
    // ----------------------
    // Phase 1: Schedule log purge if possible
    // ----------------------
    // Log retention is intentionally decoupled from the snapshot boundary.
    // last_included == last_applied (snapshot is always truthful).
    // purge_upto is set back by retained_log_entries so lagging followers
    // can still catch up via AppendEntries instead of InstallSnapshot.
    let retained = ctx.node_config().raft.snapshot.retained_log_entries;
    let purge_upto_index = last_included.index.saturating_sub(retained);
    info!("purge_upto_index={purge_upto_index}");
    // retained >= last_included.index → nothing to purge; skip log lookup entirely.
    if purge_upto_index == 0 {
        return Ok(());
    }
    if let Some(term) = ctx.raft_log().entry_term(purge_upto_index) {
        let purge_upto = LogId {
            index: purge_upto_index,
            term,
        };
        let idx = purge_upto.index;
        let monotonic = last_purged_index.map(|l| l.index < idx).unwrap_or(true);
        if idx > 0
            && idx < commit_index
            && monotonic
            && pending_purge_upto.map(|e| e.index < idx).unwrap_or(true)
        {
            *pending_purge_upto = Some(purge_upto);
        }
    }

    // ----------------------
    // Phase 2: Execute local purge
    // ----------------------
    // Per Raft §7: Leader purges independently without peer coordination
    execute_pending_purge(pending_purge_upto, ctx, internal_event_tx).await
}

/// Cleanup after an InstallSnapshotChunk install (or a confirmed no-op at/beyond
/// `target`). Shares `pending_purge_upto` with path①'s watermark — failure isn't cleared,
/// retried by whichever purge trigger runs next. No `retained_log_entries` subtraction:
/// entries below `target` are redundant with a snapshot we already have, not held back for
/// a lagging peer.
pub(super) async fn schedule_installed_snapshot_purge<T: TypeConfig>(
    target: LogId,
    ctx: &RaftContext<T>,
    pending_purge_upto: &mut Option<LogId>,
    internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
) -> Result<()> {
    if pending_purge_upto.map(|e| e.index < target.index).unwrap_or(true) {
        *pending_purge_upto = Some(target);
    }
    execute_pending_purge(pending_purge_upto, ctx, internal_event_tx).await
}

/// Send a InboundEvent back into the internal event loop for reprocessing.
pub(super) fn send_replay_inbound_event(
    internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
    inbound_event: InboundEvent,
) -> Result<()> {
    internal_event_tx
        .send(InternalEvent::ReprocessEvent(Box::new(inbound_event)))
        .map_err(|e| {
            error!("Failed to send: {e:?}");
            NetworkError::SingalSendFailed(format!("{:?}", e)).into()
        })
}

/// Compares an incoming RPC's term against this node's own current term — the
/// same Raft rule AppendEntries and InstallSnapshot both need, just checked at
/// different points (AppendEntries's term is a plain request field; InstallSnapshot's
/// is only known once the first chunk arrives) (#436-adjacent).
pub(super) enum TermDecision {
    /// Incoming term is behind ours — caller must reject without touching state.
    RejectStale,
    /// Incoming term matches ours — proceed, no term update needed.
    Accept,
    /// Incoming term is ahead of ours — proceed, but adopt this term.
    AcceptAndUpdateTerm(u64),
}

pub(super) fn check_incoming_term(
    my_term: u64,
    incoming_term: u64,
) -> TermDecision {
    match incoming_term.cmp(&my_term) {
        std::cmp::Ordering::Less => TermDecision::RejectStale,
        std::cmp::Ordering::Equal => TermDecision::Accept,
        std::cmp::Ordering::Greater => TermDecision::AcceptAndUpdateTerm(incoming_term),
    }
}

/// Check snapshot condition and trigger if met. Used by all role states after SM apply.
///
/// Per Raft §7: each server takes snapshots independently. Called from ApplyCompleted
/// handlers in leader, follower, learner, and candidate states.
pub(super) fn check_and_trigger_snapshot<T: TypeConfig>(
    last_index: u64,
    role: i32,
    current_term: u64,
    ctx: &RaftContext<T>,
    internal_event_tx: &mpsc::UnboundedSender<InternalEvent>,
) -> Result<()> {
    if ctx.node_config.raft.snapshot.enable
        && ctx.state_machine_handler().should_snapshot(NewCommitData {
            new_commit_index: last_index,
            role,
            current_term,
        })
    {
        internal_event_tx.send(InternalEvent::CreateSnapshotEvent).map_err(|e| {
            error!("Failed to send: {e:?}");
            NetworkError::SingalSendFailed(format!("{:?}", e))
        })?;
    }
    Ok(())
}
