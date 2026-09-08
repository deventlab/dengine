// Re-export LeaderInfo from proto (application layer use)
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tokio::time::sleep_until;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::warn;

use super::InboundEvent;
use super::InternalEvent;
use super::NewCommitData;
use super::RaftContext;
use super::RaftCoreHandlers;
use super::RaftRole;
use super::RaftStorageHandles;
#[cfg(test)]
use super::inbound_event_to_test_event;
use crate::Membership;
use crate::NetworkError;
use crate::RaftLog;
use crate::RaftNodeConfig;
use crate::Result;
use crate::TypeConfig;
use crate::alias::MOF;
use crate::alias::TROF;
use crate::role_state::PeerReplicationState;
pub use d_engine_proto::common::LeaderInfo;
use d_engine_proto::server::election::VotedFor;

pub struct Raft<T>
where
    T: TypeConfig,
{
    pub node_id: u32,
    pub role: RaftRole<T>,
    pub ctx: RaftContext<T>,

    // Network & Storage events
    event_tx: mpsc::Sender<InboundEvent>,
    event_rx: mpsc::Receiver<InboundEvent>,
    buffered_inbound_event: VecDeque<InboundEvent>,

    // Client commands (drain-driven)
    cmd_tx: mpsc::Sender<super::ClientCmd>,
    cmd_rx: mpsc::Receiver<super::ClientCmd>,

    // Timer
    internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    internal_event_rx: mpsc::UnboundedReceiver<InternalEvent>,
    buffered_internal_event: VecDeque<InternalEvent>,

    // For business logic to apply logs into state machine
    new_commit_listener: Vec<mpsc::UnboundedSender<NewCommitData>>,

    // Leader change notification
    // Uses watch::Sender for efficient multi-subscriber pattern
    leader_change_listener: Option<watch::Sender<Option<LeaderInfo>>>,

    // Shutdown signal
    shutdown_signal: watch::Receiver<()>,

    // For unit test
    #[cfg(test)]
    test_role_transition_listener: Vec<mpsc::UnboundedSender<i32>>,

    #[cfg(test)]
    test_inbound_event_listener: Vec<mpsc::UnboundedSender<super::TestEvent>>,
}

pub struct SignalParams {
    pub(crate) internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
    pub(crate) internal_event_rx: mpsc::UnboundedReceiver<InternalEvent>,
    pub(crate) event_tx: mpsc::Sender<InboundEvent>,
    pub(crate) event_rx: mpsc::Receiver<InboundEvent>,
    pub(crate) cmd_tx: mpsc::Sender<super::ClientCmd>,
    pub(crate) cmd_rx: mpsc::Receiver<super::ClientCmd>,
    pub(crate) shutdown_signal: watch::Receiver<()>,
}

impl SignalParams {
    /// Creates a new SignalParams with the provided channels.
    ///
    /// This is the only way to construct SignalParams from outside d-engine-core,
    /// ensuring controlled initialization of the internal communication channels.
    pub fn new(
        internal_event_tx: mpsc::UnboundedSender<InternalEvent>,
        internal_event_rx: mpsc::UnboundedReceiver<InternalEvent>,
        event_tx: mpsc::Sender<InboundEvent>,
        event_rx: mpsc::Receiver<InboundEvent>,
        cmd_tx: mpsc::Sender<super::ClientCmd>,
        cmd_rx: mpsc::Receiver<super::ClientCmd>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            internal_event_tx,
            internal_event_rx,
            event_tx,
            event_rx,
            cmd_tx,
            cmd_rx,
            shutdown_signal,
        }
    }
}

impl<T> Raft<T>
where
    T: TypeConfig,
{
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u32,
        role: RaftRole<T>,
        storage: RaftStorageHandles<T>,
        transport: TROF<T>,
        handlers: RaftCoreHandlers<T>,
        membership: Arc<MOF<T>>,
        signal_params: SignalParams,
        node_config: Arc<RaftNodeConfig>,
    ) -> Self {
        let ctx = Self::build_context(
            node_id,
            storage,
            transport,
            membership,
            handlers,
            node_config.clone(),
        );

        Raft {
            node_id,
            ctx,
            role,

            event_tx: signal_params.event_tx,
            event_rx: signal_params.event_rx,
            buffered_inbound_event: VecDeque::new(),

            cmd_tx: signal_params.cmd_tx,
            cmd_rx: signal_params.cmd_rx,

            internal_event_tx: signal_params.internal_event_tx,
            internal_event_rx: signal_params.internal_event_rx,
            buffered_internal_event: VecDeque::new(),

            new_commit_listener: Vec::new(),

            shutdown_signal: signal_params.shutdown_signal,

            leader_change_listener: None,

            #[cfg(test)]
            test_role_transition_listener: Vec::new(),

            #[cfg(test)]
            test_inbound_event_listener: Vec::new(),
        }
    }

    /// Register a listener for leader election events.
    ///
    /// The listener will receive LeaderInfo updates:
    /// - Some(LeaderInfo) when a leader is elected
    /// - None when no leader exists (during election)
    ///
    /// # Performance
    /// Event-driven notification (no polling), multi-subscriber support via watch channel
    pub fn register_leader_change_listener(
        &mut self,
        tx: watch::Sender<Option<LeaderInfo>>,
    ) {
        self.leader_change_listener = Some(tx);
    }

    /// Notify all leader change listeners.
    ///
    /// Called internally when role transitions occur.
    /// Uses send_if_modified to avoid redundant notifications.
    fn notify_leader_change(
        &self,
        leader_id: Option<u32>,
        term: u64,
    ) {
        if let Some(tx) = &self.leader_change_listener {
            tx.send_if_modified(|current| {
                let new_info = leader_id.map(|id| LeaderInfo {
                    leader_id: id,
                    term,
                });
                if *current != new_info {
                    *current = new_info;
                    true
                } else {
                    false
                }
            });
        }
    }

    fn build_context(
        id: u32,
        storage: RaftStorageHandles<T>,
        transport: TROF<T>,
        membership: Arc<MOF<T>>,
        handlers: RaftCoreHandlers<T>,
        node_config: Arc<RaftNodeConfig>,
    ) -> RaftContext<T> {
        RaftContext {
            node_id: id,
            storage,
            transport: Arc::new(transport),
            membership,
            handlers,

            node_config,
        }
    }

    pub async fn join_cluster(&self) -> Result<()> {
        self.role.join_cluster(&self.ctx).await
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Node is running");

        if self.role.is_timer_expired() {
            self.role.reset_timer();
        }

        // Note: next_deadline wil be reset in each role's tick function
        let tick = sleep_until(self.role.next_deadline());
        tokio::pin!(tick);

        loop {
            tokio::select! {
                // Use biased to ensure branch order
                biased;

                // P0: shutdown received;
                _ = self.shutdown_signal.changed() => {
                    return self.handle_shutdown().await;

                }

                // P1: Tick: start Heartbeat(replication) or start Election
                _ = &mut tick => {
                    trace!("receive tick");
                    let internal_event_tx = &self.internal_event_tx;
                    let event_tx = &self.event_tx;

                    if let Err(e) = self.role.tick(internal_event_tx, event_tx, &self.ctx).await {
                        error!("tick failed: {:?}", e);
                    } else {
                        trace!("tick success");
                    }

                    tick.as_mut().reset(self.role.next_deadline());
                }

                // P2: internal events — handle first, drain rest after select
                Some(internal_event) = self.internal_event_rx.recv() => {
                    debug!(%self.node_id, ?internal_event, "receive internal event");
                    self.buffered_internal_event.push_back(internal_event);
                    self.drain_internal_events().await?;
                }

                // P3: Client commands — push first, drain rest after select
                Some(first_cmd) = self.cmd_rx.recv() => {
                    trace!(%self.node_id, "receive first client command");
                    self.role.push_client_cmd(first_cmd, &self.ctx);
                    self.drain_client_cmds().await?;
                }

                // P4: Other events — handle first, drain rest after select
                Some(inbound_event) = self.event_rx.recv() => {
                    trace!(%self.node_id, ?inbound_event, "receive inbound event");
                    self.buffered_inbound_event.push_back(inbound_event);
                    self.drain_inbound_events().await?;
                }

            }

            self.process_internal_events().await?;
            self.process_client_cmds().await?;
            self.process_inbound_events().await?;

            let new_deadline = self.role.next_deadline();
            if new_deadline != tick.deadline() {
                tick.as_mut().reset(new_deadline);
            }
        }
    }

    async fn handle_shutdown(&mut self) -> Result<()> {
        info!("[Raft:{}] shutdown signal received.", self.node_id);
        // Close IO thread BEFORE returning (before runtime shutdown)
        // This ensures RocksDB file lock is released before tokio runtime shuts down
        self.ctx.storage.raft_log.close().await;
        // Unblock any tasks stuck in event_tx.send().await (e.g. gRPC stream handlers).
        // Without this, serve_with_shutdown never completes and Arc<Node> is never
        // released, keeping Arc<DB> alive and the RocksDB LOCK held indefinitely.
        self.event_rx.close();
        Ok(())
    }

    /// Drain all pending inbound events (up to max_batch_size).
    async fn drain_inbound_events(&mut self) -> Result<()> {
        let max = self.ctx.node_config.raft.batching.max_batch_size;
        let mut count = 0;
        while count < max {
            match self.event_rx.try_recv() {
                Ok(inbound_event) => {
                    self.buffered_inbound_event.push_back(inbound_event);
                    count += 1;
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    /// Drain all pending internal events (up to max_batch_size).
    async fn drain_internal_events(&mut self) -> Result<()> {
        let max = self.ctx.node_config.raft.batching.max_batch_size;
        let mut count = 0;
        while count < max {
            match self.internal_event_rx.try_recv() {
                Ok(internal_event) => {
                    self.buffered_internal_event.push_back(internal_event);
                    count += 1;
                }
                Err(_) => break,
            }
        }
        Ok(())
    }

    /// Drain all pending client commands (up to max_batch_size) and flush.
    async fn drain_client_cmds(&mut self) -> Result<()> {
        let max = self.ctx.node_config.raft.batching.max_batch_size;
        let mut count = 0;
        while count < max {
            match self.cmd_rx.try_recv() {
                Ok(cmd) => {
                    self.role.push_client_cmd(cmd, &self.ctx);
                    count += 1;
                }
                Err(_) => break,
            }
        }
        if count > 0 {
            trace!("Drained {} client commands", count);
        }
        Ok(())
    }

    async fn process_internal_events(&mut self) -> Result<()> {
        while let Some(event) = self.buffered_internal_event.pop_front() {
            if let Err(e) = self.handle_internal_event(event).await {
                if e.is_fatal() {
                    error!(%self.node_id, ?e, "Fatal error in process_internal_events, shutting down");
                    return Err(e);
                }
                warn!(%self.node_id, ?e, "Non-fatal error in process_internal_events, continuing");
            }
        }
        Ok(())
    }

    async fn process_inbound_events(&mut self) -> Result<()> {
        while !self.buffered_inbound_event.is_empty() {
            // Avoid none AE event pop and push into queue
            if matches!(
                self.buffered_inbound_event.front(),
                Some(InboundEvent::AppendEntries(..))
            ) {
                self.merge_append_entries();
            }

            let Some(event) = self.buffered_inbound_event.pop_front() else {
                break;
            };

            #[cfg(test)]
            let test_event = inbound_event_to_test_event(&event);

            if let Err(e) = self
                .role
                .handle_inbound_event(event, &self.ctx, self.internal_event_tx.clone())
                .await
            {
                if e.is_fatal() {
                    error!(%self.node_id, ?e, "Fatal error in drain_inbound_events, shutting down");
                    return Err(e);
                }
                warn!(%self.node_id, ?e, "Non-fatal error in drain_inbound_events, continuing");
            }

            #[cfg(test)]
            self.notify_inbound_event(test_event);
        }
        Ok(())
    }

    async fn process_client_cmds(&mut self) -> Result<()> {
        // Always flush: the P3 select arm may have pushed a command before this drain ran.
        // flush_cmd_buffers checks is_empty() internally and is a no-op when nothing is buffered.
        self.role.flush_cmd_buffers(&self.ctx, &self.internal_event_tx).await
    }

    /// Drain all pending inbound events (up to max_batch_size).
    fn merge_append_entries(&mut self) {
        let Some(event) = self.buffered_inbound_event.pop_front() else {
            return;
        };

        let InboundEvent::AppendEntries(mut first_req, first_senders) = event else {
            self.buffered_inbound_event.push_front(event);
            return;
        };

        let max = self.ctx.node_config.raft.batching.max_merge_entries;

        let mut next_prev = first_req.prev_log_index + first_req.entries.len() as u64;
        let mut merged_entries = first_req.entries;
        let term = first_req.term;
        let mut merged_senders = first_senders;

        while let Some(event) = self.buffered_inbound_event.pop_front() {
            match event {
                InboundEvent::AppendEntries(mut req, mut senders)
                    if next_prev == req.prev_log_index && term == req.term =>
                {
                    if merged_entries.len() + req.entries.len() > max {
                        self.buffered_inbound_event
                            .push_front(InboundEvent::AppendEntries(req, senders));
                        break;
                    }

                    // Take max(leader_commit_index) to handle two cases:
                    // case 1: heartbeat (empty entries) may carry a higher commit index
                    // case 2: leader's commit index may advance between consecutive AE sends
                    first_req.leader_commit_index =
                        first_req.leader_commit_index.max(req.leader_commit_index);
                    next_prev += req.entries.len() as u64;
                    merged_entries.append(&mut req.entries);
                    merged_senders.append(&mut senders);
                }
                _ => {
                    self.buffered_inbound_event.push_front(event);
                    break;
                }
            }
        }
        first_req.entries = merged_entries;
        self.buffered_inbound_event
            .push_front(InboundEvent::AppendEntries(first_req, merged_senders));
    }

    /// `handle_internal_event` will be responsbile to process role trasnsition and
    /// role state events.
    pub async fn handle_internal_event(
        &mut self,
        internal_event: InternalEvent,
    ) -> Result<()> {
        // All inbound and outbound inbound event

        match internal_event {
            InternalEvent::BecomeFollower(leader_id_option) => {
                // Drain read buffer when stepping down from Leader; skip otherwise.
                let _ = self.role.drain_read_buffer();

                debug!("BecomeFollower");
                let mut new_role = self.role.become_follower()?;
                let withheld_acks = self.role.take_pending_acks();
                new_role.restore_pending_acks(withheld_acks);
                self.role = new_role;

                // Reset vote when stepping down (new term, no vote yet)
                self.role.state_mut().commit_vote_reset(&self.ctx)?;

                // Notify leader change listeners
                let current_term = self.role.current_term();
                self.notify_leader_change(leader_id_option, current_term);

                #[cfg(test)]
                self.notify_role_transition();

                //TODO: update membership
            }
            InternalEvent::BecomeCandidate => {
                // Drain read buffer when stepping down from Leader; skip otherwise.
                let _ = self.role.drain_read_buffer();

                debug!("BecomeCandidate");
                let mut new_role = self.role.become_candidate()?;
                let withheld_acks = self.role.take_pending_acks();
                new_role.restore_pending_acks(withheld_acks);
                self.role = new_role;

                // No leader during candidate state
                let current_term = self.role.current_term();
                self.notify_leader_change(None, current_term);

                #[cfg(test)]
                self.notify_role_transition();
            }
            InternalEvent::BecomeLeader => {
                debug!("BecomeLeader");
                let mut new_role = self.role.become_leader()?;
                let withheld_acks = self.role.take_pending_acks();
                new_role.restore_pending_acks(withheld_acks);
                self.role = new_role;

                // Mark vote as committed (candidate → leader transition)
                let current_term = self.role.current_term();
                self.role.state_mut().commit_hard_state(
                    &self.ctx,
                    None,
                    Some(VotedFor {
                        voted_for_id: self.node_id,
                        voted_for_term: current_term,
                        committed: true,
                    }),
                )?;

                let peer_ids = self.ctx.membership().get_peers_id_with_condition(|_| true);

                self.role.init_peers_next_index_and_match_index(
                    self.ctx.raft_log().last_entry_id(),
                    peer_ids,
                )?;

                // Initialize cluster metadata cache for hot path optimization
                self.role.state_mut().init_cluster_metadata(&self.ctx.membership()).await?;

                // Fire-and-forget noop to confirm quorum.
                // Completion is delivered asynchronously via InternalEvent::NoopCommitted.
                if let Err(e) =
                    self.role.initiate_noop_commit(&self.ctx, &self.internal_event_tx).await
                {
                    warn!(?e, "initiate_noop_commit failed — stepping down");
                    self.internal_event_tx.send(InternalEvent::BecomeFollower(None)).map_err(
                        |e| {
                            error!("Failed to send: {:?}", e);
                            NetworkError::SingalSendFailed(format!("{:?}", e))
                        },
                    )?;
                }

                #[cfg(test)]
                self.notify_role_transition();
            }
            InternalEvent::BecomeLearner => {
                // Drain read buffer when stepping down from Leader; skip otherwise.
                let _ = self.role.drain_read_buffer();

                debug!("BecomeLearner");
                let mut new_role = self.role.become_learner()?;
                let withheld_acks = self.role.take_pending_acks();
                new_role.restore_pending_acks(withheld_acks);
                self.role = new_role;

                // Learner has no leader initially
                let current_term = self.role.current_term();
                self.notify_leader_change(None, current_term);

                #[cfg(test)]
                self.notify_role_transition();
            }
            InternalEvent::NotifyNewCommitIndex(mut new_commit_data) => {
                // Drain all pending NotifyNewCommitIndex events (max_batch_size limit)
                // This batches multiple committed entries into a single notification
                let max_batch = self.ctx.node_config.raft.batching.max_batch_size;
                let mut count = 1;

                while count < max_batch {
                    match self.internal_event_rx.try_recv() {
                        Ok(InternalEvent::NotifyNewCommitIndex(next)) => {
                            // Only keep the largest commit_index
                            if next.new_commit_index > new_commit_data.new_commit_index {
                                new_commit_data = next;
                            }
                            count += 1;
                        }
                        Ok(other) => {
                            self.internal_event_tx.send(other).map_err(|e| {
                                error!("Failed to resend internal event: {:?}", e);
                                crate::Error::Fatal(e.to_string())
                            })?;
                            break;
                        }
                        Err(_) => break,
                    }
                }

                debug!(
                    "[{}] NotifyNewCommitIndex drained: {} events, max_commit_index={}",
                    self.node_id, count, new_commit_data.new_commit_index
                );

                self.notify_new_commit(new_commit_data);
            }
            InternalEvent::LeaderDiscovered(leader_id, term) => {
                debug!("LeaderDiscovered: leader_id={}, term={}", leader_id, term);
                // Notify leader change listeners - no state transition
                // Note: mpsc channels do not deduplicate; consumers handle dedup if needed
                self.notify_leader_change(Some(leader_id), term);
            }
            InternalEvent::ReprocessEvent(inbound_event) => {
                info!("Replay the InboundEvent: {:?}", &inbound_event);
                self.buffered_inbound_event.push_front(*inbound_event);
            }
            InternalEvent::LogFlushed { durable_index } => {
                debug!("LogFlushed: durable_index={}", durable_index);
                self.role
                    .handle_log_flushed(durable_index, &self.ctx, &self.internal_event_tx)
                    .await;
            }
            InternalEvent::FsyncCompleted { index, term } => {
                if let Some(new_durable) =
                    self.ctx.raft_log().try_advance_durable_index(index, term)
                {
                    self.role
                        .handle_log_flushed(new_durable, &self.ctx, &self.internal_event_tx)
                        .await;
                }
            }
            InternalEvent::AppendResult {
                follower_id,
                result,
            } => {
                debug!("AppendResult: follower_id={}", follower_id);
                if let Err(e) = self
                    .role
                    .handle_append_result(follower_id, result, &self.ctx, &self.internal_event_tx)
                    .await
                {
                    error!("handle_append_result failed: {:?}", e);
                }
            }
            InternalEvent::NoopCommitted { term } => {
                debug!("NoopCommitted: term={}", term);
                // on_noop_committed already called directly in drain_commit_actions.
                // Only notify leader change listeners here (requires Raft<T> access).
                self.notify_leader_change(Some(self.node_id), term);
            }
            InternalEvent::FatalError { source, error } => {
                error!(%self.node_id, %source, %error, "Fatal error from SM worker — shutting down");
                return Err(crate::Error::Fatal(format!("{source}: {error}")));
            }
            InternalEvent::ApplyCompleted {
                last_index,
                results,
            } => {
                // Routed via internal_event_tx (P2) to avoid priority inversion against AppendEntries at P4.
                if let Err(e) = self
                    .role
                    .handle_apply_completed(last_index, results, &self.ctx, &self.internal_event_tx)
                    .await
                {
                    if e.is_fatal() {
                        error!(%self.node_id, ?e, "Fatal error in ApplyCompleted handler");
                        return Err(e);
                    }
                    warn!(%self.node_id, ?e, "Non-fatal error in ApplyCompleted handler");
                }
            }
            InternalEvent::PeerStreamError { peer_id } => {
                debug!(%peer_id, "PeerStreamError: bidi stream disconnected, resetting next_index");
                self.role.handle_peer_stream_error(peer_id);
            }
            InternalEvent::ZombieDetected(node_id) => {
                debug!(%node_id, "ZombieDetected: forwarding to leader for BatchRemove");
                if let Err(e) = self
                    .role
                    .handle_zombie_detected(node_id, &self.internal_event_tx, &self.ctx)
                    .await
                {
                    error!(%node_id, ?e, "handle_zombie_detected failed");
                }
            }
            InternalEvent::SnapshotPushCompleted {
                peer_id,
                success,
                term,
                last_included_index,
            } => {
                debug!(%peer_id, %success, ?last_included_index, "SnapshotPushCompleted");
                // Staleness guard: the snapshot transfer runs in a detached task that can
                // outlive a leadership change. Only a completion dispatched under the current
                // term may touch peer progress — a straggler from an earlier term would
                // otherwise overwrite next_index and silently skip required log entries.
                if term != self.current_term() {
                    debug!(
                        %peer_id,
                        event_term = term,
                        current_term = self.current_term(),
                        "dropping stale SnapshotPushCompleted from a previous term"
                    );
                    return Ok(());
                }

                // Peer-state guard: seeding is only valid for the peer's CURRENT
                // in-flight snapshot attempt. A straggler completion that arrives after
                // the peer has moved on must not touch next_index.
                if self.role.state().peer_replication_state(peer_id)
                    != PeerReplicationState::Snapshot
                {
                    debug!(%peer_id, "dropping SnapshotPushCompleted: peer not in Snapshot state");
                    return Ok(());
                }

                if success {
                    // Reset next_index to the snapshot's own boundary + 1, so the peer resumes
                    // AppendEntries on the next heartbeat. Deliberately NOT the leader's current
                    // log tip (self.ctx.raft_log().last_entry_id()): the leader's log may have
                    // advanced further while the transfer was in flight (new writes during a
                    // slow transfer to a lagging peer are normal, expected), and the peer only
                    // actually received data up to last_included_index. Using the leader's tip
                    // here would make the leader believe the peer already has entries it never
                    // received, silently skipping them forever.
                    match last_included_index {
                        Some(last_included_index) => {
                            let _ = self.role.init_peers_next_index_and_match_index(
                                last_included_index,
                                vec![peer_id],
                            );
                        }
                        None => {
                            error!(
                                %peer_id,
                                "SnapshotPushCompleted succeeded but last_included_index was \
                                 missing; leaving next_index unchanged rather than guessing"
                            );
                        }
                    }
                }
                // Update per-peer backoff state; emit error alert + metrics when consecutive
                // failures reach the configured threshold (leader protection highest priority).
                let policy = &self.ctx.node_config.retry.install_snapshot;
                self.role.handle_snapshot_push_completed(peer_id, success, policy, self.node_id);
            }
            InternalEvent::CreateSnapshotEvent => {
                if let Err(e) =
                    self.role.handle_create_snapshot(&self.ctx, &self.internal_event_tx).await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_create_snapshot failed");
                }
            }
            InternalEvent::SnapshotCreated(result) => {
                if let Err(e) = self
                    .role
                    .handle_snapshot_created(result, &self.ctx, &self.internal_event_tx)
                    .await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_snapshot_created failed");
                }
            }
            InternalEvent::StepDownSelfRemoved => {
                if let Err(e) = self.role.handle_self_removed(&self.internal_event_tx) {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_self_removed failed");
                }
            }
            InternalEvent::MembershipApplied => {
                if let Err(e) =
                    self.role.handle_membership_applied(&self.ctx, &self.internal_event_tx).await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_membership_applied failed");
                }
            }
            InternalEvent::PromoteReadyLearners => {
                if let Err(e) = self
                    .role
                    .handle_promote_ready_learners(&self.ctx, &self.internal_event_tx)
                    .await
                {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_promote_ready_learners failed");
                }
            }
            InternalEvent::LogPurgeCompleted(log_id) => {
                if let Err(e) = self.role.handle_log_purge_completed(log_id) {
                    if e.is_fatal() {
                        return Err(e);
                    }
                    error!(%self.node_id, ?e, "handle_log_purge_completed failed");
                }
            }
        };

        Ok(())
    }

    pub fn register_new_commit_listener(
        &mut self,
        tx: mpsc::UnboundedSender<NewCommitData>,
    ) {
        self.new_commit_listener.push(tx);
    }

    pub fn notify_new_commit(
        &self,
        new_commit_data: NewCommitData,
    ) {
        debug!(?new_commit_data, "notify_new_commit",);

        for tx in &self.new_commit_listener {
            if let Err(e) = tx.send(new_commit_data.clone()) {
                error!("notify_new_commit failed: {:?}", e);
            }
        }
    }

    #[cfg(test)]
    pub fn register_role_transition_listener(
        &mut self,
        tx: mpsc::UnboundedSender<i32>,
    ) {
        self.test_role_transition_listener.push(tx);
    }

    #[cfg(test)]
    pub fn notify_role_transition(&self) {
        let new_role_i32 = self.role.as_i32();
        for tx in &self.test_role_transition_listener {
            tx.send(new_role_i32).expect("should succeed");
        }
    }

    #[cfg(test)]
    pub fn register_inbound_event_listener(
        &mut self,
        tx: mpsc::UnboundedSender<super::TestEvent>,
    ) {
        self.test_inbound_event_listener.push(tx);
    }

    #[cfg(test)]
    pub fn notify_inbound_event(
        &self,
        event: super::TestEvent,
    ) {
        debug!("unit test:: notify new inbound event: {:?}", &event);

        for tx in &self.test_inbound_event_listener {
            assert!(tx.send(event.clone()).is_ok(), "should succeed");
        }
    }

    #[cfg(test)]
    pub fn set_role(
        &mut self,
        role: RaftRole<T>,
    ) {
        self.role = role
    }

    /// Returns a cloned event sender for external use.
    ///
    /// This provides controlled access to send validated InboundEvents to the Raft core.
    /// Events sent through this sender are still processed through the normal validation
    /// pipeline in the main event loop.
    ///
    /// # Security Note
    /// While this provides access to the event channel, all events are still validated
    /// by the Raft state machine before being applied. The event handler in `handle_inbound_event`
    /// performs necessary checks based on current term, role, and state.
    pub fn event_sender(&self) -> mpsc::Sender<InboundEvent> {
        self.event_tx.clone()
    }

    pub fn cmd_sender(&self) -> mpsc::Sender<super::ClientCmd> {
        self.cmd_tx.clone()
    }

    pub fn read_lease(&self) -> Arc<super::ReadLease> {
        Arc::clone(&self.role.state().shared_state().lease)
    }

    pub fn current_term(&self) -> u64 {
        self.role.state().current_term()
    }

    /// Returns a cloned internal event sender for internal use.
    ///
    /// # Warning
    /// This is primarily for internal components that need to trigger role transitions.
    /// External callers should not use this unless they understand the Raft protocol deeply.
    #[doc(hidden)]
    pub fn internal_event_sender(&self) -> mpsc::UnboundedSender<InternalEvent> {
        self.internal_event_tx.clone()
    }
}

impl<T> Drop for Raft<T>
where
    T: TypeConfig,
{
    fn drop(&mut self) {
        info!("Raft been dropped.");

        if let Err(e) = self
            .ctx
            .raft_log()
            .save_hard_state(&self.role.state().shared_state().hard_state())
        {
            error!(?e, "State storage persist node hard state failed.");
        }

        info!("Graceful shutdown node state ...");
    }
}

#[cfg(test)]
#[path = "raft_test/leader_change_tests.rs"]
mod leader_change_tests;
#[cfg(test)]
#[path = "raft_test/leader_discovered_tests.rs"]
mod leader_discovered_tests;
#[cfg(test)]
#[path = "raft_test/merge_append_entries_tests.rs"]
mod merge_append_entries_tests;
#[cfg(test)]
#[path = "raft_test/process_inbound_events_tests.rs"]
mod process_inbound_events_tests;
#[cfg(test)]
#[path = "raft_test/raft_comprehensive_tests.rs"]
mod raft_comprehensive_tests;
