use crate::client::ClientReadRequest;
use crate::client::ClientResponse;
use crate::client::ClientWriteRequest;
use d_engine_proto::common::LogId;
use d_engine_proto::server::cluster::ClusterConfChangeRequest;
use d_engine_proto::server::cluster::ClusterConfUpdateResponse;
use d_engine_proto::server::cluster::ClusterMembership;
use d_engine_proto::server::cluster::JoinRequest;
use d_engine_proto::server::cluster::JoinResponse;
use d_engine_proto::server::cluster::LeaderDiscoveryRequest;
use d_engine_proto::server::cluster::LeaderDiscoveryResponse;
use d_engine_proto::server::cluster::MetadataRequest;
use d_engine_proto::server::election::VoteRequest;
use d_engine_proto::server::election::VoteResponse;
use d_engine_proto::server::replication::AppendEntriesRequest;
use d_engine_proto::server::replication::AppendEntriesResponse;
use d_engine_proto::server::storage::SnapshotChunk;
use d_engine_proto::server::storage::SnapshotMetadata;
use d_engine_proto::server::storage::SnapshotResponse;
use tonic::Status;

use crate::ApplyResult;
use crate::MaybeCloneOneshotSender;
use crate::Result;

#[derive(Debug, Clone, PartialEq)]
pub struct NewCommitData {
    pub new_commit_index: u64,
    pub role: i32,
    pub current_term: u64,
}

/// Client commands that require batching for performance
/// Separated from internal InboundEvent for drain-driven processing
#[derive(Debug)]
pub enum ClientCmd {
    Propose(
        ClientWriteRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
    Read(
        ClientReadRequest,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
    Scan(
        bytes::Bytes,
        MaybeCloneOneshotSender<std::result::Result<ClientResponse, Status>>,
    ),
}

#[derive(Debug)]
pub enum InternalEvent {
    BecomeFollower(Option<u32>), // BecomeFollower(Option<leader_id>)
    BecomeCandidate,
    BecomeLeader,
    BecomeLearner,

    NotifyNewCommitIndex(NewCommitData),

    /// Notify when follower/learner confirms leader via committed vote
    /// Triggered when committed vote changes from false to true
    /// No state transition - pure notification for watch channel
    LeaderDiscovered(u32, u64), // (leader_id, term)

    ReprocessEvent(Box<InboundEvent>), //Replay the inbound event when step down as another role

    /// Notify Raft loop that log entries up to `durable_index` are crash-safe (fsync complete).
    /// Sent by batch_processor after flush completes.
    /// Follower/Learner: triggers pending ACK send. Leader: triggers commit re-calculation.
    LogFlushed {
        durable_index: u64,
    },

    /// Raw fsync-completion mark — NOT yet validated. Consumer must call
    /// `raft_log().try_advance_durable_index(mark)`, which re-checks the entry's
    /// term before advancing `durable_index`.
    FsyncCompleted(LogId),

    /// AppendEntries result from a per-follower ReplicationWorker back to the Raft loop.
    /// Leader processes this in handle_append_result: updates match_index, re-calculates commit,
    /// and drains pending_client_writes when quorum is achieved.
    AppendResult {
        follower_id: u32,
        result: Result<AppendEntriesResponse>,
    },

    /// Snapshot push result from a per-follower ReplicationWorker back to the Raft loop.
    /// Emitted after `transport.send_snapshot` completes (success or failure).
    /// Leader uses this to clear the per-worker `snapshot_in_progress` flag if needed.
    SnapshotPushCompleted {
        peer_id: u32,
        success: bool,
        /// The leader term this transfer was dispatched under. The detached snapshot
        /// task can outlive a leadership change, so the handler drops completions
        /// whose term no longer matches the current one — a stale completion would
        /// otherwise overwrite the peer's current progress and skip log entries.
        term: u64,
        /// The snapshot's own last-included index (its fixed boundary at creation
        /// time), used to seed the peer's next_index. Deliberately NOT the leader's
        /// current log tip: the leader's log may have advanced further while the
        /// transfer was in flight, and the peer only actually received data up to
        /// this boundary. `None` if the metadata was unexpectedly missing it (should
        /// not happen — a snapshot always has a boundary).
        last_included_index: Option<u64>,
    },

    /// Noop entry committed — leader has confirmed quorum leadership.
    /// Sent by LeaderState::drain_commit_actions when the noop log index is committed.
    /// Raft loop responds by calling on_noop_committed() + notify_leader_change().
    NoopCommitted {
        term: u64,
    },

    /// State machine apply completed — processed at P2 (unbounded internal_event_tx) to avoid
    /// priority inversion: AppendEntries RPCs at P4 (bounded event_tx) must not starve
    /// internal commit-driven events.
    ApplyCompleted {
        last_index: u64,
        results: Vec<ApplyResult>,
    },

    /// Trigger state machine snapshot creation — routed via P2 (unbounded internal_event_tx) to
    /// prevent deadlock when P4 event_tx is saturated by inbound RPCs.
    CreateSnapshotEvent,

    /// Snapshot file is ready; contains metadata and final path.
    /// Leader: schedules log purge. Follower/Learner: updates snapshot state.
    SnapshotCreated(Result<(SnapshotMetadata, std::path::PathBuf)>),

    /// Node removed itself from cluster membership
    /// Leader must step down immediately after self-removal per Raft protocol
    StepDownSelfRemoved,

    /// Membership change has been applied to state
    /// Leader should refresh cluster metadata cache
    MembershipApplied,

    /// Check pending learners for promotion eligibility after a membership change or heartbeat.
    /// Re-queued via internal_event_tx until all pending promotions are resolved.
    PromoteReadyLearners,

    /// Log entries up to `LogId` have been purged from storage after snapshot creation.
    /// Leader updates `last_purged_index` to track the safe purge boundary.
    LogPurgeCompleted(LogId),

    /// Bidi replication stream to a follower broke (network disconnect or error).
    /// Emitted by the replication worker's recv task when it gets an Err from the stream.
    /// Raft loop resets `next_index[peer] = match_index[peer] + 1` so that the next
    /// heartbeat re-sends any unACKed entries.  Worker handles reconnection internally.
    PeerStreamError {
        peer_id: u32,
    },

    /// A peer's connection failure count crossed zombie_threshold.
    /// Emitted by RaftHealthMonitor (server layer) via an injected `Sender<u32>`.
    /// Leader responds by proposing a BatchRemove config change for that node.
    ZombieDetected(u32),

    /// Fatal error from SM worker — node must shutdown.
    /// Sent via internal_event_tx (P2) so it is not blocked behind external RPCs on event_tx (P4).
    FatalError {
        source: String,
        error: String,
    },
}

#[derive(Debug)]
pub enum InboundEvent {
    ReceiveVoteRequest(
        VoteRequest,
        MaybeCloneOneshotSender<std::result::Result<VoteResponse, Status>>,
    ),

    ClusterConf(
        MetadataRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterMembership, Status>>,
    ),

    ClusterConfUpdate(
        ClusterConfChangeRequest,
        MaybeCloneOneshotSender<std::result::Result<ClusterConfUpdateResponse, Status>>,
    ),

    AppendEntries(
        AppendEntriesRequest,
        Vec<MaybeCloneOneshotSender<std::result::Result<AppendEntriesResponse, Status>>>,
    ),

    // Response snapshot stream from Leader
    InstallSnapshotChunk(
        tokio::sync::mpsc::Receiver<SnapshotChunk>,
        MaybeCloneOneshotSender<std::result::Result<SnapshotResponse, Status>>,
    ),

    JoinCluster(
        JoinRequest,
        MaybeCloneOneshotSender<std::result::Result<JoinResponse, Status>>,
    ),

    DiscoverLeader(
        LeaderDiscoveryRequest,
        MaybeCloneOneshotSender<std::result::Result<LeaderDiscoveryResponse, Status>>,
    ),

    /// State machine apply failed - node must shutdown.
    /// Conservative: treat all SM errors as fatal (future: distinguish fatal vs application errors).
    FatalError {
        source: String, // Error source
        error: String,  // Error message
    },
}

#[cfg(test)]
#[cfg_attr(test, derive(Debug, Clone))]
#[allow(unused)]
pub enum TestEvent {
    ReceiveVoteRequest(VoteRequest),

    ClusterConf(MetadataRequest),

    ClusterConfUpdate(ClusterConfChangeRequest),

    AppendEntries(AppendEntriesRequest),

    ClientPropose(ClientWriteRequest),

    ClientReadRequest(ClientReadRequest),

    InstallSnapshotChunk,

    JoinCluster(JoinRequest),

    DiscoverLeader(LeaderDiscoveryRequest),

    // None RPC event
    CreateSnapshotEvent,

    SnapshotCreated,

    LogPurgeCompleted(LogId),

    PromoteReadyLearners,

    FatalError {
        source: String,
        error: String,
    },

    ApplyCompleted {
        last_index: u64,
        results: Vec<ApplyResult>,
    },
}

#[cfg(test)]
pub(crate) fn inbound_event_to_test_event(event: &InboundEvent) -> TestEvent {
    match event {
        InboundEvent::ReceiveVoteRequest(req, _) => TestEvent::ReceiveVoteRequest(*req),
        InboundEvent::ClusterConf(req, _) => TestEvent::ClusterConf(*req),
        InboundEvent::ClusterConfUpdate(req, _) => TestEvent::ClusterConfUpdate(req.clone()),
        InboundEvent::AppendEntries(req, _) => TestEvent::AppendEntries(req.clone()),
        InboundEvent::InstallSnapshotChunk(_, _) => TestEvent::InstallSnapshotChunk,
        InboundEvent::JoinCluster(req, _) => TestEvent::JoinCluster(req.clone()),
        InboundEvent::DiscoverLeader(req, _) => TestEvent::DiscoverLeader(req.clone()),
        InboundEvent::FatalError { source, error } => TestEvent::FatalError {
            source: source.clone(),
            error: error.clone(),
        },
    }
}
