use std::fmt::Debug;
use std::time::Duration;

use config::ConfigError;
use serde::Deserialize;
use serde::Serialize;
use tracing::warn;

use super::lease::LeaseConfig;
use crate::Error;
use crate::Result;

/// Configuration parameters for the Raft consensus algorithm implementation
#[derive(Serialize, Deserialize, Clone)]
pub struct RaftConfig {
    /// Configuration settings related to log replication
    /// Includes parameters like heartbeat interval and AppendEntries entry count limit
    #[serde(default)]
    pub replication: ReplicationConfig,

    /// Client request batching configuration
    ///
    /// Controls flush thresholds for the leader's propose and linearizable-read buffers.
    /// Separate from replication config — this governs the client ingestion layer, not
    /// the Leader→Follower replication path.
    #[serde(default)]
    pub batching: BatchingConfig,

    /// Configuration settings for leader election mechanism
    /// Controls timeouts and randomization factors for election timing
    #[serde(default)]
    pub election: ElectionConfig,

    /// Configuration settings for cluster membership changes
    /// Handles joint consensus transitions and cluster reconfiguration rules
    #[serde(default)]
    pub membership: MembershipConfig,

    /// Configuration settings for state machine behavior
    /// Controls state machine operations like lease management, compaction, etc.
    /// For backward compatibility, can also be configured via `storage` in TOML files.
    #[serde(default, alias = "storage")]
    pub state_machine: StateMachineConfig,

    /// Configuration settings for snapshot feature
    #[serde(default)]
    pub snapshot: SnapshotConfig,

    /// Configuration settings for log persistence behavior
    /// Controls how and when log entries are persisted to stable storage
    #[serde(default)]
    pub persistence: PersistenceConfig,

    /// Maximum allowed log entry gap between leader and learner nodes
    /// Learners with larger gaps than this value will trigger catch-up replication
    /// Default value is set via default_learner_catchup_threshold() function
    #[serde(default = "default_learner_catchup_threshold")]
    pub learner_catchup_threshold: u64,

    /// Throttle interval (milliseconds) for learner progress checks
    /// Prevents excessive checking of learner promotion eligibility
    /// Default value is set via default_learner_check_throttle_ms() function
    #[serde(default = "default_learner_check_throttle_ms")]
    pub learner_check_throttle_ms: u64,

    /// Base timeout duration (in milliseconds) for general Raft operations
    /// Used as fallback timeout when operation-specific timeouts are not set
    /// Default value is set via default_general_timeout() function
    #[serde(default = "default_general_timeout")]
    pub general_raft_timeout_duration_in_ms: u64,

    /// Timeout for snapshot RPC operations (milliseconds)
    #[serde(default = "default_snapshot_rpc_timeout_ms")]
    pub snapshot_rpc_timeout_ms: u64,

    /// Command channel capacity for client write requests
    /// Bounded channel prevents unbounded memory growth under high load
    /// Default value is set via default_cmd_channel_capacity() function
    #[serde(default = "default_cmd_channel_capacity")]
    pub cmd_channel_capacity: usize,

    /// Max in-flight AppendEntries requests on `stream_append_entries` that can be
    /// dispatched to the Raft loop and awaiting their response at once. Once this many
    /// are pending, the stream stops reading new requests until one completes — this
    /// bounds memory/task growth if this node's own durable_index stalls (RPO=0, #446).
    /// Also used directly as the output channel's buffer size, since completed
    /// responses can never outnumber in-flight requests.
    /// Default value is set via default_max_pending_append_responses() function
    #[serde(default = "default_max_pending_append_responses")]
    pub max_pending_append_responses: usize,

    /// ReadActor configuration — tuning for the dedicated Eventual/LeaseRead fast path.
    #[serde(default)]
    pub read_actor: ReadActorConfig,

    /// Configuration settings for new node auto join feature
    #[serde(default)]
    pub auto_join: AutoJoinConfig,

    /// Configuration for read operation consistency behavior
    /// Controls the trade-off between read performance and consistency guarantees
    #[serde(default)]
    pub read_consistency: ReadConsistencyConfig,

    /// Backpressure configuration for client request flow control
    /// Prevents unbounded memory growth by rejecting excess requests
    #[serde(default)]
    pub backpressure: BackpressureConfig,

    /// RPC compression configuration for different service types
    ///
    /// Controls which RPC service types use response compression.
    /// Allows fine-tuning for performance optimization based on
    /// deployment environment and traffic patterns.
    #[serde(default)]
    pub rpc_compression: RpcCompressionConfig,

    /// Configuration for Watch mechanism that monitors key changes
    /// Controls event queue sizes and metrics behavior
    #[serde(default)]
    pub watch: WatchConfig,
}

impl Debug for RaftConfig {
    fn fmt(
        &self,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        f.debug_struct("RaftConfig").finish()
    }
}
impl Default for RaftConfig {
    fn default() -> Self {
        Self {
            replication: ReplicationConfig::default(),
            batching: BatchingConfig::default(),
            election: ElectionConfig::default(),
            membership: MembershipConfig::default(),
            state_machine: StateMachineConfig::default(),
            snapshot: SnapshotConfig::default(),
            persistence: PersistenceConfig::default(),
            learner_catchup_threshold: default_learner_catchup_threshold(),
            learner_check_throttle_ms: default_learner_check_throttle_ms(),
            general_raft_timeout_duration_in_ms: default_general_timeout(),
            auto_join: AutoJoinConfig::default(),
            snapshot_rpc_timeout_ms: default_snapshot_rpc_timeout_ms(),
            cmd_channel_capacity: default_cmd_channel_capacity(),
            max_pending_append_responses: default_max_pending_append_responses(),
            read_actor: ReadActorConfig::default(),
            read_consistency: ReadConsistencyConfig::default(),
            backpressure: BackpressureConfig::default(),
            rpc_compression: RpcCompressionConfig::default(),
            watch: WatchConfig::default(),
        }
    }
}
impl RaftConfig {
    /// Validates all Raft subsystem configurations.
    pub(crate) fn validate(&self) -> Result<()> {
        if self.learner_catchup_threshold == 0 {
            return Err(Error::Config(ConfigError::Message(
                "learner_catchup_threshold must be greater than 0".into(),
            )));
        }

        if self.general_raft_timeout_duration_in_ms < 1 {
            return Err(Error::Config(ConfigError::Message(
                "general_raft_timeout_duration_in_ms must be at least 1ms".into(),
            )));
        }

        self.replication.validate()?;
        self.batching.validate()?;
        self.election.validate()?;
        self.membership.validate()?;
        self.state_machine.validate()?;
        self.snapshot.validate()?;
        self.read_consistency.validate(self.election.election_timeout_min)?;
        self.read_actor.validate()?;
        self.watch.validate()?;
        self.persistence.validate()?;

        Ok(())
    }
}

fn default_learner_catchup_threshold() -> u64 {
    1
}

fn default_learner_check_throttle_ms() -> u64 {
    1000 // 1 second
}

// in ms
fn default_general_timeout() -> u64 {
    50
}
fn default_snapshot_rpc_timeout_ms() -> u64 {
    // 1 hour - sufficient for large snapshots
    3_600_000
}

fn default_cmd_channel_capacity() -> usize {
    1024
}

fn default_max_pending_append_responses() -> usize {
    1024
}

/// Configuration for the ReadActor — the dedicated read task that serves
/// Eventual and LeaseRead requests without entering the Raft loop.
///
/// Exposed as `[raft.read_actor]` in TOML configuration.
///
/// # Tuning Guidelines
/// - `channel_capacity`: set to at least 2× peak concurrent Eventual/LeaseRead clients
/// - `max_drain`: rarely needs changing; must not exceed `channel_capacity` meaningfully
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReadActorConfig {
    /// mpsc channel buffer between the client layer and ReadActor.
    /// Larger values reduce backpressure under high Eventual/LeaseRead concurrency.
    /// Default: 512.
    #[serde(default = "default_read_actor_channel_capacity")]
    pub channel_capacity: usize,

    /// Max reads drained per ReadActor wakeup (mirrors Raft::drain_client_cmds).
    /// After the first recv().await fires, the actor drains up to this many
    /// additional commands with try_recv() before yielding.
    /// Default: 100.
    #[serde(default = "default_read_actor_max_drain")]
    pub max_drain: usize,
}

impl Default for ReadActorConfig {
    fn default() -> Self {
        Self {
            channel_capacity: default_read_actor_channel_capacity(),
            max_drain: default_read_actor_max_drain(),
        }
    }
}

impl ReadActorConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.channel_capacity == 0 {
            return Err(Error::Config(ConfigError::Message(
                "read_actor.channel_capacity must be at least 1 \
                 (0 causes mpsc::channel to panic at startup)"
                    .into(),
            )));
        }
        if self.max_drain == 0 {
            return Err(Error::Config(ConfigError::Message(
                "read_actor.max_drain must be at least 1 \
                 (0 disables post-wakeup batching — ReadActor drains no additional commands)"
                    .into(),
            )));
        }
        Ok(())
    }
}

fn default_read_actor_channel_capacity() -> usize {
    512
}

fn default_read_actor_max_drain() -> usize {
    100
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ReplicationConfig {
    /// Heartbeat interval (milliseconds): how often the leader sends AppendEntries RPCs.
    #[serde(default = "default_append_interval")]
    pub rpc_append_entries_clock_in_ms: u64,

    /// Maximum log entries per single AppendEntries RPC to a follower.
    #[serde(default = "default_entries_per_replication")]
    pub append_entries_max_entries_per_replication: u64,
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            rpc_append_entries_clock_in_ms: default_append_interval(),
            append_entries_max_entries_per_replication: default_entries_per_replication(),
        }
    }
}
impl ReplicationConfig {
    fn validate(&self) -> Result<()> {
        if self.rpc_append_entries_clock_in_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "rpc_append_entries_clock_in_ms cannot be 0".into(),
            )));
        }

        if self.append_entries_max_entries_per_replication == 0 {
            return Err(Error::Config(ConfigError::Message(
                "append_entries_max_entries_per_replication must be > 0".into(),
            )));
        }

        Ok(())
    }
}

/// Batching configuration for leader-side drain loops and buffer allocation.
///
/// A single value intentionally covers all drain loops because they all operate
/// within the same heartbeat period and share the same order-of-magnitude concurrency:
/// - `raft.rs` cmd_rx drain (client propose ingestion)
/// - `raft.rs` internal_event_rx drain (commit index event coalescing)
/// - `DefaultCommitHandler` new_commit_rx drain (state machine apply batching)
///
/// Also used as the initial Vec capacity for propose and linearizable-read buffers
/// (construction-time hint only; the propose buffer retains capacity across flushes
/// via `mem::swap`, while the read buffer resets each flush via `mem::take`).
///
/// # Tuning Guidelines
/// - Low latency priority: lower values → smaller batches, faster flush
/// - Throughput priority: higher values → larger batches, fewer RPCs
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct BatchingConfig {
    /// Maximum items to drain per heartbeat period across all drain loops.
    ///
    /// **Default**: 100
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    /// Maximum total entries to accumulate in a single merge_append_entries pass.
    /// Prevents unbounded memory growth when the buffer has many large batches.
    /// Default: 1000
    #[serde(default = "default_max_merge_entries")]
    pub max_merge_entries: usize,
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            max_merge_entries: default_max_merge_entries(),
        }
    }
}

impl BatchingConfig {
    fn validate(&self) -> Result<()> {
        if self.max_batch_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "batching.max_batch_size must be > 0".into(),
            )));
        }
        if self.max_merge_entries == 0 {
            return Err(Error::Config(ConfigError::Message(
                "batching.max_merge_entries must be > 0".into(),
            )));
        }
        Ok(())
    }
}

fn default_append_interval() -> u64 {
    100
}
fn default_max_batch_size() -> usize {
    100
}

fn default_max_merge_entries() -> usize {
    1000
}

fn default_entries_per_replication() -> u64 {
    100
}
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ElectionConfig {
    #[serde(default = "default_election_timeout_min")]
    pub election_timeout_min: u64,

    #[serde(default = "default_election_timeout_max")]
    pub election_timeout_max: u64,

    #[serde(default = "default_peer_monitor_interval")]
    pub rpc_peer_connectinon_monitor_interval_in_sec: u64,

    #[serde(default = "default_client_request_id")]
    pub internal_rpc_client_request_id: u32,
}

impl Default for ElectionConfig {
    fn default() -> Self {
        Self {
            election_timeout_min: default_election_timeout_min(),
            election_timeout_max: default_election_timeout_max(),
            rpc_peer_connectinon_monitor_interval_in_sec: default_peer_monitor_interval(),
            internal_rpc_client_request_id: default_client_request_id(),
        }
    }
}
impl ElectionConfig {
    fn validate(&self) -> Result<()> {
        if self.election_timeout_min >= self.election_timeout_max {
            return Err(Error::Config(ConfigError::Message(format!(
                "election_timeout_min {}ms must be less than election_timeout_max {}ms",
                self.election_timeout_min, self.election_timeout_max
            ))));
        }

        if self.rpc_peer_connectinon_monitor_interval_in_sec == 0 {
            return Err(Error::Config(ConfigError::Message(
                "rpc_peer_connectinon_monitor_interval_in_sec cannot be 0".into(),
            )));
        }

        Ok(())
    }
}
fn default_election_timeout_min() -> u64 {
    500
}
fn default_election_timeout_max() -> u64 {
    1000
}
fn default_peer_monitor_interval() -> u64 {
    30
}
fn default_client_request_id() -> u32 {
    0
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MembershipConfig {
    #[serde(default = "default_probe_service")]
    pub cluster_healthcheck_probe_service_name: String,

    #[serde(default = "default_verify_leadership_persistent_timeout")]
    pub verify_leadership_persistent_timeout: Duration,

    #[serde(default)]
    pub zombie: ZombieConfig,

    /// Configuration settings for ready learners promotion
    #[serde(default)]
    pub promotion: PromotionConfig,

    /// Bound on how long a voter waits at startup for quorum to become reachable before failing fast; independent of retry.membership.
    #[serde(default = "default_startup_quorum_timeout")]
    pub startup_quorum_timeout: Duration,
}
impl Default for MembershipConfig {
    fn default() -> Self {
        Self {
            cluster_healthcheck_probe_service_name: default_probe_service(),
            verify_leadership_persistent_timeout: default_verify_leadership_persistent_timeout(),
            zombie: ZombieConfig::default(),
            promotion: PromotionConfig::default(),
            startup_quorum_timeout: default_startup_quorum_timeout(),
        }
    }
}
fn default_probe_service() -> String {
    "d_engine.server.cluster.ClusterManagementService".to_string()
}

/// Default timeout for leader to keep verifying its leadership.
///
/// In Raft, the leader may retry sending no-op entries to confirm it still holds leadership.
/// This timeout defines how long the leader will keep retrying before stepping down.
///
/// Default: 1 hour.
fn default_verify_leadership_persistent_timeout() -> Duration {
    Duration::from_secs(3600)
}

/// Default 30s — enough for peers to finish their own bootstrap race, short enough to fail fast if truly unreachable.
fn default_startup_quorum_timeout() -> Duration {
    Duration::from_secs(30)
}

impl MembershipConfig {
    fn validate(&self) -> Result<()> {
        if self.cluster_healthcheck_probe_service_name.is_empty() {
            return Err(Error::Config(ConfigError::Message(
                "cluster_healthcheck_probe_service_name cannot be empty".into(),
            )));
        }
        if self.startup_quorum_timeout.is_zero() {
            return Err(Error::Config(ConfigError::Message(
                "startup_quorum_timeout must be greater than 0".into(),
            )));
        }
        Ok(())
    }
}

/// State machine behavior configuration
///
/// Controls state machine operations including lease management, compaction policies,
/// and other data lifecycle features. This configuration affects how the state machine
/// processes applied log entries and manages data.
#[derive(Serialize, Deserialize, Clone, Debug)]
#[derive(Default)]
pub struct StateMachineConfig {
    /// Lease (time-based expiration) configuration
    ///
    /// For backward compatibility, can also be configured via `ttl` in TOML files.
    #[serde(alias = "ttl")]
    pub lease: LeaseConfig,
}

impl StateMachineConfig {
    pub fn validate(&self) -> Result<()> {
        self.lease.validate()?;
        Ok(())
    }
}

/// Submit processor-specific configuration
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SnapshotConfig {
    /// If enable the snapshot or not
    #[serde(default = "default_snapshot_enabled")]
    pub enable: bool,

    /// Maximum number of log entries to accumulate before triggering snapshot creation
    /// This helps control memory usage by enforcing periodic state compaction
    #[serde(default = "default_max_log_entries_before_snapshot")]
    pub max_log_entries_before_snapshot: u64,

    /// Number of historical snapshot versions to retain during cleanup
    /// Ensures we maintain a safety buffer of previous states for recovery
    #[serde(default = "default_cleanup_retain_count")]
    pub cleanup_retain_count: u64,

    #[serde(default = "default_snapshots_dir_prefix")]
    pub snapshots_dir_prefix: String,

    /// Size (in bytes) of individual chunks when transferring snapshots
    ///
    /// Default: `default_chunk_size()` (typically 1MB)
    #[serde(default = "default_chunk_size")]
    pub chunk_size: usize,

    /// Number of log entries kept *behind* the snapshot boundary after compaction.
    ///
    /// This is the AppendEntries catch-up window: a follower whose `match_index`
    /// is within `retained_log_entries` of `last_included` can be caught up via
    /// cheap AppendEntries; any further behind requires a full InstallSnapshot
    /// (expensive, transfers the entire state). Larger values trade log disk space
    /// for fewer snapshot transfers to lagging peers.
    ///
    /// Must be >= 1 (0 is rejected by `validate`). Should be kept smaller than
    /// `max_log_entries_before_snapshot`, otherwise compaction never actually
    /// purges anything on the first snapshot.
    #[serde(default = "default_retained_log_entries")]
    pub retained_log_entries: u64,

    /// Number of chunks to process before yielding the task
    #[serde(default = "default_sender_yield_every_n_chunks")]
    pub sender_yield_every_n_chunks: usize,

    /// Number of chunks to process before yielding the task
    #[serde(default = "default_receiver_yield_every_n_chunks")]
    pub receiver_yield_every_n_chunks: usize,

    #[serde(default = "default_push_queue_size")]
    pub push_queue_size: usize,

    #[serde(default = "default_cache_size")]
    pub cache_size: usize,

    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    #[serde(default = "default_transfer_timeout_in_sec")]
    pub transfer_timeout_in_sec: u64,

    #[serde(default = "default_retry_interval_in_ms")]
    pub retry_interval_in_ms: u64,

    #[serde(default = "default_snapshot_push_backoff_in_ms")]
    pub snapshot_push_backoff_in_ms: u64,

    #[serde(default = "default_snapshot_push_max_retry")]
    pub snapshot_push_max_retry: u32,

    #[serde(default = "default_push_timeout_in_ms")]
    pub push_timeout_in_ms: u64,

    /// Maximum duration to wait for a single chunk during snapshot reception.
    /// Applies per-chunk on the follower side. Increase for slow networks or large chunks.
    ///
    /// Default: 30 seconds
    #[serde(default = "default_receive_chunk_timeout_in_sec")]
    pub receive_chunk_timeout_in_sec: u64,
}
impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            max_log_entries_before_snapshot: default_max_log_entries_before_snapshot(),
            cleanup_retain_count: default_cleanup_retain_count(),
            snapshots_dir_prefix: default_snapshots_dir_prefix(),
            chunk_size: default_chunk_size(),
            retained_log_entries: default_retained_log_entries(),
            sender_yield_every_n_chunks: default_sender_yield_every_n_chunks(),
            receiver_yield_every_n_chunks: default_receiver_yield_every_n_chunks(),
            push_queue_size: default_push_queue_size(),
            cache_size: default_cache_size(),
            max_retries: default_max_retries(),
            transfer_timeout_in_sec: default_transfer_timeout_in_sec(),
            retry_interval_in_ms: default_retry_interval_in_ms(),
            snapshot_push_backoff_in_ms: default_snapshot_push_backoff_in_ms(),
            snapshot_push_max_retry: default_snapshot_push_max_retry(),
            push_timeout_in_ms: default_push_timeout_in_ms(),
            receive_chunk_timeout_in_sec: default_receive_chunk_timeout_in_sec(),
            enable: default_snapshot_enabled(),
        }
    }
}
impl SnapshotConfig {
    fn validate(&self) -> Result<()> {
        if self.max_log_entries_before_snapshot == 0 {
            return Err(Error::Config(ConfigError::Message(
                "max_log_entries_before_snapshot must be greater than 0".into(),
            )));
        }

        if self.cleanup_retain_count == 0 {
            return Err(Error::Config(ConfigError::Message(
                "cleanup_retain_count must be greater than 0".into(),
            )));
        }
        // chunk_size should be > 0
        if self.chunk_size == 0 {
            return Err(Error::Config(ConfigError::Message(format!(
                "chunk_size must be at least {} bytes (got {})",
                0, self.chunk_size
            ))));
        }

        if self.retained_log_entries < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "retained_log_entries must be >= 1, (got {})",
                self.retained_log_entries
            ))));
        }

        if self.sender_yield_every_n_chunks < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "sender_yield_every_n_chunks must be >= 1, (got {})",
                self.sender_yield_every_n_chunks
            ))));
        }

        if self.receiver_yield_every_n_chunks < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "receiver_yield_every_n_chunks must be >= 1, (got {})",
                self.receiver_yield_every_n_chunks
            ))));
        }

        if self.push_queue_size < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "push_queue_size must be >= 1, (got {})",
                self.push_queue_size
            ))));
        }

        if self.receive_chunk_timeout_in_sec == 0 {
            return Err(Error::Config(ConfigError::Message(
                "receive_chunk_timeout_in_sec must be greater than 0".into(),
            )));
        }

        if self.snapshot_push_max_retry < 1 {
            return Err(Error::Config(ConfigError::Message(format!(
                "snapshot_push_max_retry must be >= 1, (got {})",
                self.snapshot_push_max_retry
            ))));
        }

        Ok(())
    }
}

fn default_snapshot_enabled() -> bool {
    true
}

/// Default threshold for triggering snapshot creation
fn default_max_log_entries_before_snapshot() -> u64 {
    10000
}

/// Default number of historical snapshots to retain
fn default_cleanup_retain_count() -> u64 {
    2
}
/// Default snapshots directory prefix
fn default_snapshots_dir_prefix() -> String {
    "snapshot-".to_string()
}

/// 1KB chunks by default
fn default_chunk_size() -> usize {
    1024
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct AutoJoinConfig {
    #[serde(default = "default_rpc_enable_compression")]
    pub rpc_enable_compression: bool,
}
impl Default for AutoJoinConfig {
    fn default() -> Self {
        Self {
            rpc_enable_compression: default_rpc_enable_compression(),
        }
    }
}
fn default_rpc_enable_compression() -> bool {
    true
}

fn default_retained_log_entries() -> u64 {
    100
}

fn default_sender_yield_every_n_chunks() -> usize {
    1
}

fn default_receiver_yield_every_n_chunks() -> usize {
    1
}

fn default_push_queue_size() -> usize {
    100
}

fn default_cache_size() -> usize {
    10000
}
fn default_max_retries() -> u32 {
    1
}
fn default_transfer_timeout_in_sec() -> u64 {
    600
}
fn default_retry_interval_in_ms() -> u64 {
    10
}
fn default_snapshot_push_backoff_in_ms() -> u64 {
    100
}
fn default_snapshot_push_max_retry() -> u32 {
    3
}
fn default_push_timeout_in_ms() -> u64 {
    300_000
}
fn default_receive_chunk_timeout_in_sec() -> u64 {
    30
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ZombieConfig {
    /// zombie connection failed threshold
    #[serde(default = "default_zombie_threshold")]
    pub threshold: u32,

    #[serde(default = "default_zombie_purge_interval")]
    pub purge_interval: Duration,

    /// Grace period after startup during which repeated connection failures.
    #[serde(default = "default_zombie_startup_grace")]
    pub startup_grace: Duration,
}

impl Default for ZombieConfig {
    fn default() -> Self {
        Self {
            threshold: default_zombie_threshold(),
            purge_interval: default_zombie_purge_interval(),
            startup_grace: default_zombie_startup_grace(),
        }
    }
}

fn default_zombie_threshold() -> u32 {
    3
}
// 30 seconds
fn default_zombie_purge_interval() -> Duration {
    Duration::from_secs(30)
}

fn default_zombie_startup_grace() -> Duration {
    Duration::from_secs(20)
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct PromotionConfig {
    #[serde(default = "default_stale_learner_threshold")]
    pub stale_learner_threshold: Duration,
}

impl Default for PromotionConfig {
    fn default() -> Self {
        Self {
            stale_learner_threshold: default_stale_learner_threshold(),
        }
    }
}

// 5 minutes
fn default_stale_learner_threshold() -> Duration {
    Duration::from_secs(300)
}

/// Interval (ms) between periodic fsyncs on the IO thread. Must be > 0.
///
/// Writes fsync on their own path (`flush()`, `append_entries` →
/// `IOTask::Persist`). This timer only re-fsyncs `(durable_index,
/// memory_max_index]` when the log is idle, so `durable_index` still advances
/// if a fsync-completion notification is lost.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub enum FlushPolicy {
    Batch { idle_flush_interval_ms: u64 },
}

/// Configuration parameters for log persistence behavior
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PersistenceConfig {
    /// Flush policy for asynchronous strategies
    ///
    /// This controls when log entries are flushed to disk. The choice impacts
    /// write performance and durability guarantees.
    #[serde(default = "default_flush_policy")]
    pub flush_policy: FlushPolicy,

    /// Maximum time to wait, on shutdown, for an in-flight fsync task to finish
    /// before giving up. Bounds close() against a stuck/slow disk — the task
    /// itself is not cancelled, it keeps running in the background regardless.
    #[serde(default = "default_shutdown_timeout_ms")]
    pub shutdown_timeout_ms: u64,
}

/// Default flush policy for asynchronous strategies
///
/// This controls when log entries are flushed to disk. The choice impacts
/// write performance and durability guarantees.
fn default_flush_policy() -> FlushPolicy {
    FlushPolicy::Batch {
        idle_flush_interval_ms: 1000,
    }
}

fn default_shutdown_timeout_ms() -> u64 {
    5_000
}

impl PersistenceConfig {
    pub fn validate(&self) -> Result<()> {
        let FlushPolicy::Batch {
            idle_flush_interval_ms,
        } = self.flush_policy;
        if idle_flush_interval_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "flush_policy.idle_flush_interval_ms must be greater than 0".into(),
            )));
        }
        if self.shutdown_timeout_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "shutdown_timeout_ms must be greater than 0".into(),
            )));
        }

        Ok(())
    }
}

impl Default for PersistenceConfig {
    fn default() -> Self {
        Self {
            flush_policy: default_flush_policy(),
            shutdown_timeout_ms: default_shutdown_timeout_ms(),
        }
    }
}

/// Backpressure configuration for client request flow control
///
/// Prevents unbounded memory growth by limiting pending client requests.
/// When limits are reached, new requests are rejected with RESOURCE_EXHAUSTED
/// error until the system processes existing requests.
///
/// # Value Semantics
/// - `0` = unlimited (no backpressure)
/// - `> 0` = maximum pending requests allowed
///
/// # Tuning Guidelines(only for reference)
/// - Low memory (< 4GB): 1000-5000
/// - Medium memory (4-16GB): 5000-20000
/// - High memory (> 16GB): 20000-50000
///
/// # Example
/// ```toml
/// [raft.backpressure]
/// max_pending_writes = 10000
/// max_pending_reads = 50000
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackpressureConfig {
    /// Maximum pending write (propose) requests
    ///
    /// Limits the number of client write requests waiting in the leader's
    /// propose buffer. Write requests typically consume more resources
    /// (replication, persistence) than reads.
    ///
    /// **Default**: 10000 (0 = unlimited)
    #[serde(default = "default_max_pending_writes")]
    pub max_pending_writes: usize,

    /// Maximum pending linearizable read requests
    ///
    /// Limits the number of client linearizable read requests waiting in
    /// the leader's read buffer. Read requests can tolerate higher limits
    /// as they don't require replication.
    ///
    /// **Default**: 50000 (0 = unlimited)
    #[serde(default = "default_max_pending_reads")]
    pub max_pending_reads: usize,
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self {
            max_pending_writes: default_max_pending_writes(),
            max_pending_reads: default_max_pending_reads(),
        }
    }
}

fn default_max_pending_writes() -> usize {
    10_000
}

fn default_max_pending_reads() -> usize {
    50_000
}

impl BackpressureConfig {
    /// Check if write request should be rejected due to backpressure
    ///
    /// Returns true if the current pending count exceeds the limit.
    /// When `max_pending_writes == 0`, always returns false (unlimited).
    pub fn should_reject_write(
        &self,
        current_pending: usize,
    ) -> bool {
        self.max_pending_writes > 0 && current_pending >= self.max_pending_writes
    }

    /// Check if read request should be rejected due to backpressure
    ///
    /// Returns true if the current pending count exceeds the limit.
    /// When `max_pending_reads == 0`, always returns false (unlimited).
    pub fn should_reject_read(
        &self,
        current_pending: usize,
    ) -> bool {
        self.max_pending_reads > 0 && current_pending >= self.max_pending_reads
    }
}

/// Policy for read operation consistency guarantees
///
/// Determines the trade-off between read consistency and performance.
/// Clients can choose the appropriate level based on their requirements.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadConsistencyPolicy {
    /// Lease-based reads for better performance with weaker consistency
    ///
    /// The leader serves reads locally without contacting followers
    /// during the valid lease period. Assumes bounded clock drift between nodes.
    /// Provides lower latency but slightly weaker consistency guarantees
    /// compared to LinearizableRead.
    LeaseRead,

    /// Fully linearizable reads for strongest consistency
    ///
    /// The leader verifies its leadership with a quorum before serving
    /// the read, ensuring strict linearizability. This guarantees that
    /// all reads reflect the most recent committed value in the cluster.
    #[default]
    LinearizableRead,

    /// Eventually consistent reads from any node
    ///
    /// Allows reading from any node (leader, follower, or candidate) without
    /// additional consistency checks. May return stale data but provides
    /// best read performance and availability. Suitable for scenarios where
    /// eventual consistency is acceptable.
    /// **Can be served by non-leader nodes.**
    EventualConsistency,
}

/// Configuration for read operation consistency behavior
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReadConsistencyConfig {
    /// Default read consistency policy for the cluster
    ///
    /// This sets the cluster-wide default behavior. Individual read requests
    /// can still override this setting when needed for specific use cases.
    #[serde(default)]
    pub default_policy: ReadConsistencyPolicy,

    /// Lease duration in milliseconds for LeaseRead policy.
    ///
    /// How long the leader treats its lease as valid after the last quorum ACK.
    ///
    /// **Safety constraint** (enforced by `RaftConfig::validate()`):
    ///   `lease_duration_ms + network_rtt_p99_ms / 2 < election_timeout_min`
    ///
    /// Full theoretical bound (Raft §6.4):
    ///   `lease_duration_ms < election_timeout_min - rtt_p99/2 - max_clock_drift`
    ///
    /// The lease deadline is anchored to heartbeat *send* time, but the follower's
    /// election timer resets at *receive* time (~rtt/2 later). Without accounting for
    /// rtt/2 the safety invariant is violated under network latency — confirmed by
    /// Jepsen set workload (19 elements lost). etcd absorbs this via `electionTimeout × 2/3`;
    /// d-engine makes it explicit via `network_rtt_p99_ms`.
    #[serde(default = "default_lease_duration_ms")]
    pub lease_duration_ms: u64,

    /// Whether to allow clients to override the default policy per request
    ///
    /// When true, clients can specify consistency requirements per read request.
    /// When false, all reads use the cluster's default_policy setting.
    #[serde(default = "default_allow_client_override")]
    pub allow_client_override: bool,

    /// Timeout in milliseconds to wait for state machine to catch up with commit index
    ///
    /// Used by LinearizableRead to ensure the state machine has applied all committed
    /// entries before serving reads. Typical apply latency is <1ms on local SSD.
    /// Default: 10ms (safe buffer for single-node local deployments)
    #[serde(default = "default_state_machine_sync_timeout_ms")]
    pub state_machine_sync_timeout_ms: u64,

    /// Estimated p99 round-trip network latency between leader and followers, in milliseconds.
    ///
    /// Used to tighten the lease safety constraint beyond the basic
    /// `lease_duration_ms < election_timeout_min` check.
    ///
    /// **Why this matters**: the lease deadline is anchored to the heartbeat *send* time,
    /// but a follower's election timer resets at heartbeat *receive* time (~RTT/2 later).
    /// The precise safety condition is:
    ///   `lease_duration_ms + network_rtt_p99_ms / 2 < election_timeout_min`
    ///
    /// Typical values:
    /// - Same host / loopback: 0–1 ms
    /// - Same datacenter:      1–2 ms
    /// - Cross-AZ (AWS):       1–3 ms
    /// - Cross-region:         50+ ms (LeaseRead not recommended)
    ///
    /// Default: 2ms (safe for same-datacenter and typical cross-AZ deployments).
    #[serde(default = "default_network_rtt_p99_ms")]
    pub network_rtt_p99_ms: u64,
}

impl Default for ReadConsistencyConfig {
    fn default() -> Self {
        Self {
            default_policy: ReadConsistencyPolicy::default(),
            lease_duration_ms: default_lease_duration_ms(),
            allow_client_override: default_allow_client_override(),
            state_machine_sync_timeout_ms: default_state_machine_sync_timeout_ms(),
            network_rtt_p99_ms: default_network_rtt_p99_ms(),
        }
    }
}

fn default_lease_duration_ms() -> u64 {
    // 2.5× the default heartbeat interval (100ms); safely below election_timeout_min (500ms).
    250
}

fn default_allow_client_override() -> bool {
    // Allow flexibility by default — clients can choose stronger consistency when needed
    true
}

fn default_network_rtt_p99_ms() -> u64 {
    // 2ms covers same-datacenter and typical AWS cross-AZ deployments.
    // Cross-region deployments should increase this value and reconsider using LeaseRead.
    2
}

fn default_state_machine_sync_timeout_ms() -> u64 {
    10 // 10ms is safe for typical <1ms apply latency on local SSD
}

impl ReadConsistencyConfig {
    pub(super) fn validate(
        &self,
        election_timeout_min: u64,
    ) -> Result<()> {
        if self.lease_duration_ms == 0 {
            return Err(Error::Config(ConfigError::Message(
                "read_consistency.lease_duration_ms must be greater than 0".into(),
            )));
        }
        // Safety constraint (Raft §6.4):
        //   lease_duration_ms + rtt_p99/2 < election_timeout_min
        //
        // The follower's election timer resets at heartbeat receive time (~rtt/2 after send).
        // Without this margin the effective lease window can exceed election_timeout_min,
        // allowing stale reads after a network partition that heals before lease expiry.
        let rtt_half_ms = self.network_rtt_p99_ms / 2;
        // Use saturating_add: if the sum overflows u64 it saturates to u64::MAX,
        // which is guaranteed >= any election_timeout_min, so the config is correctly rejected.
        if self.lease_duration_ms.saturating_add(rtt_half_ms) >= election_timeout_min {
            return Err(Error::Config(ConfigError::Message(format!(
                "read_consistency.lease_duration_ms ({}) + network_rtt_p99_ms/2 ({}) \
                 must be strictly less than election_timeout_min ({}ms) — \
                 required for lease safety under partition (see network_rtt_p99_ms config)",
                self.lease_duration_ms, rtt_half_ms, election_timeout_min,
            ))));
        }
        Ok(())
    }
}

impl From<d_engine_proto::client::ReadConsistencyPolicy> for ReadConsistencyPolicy {
    fn from(proto_policy: d_engine_proto::client::ReadConsistencyPolicy) -> Self {
        match proto_policy {
            d_engine_proto::client::ReadConsistencyPolicy::LeaseRead => Self::LeaseRead,
            d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead => {
                Self::LinearizableRead
            }
            d_engine_proto::client::ReadConsistencyPolicy::EventualConsistency => {
                Self::EventualConsistency
            }
        }
    }
}

impl From<ReadConsistencyPolicy> for d_engine_proto::client::ReadConsistencyPolicy {
    fn from(config_policy: ReadConsistencyPolicy) -> Self {
        match config_policy {
            ReadConsistencyPolicy::LeaseRead => {
                d_engine_proto::client::ReadConsistencyPolicy::LeaseRead
            }
            ReadConsistencyPolicy::LinearizableRead => {
                d_engine_proto::client::ReadConsistencyPolicy::LinearizableRead
            }
            ReadConsistencyPolicy::EventualConsistency => {
                d_engine_proto::client::ReadConsistencyPolicy::EventualConsistency
            }
        }
    }
}

/// Configuration for controlling gRPC compression settings per service type
///
/// Provides fine-grained control over when to enable compression based on
/// the RPC service type and deployment environment. Each service can be
/// independently configured to use compression based on its data
/// characteristics and frequency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RpcCompressionConfig {
    /// Controls compression for Raft replication response data
    ///
    /// Replication traffic is typically high-frequency with small payloads
    /// in LAN environments, making compression less beneficial. In WAN
    /// deployments with bandwidth constraints, enabling may help.
    ///
    /// **Default**: `false` - Optimized for LAN/same-VPC deployments
    #[serde(default = "default_replication_compression")]
    pub replication_response: bool,

    /// Controls compression for Raft election response data
    ///
    /// Election traffic is low-frequency but time-sensitive. Compression
    /// rarely benefits election traffic due to small payload size.
    ///
    /// **Default**: `true` for backward compatibility
    #[serde(default = "default_election_compression")]
    pub election_response: bool,

    /// Controls compression for snapshot transfer response data
    ///
    /// Snapshot transfers involve large data volumes where compression
    /// is typically beneficial, even in low-latency environments.
    ///
    /// **Default**: `true` - Recommended for all environments
    #[serde(default = "default_snapshot_compression")]
    pub snapshot_response: bool,

    /// Controls compression for cluster management response data
    ///
    /// Cluster operations are infrequent but may contain configuration data.
    /// Compression is generally beneficial for these operations.
    ///
    /// **Default**: `true` for backward compatibility
    #[serde(default = "default_cluster_compression")]
    pub cluster_response: bool,

    /// Controls compression for client request response data
    ///
    /// Client responses may vary in size. In LAN/VPC environments,
    /// compression CPU overhead typically outweighs network benefits.
    ///
    /// **Default**: `false` - Optimized for LAN/same-VPC deployments
    #[serde(default = "default_client_compression")]
    pub client_response: bool,
}

impl Default for RpcCompressionConfig {
    fn default() -> Self {
        Self {
            replication_response: default_replication_compression(),
            election_response: default_election_compression(),
            snapshot_response: default_snapshot_compression(),
            cluster_response: default_cluster_compression(),
            client_response: default_client_compression(),
        }
    }
}

// Default values for RPC compression settings
fn default_replication_compression() -> bool {
    // Replication traffic is high-frequency with typically small payloads
    // For LAN/VPC deployments, compression adds CPU overhead without significant benefit
    false
}

fn default_election_compression() -> bool {
    // Kept enabled for backward compatibility, though minimal benefit
    true
}

fn default_snapshot_compression() -> bool {
    // Snapshot data is large and benefits from compression in all environments
    true
}

fn default_cluster_compression() -> bool {
    // Kept enabled for backward compatibility
    true
}

fn default_client_compression() -> bool {
    // Client responses in LAN/VPC environments typically benefit from no compression
    false
}

/// Configuration for the Watch mechanism that monitors key changes
///
/// The watch system allows clients to monitor specific keys for changes with
/// minimal overhead on the write path. It uses a lock-free event queue and
/// configurable buffer sizes to balance performance and memory usage.
///
/// # Performance Characteristics
///
/// - Write path overhead: < 0.01% with 100+ watchers
/// - Event notification latency: typically < 100μs end-to-end
/// - Memory per watcher: ~2.4KB with default buffer size
///
/// # Configuration Example
///
/// ```toml
/// [raft.watch]
/// event_queue_size = 10240
/// watcher_buffer_size = 256
/// enable_metrics = false
/// max_watcher_count = 10000
/// ```
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WatchConfig {
    /// Buffer size for the global event queue shared across all watchers
    ///
    /// This queue sits between the write path and the dispatcher thread.
    /// A larger queue reduces the chance of dropped events under burst load,
    /// but increases memory usage.
    ///
    /// **Performance Impact**:
    /// - Memory: ~24 bytes per slot (key + value pointers + event type)
    /// - Default 1000 slots ≈ 24KB memory
    ///
    /// **Tuning Guidelines**:
    /// - Low traffic (< 1K writes/sec): 500-1000
    /// - Medium traffic (1K-10K writes/sec): 1000-2000
    /// - High traffic (> 10K writes/sec): 2000-5000
    ///
    /// **Default**: 1000
    #[serde(default = "default_event_queue_size")]
    pub event_queue_size: usize,

    /// Buffer size for each individual watcher's channel
    ///
    /// Each registered watcher gets its own channel to receive events.
    /// Smaller buffers reduce memory usage but increase the risk of
    /// dropping events for slow consumers.
    ///
    /// **Performance Impact**:
    /// - Memory: ~240 bytes per slot per watcher
    /// - 10 slots × 100 watchers = ~240KB total
    ///
    /// **Tuning Guidelines**:
    /// - Fast consumers (< 1ms processing): 5-10
    /// - Normal consumers (1-10ms processing): 10-20
    /// - Slow consumers (> 10ms processing): 20-50
    ///
    /// **Default**: 10
    #[serde(default = "default_watcher_buffer_size")]
    pub watcher_buffer_size: usize,

    /// Enable detailed metrics and logging for watch operations
    ///
    /// When enabled, logs warnings for dropped events and tracks watch
    /// performance metrics. Adds minimal overhead (~0.001%) but useful
    /// for debugging and monitoring.
    ///
    /// **Default**: false (minimal overhead in production)
    #[serde(default = "default_enable_watch_metrics")]
    pub enable_metrics: bool,

    /// Hard cap on total active watchers (exact + prefix combined).
    ///
    /// `register()` and `register_prefix()` return `WatchError::LimitExceeded`
    /// once this count is reached.  Leave at the default (effectively unlimited)
    /// or set to a finite value to prevent runaway watcher growth.
    ///
    /// **Default**: i64::MAX (effectively unlimited)
    #[serde(default = "default_max_watcher_count")]
    pub max_watcher_count: usize,

    /// Heartbeat interval in milliseconds for progress notifications.
    ///
    /// The dispatcher broadcasts a `Progress` event to all active watchers on
    /// each tick so clients can confirm the stream is alive even during quiet
    /// periods.  Set to 0 to disable heartbeats entirely.
    ///
    /// Use milliseconds (not seconds) so tests can set short intervals (e.g. 50ms)
    /// without sleeping for a full second.
    ///
    /// **Default**: 30_000 (30 seconds)
    #[serde(default = "default_heartbeat_interval_ms")]
    pub heartbeat_interval_ms: u64,
}

impl Default for WatchConfig {
    fn default() -> Self {
        Self {
            event_queue_size: default_event_queue_size(),
            watcher_buffer_size: default_watcher_buffer_size(),
            enable_metrics: default_enable_watch_metrics(),
            max_watcher_count: default_max_watcher_count(),
            heartbeat_interval_ms: default_heartbeat_interval_ms(),
        }
    }
}

impl WatchConfig {
    /// Validates watch configuration parameters
    pub fn validate(&self) -> Result<()> {
        if self.event_queue_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "watch.event_queue_size must be greater than 0".into(),
            )));
        }

        if self.event_queue_size > 100_000 {
            warn!(
                "watch.event_queue_size ({}) is very large and may consume significant memory (~{}MB)",
                self.event_queue_size,
                (self.event_queue_size * 24) / 1_000_000
            );
        }

        if self.watcher_buffer_size == 0 {
            return Err(Error::Config(ConfigError::Message(
                "watch.watcher_buffer_size must be greater than 0".into(),
            )));
        }

        if self.watcher_buffer_size > 1000 {
            warn!(
                "watch.watcher_buffer_size ({}) is very large. Each watcher will consume ~{}KB memory",
                self.watcher_buffer_size,
                (self.watcher_buffer_size * 240) / 1000
            );
        }

        Ok(())
    }
}

const fn default_event_queue_size() -> usize {
    10240
}

const fn default_watcher_buffer_size() -> usize {
    256
}

const fn default_enable_watch_metrics() -> bool {
    false
}

const fn default_max_watcher_count() -> usize {
    // i64::MAX — the config crate uses signed 64-bit internally, so usize::MAX overflows it.
    // This value is effectively unlimited for any realistic deployment.
    9_223_372_036_854_775_807
}

const fn default_heartbeat_interval_ms() -> u64 {
    30_000
}
