//! A builder pattern implementation for constructing a [`Node`] instance in a
//! Raft cluster.
//!
//! The [`NodeBuilder`] provides a fluent interface to configure and assemble
//! components required by the Raft node, including storage engines, state machines,
//! transport layers, membership management, and asynchronous handlers.
//!
//! ## Key Design Points
//! - **Explicit Component Initialization**: Requires explicit configuration of storage engines and
//!   state machines (no implicit defaults).
//! - **Customization**: Allows overriding components via setter methods (e.g., `storage_engine()`,
//!   `state_machine()`, `transport()`).
//! - **Simple API**: Single `start().await?` method to build and launch the node.
//!
//! ## Example
//! ```ignore
//! let (shutdown_tx, shutdown_rx) = watch::channel(());
//! let node = NodeBuilder::from_node_config(config, shutdown_rx)
//!     .storage_engine(custom_storage_engine)  // Required component
//!     .state_machine(custom_state_machine)    // Required component
//!     .start().await?;
//! ```
//!
//! ## Notes
//! - **Thread Safety**: All components wrapped in `Arc`/`Mutex` for shared ownership and thread
//!   safety.
//! - **Resource Cleanup**: Uses `watch::Receiver` for cooperative shutdown signaling.
//! - **Configuration Loading**: Supports loading cluster configuration from file or in-memory
//!   config.

use crate::read_actor::run_read_actor;
use d_engine_core::CommitHandler;
use d_engine_core::CommitHandlerDependencies;
use d_engine_core::DefaultCommitHandler;
use d_engine_core::DefaultPurgeExecutor;
use d_engine_core::ElectionHandler;
use d_engine_core::InternalEvent;
use d_engine_core::LogSizePolicy;
use d_engine_core::NewCommitData;
use d_engine_core::Raft;
use d_engine_core::RaftCoreHandlers;
use d_engine_core::RaftLog;
use d_engine_core::RaftNodeConfig;
use d_engine_core::RaftRole;
use d_engine_core::RaftStorageHandles;
use d_engine_core::ReplicationHandler;
use d_engine_core::Result;
use d_engine_core::SignalParams;
use d_engine_core::StateMachine;
use d_engine_core::StateMachineCommand;
use d_engine_core::StateMachineWorker;
use d_engine_core::StorageEngine;
use d_engine_core::SystemError;
use d_engine_core::alias::MOF;
use d_engine_core::alias::SMHOF;
use d_engine_core::alias::SMWOF;
use d_engine_core::alias::SNP;
use d_engine_core::alias::TROF;
use d_engine_core::follower_state::FollowerState;
use d_engine_core::learner_state::LearnerState;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatchDispatcher;
#[cfg(feature = "watch")]
use d_engine_core::watch::WatchRegistry;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::watch;
use tracing::debug;
use tracing::error;
use tracing::info;

use super::LeaderNotifier;
use super::RaftTypeConfig;
use crate::Node;
use crate::membership::RaftMembership;
use crate::network::grpc;
use crate::network::grpc::grpc_transport::GrpcTransport;
use crate::storage::BufferedRaftLog;

type StateMachineHandler<SE, SM> = (
    Arc<SMHOF<RaftTypeConfig<SE, SM>>>,
    Arc<SMWOF<RaftTypeConfig<SE, SM>>>,
);
/// Builder for creating a Raft node
///
/// Provides a fluent API for configuring and constructing a [`Node`].
///
/// # Example
///
/// ```rust,ignore
/// use d_engine_server::{NodeBuilder, FileStorageEngine, FileStateMachine};
///
/// let node = NodeBuilder::new(None, shutdown_rx)
///     .storage_engine(Arc::new(FileStorageEngine::new(...)?))
///     .state_machine(Arc::new(FileStateMachine::new(...).await?))
///     .start().await?;
/// ```
pub(crate) struct NodeBuilder<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    node_id: u32,

    pub(super) node_config: RaftNodeConfig,
    pub(super) storage_engine: Option<Arc<SE>>,
    pub(super) membership: Option<MOF<RaftTypeConfig<SE, SM>>>,
    pub(super) state_machine: Option<Arc<SM>>,
    pub(super) transport: Option<TROF<RaftTypeConfig<SE, SM>>>,
    pub(super) state_machine_handler: Option<StateMachineHandler<SE, SM>>,
    pub(super) snapshot_policy: Option<SNP<RaftTypeConfig<SE, SM>>>,
    pub(super) shutdown_signal: watch::Receiver<()>,

    pub(super) data_dir: std::path::PathBuf,
    /// Pre-acquired lock, for callers that must open storage before
    /// NodeBuilder ever runs — see `data_dir_lock()`'s doc comment.
    pub(super) data_dir_lock: Option<super::DataDirLock>,

    pub(super) node: Option<Arc<Node<RaftTypeConfig<SE, SM>>>>,
}

impl<SE, SM> NodeBuilder<SE, SM>
where
    SE: StorageEngine + Debug,
    SM: StateMachine + Debug,
{
    /// Creates a new NodeBuilder with cluster configuration loaded from file
    ///
    /// # Arguments
    /// * `data_dir` - Node data directory
    /// * `cluster_path` - Optional path to node-specific cluster configuration
    /// * `shutdown_signal` - Watch channel for graceful shutdown signaling
    ///
    /// # Panics
    /// Will panic if configuration loading fails (consider returning Result
    /// instead)
    ///
    /// Test-only: production code always goes through `init()`.
    #[cfg(test)]
    pub fn new(
        data_dir: impl Into<std::path::PathBuf>,
        cluster_path: Option<&str>,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        let node_config = if let Some(p) = cluster_path {
            info!("with_override_config from: {}", &p);
            RaftNodeConfig::new()
                .expect("Load node_config successfully")
                .with_override_config(p)
                .expect("Overwrite node_config successfully")
                .validate()
                .expect("Validate node_config successfully")
        } else {
            RaftNodeConfig::new()
                .expect("Load node_config successfully")
                .validate()
                .expect("Validate node_config successfully")
        };

        Self::init(data_dir, node_config, shutdown_signal)
    }

    /// Constructs NodeBuilder from a fully-built [`RaftNodeConfig`].
    ///
    /// Use this when you have already assembled a complete `RaftNodeConfig` and
    /// want to avoid the implicit `RaftNodeConfig::new()` + `validate()` call
    /// that [`new`](NodeBuilder::new) performs internally before applying your
    /// config. That detour can panic in environments where no default config
    /// file or environment variables are present.
    ///
    /// # Arguments
    /// * `data_dir` - Node data directory
    /// * `node_config` - Fully assembled and validated node configuration
    /// * `shutdown_signal` - Watch channel for graceful shutdown signaling
    ///
    /// # Usage
    /// ```ignore
    /// let config: RaftNodeConfig = /* build and validate your config */;
    /// let builder = NodeBuilder::from_node_config("./data/my-node", config, shutdown_rx);
    /// ```
    ///
    /// Test-only: production code always goes through `init()`.
    #[cfg(test)]
    pub fn from_node_config(
        data_dir: impl Into<std::path::PathBuf>,
        node_config: RaftNodeConfig,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self::init(data_dir, node_config, shutdown_signal)
    }

    /// Core initialization logic shared by all construction paths
    pub(crate) fn init(
        data_dir: impl Into<std::path::PathBuf>,
        node_config: RaftNodeConfig,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        Self {
            node_id: node_config.cluster.node_id,
            storage_engine: None,
            state_machine: None,
            transport: None,
            membership: None,
            node_config,
            shutdown_signal,
            state_machine_handler: None,
            snapshot_policy: None,
            data_dir: data_dir.into(),
            data_dir_lock: None,
            node: None,
        }
    }

    /// Sets a custom storage engine implementation
    pub fn storage_engine(
        mut self,
        storage_engine: Arc<SE>,
    ) -> Self {
        self.storage_engine = Some(storage_engine);
        self
    }

    /// Sets a custom state machine implementation
    pub fn state_machine(
        mut self,
        state_machine: Arc<SM>,
    ) -> Self {
        self.state_machine = Some(state_machine);
        self
    }

    /// Replaces the entire node configuration.
    ///
    /// Test-only: production code always goes through `init()`.
    #[cfg(test)]
    pub fn node_config(
        mut self,
        node_config: RaftNodeConfig,
    ) -> Self {
        self.node_id = node_config.cluster.node_id;
        self.node_config = node_config;
        self
    }

    /// Supplies a pre-acquired data_dir lock. For callers that must open
    /// storage (e.g. RocksDB) *before* NodeBuilder ever runs — the lock has
    /// to be acquired before that happens, not after, or two racing
    /// processes both get partway into opening the same storage directory
    /// before either one even checks this lock.
    ///
    /// If unset, `build()` acquires its own lock — this stays the default
    /// so NodeBuilder remains the guaranteed choke point for every path
    /// that doesn't pre-open storage itself (`start_custom`, `run_custom`,
    /// direct `NodeBuilder` use).
    ///
    /// Crate-private: `DataDirLock` itself isn't exposed outside
    /// `d-engine-server` — external callers (including `NodeBuilder`'s own
    /// public API surface, e.g. d-lmdb) never need this.
    pub(crate) fn data_dir_lock(
        mut self,
        lock: super::DataDirLock,
    ) -> Self {
        self.data_dir_lock = Some(lock);
        self
    }

    /// Finalizes the builder and constructs the Raft node instance.
    ///
    /// Initializes default implementations for any unconfigured components:
    /// - Creates file-based databases for state machine and logs
    /// - Sets up default gRPC transport
    /// - Initializes commit handling subsystem
    /// - Configures membership management
    ///
    /// # Panics
    /// Panics if essential components cannot be initialized
    pub async fn build(mut self) -> Result<Self> {
        let node_id = self.node_id;
        let node_config = self.node_config.clone();

        let data_dir = super::DataDir::new(self.data_dir.clone())?;

        // Use a pre-acquired lock if the caller already opened storage before
        // reaching us (see `data_dir_lock()`'s doc comment); otherwise this is
        // the first and only chance to fail immediately if another process
        // already holds this data_dir, before any storage/state-machine
        // initialization runs.
        let data_dir_lock = match self.data_dir_lock.take() {
            Some(lock) => {
                if !lock.matches(data_dir.as_path()) {
                    return Err(SystemError::NodeStartFailed(
                        "pre-acquired data_dir_lock does not match builder data_dir".to_string(),
                    )
                    .into());
                }
                lock
            }
            None => super::DataDirLock::acquire(data_dir.as_path())?,
        };

        // snapshots_dir is not configurable — always data_dir/snapshots (see #10).
        let snapshots_dir = data_dir.snapshots_dir()?;

        // Init CommitHandler
        let (new_commit_event_tx, new_commit_event_rx) = mpsc::unbounded_channel::<NewCommitData>();

        // Handle state machine initialization
        let state_machine = self.state_machine.take().ok_or_else(|| {
            SystemError::NodeStartFailed(
                "State machine must be set before calling build()".to_string(),
            )
        })?;

        // Note: Lease configuration should be injected BEFORE wrapping in Arc.
        // See StandaloneServer::start() or EmbeddedEngine::with_rocksdb() for correct pattern.
        // If state_machine is passed from user code, they are responsible for lease injection.

        // Start state machine: flip flags and load persisted data
        state_machine.start().await?;

        // Spawn lease background cleanup task (TTL is always active)
        info!(
            "Starting lease background cleanup worker (interval: {}ms)",
            node_config.raft.state_machine.lease.cleanup_interval_ms
        );
        let lease_cleanup_handle = Some(Self::spawn_background_cleanup_worker(
            Arc::clone(&state_machine),
            node_config.raft.state_machine.lease.cleanup_interval_ms,
            self.shutdown_signal.clone(),
        ));

        // Handle storage engine initialization
        let storage_engine = self.storage_engine.take().ok_or_else(|| {
            SystemError::NodeStartFailed(
                "Storage engine must be set before calling build()".to_string(),
            )
        })?;

        //Retrieve last applied index from state machine
        let last_applied_index = state_machine.last_applied().index;
        info!("Node startup, Last applied index: {}", last_applied_index);

        // Create role channel before raft_log so log_flush_tx can be passed to start().
        // internal_event_rx is passed to Raft::new() below; only the creation order changes.
        let (internal_event_tx, internal_event_rx) = mpsc::unbounded_channel();

        let raft_log = {
            let (log, receiver) = BufferedRaftLog::new(
                node_id,
                node_config.raft.persistence.clone(),
                storage_engine.clone(),
            );

            // Start processor and get Arc-wrapped instance.
            // Pass internal_event_tx so batch_processor sends InternalEvent::LogFlushed after each fsync.
            log.start(receiver, Some(internal_event_tx.clone()))
        };

        // Peer health channels: transport fires try_send(peer_id) on stream failure/success.
        // Bridge tasks below relay to membership so zombie detection and health recovery both work.
        let (peer_failure_tx, peer_failure_rx) = mpsc::channel::<u32>(64);
        let (peer_success_tx, peer_success_rx) = mpsc::channel::<u32>(64);
        let transport = self.transport.take().unwrap_or_else(|| {
            GrpcTransport::new_with_channels(node_id, peer_failure_tx, peer_success_tx)
        });

        let max_log_entries = node_config.raft.snapshot.max_log_entries_before_snapshot;

        // Startup memory-budget line — visible on stdout regardless of log setup.
        {
            const EST_LOG_ENTRY_BYTES: u64 = 512; // small/medium KV write + proto + SkipMap node overhead
            let peak_mb = max_log_entries.saturating_mul(EST_LOG_ENTRY_BYTES) / (1024 * 1024);
            tracing::info!(
                node_id,
                max_log_entries,
                est_peak_ram_mb = peak_mb,
                "Raft log memory: peak ~{peak_mb} MB in memory between snapshots \
                        ({max_log_entries} entries × ~512 B/entry est.), purged after each snapshot"
            );
            if peak_mb > 100 {
                tracing::warn!(
                    node_id,
                    est_peak_ram_mb = peak_mb,
                    "in-memory Raft log budget > 100 MB — lower \
                       raft.snapshot.max_log_entries_before_snapshot if RAM-constrained"
                );
            }
        }

        let snapshot_policy =
            self.snapshot_policy.take().unwrap_or(LogSizePolicy::new(max_log_entries));

        let shutdown_signal = self.shutdown_signal.clone();

        // Initialize watch system (controlled by feature flag at compile time)
        // All resource allocation is explicit and visible here (no hidden spawns)
        #[cfg(feature = "watch")]
        let watch_system = {
            let (broadcast_tx, broadcast_rx) =
                tokio::sync::broadcast::channel(node_config.raft.watch.event_queue_size);

            // Create unregister channel
            let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();

            // Create shared registry
            let registry = Arc::new(WatchRegistry::new_with_limits(
                node_config.raft.watch.watcher_buffer_size,
                node_config.raft.watch.max_watcher_count,
                unregister_tx,
            ));

            // Shared last_applied for Progress event revision; written by handler, read by dispatcher.
            let last_applied_ref = Arc::new(std::sync::atomic::AtomicU64::new(last_applied_index));

            // Create dispatcher
            let dispatcher = WatchDispatcher::new(
                Arc::clone(&registry),
                broadcast_rx,
                unregister_rx,
                Arc::clone(&last_applied_ref),
                node_config.raft.watch.heartbeat_interval_ms,
            );

            // Explicitly spawn dispatcher task (resource allocation visible)
            let dispatcher_handle = tokio::spawn(async move {
                dispatcher.run().await;
            });

            Some((broadcast_tx, registry, dispatcher_handle, last_applied_ref))
        };

        let (state_machine_handler, state_machine_writer) =
            self.state_machine_handler.take().unwrap_or_else(|| {
                #[cfg(feature = "watch")]
                let watch_event_tx = watch_system.as_ref().map(|(tx, _, _, _)| tx.clone());

                // Share the registry's live prev_kv counter Arc so the handler sees real-time updates.
                #[cfg(feature = "watch")]
                let prev_kv_count = watch_system
                    .as_ref()
                    .map(|(_, registry, _, _)| registry.prev_kv_watcher_count_arc())
                    .unwrap_or_else(|| Arc::new(std::sync::atomic::AtomicUsize::new(0)));

                #[cfg(not(feature = "watch"))]
                let watch_event_tx: Option<
                    tokio::sync::broadcast::Sender<d_engine_proto::client::WatchResponse>,
                > = None;

                let (reader, writer) =
                    d_engine_core::new_reader_writer_pair::<RaftTypeConfig<SE, SM>>(
                        node_id,
                        last_applied_index,
                        state_machine.clone(),
                        snapshots_dir,
                        node_config.raft.snapshot.clone(),
                        snapshot_policy,
                        #[cfg(feature = "watch")]
                        watch_event_tx,
                        #[cfg(not(feature = "watch"))]
                        watch_event_tx,
                        #[cfg(feature = "watch")]
                        prev_kv_count,
                        #[cfg(not(feature = "watch"))]
                        Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                    );
                (Arc::new(reader), Arc::new(writer))
            });
        let (membership_inner, zombie_rx) = self.membership.take().map_or_else(
            || {
                RaftMembership::new(
                    node_id,
                    node_config.cluster.initial_cluster.clone(),
                    node_config.clone(),
                )
            },
            // Pre-built membership has no zombie_rx; create a dummy closed channel.
            |m| {
                let (_tx, rx) = tokio::sync::mpsc::channel(1);
                (m, rx)
            },
        );
        let membership = Arc::new(membership_inner);
        // Capture the membership watch receiver before membership is moved into Node.
        let membership_rx = membership.subscribe_membership();

        let purge_executor = DefaultPurgeExecutor::new(raft_log.clone());

        // internal_event_tx / internal_event_rx created earlier (before raft_log) to pass log_flush_tx to start().
        let (event_tx, event_rx) = mpsc::channel(10240);
        let (cmd_tx, cmd_rx) = mpsc::channel(node_config.raft.cmd_channel_capacity);
        let internal_event_tx_clone = internal_event_tx.clone();
        let internal_event_tx_for_sm = internal_event_tx.clone(); // used in SM worker (ApplyCompleted → P2)

        // Bridge zombie signals from health monitor → internal event loop.
        // Exits naturally when zombie_tx drops with RaftMembership (channel closed → recv None).
        // Validates each signal against the health monitor before forwarding: if the peer
        // recovered (record_success called) after the signal was queued, drop the stale signal
        // to prevent BatchRemove for a healthy node.
        let internal_event_tx_for_zombie = internal_event_tx.clone();
        let membership_for_zombie = Arc::clone(&membership);
        tokio::spawn(async move {
            let mut zombie_rx = zombie_rx;
            while let Some(node_id) = zombie_rx.recv().await {
                if membership_for_zombie.is_zombie_valid(node_id) {
                    let _ =
                        internal_event_tx_for_zombie.send(InternalEvent::ZombieDetected(node_id));
                }
            }
        });

        // Bridge peer stream failures from transport → health monitor.
        // Exits naturally when transport (and its peer_failure_tx) drops (channel closed → recv None).
        let membership_for_peer_failure = Arc::clone(&membership);
        tokio::spawn(async move {
            let mut peer_failure_rx = peer_failure_rx;
            while let Some(failed_peer_id) = peer_failure_rx.recv().await {
                membership_for_peer_failure.on_peer_stream_failed(failed_peer_id).await;
            }
        });

        // Bridge peer stream successes from transport → health monitor.
        // Resets failure counter so transient errors during failover don't accumulate into false zombies.
        let membership_for_peer_success = Arc::clone(&membership);
        tokio::spawn(async move {
            let mut peer_success_rx = peer_success_rx;
            while let Some(succeeded_peer_id) = peer_success_rx.recv().await {
                membership_for_peer_success.on_peer_stream_success(succeeded_peer_id).await;
            }
        });

        let node_config_arc = Arc::new(node_config);

        // Construct my role
        // Role initialization flow:
        // 1. Check if node is learner from config
        // 2. Load persisted hard state from storage
        // 3. Determine initial role based on cluster state
        // 4. Inject dependencies to role state
        let last_applied_index = Some(state_machine.last_applied().index);
        let my_role = if node_config_arc.is_learner() {
            RaftRole::Learner(Box::new(LearnerState::new(
                node_id,
                node_config_arc.clone(),
            )))
        } else {
            RaftRole::Follower(Box::new(FollowerState::new(
                node_id,
                node_config_arc.clone(),
                raft_log.load_hard_state().expect("Failed to load hard state"),
                last_applied_index,
            )))
        };
        let my_role_i32 = my_role.as_i32();
        let my_current_term = my_role.current_term();
        info!(
            "Start node with role: {} and term: {}",
            my_role_i32, my_current_term
        );

        // Create SM Worker channel for decoupled apply operations
        let (sm_apply_tx, sm_apply_rx) = mpsc::unbounded_channel::<StateMachineCommand>();
        let state_machine_commands =
            d_engine_core::StateMachineCommandSender::new(sm_apply_tx.clone());

        // Construct raft core
        let mut raft_core = Raft::<RaftTypeConfig<SE, SM>>::new(
            node_id,
            my_role,
            RaftStorageHandles::<RaftTypeConfig<SE, SM>> {
                raft_log,
                state_machine: state_machine.clone(),
            },
            transport,
            RaftCoreHandlers::<RaftTypeConfig<SE, SM>> {
                election_handler: ElectionHandler::new(node_id),
                replication_handler: ReplicationHandler::new(node_id),
                state_machine_handler: state_machine_handler.clone(),
                state_machine_commands: state_machine_commands.clone(),
                purge_executor: Arc::new(purge_executor),
            },
            membership.clone(),
            SignalParams::new(
                internal_event_tx,
                internal_event_rx,
                event_tx,
                event_rx,
                cmd_tx.clone(),
                cmd_rx,
                shutdown_signal.clone(),
            ),
            node_config_arc.clone(),
        );

        // Register commit event listener
        raft_core.register_new_commit_listener(new_commit_event_tx);

        // Create leader notification channel and register with Raft core
        let leader_notifier = LeaderNotifier::new();
        raft_core.register_leader_change_listener(leader_notifier.sender());

        // Spawn SM Worker task
        let sm_worker = StateMachineWorker::new(
            node_id,
            state_machine_writer.clone(),
            sm_apply_tx.clone(),
            sm_apply_rx,
            internal_event_tx_for_sm,
            self.shutdown_signal.clone(),
        );
        let sm_worker_handle = Self::spawn_state_machine_worker(sm_worker);

        // Start CommitHandler in a single thread
        let deps = CommitHandlerDependencies {
            state_machine_handler,
            raft_log: raft_core.ctx.storage.raft_log.clone(),
            membership: membership.clone(),
            internal_event_tx: internal_event_tx_clone,
            sm_apply_tx: state_machine_commands,
            shutdown_signal,
            max_batch_size: raft_core.ctx.node_config.raft.batching.max_batch_size,
        };

        let commit_handler = DefaultCommitHandler::<RaftTypeConfig<SE, SM>>::new(
            node_id,
            my_role_i32,
            my_current_term,
            deps,
            new_commit_event_rx,
        );
        // Spawn commit listener via Builder method
        // This ensures all tokio::spawn calls are visible in one place (Builder impl)
        // Following the "one page visible" Builder pattern principle
        let commit_handler_handle = Self::spawn_state_machine_commit_listener(commit_handler);

        let event_tx = raft_core.event_sender();
        let read_lease = raft_core.read_lease();
        let (rpc_ready_tx, _rpc_ready_rx) = watch::channel(false);

        // Spawn ReadActor — sole owner of Arc<SM> on the read fast path.
        let (read_tx, read_rx) = mpsc::channel(node_config_arc.raft.read_actor.channel_capacity);
        let max_drain = node_config_arc.raft.read_actor.max_drain;
        let read_actor_handle = tokio::spawn(run_read_actor(
            read_rx,
            Arc::clone(&read_lease),
            state_machine,
            max_drain,
        ));
        let read_handle = crate::api::StandaloneReadHandle::new(Some(read_tx), cmd_tx.clone());

        let node = Node::<RaftTypeConfig<SE, SM>> {
            node_id,
            raft_core: Arc::new(Mutex::new(raft_core)),
            membership,
            event_tx: event_tx.clone(),
            read_handle,
            cmd_tx,
            ready: AtomicBool::new(false),
            rpc_ready_tx,
            leader_notifier,
            membership_rx,
            node_config: node_config_arc,
            #[cfg(feature = "watch")]
            watch_registry: watch_system.as_ref().map(|(_, reg, _, _)| Arc::clone(reg)),
            #[cfg(feature = "watch")]
            _watch_dispatcher_handle: watch_system.map(|(_, _, handle, _)| handle),
            sm_worker_handle: std::sync::Mutex::new(Some(sm_worker_handle)),
            read_actor_handle: std::sync::Mutex::new(Some(read_actor_handle)),
            rpc_server_handle: std::sync::Mutex::new(None),
            _commit_handler_handle: Some(commit_handler_handle),
            _lease_cleanup_handle: lease_cleanup_handle,
            shutdown_signal: self.shutdown_signal.clone(),
            read_lease,
            _data_dir: data_dir,
            _data_dir_lock: data_dir_lock,
        };

        self.node = Some(Arc::new(node));
        Ok(self)
    }

    /// Spawn state machine commit listener as background task.
    ///
    /// This method is called during node build() to start the commit handler thread.
    /// All spawn_* methods are centralized in NodeBuilder so developers can see
    /// all resource consumption (threads/tasks) in one place.
    ///
    /// # Returns
    /// * `JoinHandle` - Task handle for lifecycle management
    fn spawn_state_machine_commit_listener(
        mut commit_handler: DefaultCommitHandler<RaftTypeConfig<SE, SM>>
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            match commit_handler.run().await {
                Ok(_) => {
                    info!("commit_handler exit program");
                }
                Err(e) => {
                    error!("commit_handler exit program with unexpected error: {:?}", e);
                    println!("commit_handler exit program");
                }
            }
        })
    }

    /// Spawn state machine worker on a dedicated OS thread.
    ///
    /// Runs on `sm-apply-{node_id}` OS thread (not a tokio worker) so that
    /// synchronous RocksDB writes inside `apply_chunk` never block the async runtime
    /// or compete with tokio's blocking thread pool.
    ///
    /// # Returns
    /// * `JoinHandle` - Thread handle for lifecycle management
    fn spawn_state_machine_worker(
        sm_worker: StateMachineWorker<RaftTypeConfig<SE, SM>>
    ) -> std::thread::JoinHandle<()> {
        let node_id = sm_worker.node_id();
        let handle = tokio::runtime::Handle::current();
        std::thread::Builder::new()
            .name(format!("sm-apply-{}", node_id))
            .spawn(move || {
                handle.block_on(async move {
                    match sm_worker.run().await {
                        Ok(_) => info!("state_machine_worker exit"),
                        Err(e) => error!("state_machine_worker exit with error: {:?}", e),
                    }
                });
            })
            .expect("failed to spawn sm-apply thread")
    }

    /// Spawn lease background cleanup task (if enabled).
    ///
    /// This task runs independently from Raft apply pipeline, avoiding any blocking.
    /// Only spawns when cleanup_strategy = "background" (not "disabled" or "piggyback").
    ///
    /// # Arguments
    /// * `state_machine` - Arc reference to state machine for accessing lease manager
    /// * `lease_config` - Lease configuration determining cleanup behavior
    /// * `shutdown_signal` - Watch channel for graceful shutdown notification
    ///
    /// # Returns
    /// * `Option<JoinHandle>` - Task handle if background cleanup is enabled, None otherwise
    ///
    /// # Design Principles
    /// - **Zero overhead**: If disabled, returns None immediately (no task spawned)
    /// - **One page visible**: All long-running tasks spawned here in builder.rs
    /// - **Graceful shutdown**: Monitors shutdown signal for clean termination
    fn spawn_background_cleanup_worker(
        state_machine: Arc<SM>,
        cleanup_interval_ms: u64,
        mut shutdown_signal: watch::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval =
                tokio::time::interval(std::time::Duration::from_millis(cleanup_interval_ms));

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Call state machine's lease background cleanup
                        match state_machine.lease_background_cleanup().await {
                            Ok(deleted_keys) => {
                                if !deleted_keys.is_empty() {
                                    debug!(
                                        "Lease background cleanup: deleted {} expired keys",
                                        deleted_keys.len()
                                    );
                                }
                            }
                            Err(e) => {
                                error!("Lease background cleanup failed: {:?}", e);
                            }
                        }
                    }
                    _ = shutdown_signal.changed() => {
                        info!("Lease background cleanup received shutdown signal");
                        break;
                    }
                }
            }

            debug!("Lease background cleanup worker stopped");
        })
    }

    /// Starts the gRPC server for cluster communication.
    ///
    /// # Panics
    /// Panics if node hasn't been built or address binding fails
    async fn start_rpc_server(self) -> Self {
        debug!("1. --- start RPC server --- ");
        if let Some(ref node) = self.node {
            let node_clone = node.clone();
            let shutdown = self.shutdown_signal.clone();
            let listen_address = self.node_config.cluster.listen_address;
            let node_config = self.node_config.clone();
            let handle = tokio::spawn(async move {
                if let Err(e) =
                    grpc::start_rpc_server(node_clone, listen_address, node_config, shutdown).await
                {
                    eprintln!("RPC server stops. {e:?}");
                    error!("RPC server stops. {:?}", e);
                }
            });
            *node.rpc_server_handle.lock().unwrap() = Some(handle);
            self
        } else {
            panic!("failed to start RPC server");
        }
    }

    /// Builds and starts the Raft node.
    ///
    /// This is the primary method to initialize and start a node. It performs:
    /// 1. State machine initialization (including lease injection if applicable)
    /// 2. Raft core construction
    /// 3. Background task spawning (commit handler, replication, election)
    /// 4. gRPC server startup for cluster communication
    ///
    /// # Returns
    /// An `Arc<Node>` ready for operation
    ///
    /// # Errors
    /// Returns an error if any initialization step fails
    ///
    /// # Example
    /// ```ignore
    /// let node = NodeBuilder::from_node_config(config, shutdown_rx)
    ///     .storage_engine(storage)
    ///     .state_machine(state_machine)
    ///     .start().await?;
    /// ```
    pub async fn start(self) -> Result<Arc<Node<RaftTypeConfig<SE, SM>>>> {
        let builder = self.build().await?;
        let builder = builder.start_rpc_server().await;
        builder.node.ok_or_else(|| {
            SystemError::NodeStartFailed("Node build failed unexpectedly".to_string()).into()
        })
    }

    /// Test constructor with custom database path
    ///
    /// Test-only helper using the default validated node configuration.
    #[cfg(test)]
    pub(crate) fn new_from_db_path(
        db_path: &str,
        shutdown_signal: watch::Receiver<()>,
    ) -> Self {
        let node_config = RaftNodeConfig::new().expect("Load node_config successfully");
        let node_config = node_config.validate().expect("Validate node_config successfully");

        Self::init(db_path, node_config, shutdown_signal)
    }
}

#[cfg(test)]
#[path = "builder_test.rs"]
mod builder_test;
