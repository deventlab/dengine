//! Integration test module for simulating RaftContext components with actual
//! underlying operations.
//!
//! This module provides real-world implementations of storage, network and
//! handler components rather than using mocking frameworks. Designed for
//! testing Raft consensus algorithm internals with the following
//! characteristics:
//!
//! - Uses **real storage backends** (SledRaftLog, SledStateStorage) with test isolation through
//!   temporary databases
//! - Implements **actual network transport** (GrpcTransport) for RPC communication
//! - Contains complete handler implementations (ElectionHandler, ReplicationHandler)
//! - Maintains real cluster membership state
//!
//! The [`TestContext`] struct encapsulates a complete testing environment
//! containing:
//! - Storage components (raft log, state machine, snapshots)
//! - Network transport layer
//! - Cluster membership configuration
//! - Core Raft handlers
//!
//! The [`setup_raft_context`] function initializes a test environment with:
//! - Fresh database instances via [`reset_dbs`]
//! - Default or custom peer configurations
//! - Real gRPC transport binding
//! - Complete handler chains
//!
//! This differs from unit test mocks in that:
//! - All I/O operations use actual storage implementations
//! - Network communication uses real transport layers
//! - State changes persist across operations
//! - Full Raft state transitions are exercised
//!
//! Typical usage scenarios:
//! - Integration testing of Raft protocol implementation
//! - End-to-end simulation of node behavior
//! - Cluster formation and interaction tests
//! - Failure scenario testing with real component interactions
#![allow(dead_code)]

use std::ops::RangeInclusive;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use d_engine_core::DefaultStateMachineHandler;
use d_engine_core::ElectionHandler;
use d_engine_core::FlushPolicy;
use d_engine_core::LogSizePolicy;
use d_engine_core::MockStateMachine;
use d_engine_core::PersistenceConfig;
use d_engine_core::RaftLog;
use d_engine_core::RaftNodeConfig;
use d_engine_core::ReplicationHandler;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use d_engine_core::TypeConfig;
use d_engine_core::alias::MOF;
use d_engine_core::alias::ROF;
use d_engine_core::alias::SMOF;
use d_engine_core::alias::TROF;
use d_engine_core::generate_delete_commands;
use d_engine_core::generate_insert_commands;
use d_engine_core::{ApplyEntry, Command};
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::common::NodeRole::Follower;
use d_engine_proto::common::NodeStatus;
use d_engine_proto::server::cluster::NodeMeta;
use tokio::sync::watch;
use tracing::debug;

use super::mock::mock_state_machine;
use crate::FileStateMachine;
use crate::FileStorageEngine;
use crate::membership::RaftMembership;
use crate::network::grpc::grpc_transport::GrpcTransport;
use crate::node::RaftTypeConfig;
use crate::storage::BufferedRaftLog;

/// Complete testing environment for Raft consensus algorithm integration tests.
///
/// This struct encapsulates all necessary components for testing Raft protocol
/// implementation with real storage, network, and handler components. It provides
/// a self-contained testing context that can be initialized via [`setup_raft_components`].
///
/// # Fields
///
/// * `id` - The node ID in the cluster
/// * `raft_log` - The persistent Raft log storage with buffering
/// * `state_machine` - The state machine for applying committed entries
/// * `transport` - The gRPC transport layer for inter-node communication
/// * `membership` - The cluster membership management
/// * `election_handler` - Handles election protocol logic
/// * `replication_handler` - Handles log replication between nodes
/// * `state_machine_handler` - Handles state machine commit operations
/// * `node_config` - Raft node configuration settings
/// * `arc_node_config` - Shared reference to node configuration
pub struct TestContext<T>
where
    T: TypeConfig,
{
    /// The node ID in the cluster
    pub id: u32,

    // Storages
    /// The persistent Raft log storage with buffering
    pub raft_log: Arc<ROF<T>>,
    /// The state machine for applying committed entries
    pub state_machine: Arc<SMOF<T>>,

    // Network
    /// The gRPC transport layer for inter-node communication
    pub transport: Arc<TROF<T>>,

    // Cluster Membership
    /// The cluster membership management
    pub membership: Arc<MOF<T>>,

    // Handlers
    /// Handles election protocol logic
    pub election_handler: ElectionHandler<T>,
    /// Handles log replication between nodes
    pub replication_handler: ReplicationHandler<T>,
    /// Handles state machine commit operations
    pub state_machine_handler: DefaultStateMachineHandler<T>,

    /// Raft node configuration settings
    pub node_config: RaftNodeConfig,
    /// Shared reference to node configuration
    pub arc_node_config: Arc<RaftNodeConfig>,
}

/// Initializes a complete Raft testing environment with real components.
///
/// This function sets up a fully functional test context containing real storage,
/// network, and handler components for integration testing. It configures:
/// - Fresh database instances with real file-based storage
/// - gRPC transport for network communication
/// - Cluster membership with configurable peer nodes
/// - All necessary Raft handlers (election, replication, state machine)
///
/// # Arguments
///
/// * `db_path` - The file system path for the database storage
/// * `peers_meta_option` - Optional cluster peer configuration. If `None`, defaults to a 3-node
///   cluster with nodes 1, 2, and 3 on ports 8080-8082
/// * `restart` - If `true`, skips database reset and reuses existing state. If `false`, performs a
///   fresh database initialization
///
/// # Returns
///
/// A fully initialized [`TestContext`] ready for integration testing with
/// [`FileStorageEngine`] and [`MockStateMachine`].
///
/// # Panics
///
/// Panics if storage engine creation or initialization fails.
pub fn setup_raft_components(
    db_path: &str,
    peers_meta_option: Option<Vec<NodeMeta>>,
    restart: bool,
) -> TestContext<RaftTypeConfig<FileStorageEngine, MockStateMachine>> {
    debug!("Test setup_raft_components ...");

    unsafe {
        std::env::remove_var("CONFIG_PATH");
        std::env::remove_var("RAFT__INITIAL_CLUSTER");
    }
    //start from fresh
    let id = 1;

    // let tempdir = tempfile::tempdir().unwrap();
    let path = PathBuf::from(db_path);
    let storage_engine = Arc::new(FileStorageEngine::new(path).unwrap());
    if restart {
        storage_engine.log_store().reset_sync().unwrap();
    }

    let (buffered_raft_log, receiver) = BufferedRaftLog::new(
        id,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            shutdown_timeout_ms: 5000,
        },
        storage_engine.clone(),
    );
    let buffered_raft_log = buffered_raft_log.start(receiver, None);
    let mock_state_machine = mock_state_machine();
    let last_applied_pair = mock_state_machine.last_applied();

    let grpc_transport = GrpcTransport::new(id);

    let peers_meta = if let Some(meta) = peers_meta_option {
        meta
    } else {
        let follower_role = Follower as i32;
        vec![
            NodeMeta {
                id: 1,
                address: "127.0.0.1:8080".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 2,
                address: "127.0.0.1:8081".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
            NodeMeta {
                id: 3,
                address: "127.0.0.1:8082".to_string(),
                role: follower_role,
                status: NodeStatus::Active.into(),
            },
        ]
    };

    let (_graceful_tx, _graceful_rx) = watch::channel(());

    // Each unit test db path will be different
    let mut node_config = RaftNodeConfig::default();
    node_config.cluster.initial_cluster = peers_meta.clone();

    let snapshot_policy =
        LogSizePolicy::new(node_config.raft.snapshot.max_log_entries_before_snapshot);

    let state_machine = Arc::new(mock_state_machine);
    let state_machine_handler = DefaultStateMachineHandler::new(
        id,
        last_applied_pair.index,
        state_machine.clone(),
        PathBuf::from(db_path).join("snapshots"),
        node_config.raft.snapshot.clone(),
        snapshot_policy,
    );

    let node_config_clone = node_config.clone();
    let arc_node_config = Arc::new(node_config);

    TestContext::<RaftTypeConfig<FileStorageEngine, MockStateMachine>> {
        id,
        raft_log: buffered_raft_log,
        state_machine,
        transport: Arc::new(grpc_transport),
        membership: Arc::new(
            RaftMembership::new(
                id,
                arc_node_config.cluster.initial_cluster.clone(),
                node_config_clone.clone(),
            )
            .0,
        ),
        election_handler: ElectionHandler::new(id),
        replication_handler: ReplicationHandler::new(id),
        node_config: node_config_clone,
        arc_node_config,
        state_machine_handler,
    }
}

/// Inserts entries into the Raft log with specified IDs and term.
///
/// Creates insert command entries and appends them to the Raft log storage.
/// Each ID becomes a separate log entry with the given term.
///
/// # Arguments
///
/// * `raft_log` - Reference to the Raft log storage
/// * `ids` - Vector of IDs to insert as separate log entries
/// * `term` - The term for all inserted entries
///
/// # Panics
///
/// Panics if the batch insert operation fails.
#[allow(dead_code)]
pub(crate) async fn insert_raft_log(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries).await {
        panic!("error:{e:?}");
    }
}

/// Applies entries to the state machine with specified IDs and term.
///
/// Creates insert command entries and applies them directly to the state machine,
/// simulating the application of committed log entries. Each ID becomes a separate
/// entry with the given term.
///
/// # Arguments
///
/// * `state_machine` - Reference to the state machine
/// * `ids` - Vector of IDs to insert as separate entries
/// * `term` - The term for all inserted entries
///
/// # Panics
///
/// Panics if the chunk application operation fails.
#[allow(dead_code)]
pub(crate) async fn insert_state_machine(
    state_machine: &SMOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        entries.push(ApplyEntry {
            index: id,
            term,
            command: Command::Insert {
                key: Bytes::from(id.to_be_bytes().to_vec()),
                value: Bytes::from(id.to_be_bytes().to_vec()),
                ttl_secs: None,
            },
        });
    }
    if let Err(e) = state_machine.apply_chunk(&entries).await {
        panic!("error: {e:?}");
    }
}

/// Simulates inserting commands into the Raft log with automatic log entry allocation.
///
/// Creates insert command entries and appends them to the Raft log storage, then
/// flushes the log to ensure durability. The log entry indices are automatically
/// allocated by the Raft log, not specified externally.
///
/// # Arguments
///
/// * `raft_log` - Reference to the Raft log storage
/// * `ids` - Vector of IDs to insert as separate commands
/// * `term` - The term for all inserted entries
///
/// # Panics
///
/// Panics if the batch insert or flush operation fails.
///
/// # Note
///
/// If duplicate IDs are specified, only one entry per ID will be inserted due to
/// the internal command generation logic.
pub async fn simulate_insert_command(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, MockStateMachine>>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

/// Simulates deleting entries from the log for a range of IDs.
///
/// Creates delete command entries for each ID in the specified range and appends
/// them to the Raft log storage. Each ID in the range becomes a separate delete
/// command entry with the given term.
///
/// # Arguments
///
/// * `raft_log` - Reference to the Raft log storage
/// * `id_range` - An inclusive range of IDs to delete
/// * `term` - The term for all delete entries
///
/// # Panics
///
/// Panics if the batch insert operation fails.
#[allow(dead_code)]
pub async fn simulate_delete_command(
    raft_log: &Arc<ROF<RaftTypeConfig<FileStorageEngine, MockStateMachine>>>,
    id_range: RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let log = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(generate_delete_commands(id..=id))),
        };
        entries.push(log);
    }
    if let Err(e) = raft_log.insert_batch(entries).await {
        panic!("error: {e:?}");
    }
}
