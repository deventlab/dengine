#![allow(dead_code)]

use bytes::Bytes;
use bytes::BytesMut;
use config::Config;
use d_engine_client::Client;
use d_engine_core::ClientApi;
use d_engine_core::ClientApiError;
use d_engine_core::alias::SMOF;
use d_engine_core::alias::SOF;
use d_engine_core::client::ErrorCode;
use d_engine_core::config::BackoffPolicy;
use d_engine_core::config::ElectionConfig;
use d_engine_core::config::FlushPolicy;
use d_engine_core::config::PersistenceConfig;
use d_engine_core::config::RaftConfig;
use d_engine_core::config::RaftNodeConfig;
use d_engine_core::config::SnapshotConfig;
use d_engine_core::convert::safe_kv_bytes;
use d_engine_proto::client::WriteCommand;
use d_engine_proto::common::Entry;
use d_engine_proto::common::EntryPayload;
use d_engine_proto::server::election::VotedFor;
use d_engine_server::FileStateMachine;
use d_engine_server::FileStorageEngine;
use d_engine_server::HardState;
use d_engine_server::LeaderInfo;
use d_engine_server::LogStore;
use d_engine_server::MetaStore;
use d_engine_server::StandaloneEngine;
use d_engine_server::StorageEngine;
use d_engine_server::node::RaftTypeConfig;
use prost::Message;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::fs::remove_dir_all;
use tokio::fs::{self};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::sync::watch::Sender;
use tokio::task::JoinHandle;
use tokio::time;
use tracing::debug;
use tracing::error;

use crate::client_manager::ClientManager;

pub const WAIT_FOR_NODE_READY_IN_SEC: u64 = 6;

// we are testing linearizable read from Leader directly, the latency should less than 1ms ideally
pub const LATENCY_IN_MS: u64 = 50;

// to make sure the result is consistent
pub const ITERATIONS: u64 = 10;

#[derive(Debug)]
pub enum ClientCommands {
    Put,
    Read,
    Lread,
    Delete,
}

pub struct TestContext {
    pub graceful_txs: Vec<Sender<()>>,
    pub node_handles: Vec<JoinHandle<Result<(), ClientApiError>>>,
}

impl TestContext {
    pub async fn shutdown(self) -> Result<(), ClientApiError> {
        for tx in self.graceful_txs {
            let _ = tx.send(());
        }

        for handle in self.node_handles {
            handle.await??;
        }

        Ok(())
    }
}

// static LOGGER_INIT: once_cell::sync::Lazy<()> = once_cell::sync::Lazy::new(|| {
//     tracing_subscriber::registry()
//         .with(fmt::layer())
//         .with(EnvFilter::from_default_env())
//         .init();
// });

// pub fn enable_logger() {
//     *LOGGER_INIT;
//     println!("setup logger for unit test.");
// }

pub async fn create_node_config(
    node_id: u64,
    port: u16,
    cluster_ports: &[u16],
    _data_dir: &str,
    _log_dir: &str,
) -> String {
    let initial_cluster_entries = cluster_ports
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            let id = i as u64 + 1;
            format!(
                "{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = 1, status = 3 }}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n            ");

    format!(
        r#"
        [cluster]
        node_id = {node_id}
        listen_address = '127.0.0.1:{port}'
        initial_cluster = [
            {initial_cluster_entries}
        ]

        [raft.persistence]
        flush_policy = {{ Batch = {{ threshold = 100, idle_flush_interval_ms = 1 }} }}

        [raft.election]
        election_timeout_min = 300
        election_timeout_max = 3000

        [retry.election]
        max_retries = 5
        timeout_ms = 2000
        base_delay_ms = 100
        max_delay_ms = 5000
        "#
    )
}

/// Create node config with custom role for specific node
/// Allows setting a node as LEARNER (role=4) instead of VOTER (role=1)
pub async fn create_node_config_with_role(
    node_id: u64,
    port: u16,
    cluster_ports: &[u16],
    node_role: i32, // 1 = VOTER, 4 = LEARNER
    _data_dir: &str,
    _log_dir: &str,
) -> String {
    let initial_cluster_entries = cluster_ports
        .iter()
        .enumerate()
        .map(|(i, &p)| {
            let id = i as u64 + 1;
            // Use custom role if this is the current node, otherwise default to VOTER
            let role = if id == node_id { node_role } else { 1 };
            format!(
                "{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = {role}, status = 3 }}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n            ");

    format!(
        r#"
        [cluster]
        node_id = {node_id}
        listen_address = '127.0.0.1:{port}'
        initial_cluster = [
            {initial_cluster_entries}
        ]

        [raft.persistence]
        flush_policy = {{ Batch = {{ threshold = 1, idle_flush_interval_ms = 1 }} }}

        [raft.election]
        election_timeout_min = 300
        election_timeout_max = 3000

        [retry.election]
        max_retries = 5
        timeout_ms = 2000
        base_delay_ms = 100
        max_delay_ms = 5000
        "#
    )
}

pub fn node_config(cluster_toml: &str) -> RaftNodeConfig {
    let base_config = RaftNodeConfig::default();

    let settings = Config::builder()
        .add_source(Config::try_from(&base_config).unwrap())
        .add_source(config::File::from_str(
            cluster_toml,
            config::FileFormat::Toml,
        ))
        .build()
        .unwrap();

    let loaded_config: RaftNodeConfig = settings.try_deserialize().unwrap();

    let mut config = loaded_config;

    println!("Parsed cluster: {:#?}", config.cluster);

    let raft = RaftConfig {
        general_raft_timeout_duration_in_ms: 10000,
        snapshot_rpc_timeout_ms: 300_000,
        snapshot: SnapshotConfig {
            max_log_entries_before_snapshot: 1,
            cleanup_retain_count: 2,
            retained_log_entries: 1,
            ..Default::default()
        },
        persistence: PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 1,
            },
            ..Default::default()
        },
        election: ElectionConfig {
            // Lower min timeout (300 ms vs 1000 ms) lets the first election complete faster
            // under CPU saturation, reducing the chance the test hits the overall timeout.
            // Window width is intentionally kept wide (2700 ms) so staggered nodes rarely
            // fire simultaneously.
            election_timeout_min: 300,
            election_timeout_max: 3000,
            ..Default::default()
        },
        ..Default::default()
    };

    let append_policy = BackoffPolicy {
        max_retries: 2,
        timeout_ms: 200,
        base_delay_ms: 10,
        max_delay_ms: 100,
    };

    // Election retry policy - increased for test stability during concurrent node startup
    // When multiple nodes start simultaneously, RPC servers may not be ready immediately
    let election_policy = BackoffPolicy {
        max_retries: 5,
        timeout_ms: 2000, // Increased from default 100ms to handle slow RPC server startup
        base_delay_ms: 100,
        max_delay_ms: 5000,
    };

    config.raft = raft;
    config.retry.append_entries = append_policy;
    config.retry.election = election_policy;

    config
}

pub async fn start_cluster(
    nodes_config: Vec<RaftNodeConfig>
) -> std::result::Result<(), ClientApiError> {
    // Start all nodes
    let mut controllers = vec![];
    for config in nodes_config {
        let node_data_dir = tempfile::Builder::new()
            .prefix("d-engine-start-cluster-")
            .tempdir()
            .expect("tempdir");
        let (tx, handle) = start_node(node_data_dir.path(), config, None, None).await?;
        // Keep the TempDir guard alive alongside its node until shutdown — it
        // cleans up automatically on drop instead of leaking into /tmp.
        controllers.push((tx, handle, node_data_dir));
    }

    // Perform test operations...

    // Shut down all nodes
    for (tx, handle, _node_data_dir) in controllers {
        tx.send(()).expect("Should succeed to send shutdown");
        handle.await??;
    }

    Ok(())
}
/// Starts a node the same way an external client would — through the public
/// `StandaloneEngine::start_node` API, not the crate-internal `NodeBuilder`.
/// These integration tests simulate external client behavior; they never
/// touch the raw `Node` type.
pub async fn start_node(
    data_dir: impl AsRef<Path>,
    config: RaftNodeConfig,
    state_machine: Option<Arc<SMOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
    storage_engine: Option<Arc<SOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>>,
) -> std::result::Result<
    (
        watch::Sender<()>,
        tokio::task::JoinHandle<std::result::Result<(), ClientApiError>>,
    ),
    ClientApiError,
> {
    let (graceful_tx, graceful_rx) = watch::channel(());
    let data_dir = data_dir.as_ref().to_path_buf();

    // Use provided storage engine or create a new one with temp directory
    let storage_engine = match storage_engine {
        Some(s) => s,
        None => {
            let storage_path = data_dir.join("storage_engine");
            Arc::new(FileStorageEngine::new(storage_path).map_err(|e| {
                std::io::Error::other(format!("failed to create file storage engine: {e}"))
            })?)
        }
    };

    // Use provided state machine or create a new one with temp directory
    let state_machine = match state_machine {
        Some(sm) => sm,
        None => {
            let state_machine_path = data_dir.join("state_machine");
            Arc::new(
                FileStateMachine::new(state_machine_path).await.map_err(|e| {
                    std::io::Error::other(format!("failed to create file state machine: {e}"))
                })?,
            )
        }
    };

    let mut handle = tokio::spawn(async move {
        StandaloneEngine::start_node(data_dir, storage_engine, state_machine, graceful_rx, config)
            .await
            .map_err(|e| std::io::Error::other(format!("node exited with error: {e}")).into())
    });

    // `start_node` blocks until shutdown once it actually starts, so a real
    // startup failure (e.g. the data_dir lock rejecting a concurrent start)
    // surfaces here within milliseconds. Give it a short window: if it exits
    // that fast, something is already wrong — report it now instead of
    // letting the caller find out much later at shutdown.
    tokio::select! {
        joined = &mut handle => {
            let result = joined.map_err(|e| std::io::Error::other(format!("node task panicked: {e}")))?;
            return match result {
                Ok(()) => Err(std::io::Error::other("node exited before it started running").into()),
                Err(e) => Err(e),
            };
        }
        _ = time::sleep(Duration::from_millis(300)) => {}
    }

    Ok((graceful_tx, handle))
}

pub fn get_root_path() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

pub fn prepare_storage_engine(
    _node_id: u32,
    db_path: &str,
    _last_applied_index: u64,
) -> Arc<FileStorageEngine> {
    let path = PathBuf::from(format!("{db_path}/raft_log"));
    println!("Creating FileStorageEngine at path: {path:?}");
    Arc::new(FileStorageEngine::new(path).expect("Failed to create FileStorageEngine"))
}

pub async fn prepare_state_machine(
    _node_id: u32,
    db_path: &str,
) -> FileStateMachine {
    let state_machine_db_path = format!("{db_path}/state_machine",);
    FileStateMachine::new(PathBuf::from(state_machine_db_path)).await.unwrap()
}

pub async fn manipulate_log(
    storage_engine: &Arc<SOF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    log_ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in log_ids {
        println!("pre_allocate_raft_logs_next_index: {id}",);

        let log = Entry {
            index: id,
            term,
            payload: Some(EntryPayload::command(generate_insert_commands(vec![id]))),
        };
        entries.push(log);
    }
    assert!(storage_engine.log_store().persist_entries(entries).await.is_ok());
    assert!(storage_engine.log_store().flush().is_ok());
}

pub fn init_hard_state(
    storage_engine: &Arc<impl StorageEngine>,
    current_term: u64,
    voted_for: Option<VotedFor>,
) {
    assert!(
        storage_engine
            .meta_store()
            .save_hard_state(&HardState {
                current_term,
                voted_for,
            })
            .is_ok()
    );
}

pub async fn test_put_get(
    client_manager: &mut ClientManager,
    key: u64,
    value: u64,
) -> Result<(), ClientApiError> {
    println!("put {key} {value}");
    assert!(
        client_manager
            .execute_command(ClientCommands::Put, key, Some(value))
            .await
            .is_ok(),
        "Put command failed for key {key} value {value}"
    );
    tokio::time::sleep(Duration::from_millis(LATENCY_IN_MS)).await;
    client_manager.verify_read(key, value, ITERATIONS).await;
    Ok(())
}

/// Test PUT followed immediately by linearizable read with NO sleep
///
/// This verifies that get_linearizable() waits for state machine to apply
/// the committed entry before returning, ensuring strong consistency.
pub async fn test_put_get_no_sleep(
    client_manager: &mut ClientManager,
    key: u64,
    value: u64,
) -> Result<(), ClientApiError> {
    println!("put {key} {value} (no sleep)");
    client_manager.execute_command(ClientCommands::Put, key, Some(value)).await?;

    // Immediately read - linearizable read should wait for apply
    client_manager.verify_read(key, value, 1).await;
    Ok(())
}

/// Helper function to create bootstrap URLs
pub fn create_bootstrap_urls(ports: &[u16]) -> Vec<String> {
    ports.iter().map(|port| format!("http://127.0.0.1:{port}")).collect()
}

pub fn generate_insert_commands(ids: Vec<u64>) -> Bytes {
    let mut buffer = BytesMut::new();

    for id in ids {
        let cmd = WriteCommand::insert(safe_kv_bytes(id), safe_kv_bytes(id));
        cmd.encode(&mut buffer).expect("Failed to encode insert command");
    }

    buffer.freeze()
}

pub async fn reset(case_name: &str) -> std::result::Result<(), ClientApiError> {
    let root_path = get_root_path();
    // Define path
    let logs_dir = format!("{}/logs/{case_name}", root_path.display(),);
    let db_dir = format!("{}/db/{case_name}", root_path.display(),);
    let snapshots_dir = format!("{}/snapshots/{case_name}", root_path.display(),);

    debug!(?logs_dir, ?db_dir, ?snapshots_dir, "reset path");

    // Make sure the parent directory exists
    fs::create_dir_all(&logs_dir).await?;
    fs::create_dir_all(&db_dir).await?;
    fs::create_dir_all(&snapshots_dir).await?;

    // Clean up the log directory (ignore errors that do not exist)
    let _ = remove_dir_all(Path::new(&logs_dir)).await;
    let _ = remove_dir_all(Path::new(&db_dir)).await;
    let _ = remove_dir_all(Path::new(&snapshots_dir)).await;

    Ok(())
}

pub async fn check_cluster_is_ready(
    peer_addr: &str,
    timeout_secs: u64,
) -> std::result::Result<(), std::io::Error> {
    let timeout_duration = Duration::from_secs(timeout_secs);
    let retry_interval = Duration::from_millis(500);

    let result = time::timeout(timeout_duration, async {
        loop {
            if TcpStream::connect(peer_addr).await.is_ok() {
                println!("Node is ready!");
                return Ok::<(), std::io::Error>(());
            } else {
                error!("Node({:?}) not ready, retrying...", peer_addr);
                time::sleep(retry_interval).await;
            }
        }
    })
    .await;

    match result {
        Ok(_) => Ok(()),
        Err(_) => {
            let err_msg =
                format!("Node({peer_addr:?}) did not become ready within {timeout_secs} seconds.");
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, err_msg))
        }
    }
}

/// Create TOML config for a node rejoining an existing all-voter cluster.
///
/// Uses `election_timeout_min = 3000ms` so the rejoining node's election timer
/// outlasts the replication worker's max reconnect backoff (`max_delay_ms = 1000ms`),
/// ensuring the leader's first heartbeat arrives before any election fires.
pub fn create_rejoin_node_config(
    node_id: u32,
    port: u16,
    peers: &[(u32, u16)],
) -> String {
    let cluster_entries = peers
        .iter()
        .map(|(id, p)| {
            format!(
                "{{ id = {id}, name = 'n{id}', address = '127.0.0.1:{p}', role = 1, status = 3 }}"
            )
        })
        .collect::<Vec<_>>()
        .join(",\n    ");

    format!(
        r#"[cluster]
node_id = {node_id}
listen_address = '127.0.0.1:{port}'
initial_cluster = [
    {cluster_entries}
]

[raft]
general_raft_timeout_duration_in_ms = 5000
[raft.election]
election_timeout_min = 3000
election_timeout_max = 6000
"#
    )
}

/// Wait until the cluster has a stable leader capable of serving linearizable reads.
///
/// After a node restart or failover, cascading elections may occur before a stable
/// leader emerges. A single `refresh()` is insufficient — the discovered leader may
/// step down before completing a request. This function is the authoritative check:
/// a successful linearizable read proves the leader is actively serving end-to-end.
///
/// Equivalent to the Phase 2+3 retry loop in `leader_failover_cas_standalone`.
///
/// Bounded to 30s: if elections never stabilize (a real regression, not just a slow
/// CI box), this returns the last transient error instead of hanging the test forever
/// with no diagnostic — an unbounded loop here turns "election liveness broke" into a
/// bare test-runner timeout with no indication of what was actually still failing.
pub async fn wait_for_stable_leader(client: &Client) -> Result<(), ClientApiError> {
    const DEADLINE: Duration = Duration::from_secs(30);
    let deadline = tokio::time::Instant::now() + DEADLINE;
    let mut last_err: Option<ClientApiError> = None;

    loop {
        if tokio::time::Instant::now() >= deadline {
            return Err(last_err.expect(
                "deadline reached without ever observing a transient error — unreachable, \
                 the loop only re-checks the deadline after recording one",
            ));
        }

        client.refresh(None).await.ok();
        match client.get(b"__stability_probe__").await {
            Ok(_) => return Ok(()),
            Err(
                e @ ClientApiError::Business {
                    code: ErrorCode::StaleOperation,
                    ..
                },
            ) => {
                last_err = Some(e);
                continue;
            }
            Err(
                e @ ClientApiError::Network {
                    code: ErrorCode::ConnectionTimeout,
                    ..
                },
            ) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            // Cascading elections: the leader `refresh()` just discovered may have
            // already stepped down by the time this read reaches it. Transient and
            // expected — retry (the next loop iteration's `refresh()` will pick up
            // the new leader, including via this error's `leader_hint`).
            Err(
                e @ ClientApiError::Network {
                    code: ErrorCode::NotLeader,
                    ..
                },
            ) => {
                last_err = Some(e);
                tokio::time::sleep(Duration::from_millis(100)).await;
                continue;
            }
            Err(e) => return Err(e),
        }
    }
}

/// Polls `f` until the result satisfies `predicate`, or `max_attempts` is reached.
/// Sleeps `delay` between attempts. Always returns the last value produced.
pub async fn retry_until<T, F, Fut>(
    max_attempts: usize,
    delay: Duration,
    mut f: F,
    predicate: impl Fn(&T) -> bool,
) -> T
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
{
    assert!(max_attempts > 0, "max_attempts must be at least 1");
    let mut last = f().await;
    for _ in 1..max_attempts {
        if predicate(&last) {
            return last;
        }
        tokio::time::sleep(delay).await;
        last = f().await;
    }
    last
}

/// Checks whether the given snapshot path exists, is a directory, and contains any files or
/// subdirectories.
///
/// # Arguments
///
/// * `snapshot_path` - A string slice that holds the path to the snapshot directory.
///
/// # Returns
///
/// * `Ok(true)` if the path exists, is a directory, and contains at least one file or subdirectory.
/// * `Ok(false)` if the path does not exist, is not a directory, or is empty.
/// * `Err(ClientApiError)` if an error occurs while reading the directory contents.
pub fn check_path_contents(snapshot_path: &str) -> Result<bool, ClientApiError> {
    let path = Path::new(snapshot_path);

    // Check if path exists first
    if !path.exists() {
        println!("Path '{snapshot_path}' does not exist",);
        return Ok(false);
    }

    // Check if it's a directory
    if !path.is_dir() {
        println!("Path '{snapshot_path}' is not a directory");
        return Ok(false);
    }

    // Read directory contents
    let entries = std::fs::read_dir(path)?;
    let mut has_contents = false;

    for entry in entries {
        let entry = entry?;
        let entry_path = entry.path();

        if entry_path.is_dir() {
            println!("Found subdirectory: {}", entry_path.display());
            has_contents = true;
        } else if entry_path.is_file() {
            println!("Found file: {}", entry_path.display());
            has_contents = true;
        }
    }

    if !has_contents {
        println!("Path '{snapshot_path}' is empty (no files or subdirectories)",);
    }

    Ok(has_contents)
}

/// Guard that holds TCP listeners to prevent port reuse until dropped.
/// This prevents race conditions in parallel test execution where multiple tests
/// might acquire the same port between allocation and actual binding.
pub struct PortGuard {
    pub ports: Vec<u16>,
    _listeners: Vec<std::net::TcpListener>,
}

impl PortGuard {
    /// Access ports as a slice for compatibility with existing test code
    pub fn as_slice(&self) -> &[u16] {
        &self.ports
    }

    /// Release TCP listeners to allow servers to bind these ports.
    /// Must be called before starting nodes that will bind to these ports.
    pub fn release_listeners(&mut self) {
        self._listeners.clear();
    }
}

impl std::ops::Deref for PortGuard {
    type Target = [u16];

    fn deref(&self) -> &Self::Target {
        &self.ports
    }
}

/// Poll `dir` until a .tar.gz file appears in it or timeout expires.
/// Replaces blind sleep() calls when waiting for snapshot generation.
///
/// Takes the exact snapshot directory — snapshots are always at
/// `data_dir/snapshots` (see #10), nested inside each node's own data_dir,
/// not siblings under one shared configurable snapshots_dir.
pub async fn wait_for_snapshot(
    dir: &Path,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if tokio::time::Instant::now() > deadline {
            return false;
        }
        let has_snapshot = std::fs::read_dir(dir)
            .ok()
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .any(|e| e.path().extension().and_then(|s| s.to_str()) == Some("gz"))
            })
            .unwrap_or(false);
        if has_snapshot {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// Allocate available ports and hold them until PortGuard is dropped.
/// This prevents port conflicts in parallel test execution.
pub async fn get_available_ports(count: usize) -> PortGuard {
    use std::net::TcpListener;
    let mut ports = Vec::new();
    let mut listeners = Vec::new();

    for _ in 0..count {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        ports.push(port);
        listeners.push(listener);
    }

    PortGuard {
        ports,
        _listeners: listeners,
    }
}

/// Wait for any surviving node to report a new leader different from `old_leader_id`.
///
/// Polls all receivers every 50ms and returns as soon as any node's view converges
/// on a valid new leader. Watching multiple nodes avoids depending on a single node's
/// task being scheduled promptly under heavy `make test` load.
pub async fn wait_for_new_leader(
    receivers: Vec<watch::Receiver<Option<LeaderInfo>>>,
    old_leader_id: u32,
    timeout: Duration,
) -> LeaderInfo {
    tokio::time::timeout(timeout, async move {
        loop {
            for rx in &receivers {
                if let Some(leader) = *rx.borrow()
                    && leader.leader_id != 0
                    && leader.leader_id != old_leader_id
                {
                    return leader;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("Timeout waiting for new leader election")
}
