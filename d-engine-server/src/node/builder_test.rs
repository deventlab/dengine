use std::path::PathBuf;
use std::sync::Arc;

use d_engine_core::Error;
use d_engine_core::FlushPolicy;
use d_engine_core::LogStore;
use d_engine_core::MockStateMachine;
use d_engine_core::MockStorageEngine;
use d_engine_core::PersistenceConfig;
use d_engine_core::RaftNodeConfig;
use d_engine_core::StateMachine;
use d_engine_core::StorageEngine;
use serial_test::serial;
use tempfile::TempDir;
use tempfile::tempdir;
use tokio::sync::watch;
use tracing_test::traced_test;

use crate::FileStateMachine;
use crate::FileStorageEngine;
use crate::node::NodeBuilder;
use crate::node::RaftTypeConfig;
use crate::storage::BufferedRaftLog;
use crate::test_utils::insert_raft_log;
use crate::test_utils::insert_state_machine;

/// These components should not be initialized during builder setup; developers should have the
/// highest priority to customize them first.
#[test]
fn test_new_initializes_default_components_with_none() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_new_initializes_default_components",
        shutdown_rx,
    );

    assert!(builder.storage_engine.is_none());
    assert!(builder.state_machine.is_none());
    assert!(builder.transport.is_none());
    assert!(builder.membership.is_none());
    assert!(builder.state_machine_handler.is_none());
    assert!(builder.snapshot_policy.is_none());
    assert!(builder.node.is_none());
}

#[tokio::test]
#[traced_test]
async fn test_set_raft_log_replaces_default() {
    // Prepare RaftTypeConfig components
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let id = 1;

    let mock_storage_engine =
        Arc::new(FileStorageEngine::new(temp_dir.path().join("storage_engine")).unwrap());

    let (buffered_raft_log, receiver) =
        BufferedRaftLog::<RaftTypeConfig<FileStorageEngine, FileStateMachine>>::new(
            id,
            PersistenceConfig {
                flush_policy: FlushPolicy::Batch {
                    idle_flush_interval_ms: 1,
                },
                shutdown_timeout_ms: 5000,
            },
            mock_storage_engine.clone(),
        );
    let buffered_raft_log = buffered_raft_log.start(receiver, None);

    // diff customization raft_log with orgional one
    let expected_raft_log_ids = vec![1, 2];
    insert_raft_log(&buffered_raft_log, expected_raft_log_ids.clone(), 1).await;

    let mock_state_machine =
        Arc::new(FileStateMachine::new(temp_dir.path().join("state_machine")).await.unwrap());

    let expected_state_machine_ids = vec![1, 2, 3];
    insert_state_machine(&mock_state_machine, expected_state_machine_ids.clone(), 1).await;

    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<FileStorageEngine, FileStateMachine>::new_from_db_path(
        temp_dir.path().join("db_path").to_str().unwrap(),
        shutdown_rx,
    )
    .storage_engine(mock_storage_engine)
    .state_machine(mock_state_machine);

    // Verify that raft_log is replaced with customization one
    assert_eq!(
        builder.storage_engine.as_ref().unwrap().log_store().last_index(),
        expected_raft_log_ids[1]
    );
    assert_eq!(
        builder.state_machine.as_ref().unwrap().len(),
        expected_state_machine_ids.len()
    );
}

#[tokio::test]
#[traced_test]
async fn test_build_creates_node() {
    let (_, shutdown_rx) = watch::channel(());
    let temp_dir = tempdir().unwrap();

    // Use FileStateMachine instead of MockStateMachine to properly test try_inject_lease
    // MockStateMachine has issues with mockall not generating proper expectations for &mut self
    // methods
    let file_sm = FileStateMachine::new(temp_dir.path().join("sm")).await.expect("create file sm");

    let builder = NodeBuilder::<MockStorageEngine, FileStateMachine>::new_from_db_path(
        temp_dir.path().to_str().unwrap(),
        shutdown_rx,
    )
    .state_machine(Arc::new(file_sm))
    .storage_engine(Arc::new(MockStorageEngine::new()))
    .build()
    .await
    .expect("build should succeed");

    // Verify that the node instance is generated
    assert!(builder.node.is_some());
}

#[tokio::test]
async fn test_start_fails_without_components() {
    let (_, shutdown_rx) = watch::channel(());
    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::new_from_db_path(
        "/tmp/test_start_fails_without_components",
        shutdown_rx,
    );

    // start() should fail if storage_engine or state_machine not set
    let result = builder.start().await;
    assert!(result.is_err());
}

// Test helper function: create a temporary configuration file
fn create_temp_config(content: &str) -> (PathBuf, String) {
    let dir = tempdir().unwrap();
    let file_path = dir.path().join("test_config.toml");
    std::fs::write(&file_path, content).unwrap();
    (dir.keep(), file_path.to_str().unwrap().to_string())
}

#[test]
fn test_config_override_invalid_path() {
    let base_config = RaftNodeConfig::new().unwrap();
    let base_config = base_config.validate().unwrap();
    let result = base_config.with_override_config("non_existent.toml");

    assert!(
        matches!(result, Err(Error::Config(_))),
        "Should return ConfigError for invalid path"
    );
}

#[test]
#[serial]
fn test_config_override_invalid_format() {
    let (_dir, config_path) = create_temp_config(
        r#"
    invalid_toml_format
    cluster: {
    data_dir: "./custom_db"
    }
    "#,
    );

    let base_config = RaftNodeConfig::new().unwrap();
    let base_config = base_config.validate().unwrap();
    let result = base_config.with_override_config(&config_path);

    assert!(
        matches!(result, Err(Error::Config(_))),
        "Should return ConfigError for invalid format"
    );
}

#[test]
#[serial]
fn test_config_override_priority() {
    // default listen_address = 127.0.0.1:9081 < file override (:9082) < env override (:9083).
    // Uses listen_address rather than node_id: overriding node_id alone would fail the
    // separate self-in-cluster validate() check against the default initial_cluster (id=1).
    let (_dir, config_path) = create_temp_config(
        r#"
    [cluster]
    listen_address = "127.0.0.1:9082"
    "#,
    );

    temp_env::with_vars(
        vec![
            ("CONFIG_PATH", Some(config_path.as_str())),
            ("RAFT__CLUSTER__LISTEN_ADDRESS", Some("127.0.0.1:9083")),
        ],
        || {
            let config = RaftNodeConfig::new().unwrap().validate().unwrap();

            // Verify that environment variables have higher priority than configuration
            // files
            assert_eq!(
                config.cluster.listen_address,
                "127.0.0.1:9083".parse::<std::net::SocketAddr>().unwrap(),
                "Environment variable should override file config"
            );
        },
    );
}

#[test]
#[serial]
fn test_partial_override() {
    let (_dir, config_path) = create_temp_config(
        r#"
    [raft.election]
    election_timeout_min = 500
    "#,
    );

    let base_config = RaftNodeConfig::new().unwrap();
    let base_config = base_config.validate().unwrap();
    let updated_config = base_config.with_override_config(&config_path).unwrap();

    // Validate covered fields
    assert_eq!(
        updated_config.raft.election.election_timeout_min, 500,
        "Raft config should be overridden"
    );

    // Verify unmodified fields
    assert_eq!(
        updated_config.cluster.node_id, base_config.cluster.node_id,
        "Cluster config should remain unchanged"
    );
}

#[test]
fn test_from_node_config_preserves_caller_config() {
    let (_, shutdown_rx) = watch::channel(());
    let config = RaftNodeConfig::new().unwrap();
    let mut config = config.validate().unwrap();
    config.cluster.node_id = 99;

    let builder = NodeBuilder::<MockStorageEngine, MockStateMachine>::from_node_config(
        "test-data",
        config,
        shutdown_rx,
    );

    // Verify caller's config is used directly — no detour through RaftNodeConfig::new() defaults
    assert_eq!(
        builder.node_config.cluster.node_id, 99,
        "from_node_config must use caller's config without going through RaftNodeConfig::new()"
    );
}

#[test]
#[serial]
fn test_node_config_setter_syncs_node_id() {
    let (_, shutdown_rx) = watch::channel(());
    let config = RaftNodeConfig::new().unwrap();
    let mut config = config.validate().unwrap();
    config.cluster.node_id = 7;

    let builder =
        NodeBuilder::<MockStorageEngine, MockStateMachine>::new("test-data", None, shutdown_rx)
            .node_config(config);

    // node_config on the builder must reflect the new config after setter
    assert_eq!(
        builder.node_config.cluster.node_id, 7,
        "node_config() setter must sync node_id into both node_config and internal node_id field"
    );
}
