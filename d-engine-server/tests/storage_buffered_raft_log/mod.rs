//! Integration tests for BufferedRaftLog with real FileStorageEngine
//!
//! These tests verify BufferedRaftLog behavior with actual disk I/O,
//! crash recovery semantics, and performance characteristics.
//!
//! ## Test Modules
//!
//! - `crash_recovery_test`: Real disk persistence and crash recovery
//! - `performance_test`: I/O performance benchmarks
//! - `stress_test`: High concurrency stress testing
//! - `storage_integration_test`: Storage-level integration

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use d_engine_core::{BufferedRaftLog, FlushPolicy, PersistenceConfig, RaftLog, alias::ROF};
use d_engine_proto::common::{Entry, EntryPayload};
use d_engine_server::{FileStateMachine, FileStorageEngine, node::RaftTypeConfig};
use tempfile::tempdir;

mod crash_recovery_test;
mod performance_test;
mod quorum_crash_recovery_test;
mod storage_integration_test;
mod stress_test;

/// Test context with real FileStorageEngine for integration tests
pub struct TestContext {
    pub raft_log: Arc<ROF<RaftTypeConfig<FileStorageEngine, FileStateMachine>>>,
    pub storage: Arc<FileStorageEngine>,
    pub _temp_dir: Option<tempfile::TempDir>,
    pub flush_policy: FlushPolicy,
    pub path: String,
    log_flush_rx: tokio::sync::mpsc::UnboundedReceiver<d_engine_core::InternalEvent>,
}

impl TestContext {
    /// Create new test context with FileStorageEngine
    pub fn new(
        flush_policy: FlushPolicy,
        instance_id: &str,
    ) -> Self {
        let temp_dir = tempdir().unwrap();
        let path = temp_dir.path().to_path_buf().join(instance_id);
        let storage = Arc::new(FileStorageEngine::new(path.clone()).unwrap());

        let (raft_log, receiver) = BufferedRaftLog::new(
            1,
            PersistenceConfig {
                flush_policy: flush_policy.clone(),
                shutdown_timeout_ms: 5000,
            },
            storage.clone(),
        );
        let (log_flush_tx, log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft_log = raft_log.start(receiver, Some(log_flush_tx));

        // Small delay to ensure processor is ready
        std::thread::sleep(Duration::from_millis(10));

        Self {
            path: path.to_str().unwrap().to_string(),
            raft_log,
            storage,
            flush_policy,
            _temp_dir: Some(temp_dir),
            log_flush_rx,
        }
    }

    /// Stands in for `raft.rs`'s `InternalEvent::FsyncCompleted` handler,
    /// which isn't running in these `BufferedRaftLog`-only integration
    /// tests. Since #446/#447, `durable_index` only advances when something
    /// drains that event and calls `try_advance_durable_index` — call this
    /// after any operation that should make `durable_index` advance and
    /// before asserting on it. Not needed after `recover_from_crash()`: the
    /// recovered context's `durable_index` is derived directly from on-disk
    /// state at construction, not from this event.
    pub fn drain_fsync_completions(&mut self) {
        while let Ok(event) = self.log_flush_rx.try_recv() {
            if let d_engine_core::InternalEvent::FsyncCompleted(mark) = event {
                self.raft_log.try_advance_durable_index(mark);
            }
        }
    }

    /// Explicitly close the raft log IO thread.
    ///
    /// Must be called at the end of tests using graceful-shutdown semantics.
    /// Unlike `drop()` which only sends Shutdown (fire-and-forget), `close()`
    /// joins the IO thread before returning, preventing Tokio runtime
    /// shutdown panics.
    pub async fn close(self) {
        self.raft_log.close().await;
        // _temp_dir drops here, after the IO thread has fully exited
    }

    /// Simulate crash recovery by creating new context from same storage path
    pub fn recover_from_crash(&self) -> Self {
        let temp_dir = tempdir().unwrap();
        let storage = Arc::new(FileStorageEngine::new(PathBuf::from(self.path.clone())).unwrap());

        let (raft_log, receiver) = BufferedRaftLog::new(
            1,
            PersistenceConfig {
                flush_policy: self.flush_policy.clone(),
                shutdown_timeout_ms: 5000,
            },
            storage.clone(),
        );
        let (log_flush_tx, log_flush_rx) = tokio::sync::mpsc::unbounded_channel();
        let raft_log = raft_log.start(receiver, Some(log_flush_tx));

        std::thread::sleep(Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            flush_policy: self.flush_policy.clone(),
            _temp_dir: Some(temp_dir),
            path: self.path.clone(),
            log_flush_rx,
        }
    }

    /// Helper to append a batch of entries
    pub async fn append_entries(
        &self,
        start: u64,
        count: u64,
        term: u64,
    ) {
        let entries: Vec<_> = (start..start + count)
            .map(|index| Entry {
                index,
                term,
                payload: Some(EntryPayload::command(Bytes::from(b"data".to_vec()))),
            })
            .collect();

        self.raft_log.append_entries(entries).await.unwrap();
    }
}
