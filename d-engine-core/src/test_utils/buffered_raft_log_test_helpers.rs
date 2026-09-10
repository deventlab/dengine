//! Test helpers for BufferedRaftLog testing
//!
//! Provides utilities to simplify BufferedRaftLog unit tests:
//! - Test context management
//! - Mock entry generation
//! - Crash recovery simulation

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload};

use crate::{
    BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig, RaftLog,
};

/// Test context for BufferedRaftLog tests
pub struct BufferedRaftLogTestContext {
    pub raft_log: Arc<BufferedRaftLog<MockTypeConfig>>,
    pub storage: Arc<MockStorageEngine>,
    pub flush_policy: FlushPolicy,
    pub instance_id: String,
    log_flush_rx: tokio::sync::mpsc::UnboundedReceiver<crate::InternalEvent>,
}

impl BufferedRaftLogTestContext {
    /// Create a new test context with specified strategy and flush policy
    pub fn new(
        flush_policy: FlushPolicy,
        instance_id: &str,
    ) -> Self {
        let storage = Arc::new(MockStorageEngine::with_id(instance_id.to_string()));

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
        std::thread::sleep(std::time::Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            flush_policy,
            instance_id: instance_id.to_string(),
            log_flush_rx,
        }
    }

    /// Stands in for `raft.rs`'s `InternalEvent::FsyncCompleted` handler,
    /// which isn't running in these `BufferedRaftLog`-only unit tests. Call
    /// after any operation that should make `durable_index` advance
    /// (`append_entries`, `flush`, truncation + resync, ...) and before
    /// asserting on `durable_index()` — see
    /// `drain_and_apply_fsync_completions` for why this is necessary since
    /// #446/#447.
    pub fn drain_fsync_completions(&mut self) {
        drain_and_apply_fsync_completions(&self.raft_log, &mut self.log_flush_rx);
    }

    /// Helper to append a batch of entries with specified range and term
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

    /// Create a context where `is_write_durable()=false`.
    ///
    /// Returns the context and a counter incremented on every `flush()` call.
    /// Use this to verify:
    ///   - `durable_index` advances only after flush, not after write
    ///   - Multiple rapid writes are batched into fewer flushes
    pub fn new_not_durable(
        flush_policy: FlushPolicy,
        instance_id: &str,
    ) -> (Self, Arc<AtomicU64>) {
        let (storage, flush_count) = MockStorageEngine::not_durable(instance_id.to_string());
        let storage = Arc::new(storage);

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
        std::thread::sleep(std::time::Duration::from_millis(10));

        let ctx = Self {
            raft_log,
            storage,
            flush_policy,
            instance_id: instance_id.to_string(),
            log_flush_rx,
        };
        (ctx, flush_count)
    }

    /// Simulate crash recovery from the same storage instance
    pub fn recover_from_crash(&self) -> Self {
        // Use same instance ID to recover data from thread_local storage
        let storage = Arc::new(MockStorageEngine::with_id(self.instance_id.clone()));

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

        // Small delay to ensure processor is ready
        std::thread::sleep(std::time::Duration::from_millis(10));

        Self {
            raft_log,
            storage,
            flush_policy: self.flush_policy.clone(),
            instance_id: self.instance_id.clone(),
            log_flush_rx,
        }
    }
}

/// Generate mock log entries with sequential indexes
pub fn mock_entries(
    start: u64,
    count: u64,
    term: u64,
) -> Vec<Entry> {
    (start..start + count)
        .map(|index| Entry {
            index,
            term,
            payload: Some(EntryPayload::command(Bytes::from(
                format!("data_{index}").into_bytes(),
            ))),
        })
        .collect()
}

/// Generate empty mock entries (no payload)
pub fn mock_empty_entries(
    start: u64,
    count: u64,
    term: u64,
) -> Vec<Entry> {
    (start..start + count)
        .map(|index| Entry {
            index,
            term,
            payload: None,
        })
        .collect()
}

/// Insert single entry helper
pub async fn insert_single_entry(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    index: u64,
    term: u64,
) {
    let entry = Entry {
        index,
        term,
        payload: None,
    };
    raft_log.insert_batch(vec![entry]).await.expect("insert should succeed");
}

/// Generate mock insert command payload bytes
fn mock_insert_command_payload(ids: Vec<u64>) -> Bytes {
    let commands: Vec<String> = ids.iter().map(|id| format!("insert_{id}")).collect();
    Bytes::from(commands.join(","))
}

/// Simulate inserting command entries into the log
///
/// Creates command entries with pre-allocated indexes and appends them to the log.
/// Each ID becomes a command payload with the given term.
pub async fn simulate_insert_command(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    ids: Vec<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in ids {
        let entry = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(mock_insert_command_payload(vec![id]))),
        };
        entries.push(entry);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

/// Simulate deleting entries from the log for a range of IDs
///
/// Creates delete command entries for each ID in the specified range and appends
/// them to the log. Each ID in the range becomes a separate delete command entry.
pub async fn simulate_delete_command(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    id_range: std::ops::RangeInclusive<u64>,
    term: u64,
) {
    let mut entries = Vec::new();
    for id in id_range {
        let entry = Entry {
            index: raft_log.pre_allocate_raft_logs_next_index(),
            term,
            payload: Some(EntryPayload::command(Bytes::from(format!("delete_{id}")))),
        };
        entries.push(entry);
    }
    raft_log.insert_batch(entries).await.unwrap();
    raft_log.flush().await.unwrap();
}

/// Stands in for `raft.rs`'s `InternalEvent::FsyncCompleted` handler, which
/// `BufferedRaftLog`-only unit tests don't have running. Since #446/#447,
/// `durable_index` only advances when something calls
/// `try_advance_durable_index(index, term)` in response to that event —
/// `FsyncCoordinator`/`IOTask::ReplaceRange`'s `notify_fsync_completed` only
/// *sends* the event, it never writes `durable_index` itself. A test that
/// registers a `log_flush_tx` and wants to see `durable_index()` actually
/// advance must drain that channel through this helper — otherwise the
/// event sits unread and `durable_index()` never moves, no matter how long
/// you sleep.
pub fn drain_and_apply_fsync_completions(
    raft_log: &Arc<BufferedRaftLog<MockTypeConfig>>,
    log_flush_rx: &mut tokio::sync::mpsc::UnboundedReceiver<crate::InternalEvent>,
) {
    while let Ok(event) = log_flush_rx.try_recv() {
        if let crate::InternalEvent::FsyncCompleted(mark) = event {
            raft_log.try_advance_durable_index(mark);
        }
    }
}
