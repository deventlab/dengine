use std::collections::HashMap;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use uuid::Uuid;

use crate::HardState;
use crate::MockLogStore;
use crate::MockMetaStore;
use crate::StorageEngine;

// Process-global storage so data persists across threads (e.g. raft-io std::thread vs test thread).
// Tests are isolated by unique instance_id keys; nextest runs each test in its own process.
static MOCK_STORAGE_DATA: LazyLock<Mutex<HashMap<String, Vec<u8>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

#[derive(Debug, Clone)]
pub struct MockStorageEngine {
    log_store: Arc<MockLogStore>,
    meta_store: Arc<MockMetaStore>,

    // Add an ID to differentiate between instances while sharing data
    #[allow(unused)]
    instance_id: String,
}

impl MockStorageEngine {
    pub fn new() -> Self {
        Self::with_id(Uuid::new_v4().to_string())
    }

    pub fn with_id(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        Self::configure_durable(&mut mock_log_store);

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `is_write_durable()=false` and `flush()` is tracked.
    ///
    /// Returns the engine and a shared counter incremented on every `flush()` call.
    /// Use this to test the write/flush separation and batch-flush efficiency.
    pub fn not_durable(id: String) -> (Self, Arc<AtomicU64>) {
        let flush_count = Arc::new(AtomicU64::new(0));
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        // configure_not_durable is called separately so its expectations are the only
        // ones for is_write_durable/flush — no FIFO conflict with configure_durable.
        Self::configure_not_durable(&mut mock_log_store, flush_count.clone());

        let engine = Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        };

        (engine, flush_count)
    }

    pub fn from(
        log_store: MockLogStore,
        meta_store: MockMetaStore,
    ) -> Self {
        Self {
            log_store: Arc::new(log_store),
            meta_store: Arc::new(meta_store),
            instance_id: Uuid::new_v4().to_string(),
        }
    }

    fn configure_mocks(
        log_store: &mut MockLogStore,
        meta_store: &mut MockMetaStore,
        instance_id: &str,
    ) {
        let instance_id = instance_id.to_string();

        // Last index implementation with persistence simulation
        log_store.expect_last_index().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_last_index");
                data.get(&key)
                    .map(|v| {
                        let bytes: [u8; 8] = v.as_slice().try_into().unwrap();
                        u64::from_be_bytes(bytes)
                    })
                    .unwrap_or(0)
            }
        });

        // persist_entries is NOT configured here — extracted into
        // configure_persist_entries_success() so failure-variants (e.g.
        // not_durable_first_persist_fails) can install their own expectation
        // first without a mockall FIFO conflict (an always-succeeds catch-all
        // registered here would silently absorb every call before a
        // stricter, later expectation ever got a turn).

        // Get entry implementation with persistence simulation
        log_store.expect_entry().returning({
            let instance_id_ref = instance_id.clone();
            move |index| {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_entry_{index}");
                Ok(data.get(&key).map(|v| bincode::deserialize(v).unwrap()))
            }
        });

        // Get entries range implementation with persistence simulation
        log_store.expect_get_entries().returning({
            let instance_id_ref = instance_id.clone();
            move |range| {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let mut result = Vec::new();
                for index in *range.start()..=*range.end() {
                    let key = format!("{instance_id_ref}_entry_{index}");
                    if let Some(value) = data.get(&key) {
                        result.push(bincode::deserialize(value).unwrap());
                    }
                }
                Ok(result)
            }
        });

        // replace_range is NOT configured here — extracted into
        // configure_replace_range_success() for the same FIFO-ordering
        // reason as persist_entries above.

        // purge is NOT configured here — extracted into
        // configure_purge_success() for the same FIFO-ordering reason as
        // persist_entries/replace_range above.
        log_store.expect_load_purge_boundary().returning(|| Ok(None));
        log_store.expect_truncate().returning({
            let instance_id_ref = instance_id.clone();
            move |from_index| {
                let mut data = MOCK_STORAGE_DATA.lock().unwrap();
                let prefix = format!("{instance_id_ref}_entry_");
                let keys_to_delete: Vec<String> = data
                    .keys()
                    .filter(|k| {
                        k.strip_prefix(&prefix)
                            .and_then(|s| s.parse::<u64>().ok())
                            .is_some_and(|idx| idx >= from_index)
                    })
                    .cloned()
                    .collect();
                for key in keys_to_delete {
                    data.remove(&key);
                }
                let new_last = from_index.saturating_sub(1);
                let last_key = format!("{instance_id_ref}_last_index");
                if new_last == 0 {
                    data.remove(&last_key);
                } else {
                    data.insert(last_key, new_last.to_be_bytes().to_vec());
                }
                Ok(())
            }
        });
        // reset is NOT configured here — extracted into
        // configure_reset_success() for the same FIFO-ordering reason as
        // persist_entries/replace_range/purge above.

        // save_hard_state is NOT configured here — extracted into
        // configure_save_hard_state_success() for the same FIFO-ordering
        // reason as persist_entries/replace_range/purge/reset above.
        meta_store.expect_load_hard_state().returning({
            let instance_id_ref = instance_id.clone();
            move || {
                let data = MOCK_STORAGE_DATA.lock().unwrap();
                let key = format!("{instance_id_ref}_meta_hard_state");
                Ok(data.get(&key).map(|bytes| {
                    bincode::deserialize::<HardState>(bytes).expect("deserialize hard_state")
                }))
            }
        });
        meta_store.expect_flush().returning(|| Ok(()));
        meta_store.expect_flush_async().returning(|| Ok(()));
    }

    /// Default `persist_entries()`: always succeeds, writes into
    /// `MOCK_STORAGE_DATA`. Extracted out of `configure_mocks` so that
    /// failure-variants (e.g. `not_durable_first_persist_fails`) can install
    /// their own stricter expectation *first* — see the comment left in
    /// `configure_mocks` for why ordering here matters (mockall FIFO
    /// matching).
    fn configure_persist_entries_success(
        log_store: &mut MockLogStore,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        log_store.expect_persist_entries().returning(move |entries| {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            for entry in &entries {
                let key = format!("{}_entry_{}", instance_id_ref, entry.index);
                let value = bincode::serialize(entry).unwrap();
                data.insert(key, value);
            }
            if let Some(last_entry) = entries.last() {
                let key = format!("{instance_id_ref}_last_index");
                data.insert(key, last_entry.index.to_be_bytes().to_vec());
            }
            Ok(())
        });
    }

    /// Default `replace_range()`: always succeeds, atomically deletes entries >= `from_index` then inserts `new_entries`. Extracted out of `configure_mocks` for the same reason as `configure_persist_entries_success`.
    fn configure_replace_range_success(
        log_store: &mut MockLogStore,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        log_store.expect_replace_range().returning(move |from_index, new_entries| {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            let prefix = format!("{instance_id_ref}_entry_");
            let keys_to_delete: Vec<String> = data
                .keys()
                .filter(|k| {
                    k.strip_prefix(&prefix)
                        .and_then(|s| s.parse::<u64>().ok())
                        .is_some_and(|idx| idx >= from_index)
                })
                .cloned()
                .collect();
            for key in keys_to_delete {
                data.remove(&key);
            }
            let new_last_index = if new_entries.is_empty() {
                from_index.saturating_sub(1)
            } else {
                for entry in &new_entries {
                    let key = format!("{}_entry_{}", instance_id_ref, entry.index);
                    data.insert(key, bincode::serialize(entry).unwrap());
                }
                new_entries.last().unwrap().index
            };
            let last_key = format!("{instance_id_ref}_last_index");
            if new_last_index == 0 {
                data.remove(&last_key);
            } else {
                data.insert(last_key, new_last_index.to_be_bytes().to_vec());
            }
            Ok(())
        });
    }

    /// Default `purge()`: always succeeds. Extracted out of `configure_mocks`
    /// for the same reason as persist_entries/replace_range.
    fn configure_purge_success(log_store: &mut MockLogStore) {
        log_store.expect_purge().returning(|_| Ok(()));
    }

    /// Default `reset()`: always succeeds, clears entries and last_index for
    /// this instance. Extracted out of `configure_mocks` for the same reason
    /// as persist_entries/replace_range/purge.
    fn configure_reset_success(
        log_store: &mut MockLogStore,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        log_store.expect_reset().returning(move || {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            let last_index_key = format!("{instance_id_ref}_last_index");
            let keys: Vec<String> = data
                .keys()
                .filter(|k| {
                    k.starts_with(&format!("{instance_id_ref}_entry_"))
                        || k.as_str() == last_index_key
                })
                .cloned()
                .collect();
            for key in keys {
                data.remove(&key);
            }
            Ok(())
        });
    }

    /// Default `save_hard_state()`: always succeeds, writes into
    /// `MOCK_STORAGE_DATA`. Extracted out of `configure_mocks` for the same
    /// reason as persist_entries/replace_range/purge/reset.
    fn configure_save_hard_state_success(
        meta_store: &mut MockMetaStore,
        instance_id: &str,
    ) {
        let instance_id_ref = instance_id.to_string();
        meta_store.expect_save_hard_state().returning(move |state| {
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            let key = format!("{instance_id_ref}_meta_hard_state");
            let value = bincode::serialize(state).expect("serialize hard_state");
            data.insert(key, value);
            Ok(())
        });
    }

    /// Create a MockStorageEngine where `is_write_durable()=false` and the **first**
    /// `flush()` call returns an error, simulating a transient fsync failure.
    ///
    /// After a failed fsync, `batch_processor` logs the error and does NOT zero
    /// `pending_max` (the success branch `else { pending_max = 0 }` is not taken).
    /// This is the deterministic pre-condition needed to exercise the bug where
    /// `run_storage_tasks(IOTask::Reset)` forgets to zero `pending_max`.
    pub fn not_durable_first_flush_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        // First flush fails — leaves pending_max non-zero (only success path zeros it).
        mock_log_store
            .expect_flush()
            .once()
            .returning(|| Err(crate::Error::Fatal("simulated fsync failure".into())));
        // All subsequent flushes succeed.
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `is_write_durable()=false` and the **first**
    /// `persist_entries()` call (page-cache write) returns an error, simulating a
    /// transient write failure (e.g. disk full, backend rejects the write batch).
    ///
    /// Distinct from `not_durable_first_flush_fails`, which only fails the later
    /// fsync step — `persist_entries()` and `flush()` are two independent failure
    /// surfaces in `BufferedRaftLog` (`persist_pending_range` vs
    /// `FsyncCoordinator::run_until_caught_up`). `flush()` itself always succeeds
    /// here, keeping the two surfaces isolated. All subsequent `persist_entries()`
    /// calls succeed.
    pub fn not_durable_first_persist_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        // First persist_entries() call fails. Registered before the
        // always-succeeds fallback below — mockall tries expectations in
        // registration order, so the stricter `.once()` one must come first.
        mock_log_store.expect_persist_entries().once().returning(|_| {
            Err(crate::Error::Fatal(
                "simulated persist_entries failure".into(),
            ))
        });
        // All subsequent persist_entries() calls succeed.
        Self::configure_persist_entries_success(&mut mock_log_store, &id);

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `replace_range()` always fails,
    /// simulating a fatal storage error during conflict-resolution
    /// (truncate + write). `run_storage_tasks`'s `IOTask::ReplaceRange`
    /// arm treats this as unrecoverable — disk state is now uncertain.
    pub fn not_durable_replace_range_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));
        mock_log_store.expect_replace_range().returning(|_, _| {
            Err(crate::Error::Fatal(
                "simulated replace_range failure".into(),
            ))
        });

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `purge()` always fails, simulating a
    /// fatal storage error during log compaction.
    pub fn not_durable_purge_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));
        mock_log_store
            .expect_purge()
            .returning(|_| Err(crate::Error::Fatal("simulated purge failure".into())));

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `reset()` always fails, simulating a
    /// fatal storage error during snapshot install / log rewind.
    pub fn not_durable_reset_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));
        mock_log_store
            .expect_reset()
            .returning(|| Err(crate::Error::Fatal("simulated reset failure".into())));

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `save_hard_state()` always fails,
    /// simulating a fatal storage error while persisting current_term/voted_for.
    pub fn not_durable_save_hard_state_fails(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(|| Ok(()));
        mock_log_store.expect_flush_async().returning(|| Ok(()));
        mock_meta_store.expect_save_hard_state().returning(|_| {
            Err(crate::Error::Fatal(
                "simulated save_hard_state failure".into(),
            ))
        });

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `is_write_durable()=false` and every
    /// `flush()` call returns an error, simulating a persistent IO device failure.
    ///
    /// Use this to verify that callers (e.g. `flush()`) propagate the error back
    /// rather than hanging forever waiting for a `durable_index` that never advances.
    pub fn not_durable_always_failing_flush(id: String) -> Self {
        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store
            .expect_flush()
            .returning(|| Err(crate::Error::Fatal("simulated fsync failure".into())));
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        }
    }

    /// Create a MockStorageEngine where `is_write_durable()=false` and the FIRST
    /// `flush()` call blocks until the returned sender is signalled — every call
    /// after that returns `Ok(())` immediately.
    ///
    /// Use this to deterministically test behavior *while* a physical fsync is
    /// in flight (e.g. `durable_index` must not advance yet), without relying on
    /// a fixed `sleep()` and hoping the assertion runs inside the window.
    pub fn not_durable_gated_flush(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(move || {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Ok(())
        });
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        let engine = Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        };

        (engine, tx)
    }

    /// Same as `not_durable_gated_flush`, but also returns a shared counter of how
    /// many times `flush()` was physically invoked — for asserting how many fsync
    /// rounds a batch of concurrent `flush()` callers actually triggered.
    pub fn not_durable_gated_flush_counted(
        id: String
    ) -> (Self, std::sync::mpsc::Sender<()>, Arc<AtomicU64>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));
        let flush_count = Arc::new(AtomicU64::new(0));

        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        let counter = flush_count.clone();
        mock_log_store.expect_flush().returning(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Ok(())
        });
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        let engine = Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        };

        (engine, tx, flush_count)
    }

    /// Same as `not_durable_gated_flush`, but every `flush()` call — the first
    /// gated one and any that follow — returns `Err` once unblocked, simulating
    /// a disk failure discovered right as the fsync finally runs.
    pub fn not_durable_gated_flush_failing(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        Self::configure_persist_entries_success(&mut mock_log_store, &id);
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        mock_log_store.expect_is_write_durable().returning(|| false);
        mock_log_store.expect_flush().returning(move || {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            Err(crate::Error::Fatal("simulated fsync failure".into()))
        });
        mock_log_store.expect_flush_async().returning(|| Ok(()));

        let engine = Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        };

        (engine, tx)
    }

    /// Create a MockStorageEngine where the first `persist_entries()` call blocks
    /// until the returned sender fires. `flush()`/`is_write_durable()` are left at
    /// their always-succeeds default (`configure_durable`) — this gate is only
    /// about the write-to-storage-engine step, not fsync.
    ///
    /// Use this to freeze the IO thread mid-persist so a concurrent truncation
    /// can be driven deterministically — see
    /// `durable_index_truncation_clamp_test.rs`.
    pub fn not_durable_gated_persist(id: String) -> (Self, std::sync::mpsc::Sender<()>) {
        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let rx = Mutex::new(Some(rx));

        let mut mock_log_store = MockLogStore::new();
        let mut mock_meta_store = MockMetaStore::new();

        Self::configure_mocks(&mut mock_log_store, &mut mock_meta_store, &id);
        // persist_entries is gated below instead of via configure_persist_entries_success.
        Self::configure_replace_range_success(&mut mock_log_store, &id);
        Self::configure_purge_success(&mut mock_log_store);
        Self::configure_reset_success(&mut mock_log_store, &id);
        Self::configure_save_hard_state_success(&mut mock_meta_store, &id);
        Self::configure_durable(&mut mock_log_store);

        let instance_id_ref = id.clone();
        mock_log_store.expect_persist_entries().returning(move |entries| {
            // Only the first call blocks — take() leaves None for subsequent calls.
            if let Some(gate) = rx.lock().unwrap().take() {
                let _ = gate.recv(); // blocks until the test sends ()
            }
            let mut data = MOCK_STORAGE_DATA.lock().unwrap();
            for entry in &entries {
                let key = format!("{instance_id_ref}_entry_{}", entry.index);
                let value = bincode::serialize(entry).unwrap();
                data.insert(key, value);
            }
            if let Some(last_entry) = entries.last() {
                let key = format!("{instance_id_ref}_last_index");
                data.insert(key, last_entry.index.to_be_bytes().to_vec());
            }
            Ok(())
        });

        let engine = Self {
            log_store: Arc::new(mock_log_store),
            meta_store: Arc::new(mock_meta_store),
            instance_id: id,
        };

        (engine, tx)
    }

    /// Configure `is_write_durable=true` and no-op flush (durable mock).
    fn configure_durable(log_store: &mut MockLogStore) {
        log_store.expect_is_write_durable().returning(|| true);
        log_store.expect_flush().returning(|| Ok(()));
        log_store.expect_flush_async().returning(|| Ok(()));
    }

    /// Configure `is_write_durable=false` with a flush counter.
    fn configure_not_durable(
        log_store: &mut MockLogStore,
        flush_count: Arc<AtomicU64>,
    ) {
        log_store.expect_is_write_durable().returning(|| false);
        let counter = flush_count.clone();
        log_store.expect_flush().returning(move || {
            counter.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
        let counter2 = flush_count.clone();
        log_store.expect_flush_async().returning(move || {
            counter2.fetch_add(1, Ordering::Relaxed);
            Ok(())
        });
    }
}
impl Default for MockStorageEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageEngine for MockStorageEngine {
    type LogStore = MockLogStore;
    type MetaStore = MockMetaStore;

    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}
