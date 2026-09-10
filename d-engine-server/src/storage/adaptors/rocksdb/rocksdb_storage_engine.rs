use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use d_engine_core::Error;
use d_engine_core::HardState;
use d_engine_core::LogStore;
use d_engine_core::MetaStore;
use d_engine_core::StorageEngine;
use d_engine_core::StorageError;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use prost::Message;
use rocksdb::WriteOptions;
use std::path::Path;

use async_trait::async_trait;
use rocksdb::Cache;
use rocksdb::ColumnFamilyDescriptor;
use rocksdb::DB;
use rocksdb::Direction;
use rocksdb::IteratorMode;
use rocksdb::WriteBatch;
use tracing::instrument;

use super::LOG_CF;
use super::META_CF;

const HARD_STATE_KEY: &[u8] = b"hard_state";
const PURGE_BOUNDARY_KEY: &[u8] = b"purge_boundary";

/// RocksDB-based log store implementation
#[derive(Debug)]
pub struct RocksDBLogStore {
    db: Arc<DB>,
    last_index: AtomicU64,
}

/// RocksDB-based metadata store implementation
#[derive(Debug)]
pub struct RocksDBMetaStore {
    db: Arc<DB>,
}

/// RocksDB-based Raft log storage
///
/// High-performance storage engine using RocksDB for durability.
/// Recommended for production deployments.
///
/// Obtain an instance via [`super::RocksDBUnifiedEngine::open`], which shares a single
/// `Arc<DB>` with [`super::RocksDBStateMachine`] to halve resource usage.
///
/// # Features
///
/// - ACID transactions
/// - Compression support
/// - Configurable write options
/// - High write throughput
#[derive(Debug)]
pub struct RocksDBStorageEngine {
    log_store: Arc<RocksDBLogStore>,
    meta_store: Arc<RocksDBMetaStore>,
}

impl RocksDBStorageEngine {
    /// Opens (or creates) a dedicated RocksDB instance for log + meta storage.
    ///
    /// Used when `unified_db = false` (default): each engine owns its own DB instance.
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let db_opts = super::base_db_options();

        let cache = Cache::new_lru_cache(128 * 1024 * 1024);
        let log_cf = ColumnFamilyDescriptor::new(LOG_CF, super::log_cf_options(&cache));
        let meta_cf = ColumnFamilyDescriptor::new(META_CF, super::meta_cf_options(&cache));

        let db = DB::open_cf_descriptors(&db_opts, path, vec![log_cf, meta_cf])
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        let db_arc = Arc::new(db);

        let log_store = Arc::new(RocksDBLogStore::new(Arc::clone(&db_arc))?);
        let meta_store = Arc::new(RocksDBMetaStore::new(Arc::clone(&db_arc))?);
        Ok(Self {
            log_store,
            meta_store,
        })
    }

    /// Creates a storage engine sharing an existing `Arc<DB>` (unified mode).
    ///
    /// Called by `RocksDBUnifiedEngine::open()` — the caller owns the DB lifecycle.
    /// The DB must already have `LOG_CF` and `META_CF` column families open.
    pub(super) fn from_shared_db(db: Arc<DB>) -> Result<Self, Error> {
        let log_store = Arc::new(RocksDBLogStore::new(Arc::clone(&db))?);
        let meta_store = Arc::new(RocksDBMetaStore::new(Arc::clone(&db))?);
        Ok(Self {
            log_store,
            meta_store,
        })
    }

    /// Helper: convert index to big-endian bytes
    #[allow(dead_code)]
    #[inline]
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }

    /// Helper: convert key bytes to index
    #[allow(dead_code)]
    #[inline]
    fn key_to_index(key: &[u8]) -> u64 {
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[0..8]);
        u64::from_be_bytes(bytes)
    }
}

impl StorageEngine for RocksDBStorageEngine {
    type LogStore = RocksDBLogStore;
    type MetaStore = RocksDBMetaStore;

    #[inline]
    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    #[inline]
    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

impl Drop for RocksDBLogStore {
    fn drop(&mut self) {
        // WAL fsync makes LOG_CF durable — memtable content survives via WAL
        // replay on next open even without the flush below. The flush is an
        // optimization only: it shrinks the WAL that must be replayed on the
        // next restart. db.flush() (no CF argument) targets RocksDB's unused
        // "default" CF and would be a no-op here — LOG_CF must be named explicitly.
        if let Err(e) = self.db.flush_wal(true) {
            tracing::error!("Failed to flush WAL on drop: {}", e);
        }
        match self.db.cf_handle(LOG_CF) {
            Some(cf) => {
                if let Err(e) = self.db.flush_cf(&cf) {
                    tracing::error!("Failed to flush LOG_CF memtable on drop: {}", e);
                } else {
                    tracing::debug!("RocksDBLogStore flushed successfully on drop");
                }
            }
            None => tracing::error!("Failed to flush on drop: LOG_CF not found"),
        }
    }
}

impl RocksDBLogStore {
    fn new(db: Arc<DB>) -> Result<Self, Error> {
        let mut last_index = 0;

        // Find the last index by seeking to the end and reading the first (largest) key
        // IteratorMode::End positions at the end, next() gives us the largest key
        if let Some(cf) = db.cf_handle(LOG_CF) {
            let mut iter = db.iterator_cf(&cf, IteratorMode::End);
            if let Some(Ok((key, _))) = iter.next()
                && key.len() == 8
            {
                last_index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
            }
        }

        Ok(Self {
            db,
            last_index: AtomicU64::new(last_index),
        })
    }

    #[inline]
    fn index_to_key(index: u64) -> [u8; 8] {
        index.to_be_bytes()
    }
}

#[async_trait]
impl LogStore for RocksDBLogStore {
    #[instrument(skip(self, entries))]
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let mut batch = WriteBatch::default();
        let mut max_index = 0;

        for entry in entries {
            let key = Self::index_to_key(entry.index);
            let value = entry.encode_to_vec();
            batch.put_cf(&cf, key, value);
            max_index = max_index.max(entry.index);
        }

        let mut write_opts = WriteOptions::default();
        // sync=false: write to OS page cache only. The IO thread calls flush_wal(true)
        // (Method C drain-then-fsync) to make entries crash-safe in a single batched fsync.
        write_opts.set_sync(false);
        self.db
            .write_opt(&batch, &write_opts)
            .map_err(|e| StorageError::DbError(e.to_string()))?;

        if max_index > 0 {
            self.last_index.store(max_index, Ordering::SeqCst);
        }

        Ok(())
    }

    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let key = Self::index_to_key(index);
        match self.db.get_cf(&cf, key).map_err(|e| StorageError::DbError(e.to_string()))? {
            Some(bytes) => Entry::decode(&*bytes)
                .map(Some)
                .map_err(|e| StorageError::SerializationError(e.to_string()).into()),
            None => Ok(None),
        }
    }

    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(*range.start());
        let _end_key = Self::index_to_key(*range.end());
        let mut entries = Vec::new();

        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, value) = item.map_err(|e| StorageError::DbError(e.to_string()))?;

            // Convert key to u64 for comparison
            if key.len() != 8 {
                continue;
            }

            let key_index = u64::from_be_bytes([
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
            ]);

            if key_index > *range.end() {
                break;
            }

            let entry = Entry::decode(&*value)
                .map_err(|e| StorageError::SerializationError(e.to_string()))?;
            entries.push(entry);
        }

        Ok(entries)
    }

    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(0);
        let _end_key = Self::index_to_key(cutoff_index.index);
        let mut batch = WriteBatch::default();

        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;

            if key.len() != 8 {
                continue;
            }

            let key_index = u64::from_be_bytes([
                key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
            ]);

            if key_index > cutoff_index.index {
                break;
            }

            batch.delete_cf(&cf, &key);
        }

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Persist purge boundary to META_CF for crash recovery.
        // BufferedRaftLog::new() reads this on restart to restore last_purged_index/term
        // so that entry_term(last_purged_index) returns the correct term after restart.
        if let Some(cf_meta) = self.db.cf_handle(META_CF) {
            let encoded = cutoff_index.encode_to_vec();
            self.db
                .put_cf(&cf_meta, PURGE_BOUNDARY_KEY, encoded)
                .map_err(|e| StorageError::DbError(e.to_string()))?;
        }

        Ok(())
    }

    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let start_key = Self::index_to_key(from_index);
        let mut batch = WriteBatch::default();

        // Get the last_index before deletion
        let current_last_index = self.last_index.load(Ordering::SeqCst);

        // Collect all keys to be deleted
        let mut keys_to_delete = Vec::new();
        let iter = self.db.iterator_cf(&cf, IteratorMode::From(&start_key, Direction::Forward));

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            keys_to_delete.push(key.clone());

            // Check if it is the last key
            if key.len() == 8 {
                let key_index = u64::from_be_bytes([
                    key[0], key[1], key[2], key[3], key[4], key[5], key[6], key[7],
                ]);
                if key_index >= current_last_index {
                    break;
                }
            }
        }

        // Batch delete
        for key in keys_to_delete {
            batch.delete_cf(&cf, &key);
        }

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;

        // Update last_index: The new last_index should be from_index - 1
        // But if from_index is 0 or 1, last_index should be 0
        let new_last_index = from_index.saturating_sub(1);

        self.last_index.store(new_last_index, Ordering::SeqCst);

        Ok(())
    }

    /// Atomically truncate from `from_index` and persist `new_entries` in one WriteBatch.
    ///
    /// Uses `delete_range_cf` (O(1) range tombstone) + puts in a single commit,
    /// eliminating the two-batch window of the default implementation.
    async fn replace_range(
        &self,
        from_index: u64,
        new_entries: Vec<Entry>,
    ) -> Result<u64, Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        let mut batch = WriteBatch::default();

        // delete_range_cf: single range tombstone, compaction reclaims space later
        let start_key = Self::index_to_key(from_index);
        let end_key = Self::index_to_key(u64::MAX);
        batch.delete_range_cf(&cf, start_key, end_key);

        let new_last_index =
            new_entries.last().map(|e| e.index).unwrap_or(from_index.saturating_sub(1));
        for entry in &new_entries {
            batch.put_cf(&cf, Self::index_to_key(entry.index), entry.encode_to_vec());
        }

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        self.last_index.store(new_last_index, Ordering::SeqCst);
        Ok(new_last_index)
    }

    fn is_write_durable(&self) -> bool {
        // Batched Level 3: db.write() uses sync=false (lands in OS page cache),
        // but durable_index does NOT advance until flush_wal(true) (fdatasync) completes.
        //
        // Process crash: OS retains page cache → WAL replay on restart → full recovery. ✅
        // Power loss:    committed data (durable_index advanced) has been fdatasync'd → safe. ✅
        //                Only entries in the current unflushed batch are at risk — a small window
        //                bounded by idle_flush_interval_ms, and not yet counted toward quorum.
        //
        // Returns false → advance_durable_after_write calls flush_wal(true) before notifying,
        // coalescing multiple db.write() calls into a single fdatasync (batched Level 3).
        false
    }

    fn load_purge_boundary(&self) -> Result<Option<LogId>, Error> {
        let cf_meta = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;
        match self
            .db
            .get_cf(&cf_meta, PURGE_BOUNDARY_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) => LogId::decode(&*bytes)
                .map(Some)
                .map_err(|e| StorageError::SerializationError(e.to_string()).into()),
            None => Ok(None),
        }
    }

    fn flush(&self) -> Result<(), Error> {
        // WAL fsync is sufficient for crash-safety: data in WAL can be replayed on restart.
        // memtable flush is RocksDB's internal concern — triggered automatically in background.
        let t0 = std::time::Instant::now();
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::DbError(format!("Failed to flush WAL: {e}")))?;
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        metrics::histogram!("server.storage.rocksdb.wal_flush_ms").record(ms);
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    async fn reset(&self) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(LOG_CF)
            .ok_or_else(|| StorageError::DbError("Log column family not found".to_string()))?;

        // Delete all keys in the log column family
        let mut batch = WriteBatch::default();
        let iter = self.db.iterator_cf(&cf, IteratorMode::Start);

        for item in iter {
            let (key, _) = item.map_err(|e| StorageError::DbError(e.to_string()))?;
            batch.delete_cf(&cf, &key);
        }

        self.db.write(&batch).map_err(|e| StorageError::DbError(e.to_string()))?;
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}

impl Drop for RocksDBMetaStore {
    fn drop(&mut self) {
        // WAL fsync makes META_CF (hard_state) durable — memtable content
        // survives via WAL replay on next open even without the flush below.
        // The flush is an optimization only: it shrinks the WAL that must be
        // replayed on the next restart. db.flush() (no CF argument) targets
        // RocksDB's unused "default" CF and would be a no-op here — META_CF
        // must be named explicitly.
        if let Err(e) = self.db.flush_wal(true) {
            tracing::error!("Failed to flush meta WAL on drop: {}", e);
        }
        match self.db.cf_handle(META_CF) {
            Some(cf) => {
                if let Err(e) = self.db.flush_cf(&cf) {
                    tracing::error!("Failed to flush META_CF memtable on drop: {}", e);
                } else {
                    tracing::debug!("RocksDBMetaStore flushed successfully on drop");
                }
            }
            None => tracing::error!("Failed to flush on drop: META_CF not found"),
        }
    }
}

impl RocksDBMetaStore {
    fn new(db: Arc<DB>) -> Result<Self, Error> {
        Ok(Self { db })
    }
}

#[async_trait]
impl MetaStore for RocksDBMetaStore {
    #[instrument(skip(self, state))]
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;

        let serialized = bincode::serialize(state).map_err(StorageError::BincodeError)?;

        self.db
            .put_cf(&cf, HARD_STATE_KEY, serialized)
            .map_err(|e| StorageError::DbError(e.to_string()))?;
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        let cf = self
            .db
            .cf_handle(META_CF)
            .ok_or_else(|| StorageError::DbError("Meta column family not found".to_string()))?;

        match self
            .db
            .get_cf(&cf, HARD_STATE_KEY)
            .map_err(|e| StorageError::DbError(e.to_string()))?
        {
            Some(bytes) => {
                let state = bincode::deserialize(&bytes).map_err(StorageError::BincodeError)?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    fn flush(&self) -> Result<(), Error> {
        // WAL fsync is sufficient: HardState in WAL survives crashes.
        // memtable flush is RocksDB's internal concern — triggered automatically in background.
        self.db
            .flush_wal(true)
            .map_err(|e| StorageError::DbError(format!("Failed to flush meta WAL: {e}")))?;
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
