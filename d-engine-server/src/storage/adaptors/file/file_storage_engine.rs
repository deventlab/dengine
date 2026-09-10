use std::collections::BTreeMap;
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::fs::{self};
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::io::Write;
use std::ops::RangeInclusive;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use async_trait::async_trait;
use d_engine_core::Error;
use d_engine_core::HardState;
use d_engine_core::LogStore;
use d_engine_core::MetaStore;
use d_engine_core::StorageEngine;
use d_engine_core::StorageError;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use prost::Message;
use tracing::info;

// Constants for file structure
const HARD_STATE_FILE_NAME: &str = "hard_state.bin";
pub(crate) const HARD_STATE_KEY: &[u8] = b"hard_state";

/// All mutable state for the log store, protected by a single Mutex.
///
/// Combining entries, file handle, and position index under one lock eliminates
/// the three-lock ordering problem and makes replace_range() atomic at the
/// memory level with no visible intermediate state.
#[derive(Debug)]
struct FileLogStoreInner {
    entries: BTreeMap<u64, Entry>,
    file: File,
    /// Maps log index to the file offset of the byte *after* the entry
    /// (i.e. the start of the next entry). truncate(from) sets file length to
    /// index_end_pos[from - 1], with no extra file read required.
    index_end_pos: BTreeMap<u64, u64>,
}

/// File-based log store implementation
#[derive(Debug)]
pub struct FileLogStore {
    #[allow(unused)]
    data_dir: PathBuf,
    inner: Mutex<FileLogStoreInner>,
    /// Cached last index for hot-path reads without locking.
    last_index: AtomicU64,
}

/// File-based metadata store implementation
#[derive(Debug)]
pub struct FileMetaStore {
    data_dir: PathBuf,
    data: Mutex<HashMap<Vec<u8>, Vec<u8>>>,
}

/// File-based Raft log storage
///
/// Stores log entries as individual files in a directory.
///
/// # Usage
///
/// ```rust,ignore
/// use d_engine_server::FileStorageEngine;
/// use std::path::PathBuf;
///
/// let engine = FileStorageEngine::new(PathBuf::from("/tmp/log"))?;
/// ```
///
/// # Performance
///
/// Suitable for development and testing. For production, consider using RocksDB storage engine
/// via the `rocksdb` feature.
#[derive(Debug)]
pub struct FileStorageEngine {
    log_store: Arc<FileLogStore>,
    meta_store: Arc<FileMetaStore>,
    data_dir: PathBuf,
}

impl StorageEngine for FileStorageEngine {
    type LogStore = FileLogStore;
    type MetaStore = FileMetaStore;

    #[inline]
    fn log_store(&self) -> Arc<Self::LogStore> {
        self.log_store.clone()
    }

    #[inline]
    fn meta_store(&self) -> Arc<Self::MetaStore> {
        self.meta_store.clone()
    }
}

impl FileStorageEngine {
    /// Creates new file-based storage engine
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        fs::create_dir_all(&data_dir)?;
        let log_store = Arc::new(FileLogStore::new(data_dir.join("logs"))?);
        let meta_store = Arc::new(FileMetaStore::new(data_dir.join("meta"))?);
        Ok(Self {
            log_store,
            meta_store,
            data_dir,
        })
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }
}

impl FileLogStore {
    /// Creates new file-based log store
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        fs::create_dir_all(&data_dir)?;

        let log_file_path = data_dir.join("log.data");
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(log_file_path)?;

        let mut inner = FileLogStoreInner {
            entries: BTreeMap::new(),
            file,
            index_end_pos: BTreeMap::new(),
        };

        let last_index = inner.load_from_file()?;

        Ok(Self {
            data_dir,
            inner: Mutex::new(inner),
            last_index: AtomicU64::new(last_index),
        })
    }

    #[allow(dead_code)]
    #[cfg(test)]
    pub(crate) fn reset_sync(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.set_len(0)?;
        inner.file.seek(SeekFrom::Start(0))?;
        inner.file.flush()?;
        inner.entries.clear();
        inner.index_end_pos.clear();
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }
}

impl FileLogStoreInner {
    /// Load entries from file on startup. Returns the last index found.
    fn load_from_file(&mut self) -> Result<u64, Error> {
        self.file.seek(SeekFrom::Start(0))?;

        let mut buffer = Vec::new();
        self.file.read_to_end(&mut buffer)?;

        let mut pos: u64 = 0;
        let mut max_index = 0;

        while pos < buffer.len() as u64 {
            if pos + 8 > buffer.len() as u64 {
                break;
            }

            let len_bytes = &buffer[pos as usize..pos as usize + 8];
            let entry_len =
                u64::from_be_bytes(len_bytes.try_into().expect("slice is exactly 8 bytes"));

            let data_start = pos + 8;
            let end_pos = data_start + entry_len;

            if end_pos > buffer.len() as u64 {
                break;
            }

            let entry_data = &buffer[data_start as usize..end_pos as usize];
            match Entry::decode(entry_data) {
                Ok(entry) => {
                    let index = entry.index;
                    self.entries.insert(index, entry);
                    self.index_end_pos.insert(index, end_pos);
                    max_index = max_index.max(index);
                }
                Err(e) => {
                    eprintln!("Failed to decode entry: {e}");
                }
            }

            pos = end_pos;
        }

        Ok(max_index)
    }

    /// Write a pre-encoded entry to the file at the current end.
    /// Returns the file end position after writing (= end_pos for this entry).
    /// Does NOT flush — caller flushes once after the batch.
    fn write_encoded(
        &mut self,
        encoded: &[u8],
    ) -> Result<u64, Error> {
        self.file.seek(SeekFrom::End(0))?;
        let len = encoded.len() as u64;
        self.file.write_all(&len.to_be_bytes())?;
        self.file.write_all(encoded)?;
        Ok(self.file.stream_position()?)
    }

    /// File end position of the last entry *before* `from_index`.
    /// Returns 0 if no such entry exists (truncate empties the file).
    fn end_pos_before(
        &self,
        from_index: u64,
    ) -> u64 {
        self.index_end_pos
            .range(..from_index)
            .next_back()
            .map(|(_, &pos)| pos)
            .unwrap_or(0)
    }

    /// Remove all in-memory state for indices >= from_index.
    fn remove_from_index(
        &mut self,
        from_index: u64,
    ) {
        let keys: Vec<u64> = self.entries.range(from_index..).map(|(&k, _)| k).collect();
        for k in keys {
            self.entries.remove(&k);
            self.index_end_pos.remove(&k);
        }
    }
}

#[async_trait]
impl LogStore for FileLogStore {
    async fn persist_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<(), Error> {
        if entries.is_empty() {
            return Ok(());
        }

        // Encode all entries before acquiring the lock (pure CPU work).
        let encoded: Vec<Vec<u8>> = entries.iter().map(|e| e.encode_to_vec()).collect();

        let mut max_index = 0;
        {
            let mut inner = self.inner.lock().unwrap();
            for (entry, enc) in entries.iter().zip(encoded.iter()) {
                let end_pos = inner.write_encoded(enc)?;
                inner.entries.insert(entry.index, entry.clone());
                inner.index_end_pos.insert(entry.index, end_pos);
                max_index = max_index.max(entry.index);
            }
            inner.file.flush()?;
        }

        self.last_index.store(max_index, Ordering::SeqCst);
        Ok(())
    }

    async fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.get(&index).cloned())
    }

    fn get_entries(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>, Error> {
        let inner = self.inner.lock().unwrap();
        Ok(inner.entries.range(range).map(|(_, e)| e.clone()).collect())
    }

    async fn purge(
        &self,
        cutoff_index: LogId,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        let entries_to_keep: Vec<Entry> = inner
            .entries
            .range((cutoff_index.index + 1)..)
            .map(|(_, e)| e.clone())
            .collect();

        // Rewrite file with only kept entries, flush once.
        inner.file.set_len(0)?;
        inner.file.seek(SeekFrom::Start(0))?;
        inner.index_end_pos.clear();

        for entry in &entries_to_keep {
            let enc = entry.encode_to_vec();
            let end_pos = inner.write_encoded(&enc)?;
            inner.index_end_pos.insert(entry.index, end_pos);
        }
        inner.file.flush()?;
        inner.file.sync_all()?;

        inner.entries.retain(|&index, _| index > cutoff_index.index);

        Ok(())
    }

    async fn truncate(
        &self,
        from_index: u64,
    ) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();

        // Compute truncation point from end_pos index — no file read required.
        let truncate_to = inner.end_pos_before(from_index);
        inner.file.set_len(truncate_to)?;

        inner.remove_from_index(from_index);

        let new_last = inner.entries.keys().next_back().copied().unwrap_or(0);
        self.last_index.store(new_last, Ordering::SeqCst);

        Ok(())
    }

    /// Atomically truncate from `from_index` and persist `new_entries` under a single lock.
    ///
    /// All memory and file mutations happen within one `Mutex` critical section, so no
    /// intermediate state is visible to concurrent readers of `last_index` or `entry()`.
    async fn replace_range(
        &self,
        from_index: u64,
        new_entries: Vec<Entry>,
    ) -> Result<u64, Error> {
        let encoded: Vec<Vec<u8>> = new_entries.iter().map(|e| e.encode_to_vec()).collect();

        let new_last = {
            let mut inner = self.inner.lock().unwrap();

            // Truncate file to the end of the last kept entry.
            let truncate_to = inner.end_pos_before(from_index);
            inner.file.set_len(truncate_to)?;

            // Remove in-memory state for truncated range.
            inner.remove_from_index(from_index);

            // Append new entries and update in-memory state.
            for (entry, enc) in new_entries.iter().zip(encoded.iter()) {
                let end_pos = inner.write_encoded(enc)?;
                inner.entries.insert(entry.index, entry.clone());
                inner.index_end_pos.insert(entry.index, end_pos);
            }

            if !new_entries.is_empty() {
                inner.file.flush()?;
            }

            inner.entries.keys().next_back().copied().unwrap_or(0)
        };

        self.last_index.store(new_last, Ordering::SeqCst);
        Ok(new_last)
    }

    fn is_write_durable(&self) -> bool {
        // File writes are buffered by the OS; sync_all() is required for crash-safety.
        false
    }

    fn flush(&self) -> Result<(), Error> {
        let t0 = std::time::Instant::now();
        let mut inner = self.inner.lock().unwrap();
        inner.file.flush()?;
        inner.file.sync_all()?;
        drop(inner);
        let ms = t0.elapsed().as_secs_f64() * 1000.0;
        metrics::histogram!("server.storage.file.flush_ms").record(ms);
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }

    async fn reset(&self) -> Result<(), Error> {
        let mut inner = self.inner.lock().unwrap();
        inner.file.set_len(0)?;
        inner.file.seek(SeekFrom::Start(0))?;
        inner.file.flush()?;
        inner.entries.clear();
        inner.index_end_pos.clear();
        self.last_index.store(0, Ordering::SeqCst);
        Ok(())
    }

    fn last_index(&self) -> u64 {
        self.last_index.load(Ordering::SeqCst)
    }
}

impl Drop for FileLogStore {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            tracing::error!("Failed to flush FileLogStore on drop: {}", e);
        } else {
            tracing::debug!("FileLogStore flushed successfully on drop");
        }
    }
}

impl FileMetaStore {
    /// Creates new file-based metadata store
    pub fn new(data_dir: PathBuf) -> Result<Self, Error> {
        fs::create_dir_all(&data_dir)?;

        let store = Self {
            data_dir,
            data: Mutex::new(HashMap::new()),
        };

        store.load_from_file()?;

        Ok(store)
    }

    /// Load metadata from file
    fn load_from_file(&self) -> Result<(), Error> {
        let hard_state_path = self.data_dir.join(HARD_STATE_FILE_NAME);

        if hard_state_path.exists() {
            let mut file = File::open(hard_state_path)?;
            let mut buffer = Vec::new();
            file.read_to_end(&mut buffer)?;

            match bincode::deserialize::<HardState>(&buffer) {
                Ok(_hard_state) => {
                    let mut data = self.data.lock().unwrap();
                    data.insert(HARD_STATE_KEY.to_vec(), buffer);
                    info!("Loaded hard state from file");
                }
                Err(e) => {
                    eprintln!("Failed to decode hard state: {e}",);
                }
            }
        }

        Ok(())
    }

    /// Save metadata to file
    fn save_to_file(
        &self,
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Error> {
        if key == HARD_STATE_KEY {
            let hard_state_path = self.data_dir.join(HARD_STATE_FILE_NAME);
            let mut file = File::create(hard_state_path)?;
            file.write_all(value)?;
            file.flush()?;
        }

        Ok(())
    }
}

#[async_trait]
impl MetaStore for FileMetaStore {
    fn save_hard_state(
        &self,
        state: &HardState,
    ) -> Result<(), Error> {
        let serialized = bincode::serialize(state).map_err(StorageError::BincodeError)?;

        let mut data = self.data.lock().unwrap();
        data.insert(HARD_STATE_KEY.to_vec(), serialized.clone());

        self.save_to_file(HARD_STATE_KEY, &serialized)?;

        info!("Persisted hard state to file");
        Ok(())
    }

    fn load_hard_state(&self) -> Result<Option<HardState>, Error> {
        let data = self.data.lock().unwrap();

        match data.get(HARD_STATE_KEY) {
            Some(bytes) => {
                let state = bincode::deserialize(bytes).map_err(StorageError::BincodeError)?;
                info!("Loaded hard state from memory");
                Ok(Some(state))
            }
            None => {
                info!("No hard state found");
                Ok(None)
            }
        }
    }

    fn flush(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn flush_async(&self) -> Result<(), Error> {
        self.flush()
    }
}
