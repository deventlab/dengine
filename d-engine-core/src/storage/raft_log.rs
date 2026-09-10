//! RaftLog trait with explicit safety contracts and documentation
//!
//! This trait defines the interface for Raft log storage with comprehensive
//! safety guarantees. Implementers MUST adhere to all documented invariants
//! to maintain Raft consensus protocol correctness.
//!
//! Key Safety Properties (from Raft paper):
//! 1. Log Matching: If two logs contain an entry with same index and term, then logs are identical
//!    in all preceding entries
//! 2. Leader Append-Only: Leaders never overwrite or delete entries
//! 3. State Machine Safety: If a server has applied a log entry at a given index, no other server
//!    will ever apply a different log entry for that index

use std::ops::RangeInclusive;

use async_trait::async_trait;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
#[cfg(any(test, feature = "__test_support"))]
use mockall::automock;

use crate::Result;

#[cfg_attr(any(test, feature = "__test_support"), automock)]
#[async_trait]
pub trait RaftLog: Send + Sync + 'static {
    // =========================================================================
    // READ OPERATIONS (Lock-free, no side effects)
    // =========================================================================

    /// Retrieves a log entry by index.
    ///
    /// # Returns
    /// - `Ok(Some(entry))` if entry exists
    /// - `Ok(None)` if index is out of range
    /// - `Err(_)` only for unrecoverable storage errors
    ///
    /// # Safety Invariants
    /// - MUST be thread-safe and lock-free for maximum performance
    /// - MUST NOT modify any state
    /// - MUST return consistent data within a single term
    fn entry(
        &self,
        index: u64,
    ) -> Result<Option<Entry>>;

    /// Returns the smallest log index (inclusive).
    ///
    /// # Returns
    /// - `0` if log is empty
    /// - First index otherwise (typically 1 after initialization)
    ///
    /// # Safety Invariants
    /// - MUST be monotonically non-decreasing (can only increase via purge)
    /// - MUST be <= last_entry_id()
    fn first_entry_id(&self) -> u64;

    /// Returns the largest log index (inclusive).
    ///
    /// # Returns
    /// - `0` if log is empty
    /// - Last index otherwise
    ///
    /// # Safety Invariants
    /// - MUST be monotonically non-decreasing during normal operation
    /// - Can decrease only during conflict resolution (filter_out_conflicts_and_append)
    /// - MUST be >= first_entry_id()
    fn last_entry_id(&self) -> u64;

    /// Returns the highest log index confirmed crash-safe on this node.
    ///
    /// Advanced only after the storage backend confirms durability (fsync complete).
    /// Used by the Raft core for quorum and commit decisions — never use `last_entry_id()`
    /// for this purpose, as it reflects in-memory state only.
    ///
    /// - MemFirst: lags `last_entry_id()` until `batch_processor` completes fsync.
    /// - DiskFirst: equals `last_entry_id()` (every append blocks until durable).
    fn durable_index(&self) -> u64;

    /// Content-validated durable-watermark advance. `index`/`term` describe
    /// what a completed fsync claims is now safe — rejected (`None`) if
    /// `entry_term(index) != Some(term)`, meaning the log content at that
    /// index has changed (truncated + replaced) since fsync started on it.
    /// `Some(new_value)` only when it actually advanced — callers use this
    /// to decide whether to fire `handle_log_flushed`.
    fn try_advance_durable_index(
        &self,
        mark: LogId,
    ) -> Option<u64>;

    /// Returns the LogId (term + index) of the last entry.
    ///
    /// # Returns
    /// - `None` if log is empty
    /// - `Some(LogId)` otherwise
    ///
    /// # Safety Invariants
    /// - MUST match last_entry().map(|e| LogId { term: e.term, index: e.index })
    /// - Critical for leader election and log matching
    fn last_log_id(&self) -> Option<LogId>;

    /// Returns the last log entry.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to entry(last_entry_id())
    /// - Performance optimization for common access pattern
    fn last_entry(&self) -> Option<Entry>;

    /// Checks if log is empty.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to (first_entry_id() == 0)
    fn is_empty(&self) -> bool;

    /// Returns the term of a specific entry.
    ///
    /// # Safety Invariants
    /// - MUST be equivalent to entry(entry_id).map(|e| e.term)
    /// - Critical for AppendEntries RPC consistency checks
    fn entry_term(
        &self,
        entry_id: u64,
    ) -> Option<u64>;
    /// Finds the first index belonging to a specific term.
    ///
    /// # Returns
    /// - `None` if no entry with this term exists
    /// - `Some(index)` of the first entry with matching term
    ///
    /// # Safety Invariants
    /// - MUST scan forward from first_entry_id()
    /// - Result MUST be <= last_index_for_term(term)
    /// - Critical for calculating commit index
    fn first_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    /// Finds the last index belonging to a specific term.
    ///
    /// # Returns
    /// - `None` if no entry with this term exists
    /// - `Some(index)` of the last entry with matching term
    ///
    /// # Safety Invariants
    /// - MUST scan backward from last_entry_id()
    /// - Result MUST be >= first_index_for_term(term)
    /// - Critical for leader election and log compaction
    fn last_index_for_term(
        &self,
        term: u64,
    ) -> Option<u64>;

    /// Retrieves a contiguous range of log entries.
    ///
    /// # Arguments
    /// - `range`: Inclusive range [start, end]
    ///
    /// # Returns
    /// - `Ok(Vec<Entry>)` with all entries in range
    /// - Empty vec if range is out of bounds
    ///
    /// # Safety Invariants
    /// - Returned entries MUST be ordered by index (ascending)
    /// - MUST include all entries in range without gaps
    /// - Critical for AppendEntries RPC replication
    fn get_entries_range(
        &self,
        range: RangeInclusive<u64>,
    ) -> Result<Vec<Entry>>;

    // =========================================================================
    // INDEX ALLOCATION (Thread-safe, atomic)
    // =========================================================================

    /// Pre-allocates the next log index atomically.
    ///
    /// # Returns
    /// - Next available index
    ///
    /// # Safety Invariants
    /// - MUST be thread-safe (atomic increment)
    /// - MUST return unique, monotonically increasing values
    /// - Each call MUST return a different value across all threads
    /// - Used by leader to assign indexes to client proposals
    fn pre_allocate_raft_logs_next_index(&self) -> u64;

    /// Pre-allocates a contiguous range of log indices atomically.
    ///
    /// # Arguments
    /// - `count`: Number of indices to allocate
    ///
    /// # Returns
    /// - Inclusive range [start, end] where (end - start + 1) == count
    /// - Empty range (u64::MAX..=u64::MAX) if count == 0
    ///
    /// # Safety Invariants
    /// - MUST be atomic across all threads
    /// - MUST NOT return overlapping ranges
    /// - MUST prevent overflow (panic if allocation would exceed u64::MAX)
    /// - Used for batch proposal optimization
    fn pre_allocate_id_range(
        &self,
        count: u64,
    ) -> RangeInclusive<u64>;

    // =========================================================================
    // WRITE OPERATIONS (CRITICAL: Durability and Atomicity Required)
    // =========================================================================

    /// Appends new entries to the log.
    ///
    /// # Arguments
    /// - `entries`: New log entries to append (must be ordered by index)
    ///
    /// # Durability Contract
    /// **CRITICAL**: Implementers MUST choose one of these strategies:
    ///
    /// 1. **Disk-First (Safest, Recommended for Leaders)**:
    ///    - Persist entries to durable storage BEFORE updating in-memory state
    ///    - Call fsync/flush before returning Ok(())
    ///    - Ensures entries survive crashes immediately
    ///    - Example: a store that fsyncs before returning
    ///
    /// 2. **Memory-First (Performance-optimized, Acceptable for Followers)**:
    ///    - Update in-memory state first
    ///    - Enqueue entries for asynchronous durability
    ///    - MUST guarantee eventual durability via background flush
    ///    - MUST call flush() before acknowledging commits
    ///    - Example: a store that fsyncs asynchronously
    ///    - WARNING: Leader MUST wait_durable() before responding to AppendEntries RPCs
    ///
    /// # Safety Invariants
    /// - Entries MUST have strictly increasing indices (no gaps)
    /// - Entries MUST NOT conflict with existing entries (use filter_out_conflicts_and_append for
    ///   that)
    /// - After successful return, entries MUST be retrievable via entry()
    /// - MUST update last_entry_id() atomically
    /// - MUST update term indexes (first/last_index_for_term) atomically
    ///
    /// # Raft Protocol Integration
    /// - Leaders using async fsync MUST call wait_durable(index) before:
    ///   * Responding success to AppendEntries RPC
    ///   * Advancing commit index
    /// - Followers can use async fsync safely because leader durability guarantees safety
    ///
    /// # Failure Semantics
    /// - On error, implementer MAY roll back partial writes
    /// - Caller MUST retry or initiate crash recovery
    async fn append_entries(
        &self,
        entries: Vec<Entry>,
    ) -> Result<()>;

    /// Waits for a specific log index to become durable.
    ///
    /// # Arguments
    /// - `index`: Log index to wait for
    ///
    /// # Returns
    /// - `Ok(())` when index is guaranteed durable
    /// - `Err(_)` if waiting failed (channel closed, timeout, etc.)
    ///
    /// # Usage Pattern
    /// ```rust,ignore
    /// // Leader with async fsync
    /// raft_log.append_entries(new_entries).await?;
    /// raft_log.wait_durable(max_index).await?;  // MUST wait before RPC response
    /// respond_to_client(Ok(()));
    /// ```
    ///
    /// # Safety Invariants
    /// - MUST NOT return until flush() for this index completes successfully
    /// - If implementation doesn't support async durability, return Ok(()) immediately
    /// - Critical for async-fsync correctness
    async fn wait_durable(
        &self,
        index: u64,
    ) -> Result<()> {
        // Default implementation for DiskFirst: no-op (already durable)
        let _ = index;
        Ok(())
    }

    /// Alias for append_entries (for backward compatibility).
    ///
    /// # Safety Invariants
    /// - MUST have identical semantics to append_entries()
    async fn insert_batch(
        &self,
        logs: Vec<Entry>,
    ) -> Result<()> {
        self.append_entries(logs).await
    }

    /// Resolves log conflicts and appends new entries atomically.
    ///
    /// This is the core of Raft's log replication safety. Implements the
    /// AppendEntries RPC log consistency check and conflict resolution.
    ///
    /// # Arguments
    /// - `prev_log_index`: Index of entry immediately preceding new entries
    /// - `prev_log_term`: Term of entry at prev_log_index
    /// - `new_entries`: New entries to append
    ///
    /// # Algorithm (from Raft paper §5.3)
    /// 1. **Virtual Log Check**: If prev_log_index == 0 && prev_log_term == 0:
    ///    - Clear entire log (snapshot installation)
    ///    - Append all new entries
    /// 2. **Consistency Check**: Verify entry at prev_log_index has term == prev_log_term
    ///    - If mismatch, return current last_log_id (reject)
    /// 3. **Conflict Resolution**: If existing entries conflict with new ones:
    ///    - Delete all entries from prev_log_index+1 onwards
    ///    - Append new entries
    /// 4. **Optimization**: If prev_log_index >= last_entry_id:
    ///    - No conflicts, directly append
    ///
    /// # Returns
    /// - `Ok(Some(new_last_log_id))` on success (entries appended)
    /// - `Ok(None)` or `Ok(Some(current_last_log_id))` on consistency failure (rejected)
    /// - `Err(_)` on storage failure
    ///
    /// # Safety Invariants (CRITICAL)
    /// - Truncation + Append MUST be atomic (no partial state visible)
    /// - If crash occurs mid-operation, log MUST be in consistent state after recovery
    /// - MUST NOT violate Log Matching Property (Raft invariant)
    /// - MUST update term indexes atomically with log changes
    /// - After success, all entries from first_entry_id to new last index MUST be contiguous
    ///
    /// # Recommended Implementation Pattern
    /// ```rust,ignore
    /// async fn filter_out_conflicts_and_append(...) -> Result<Option<LogId>> {
    ///     // 1. Validate consistency (read-only)
    ///     // 2. Begin transaction/write batch
    ///     // 3. Truncate conflicting entries
    ///     // 4. Append new entries
    ///     // 5. Commit transaction/flush write batch
    ///     // 6. Update in-memory indexes atomically
    /// }
    /// ```
    async fn filter_out_conflicts_and_append(
        &self,
        prev_log_index: u64,
        prev_log_term: u64,
        new_entries: Vec<Entry>,
    ) -> Result<Option<LogId>>;

    /// Calculates the majority-matched index for commit advancement.
    ///
    /// # Arguments
    /// - `current_term`: Leader's current term
    /// - `commit_index`: Current committed index
    /// - `matched_ids`: Follower match indexes (leader's last index added automatically)
    ///
    /// # Algorithm (Raft paper §5.3, §5.4)
    /// 1. Sort all matched indexes (including leader's) in descending order
    /// 2. Find median index (majority quorum)
    /// 3. Verify conditions:
    ///    - majority_index >= commit_index
    ///    - entry at majority_index has term == current_term
    ///
    /// # Returns
    /// - `Some(new_commit_index)` if conditions met
    /// - `None` if no advancement possible
    ///
    /// # Safety Invariants
    /// - MUST verify term matches current_term (prevents committing entries from old terms)
    /// - MUST NOT decrease commit index
    /// - Critical for State Machine Safety property
    fn calculate_majority_matched_index(
        &self,
        current_term: u64,
        commit_index: u64,
        matched_ids: Vec<u64>,
    ) -> Option<u64>;

    /// Purges committed log entries up to cutoff_index (log compaction).
    ///
    /// # Arguments
    /// - `cutoff_index`: LogId of last entry to purge (inclusive)
    ///
    /// # Safety Invariants (CRITICAL)
    /// - MUST only be called after entries applied to state machine
    /// - MUST NOT purge entries beyond last_applied_index
    /// - After purge, first_entry_id() MUST be cutoff_index.index + 1
    /// - MUST be atomic (no partial purge visible)
    /// - MUST NOT cause gaps in remaining log
    /// - MUST update term indexes correctly
    ///
    /// # Durability Contract
    /// - Changes MUST be durable before returning Ok(())
    /// - On crash, either old or new state MUST be consistent
    ///
    /// # Recommended Implementation
    /// - Coordinate with snapshot mechanism
    /// - Ensure snapshot covers purged range
    async fn purge_logs_up_to(
        &self,
        cutoff_index: LogId,
    ) -> Result<()>;

    /// Forces all pending writes to durable storage.
    ///
    /// # Durability Contract
    /// - After successful return, ALL previously written entries MUST survive crashes
    /// - MUST call fsync/fdatasync or equivalent
    /// - MUST block until durability guaranteed
    ///
    /// # When to Call
    /// - Before acknowledging commits to clients
    /// - Before voting in elections
    /// - Before responding to AppendEntries RPCs
    /// - Before taking snapshots
    ///
    /// # Performance Note
    /// - This is expensive; batch writes when possible
    /// - Consider group commit optimization
    async fn flush(&self) -> Result<()>;

    /// Resets the entire log storage (destructive operation).
    ///
    /// # Safety Invariants
    /// - MUST clear all log entries
    /// - MUST clear all metadata
    /// - MUST reset first_entry_id() and last_entry_id() to 0
    /// - MUST clear term indexes
    /// - MUST be atomic (no partial state visible)
    /// - MUST NOT be called during normal operation (only during initialization/recovery)
    ///
    /// # Durability Contract
    /// - Changes MUST be durable before returning Ok(())
    async fn reset(&self) -> Result<()>;

    // =========================================================================
    // PERSISTENT STATE (Raft Paper §5.2)
    // =========================================================================

    /// Loads persistent state (currentTerm, votedFor).
    ///
    /// # Returns
    /// - `Ok(Some(HardState))` if state exists
    /// - `Ok(None)` if no state persisted yet (first boot)
    /// - `Err(_)` on storage failure
    ///
    /// # Safety Invariants
    /// - MUST return most recently saved state
    /// - MUST survive crashes (durably persisted)
    fn load_hard_state(&self) -> Result<Option<crate::HardState>>;

    /// Saves persistent state (currentTerm, votedFor).
    ///
    /// # Arguments
    /// - `hard_state`: Current term and vote information
    ///
    /// # Durability Contract (CRITICAL)
    /// - MUST be durable BEFORE returning Ok(())
    /// - MUST call fsync/flush before returning
    /// - On crash after return, state MUST be retrievable via load_hard_state()
    ///
    /// # Safety Invariants
    /// - Violating durability breaks election safety (vote splitting)
    /// - MUST be atomic with respect to load_hard_state()
    /// - Term MUST NOT decrease (except during reset)
    ///
    /// # When to Call
    /// - Before voting for a candidate
    /// - When becoming candidate (incrementing term)
    /// - When discovering higher term
    fn save_hard_state(
        &self,
        hard_state: &crate::HardState,
    ) -> Result<()>;

    /// Returns `true` if a storage-layer failure has permanently poisoned this
    /// log — no further writes/commands will be attempted, and callers above
    /// the storage layer (e.g. the Raft protocol loop) must stop dispatching
    /// new work to this node.
    fn is_poisoned(&self) -> bool;

    /// Gracefully closes the log, ensuring all pending IO completes and any
    /// background IO threads have exited before returning.
    ///
    /// Called during node shutdown. Default is a no-op for implementations
    /// without a dedicated background IO thread.
    async fn close(&self) {}
}
