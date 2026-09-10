//! Storage-level integration tests for BufferedRaftLog
//!
//! These tests verify BufferedRaftLog integration with FileStorageEngine
//! at the storage layer, including compaction and storage-specific operations.

use d_engine_core::{FlushPolicy, RaftLog};
use d_engine_proto::common::LogId;

use super::TestContext;

// TODO: Extract 1 storage integration test from legacy_all_tests.rs:
// - test_log_compaction

#[tokio::test]
async fn test_log_compaction() {
    let mut ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_log_compaction",
    );
    ctx.append_entries(1, 100, 1).await;
    // With MemFirst, entries are buffered and flushed asynchronously.
    // Wait for all entries to become durable before checking durable_index.
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // Compact first 50 entries
    ctx.raft_log.purge_logs_up_to(LogId { index: 50, term: 1 }).await.unwrap();

    // Verify compaction
    assert!(ctx.raft_log.entry(25).unwrap().is_none());
    assert_eq!(ctx.raft_log.first_entry_id(), 51);
    assert_eq!(ctx.raft_log.durable_index(), 100);
}
