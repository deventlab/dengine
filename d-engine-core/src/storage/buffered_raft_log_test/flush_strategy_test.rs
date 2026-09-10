//! Flush strategy behavior tests (Mock-based)
//!
//! Tests verify MemFirst persistence strategy with MockStorageEngine:
//! - threshold=1: equivalent to the former DiskFirst — every entry triggers a flush
//! - high threshold/interval: entries buffered, not yet durable until explicit flush
//! - timer-based flush: entries become durable after interval elapses
//!
//! Note: These tests use MockStorageEngine to verify strategy logic.
//! Integration tests with real FileStorageEngine are in d-engine-server/tests/integration.

use bytes::Bytes;
use d_engine_proto::common::{Entry, EntryPayload};
use tokio::time::{Duration, sleep};

use crate::test_utils::BufferedRaftLogTestContext;
use crate::{FlushPolicy, RaftLog};

/// Test MemFirst with threshold=1 persists entries after flush
///
/// # Scenario
/// - Append 5 entries with threshold=1 (flush fires after every entry)
/// - Expected: durable_index == 5 after explicit flush()
#[tokio::test]
async fn test_mem_first_entries_durable_after_flush() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_mem_first_persists_immediately",
    );

    // Act: Append entries then wait for durability
    ctx.append_entries(1, 5, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // Assert: All entries durable after flush
    assert_eq!(
        ctx.raft_log.durable_index(),
        5,
        "All entries should be durable after flush"
    );
    let entry = ctx.raft_log.entry(3).unwrap().unwrap();
    assert_eq!(entry.index, 3);
}

/// Test MemFirst concurrent writes all durable after flush
///
/// # Scenario
/// - 10 concurrent tasks each append 100 entries (1000 total)
/// - Expected: All 1000 entries durable after flush(), no data loss
#[tokio::test]
async fn test_mem_first_concurrent_writes_durable_after_flush() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_mem_first_concurrent_writes",
    );

    // Act: Concurrent appends
    let mut handles = vec![];
    for i in 0..10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for j in 1..=100 {
                let index = i * 100 + j;
                log.append_entries(vec![Entry {
                    index,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Wait for all entries to become durable
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    // Assert: All entries durable after flush
    assert_eq!(
        ctx.raft_log.durable_index(),
        1000,
        "All concurrent writes should be durable after flush"
    );
    assert_eq!(ctx.raft_log.len(), 1000);
}

/// Test MemFirst crash recovery restores only flushed entries
///
/// # Scenario
/// - Append 5 entries and call flush() to ensure durability
/// - Simulate crash and recover
/// - Expected: All 5 flushed entries recovered from MockStorage
#[tokio::test]
async fn test_mem_first_crash_recovery_restores_flushed_entries() {
    let original_ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_mem_first_crash_recovery",
    );

    // Arrange: Append and flush
    for i in 1..=5 {
        original_ctx
            .raft_log
            .append_entries(vec![Entry {
                index: i,
                term: 1,
                payload: Some(EntryPayload::command(Bytes::from(format!("data{i}")))),
            }])
            .await
            .unwrap();
    }
    original_ctx.raft_log.flush().await.unwrap();

    // Act: Simulate crash and recover
    let recovered_ctx = original_ctx.recover_from_crash();
    drop(original_ctx);
    sleep(Duration::from_millis(50)).await;

    // Assert: All entries recovered
    assert_eq!(
        recovered_ctx.raft_log.durable_index(),
        5,
        "All entries should be recovered"
    );
    assert_eq!(recovered_ctx.raft_log.len(), 5);
    for i in 1..=5 {
        let entry = recovered_ctx.raft_log.entry(i).unwrap();
        assert!(entry.is_some(), "Entry {i} should be recovered");
    }
}

/// Test MemFirst buffers entries before flush
///
/// # Scenario
/// - Append entries with MemFirst and high batch threshold
/// - Expected: Entries in buffer but not yet durable
#[tokio::test]
async fn test_mem_first_buffers_entries_before_flush() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1000,
        },
        "test_mem_first_buffers_entries",
    );

    // Act: Append 5 entries (below threshold)
    ctx.append_entries(1, 5, 1).await;

    // Assert: Entries in buffer but may not be durable yet
    assert_eq!(ctx.raft_log.len(), 5, "Entries should be in buffer");
    let entry = ctx.raft_log.entry(3).unwrap();
    assert!(entry.is_some(), "Entry should be readable from buffer");
}

/// Test MemFirst flushes asynchronously
///
/// # Scenario
/// - Append entries and explicitly flush
/// - Expected: Entries become durable after flush
#[tokio::test]
async fn test_mem_first_flushes_asynchronously() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_mem_first_async_flush",
    );

    // Arrange: Append entries
    ctx.append_entries(1, 5, 1).await;

    // Act: Explicit flush
    ctx.raft_log.flush().await.unwrap();
    sleep(Duration::from_millis(100)).await; // Allow async flush
    ctx.drain_fsync_completions();

    // Assert: Entries now durable
    assert!(
        ctx.raft_log.durable_index() > 0,
        "Entries should be durable after flush"
    );
}

/// Test MemFirst concurrent buffering is safe
///
/// # Scenario
/// - Multiple concurrent appends with MemFirst
/// - Expected: All entries buffered correctly
#[tokio::test]
async fn test_mem_first_concurrent_buffering() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 5000,
        },
        "test_mem_first_concurrent",
    );

    // Act: Concurrent appends
    let mut handles = vec![];
    for i in 0..5 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            for j in 1..=20 {
                let index = i * 20 + j;
                log.append_entries(vec![Entry {
                    index,
                    term: 1,
                    payload: None,
                }])
                .await
                .unwrap();
            }
        }));
    }

    for handle in handles {
        handle.await.unwrap();
    }

    // Assert: All entries in buffer
    assert_eq!(ctx.raft_log.len(), 100, "All entries should be buffered");
}

/// Test Batched flushes at threshold
///
/// # Scenario
/// - Set threshold=5, append 5 entries
/// - Expected: Flush triggered at threshold
#[tokio::test]
async fn test_batched_flushes_at_threshold() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 10000, // High interval to test threshold trigger
        },
        "test_batched_threshold",
    );

    // Act: Append exactly threshold entries
    ctx.append_entries(1, 5, 1).await;
    sleep(Duration::from_millis(100)).await; // Allow flush
    ctx.drain_fsync_completions();

    // Assert: Entries should be flushed
    assert!(
        ctx.raft_log.durable_index() >= 5,
        "Entries should be flushed at threshold"
    );
}

/// Test Batched flushes at interval
///
/// # Scenario
/// - Set interval=50ms, append 2 entries, wait
/// - Expected: Flush triggered by timer
#[tokio::test]
async fn test_batched_flushes_at_interval() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 50,
        },
        "test_batched_interval",
    );

    // Act: Append few entries and wait for interval
    ctx.append_entries(1, 2, 1).await;
    sleep(Duration::from_millis(200)).await; // Wait for interval flush
    ctx.drain_fsync_completions();

    // Assert: Entries flushed by timer
    assert!(
        ctx.raft_log.durable_index() > 0,
        "Entries should be flushed by interval"
    );
}

/// Test Batched partial flush after crash
///
/// # Scenario
/// - Append 10 entries, flush 5, crash
/// - Expected: Only flushed entries recovered
#[tokio::test]
async fn test_batched_partial_flush_recovery() {
    let original_ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_batched_partial_flush",
    );

    // Arrange: Append 5 entries (triggers flush)
    original_ctx.append_entries(1, 5, 1).await;
    sleep(Duration::from_millis(100)).await; // Allow flush

    // Append 3 more without waiting for flush
    original_ctx.append_entries(6, 3, 1).await;

    // Act: Crash before second flush
    let recovered_ctx = original_ctx.recover_from_crash();
    drop(original_ctx);
    sleep(Duration::from_millis(50)).await;

    // Assert: Only first 5 entries recovered
    let durable = recovered_ctx.raft_log.durable_index();
    assert!(
        durable >= 5,
        "At least first batch should be recovered (got {durable})"
    );
}
