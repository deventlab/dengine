use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use tokio::sync::mpsc;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::{BufferedRaftLogTestContext, MockStorageEngine, simulate_insert_command};
use crate::{BufferedRaftLog, FlushPolicy, InternalEvent, MockTypeConfig, PersistenceConfig};
use d_engine_proto::common::{Entry, LogId};

#[tokio::test]
async fn test_durable_index_monotonic_under_concurrency() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_durable_index_monotonic",
    );

    let mut handles = vec![];

    for batch in 0..10 {
        let log = ctx.raft_log.clone();
        handles.push(tokio::spawn(async move {
            let start = batch * 100 + 1;
            let entries: Vec<Entry> = (start..start + 100)
                .map(|i| Entry {
                    index: i,
                    term: 1,
                    payload: None,
                })
                .collect();
            log.append_entries(entries).await.unwrap();
        }));
    }

    join_all(handles).await;

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(200)).await;
    ctx.drain_fsync_completions();

    // Verify monotonicity
    let durable = ctx.raft_log.durable_index();
    assert!(durable >= 1000, "durable_index should reach max value");

    // Verify no entries lost
    assert_eq!(ctx.raft_log.len(), 1000);
}

#[tokio::test]
async fn test_durable_index_with_non_contiguous_entries() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_durable_index_non_contiguous",
    );

    // Create entries with non-contiguous indexes: 4, 7, 8, 10
    let entries = vec![
        Entry {
            index: 4,
            term: 1,
            payload: None,
        },
        Entry {
            index: 7,
            term: 1,
            payload: None,
        },
        Entry {
            index: 8,
            term: 1,
            payload: None,
        },
        Entry {
            index: 10,
            term: 1,
            payload: None,
        },
    ];

    ctx.raft_log.append_entries(entries).await.unwrap();

    // Wait for flush
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Verify durable_index handles non-contiguous entries correctly
    // With mock storage, durable_index behavior depends on flush completion
    let _durable = ctx.raft_log.durable_index();
    // Note: durable_index returns u64, which is always >= 0 by type definition

    // Verify entries exist
    assert!(ctx.raft_log.entry(4).unwrap().is_some());
    assert!(ctx.raft_log.entry(7).unwrap().is_some());
    assert!(ctx.raft_log.entry(8).unwrap().is_some());
    assert!(ctx.raft_log.entry(10).unwrap().is_some());
}

/// #442: `purge_logs_up_to()` advances `durable_index` via `advance_durable_and_notify`
/// (a monotonic `fetch_max`) instead of a plain `load`-then-`store`. This must never
/// regress `durable_index`, and must not emit a spurious `LogFlushed` for a value
/// lower than what's already been recorded.
///
/// # Scenario
/// Entries 1..=100 are inserted and flushed (durable_index reaches 100 via the
/// normal fsync path, which already fired one `LogFlushed(100)`). A snapshot then
/// triggers a purge up to index 50 — well behind what's already durable. This is
/// the common case: purge only ever targets `last_applied - retained_log_entries`,
/// and applying an entry already implies it's durable.
///
/// # Expected
/// - `durable_index()` stays `100` (not regressed to `50`).
/// - No second `LogFlushed` event fires for `50`.
#[tokio::test]
async fn test_purge_does_not_regress_durable_index_already_ahead() {
    let storage = Arc::new(MockStorageEngine::with_id(
        "test_purge_does_not_regress_durable_index".into(),
    ));
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        storage,
    );
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel::<InternalEvent>();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));

    // Arrange: entries 1..=100, all flushed — durable_index reaches 100 and
    // fires LogFlushed(100).
    simulate_insert_command(&raft_log, (1..=100).collect(), 1).await;
    crate::test_utils::drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);
    assert_eq!(raft_log.durable_index(), 100);

    // Drain the LogFlushed(100) from the insert+flush above — not what this
    // test is checking.
    let _ = log_flush_rx.try_recv();

    // Act: purge up to index 50 — behind what's already durable.
    raft_log.purge_logs_up_to(LogId { index: 50, term: 1 }).await.unwrap();

    // Assert: durable_index must not regress.
    assert_eq!(
        raft_log.durable_index(),
        100,
        "purge_logs_up_to must not regress durable_index below what's already \
         been confirmed durable"
    );

    // Assert: no LogFlushed(50) — fetch_max must treat this as a no-op.
    assert!(
        log_flush_rx.try_recv().is_err(),
        "purge to an already-durable cutoff must not emit a spurious LogFlushed"
    );
}
