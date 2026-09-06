//! `FsyncCoordinator`'s `generation` fence protects against a physical fsync
//! whose result arrives after the world it was syncing no longer exists — but
//! today only `reset()` (full wipe) bumps `generation` via `fence_reset()`.
//! Term-conflict truncation (`filter_out_conflicts_and_append`'s slow path,
//! `remove_range` + `IOTask::ReplaceRange`) does not.
//!
//! Scenario: a follower has 10 entries synchronously written to its storage
//! engine but not yet fsynced — a physical fsync for "up to index 10" is
//! already dispatched and running in the background. Before that fsync
//! returns, a new leader tells the follower its log from index=2 onward is
//! wrong; the follower truncates and replaces it, ending up with only
//! entries [1, 2]. The in-flight fsync — which has no way to know any of
//! this happened — then completes and reports "index 10 is durable" anyway.
//! `durable_index` only moves up (`fetch_max`), so nothing afterward can
//! correct this: `durable_index()` gets stuck claiming durability for
//! entries [3..=10], which don't exist in this follower's log anymore.

use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::storage::raft_log::RaftLog;
use crate::{BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig};

fn entry(
    index: u64,
    term: u64,
) -> Entry {
    Entry {
        index,
        term,
        payload: None,
    }
}

/// `durable_index()` must never exceed `last_entry_id()` — a follower must
/// never claim durability for log entries a truncation has already discarded.
///
/// RED (today): the stale in-flight fsync (dispatched for index=10, before
/// the truncation) is not fenced, and blindly advances `durable_index` to 10
/// after the truncation has already shrunk the log to [1, 2].
#[tokio::test]
async fn test_durable_index_does_not_adopt_a_stale_fsync_after_truncation() {
    // Gate closed: the first flush() call — for the original 10-entry batch —
    // blocks here until we release it, letting us deterministically truncate
    // the log while that fsync is still "in flight".
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "durable_index_does_not_adopt_a_stale_fsync_after_truncation".into(),
    );
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Old leader (term=1) replicates entries 1..=10. append_entries() inserts
    // them into memory and notifies the IO thread, which persists the range and
    // dispatches a physical fsync for "up to index=10" — that fsync is now
    // running in the background, blocked on flush_gate.
    let entries: Vec<Entry> = (1..=10).map(|i| entry(i, 1)).collect();
    raft_log.append_entries(entries).await.unwrap();

    // Give the IO thread + blocking task time to reach the gated flush() call.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // New leader (term=2): index=2 conflicts, truncate and replace — the
    // stale fsync (still blocked on the gate) has no way to observe this.
    raft_log.filter_out_conflicts_and_append(1, 1, vec![entry(2, 2)]).await.unwrap();
    assert_eq!(
        raft_log.last_entry_id(),
        2,
        "log must be truncated and replaced down to [1, 2] before the stale fsync completes"
    );

    // Release the gate — the stale fsync (dispatched for index=10, before the
    // truncation) now completes.
    flush_gate.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert!(
        raft_log.durable_index() <= raft_log.last_entry_id(),
        "durable_index ({}) must never exceed last_entry_id ({}) — the stale \
         fsync for index=10 must not be adopted after truncation shrank the \
         log to [1, 2]",
        raft_log.durable_index(),
        raft_log.last_entry_id()
    );
}
