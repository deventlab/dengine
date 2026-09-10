//! Content-validated `durable_index` advance (#446 single-owner redesign).
//! Tests `try_advance_durable_index(index, term) -> Option<u64>`:
//! `Some(new)` only when it actually advanced, `None` when rejected as stale
//! (`entry_term(index) != Some(term)`) or already applied.
//!
//! Why no thread races / timing gates here (unlike `truncation_fsync_fence_test.rs`):
//! `durable_index` has one owner — raft.rs's event loop. The fsync-completion
//! report and a truncation are just two sequential calls on that thread, so no
//! interleaving inside a function body is possible. Each test drives one
//! arrival order directly.

use std::sync::Arc;
use std::time::Duration;

use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;

use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
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

async fn new_raft_log() -> Arc<BufferedRaftLog<MockTypeConfig>> {
    let storage = Arc::new(MockStorageEngine::with_id(
        "content_validated_watermark_test".into(),
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
    raft_log.start(receiver, None)
}

/// Business scenario: follower has entries 1..=100 under term 1. A physical
/// fsync for "up to 100" is still in flight when a new leader (term 2)
/// truncates 81..=100 and replaces it with its own entries. The in-flight
/// fsync's completion — a report for (index=100, term=1) — arrives after the
/// replacement. Index 100 still exists, but it's term 2 now: the report
/// describes content that's gone.
///
/// Expected: rejected. `durable_index` must not move to 100.
#[tokio::test]
async fn test_stale_durable_report_rejected_when_term_no_longer_matches() {
    let raft_log = new_raft_log().await;

    let term1_entries: Vec<Entry> = (1..=100).map(|i| entry(i, 1)).collect();
    raft_log.append_entries(term1_entries).await.unwrap();

    // New leader (term 2) truncates 81..=100 and replaces with its own tail.
    let term2_tail: Vec<Entry> = (81..=100).map(|i| entry(i, 2)).collect();
    raft_log.filter_out_conflicts_and_append(80, 1, term2_tail).await.unwrap();

    // The stale in-flight fsync's report, generated before the truncation.
    let result = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 100,
    });

    assert_eq!(
        result, None,
        "a durable report for term=1 must be rejected once index 100 belongs to term=2"
    );
    assert!(
        raft_log.durable_index() < 81,
        "durable_index ({}) must not advance into the replaced [81,100] range \
         on a rejected report",
        raft_log.durable_index()
    );
}

/// Sanity check: an unremarkable report (no truncation involved) must still
/// be applied. The new validation must not reject everything.
#[tokio::test]
async fn test_durable_report_accepted_when_term_still_matches() {
    let raft_log = new_raft_log().await;

    let entries: Vec<Entry> = (1..=100).map(|i| entry(i, 1)).collect();
    raft_log.append_entries(entries).await.unwrap();

    let result = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 100,
    });

    assert_eq!(
        result,
        Some(100),
        "a report matching current log content must be applied"
    );
    assert_eq!(raft_log.durable_index(), 100);
}

/// Same scenario as `test_stale_durable_report_rejected_when_term_no_longer_matches`,
/// but the report arrives BEFORE the truncation instead of after — the other
/// possible arrival order. Under single ownership both orders must land on
/// the same final state, because the owner processes one event at a time
/// rather than racing a background write against a live update.
#[tokio::test]
async fn test_durable_report_then_truncation_is_order_independent() {
    let raft_log = new_raft_log().await;

    let term1_entries: Vec<Entry> = (1..=100).map(|i| entry(i, 1)).collect();
    raft_log.append_entries(term1_entries).await.unwrap();

    // Report arrives first, while the log is still all term 1 — legitimately
    // applied at this point in time.
    let result = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 100,
    });
    assert_eq!(result, Some(100));
    assert_eq!(raft_log.durable_index(), 100);

    // Truncation arrives after — must still clamp durable_index down,
    // exactly as it does today via `remove_range`'s existing fetch_min.
    let term2_tail: Vec<Entry> = (81..=100).map(|i| entry(i, 2)).collect();
    raft_log.filter_out_conflicts_and_append(80, 1, term2_tail).await.unwrap();

    assert!(
        raft_log.durable_index() < 81,
        "truncation must clamp durable_index down to 80 regardless of the \
         earlier report having advanced it to 100, durable_index is {}",
        raft_log.durable_index()
    );
}

/// Regression test for the `flush()` short-circuit (`durable_index >=
/// memory_max_index` at `buffered_raft_log.rs:710`). This isn't proving a
/// live bug in the current design (`remove_range` clamps `durable_index`
/// synchronously, so the short-circuit's precondition always holds) — it's
/// pinning down that invariant so a future change that defers the clamp
/// (e.g. copying openraft's "don't touch the watermark on truncation, rely
/// on term comparison instead") doesn't silently reopen the RPO=0 violation
/// this whole fix was for: `flush()` returning `Ok(())` before the real,
/// post-truncation tail has actually been fsynced.
///
/// Scenario: entries 1..=100 durable. New leader truncates 81..=100 (term 2
/// tail 81..=85 replaces it) — `durable_index` clamps to 80,
/// `memory_max_index` becomes 85. Calling `flush()` right after must NOT
/// take the short-circuit (80 < 85) — it must dispatch a real physical
/// flush for the new, not-yet-synced tail.
#[tokio::test]
async fn test_flush_does_not_short_circuit_after_truncation_regrows_the_log() {
    let (mut ctx, flush_count) = BufferedRaftLogTestContext::new_not_durable(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000,
        },
        "flush_no_short_circuit_after_truncation",
    );

    ctx.append_entries(1, 100, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(
        ctx.raft_log.durable_index(),
        100,
        "baseline must be fully durable"
    );

    let flushes_before_truncation = flush_count.load(std::sync::atomic::Ordering::Relaxed);

    // New leader (term 2) truncates 81..=100, replaces with its own tail
    // 81..=85 — durable_index clamps to 80, memory_max_index becomes 85.
    let term2_tail: Vec<Entry> = (81..=85).map(|i| entry(i, 2)).collect();
    ctx.raft_log.filter_out_conflicts_and_append(80, 1, term2_tail).await.unwrap();
    assert!(ctx.raft_log.durable_index() < 81, "clamp must have fired");
    assert_eq!(ctx.raft_log.last_entry_id(), 85);

    ctx.raft_log.flush().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    ctx.drain_fsync_completions();

    let flushes_after = flush_count.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        flushes_after > flushes_before_truncation,
        "flush() must dispatch a real physical flush for the new tail, not \
         short-circuit on a stale-looking durable_index — before={flushes_before_truncation}, after={flushes_after}"
    );
    assert_eq!(
        ctx.raft_log.durable_index(),
        85,
        "the new tail must actually become durable, not just claimed so"
    );
}
