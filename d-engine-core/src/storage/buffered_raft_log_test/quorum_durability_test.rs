//! Quorum Durability Tests
//!
//! RPO=0 (#446): leader contributes `durable_index` (not `last_entry_id`) to quorum —
//! commit must not advance past what the leader itself has survived fsync for.
//!
//! Superseded design (kept here as history, do not resurrect): the old MemFirst model had
//! the leader contribute `last_entry_id` (in-memory) so IO persistence never sat on the
//! commit critical path. That traded away RPO=0 — a majority-acked write could still be
//! lost on correlated power loss before fsync. This file's tests now lock in the new
//! behavior instead of the old one.
//!
//! Follower ACK path (tracked separately, not yet landed): followers will ACK only after
//! their own durable_index catches up — so a follower's reported match_index is inherently
//! already durable by the time the leader sees it.
//!
//! Election-eligibility comparison must keep reading the in-memory log, never
//! `durable_index` — a separate, independent invariant from the durable-quorum change
//! above, but one a majority-count safety argument for #446 depends on. See
//! `test_election_eligibility_reads_memory_log_not_durable_index`.
//!
//! Note: these tests rely on `BufferedRaftLog`'s in-memory layer existing (they force a
//! gap between `last_entry_id` and `durable_index` via a gated mock flush). If that layer
//! is ever removed, this file's setup assumptions need revisiting — not a decided plan,
//! just a known dependency to check first.
//!
//! Tests that need a genuine, un-fsynced gap between `last_entry_id` and `durable_index`
//! use `MockStorageEngine::not_durable_gated_flush` — a real channel-based gate, not a
//! timing guess. An earlier version of this file relied on a long `idle_flush_interval_ms`
//! and assumed the dedicated `raft-io-*` OS thread just wouldn't get scheduled before the
//! assertions ran; that's a real race (the IO thread is independent of the test's own
//! runtime), and it was intermittently losing under load — flaky, not broken logic. Do not
//! reintroduce that pattern here.

use crate::BufferedRaftLog;
use crate::FlushPolicy;
use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::PersistenceConfig;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::time::Duration;

/// Entries `1..=n`, all at `term`, no payload — the shape these tests need.
fn entries(
    n: u64,
    term: u64,
) -> Vec<Entry> {
    (1..=n)
        .map(|index| Entry {
            index,
            term,
            payload: None,
        })
        .collect()
}

// ── Leader quorum uses durable_index, not last_entry_id (RPO=0) ──

/// RPO=0: leader's quorum contribution is durable_index (fsync-confirmed), not
/// last_entry_id (in-memory).
///
/// Even when a follower has already ACKed an index, the leader must not count its own
/// un-fsynced entry toward quorum — otherwise a majority-looking commit can still lose
/// data on correlated power loss (the leader's own copy was never actually durable).
///
/// This test FAILS if calculate_majority_matched_index still uses last_entry_id (the old
/// MemFirst behavior, since revoked). It replaces
/// `test_memfirst_quorum_uses_last_entry_id_not_durable_index`, which asserted the exact
/// opposite of this on purpose — that assertion documented a since-revoked design decision.
#[tokio::test]
async fn test_quorum_uses_durable_index_not_last_entry_id() {
    // Gate closed: the first flush() call blocks until we send () on `flush_gate` — fsync
    // deterministically never completes until we say so, no timing involved.
    let (storage, flush_gate) =
        MockStorageEngine::not_durable_gated_flush("test_quorum_durable_index".into());
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

    raft_log.append_entries(entries(1, 1)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await; // let it reach the gate

    assert_eq!(raft_log.last_entry_id(), 1);
    assert_eq!(raft_log.durable_index(), 0); // gate never released — fsync hasn't completed

    let result = raft_log.calculate_majority_matched_index(
        1,
        0,
        vec![1], // one follower reports match=1 (already durable, post-Stage2 semantics)
    );

    // RPO=0: leader contributes durable_index=0, not last_entry_id=1.
    // peer_matched_ids = [follower=1, leader=0], sorted desc = [1,0], median(len/2=1) = 0.
    // majority_index=0 is not < commit_index=0, so falls through to the term check on
    // entry(0) — index 0 is not a real entry (log is 1-indexed) — Ok(None) — result is None.
    assert_eq!(
        result, None,
        "RPO=0: the leader's own un-fsynced entry must not count toward quorum, even when \
         a follower has already acked it — one follower alone isn't majority without the \
         leader's own durable contribution"
    );

    let _ = flush_gate.send(()); // release so the blocked IO thread doesn't linger
}

/// Election-eligibility comparison (`last_log_id`, consumed by
/// `election_handler::handle_vote_request`) must read the in-memory log, never
/// `durable_index`. A follower with an un-fsynced tail must still be able to correctly
/// reject a candidate whose log is genuinely less up to date — voting eligibility and
/// commit-durability are two separate concerns and must not be conflated by sharing the
/// same index source.
#[tokio::test]
async fn test_election_eligibility_reads_memory_log_not_durable_index() {
    let (storage, flush_gate) =
        MockStorageEngine::not_durable_gated_flush("test_election_eligibility_memory_log".into());
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
    std::thread::sleep(Duration::from_millis(10));

    raft_log.append_entries(entries(10, 1)).await.unwrap(); // entries 1..=10, term=1
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(raft_log.durable_index(), 0, "nothing fsynced yet");
    assert_eq!(
        raft_log.last_log_id(),
        Some(LogId { index: 10, term: 1 }),
        "election-eligibility comparison must see the un-fsynced tail, not fall back to \
         durable_index=0 — a candidate with a truly-shorter log must still be rejected"
    );

    let _ = flush_gate.send(());
}

/// `calculate_majority_matched_index`'s median-based calculation requires an actual
/// majority of `peer_matched_ids` to reach an index before it counts toward commit — a
/// minority (here: 2 of 5) reporting a higher index cannot move the result past what the
/// rest of the cluster last confirmed.
///
/// This is a pure property of the median calculation itself — the function has no way to
/// know whether any of its inputs are stale or expired, so this test does not by itself
/// prove anything about stale reports being harmless. It only pins down the arithmetic
/// that a separate, broader safety argument for #446 relies on.
#[tokio::test]
async fn test_majority_matched_index_requires_actual_majority_of_reports() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_majority_requires_actual_majority",
    );

    ctx.append_entries(1, 10, 1).await;
    ctx.raft_log.flush().await.unwrap(); // leader's own entries now durable through 10
    ctx.drain_fsync_completions();

    assert_eq!(ctx.raft_log.durable_index(), 10);

    // 1 of 4 followers reports index 10; the other 3 are still at their last-known
    // value, 9.
    let result = ctx.raft_log.calculate_majority_matched_index(1, 9, vec![10, 9, 9, 9]);

    // peer_matched_ids after leader's own contribution = [10, 9, 9, 9, 10]
    // sorted desc = [10,10,9,9,9], median(len/2=2) = 9 — majority stays at 9, entry(9)
    // exists with term=1=current_term, so the result is the previously-safe Some(9), not 10.
    assert_eq!(
        result,
        Some(9),
        "a minority (2 of 5) reporting a higher index cannot move majority past what the \
         other 3 nodes last confirmed"
    );
}

/// Once the leader flushes, quorum calculation should succeed.
///
/// This test verifies the positive case: after flush, durable_index=1,
/// quorum should proceed normally.
#[tokio::test]
async fn test_quorum_succeeds_after_leader_flush() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 999_999, // only threshold trigger, no timer
        },
        "test_quorum_after_flush",
    );

    // Append entry 1 — threshold=1 so flush fires immediately
    ctx.append_entries(1, 1, 1).await;

    // Wait for flush to complete
    tokio::time::sleep(Duration::from_millis(50)).await;
    ctx.drain_fsync_completions();

    assert_eq!(ctx.raft_log.last_entry_id(), 1);
    assert_eq!(
        ctx.raft_log.durable_index(),
        1,
        "durable_index must be 1 after threshold flush"
    );

    let new_commit = ctx.raft_log.calculate_majority_matched_index(
        1,
        0,
        vec![1], // follower acked
    );

    // Correct: durable_index=1 = last_entry_id=1, quorum should pass
    assert_eq!(
        new_commit,
        Some(1),
        "quorum must succeed after leader flush"
    );
}

// ── Bug 2: gap between last_entry_id and durable_index ──

/// Demonstrates that after append_entries with a stalled fsync, last_entry_id and
/// durable_index diverge.
///
/// This is the root condition enabling the bug: both values exist,
/// but quorum calculation only uses the unsafe one.
#[tokio::test]
async fn test_last_entry_id_diverges_from_durable_index_with_mem_first() {
    let (storage, flush_gate) =
        MockStorageEngine::not_durable_gated_flush("test_diverge_mem_first".into());
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
    std::thread::sleep(Duration::from_millis(10));

    raft_log.append_entries(entries(5, 1)).await.unwrap(); // entries 1..=5, no flush
    tokio::time::sleep(Duration::from_millis(50)).await;

    assert_eq!(raft_log.last_entry_id(), 5, "memory index should be 5");
    assert_eq!(
        raft_log.durable_index(),
        0,
        "durable_index must remain 0: no flush has run"
    );
    // This gap (5 vs 0) is exactly what the quorum bug exploits.

    let _ = flush_gate.send(());
}

/// After explicit flush, durable_index must equal last_entry_id.
#[tokio::test]
async fn test_durable_index_equals_last_entry_id_after_flush() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_no_diverge_after_flush",
    );

    ctx.append_entries(1, 5, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();

    let last = ctx.raft_log.last_entry_id();
    let durable = ctx.raft_log.durable_index();

    assert_eq!(last, 5);
    assert_eq!(
        durable, last,
        "durable_index must equal last_entry_id after flush"
    );
}
