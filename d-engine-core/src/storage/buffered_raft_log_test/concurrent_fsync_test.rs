//! Tests for the concurrent fsync pipeline introduced in #422.
//!
//! Covers three correctness dimensions:
//! - **Protocol**: Raft invariants must hold regardless of fsync timing
//! - **Logic**: `FsyncCoordinator` (submit / run_until_caught_up) / shutdown_fsync /
//!   advance_durable_and_notify contract
//! - **Concurrency**: Reset races, out-of-order completion, crash recovery

use crate::test_utils::drain_and_apply_fsync_completions;
use crate::{
    BufferedRaftLog, FlushPolicy, MockStorageEngine, MockTypeConfig, PersistenceConfig, RaftLog,
};
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

// ── Protocol correctness ──────────────────────────────────────────────────────

/// `durable_index` must not advance before the physical fsync actually completes.
///
/// Use a MockLogStore with a controllable-delay flush(). While the delay is active,
/// assert `durable_index()` equals its pre-write value. After unblocking fsync,
/// assert `durable_index()` reaches the expected index.
///
/// This guards the core Level-3 contract: entries are only counted as durable
/// after fdatasync, not after page-cache write.
#[tokio::test]
async fn test_durable_index_not_advanced_before_fsync_completes() {
    // Gate closed: the first flush() call will block until we send () on `flush_gate`.
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "durable_index_not_advanced_before_fsync_completes".into(),
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
    // A real log_flush_tx is required now: durable_index only advances when
    // something drains InternalEvent::FsyncCompleted and calls
    // try_advance_durable_index — see drain_and_apply_fsync_completions.
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    let pre_write_durable_index = raft_log.durable_index();

    // append_entries → write_notify.notify_one() → IO thread picks it up →
    // FsyncCoordinator::submit() spawns run_until_caught_up() on the blocking
    // pool → log_store.flush() blocks on flush_gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // Give the IO thread + blocking task time to reach the gated flush() call.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // While the gate is closed, durable_index must still equal
    // its pre-write value — fsync hasn't physically completed yet.
    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index,
        "durable_index must not advance before fsync completes"
    );

    // Release the gate — flush() returns, notify_fsync_completed(1, 1) fires.
    flush_gate.send(()).unwrap();

    // Pick a polling/backoff strategy instead of a fixed sleep,
    // to avoid flakiness under CI load.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);

    assert_eq!(
        raft_log.durable_index(),
        1,
        "durable_index must reach the expected index after fsync completes"
    );
}

/// `calculate_majority_matched_index` uses `durable_index` (fsync-confirmed), not the
/// in-memory `last_entry_id` — even when a follower already reports the index, the
/// leader's own contribution must not count toward quorum until it has itself fsynced.
///
/// Stall every flush() call via a MockLogStore barrier, append entries, then verify that
/// majority-matched calculation does NOT advance while fsync is stalled, and does advance
/// once fsync completes.
///
/// Regression guard: RPO=0 (#446) requires the leader's own copy to be durable before it
/// counts toward commit — if this ever reverts to using `last_entry_id`, this test will
/// catch it before it reaches production.
///
/// Expected:
///   - Append entries so `last_entry_id()` reaches N (e.g. 2) while fsync is permanently
///     stalled — `durable_index()` stays at its pre-write value (0) throughout.
///   - Feed `calculate_majority_matched_index` a `match_index` map where one follower
///     already reports N=2 (majority IF the leader's own un-fsynced entry counted) —
///     assert the result is `None` while durable_index is still 0.
///   - Release the gate. Once `durable_index` reaches 2, the same call must return
///     `Some(2)`.
#[tokio::test]
async fn test_majority_matched_index_uses_durable_not_memory() {
    // Gate closed: the first flush() call will block until we send () on `flush_gate`.
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "majority_matched_index_uses_durable_not_memory".into(),
    );
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            // Safety-net disabled: only write_notify should trigger fsync here.
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    // A real log_flush_tx is required now: durable_index only advances when
    // something drains InternalEvent::FsyncCompleted and calls
    // try_advance_durable_index — see drain_and_apply_fsync_completions.
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    let pre_write_durable_index = raft_log.durable_index();
    let pre_last_entry_id = raft_log.last_entry_id();

    // append_entries → write_notify.notify_one() → IO thread picks it up →
    // FsyncCoordinator::submit() spawns run_until_caught_up() on the blocking
    // pool → log_store.flush() blocks on flush_gate.
    let entries = vec![
        Entry {
            index: 1,
            term: 1,
            payload: None,
        },
        Entry {
            index: 2,
            term: 1,
            payload: None,
        },
    ];
    let size = entries.len() as u64;
    raft_log.append_entries(entries).await.unwrap();

    // Give the IO thread + blocking task time to reach the gated flush() call.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // While the gate is closed, durable_index must still equal
    // its pre-write value — fsync hasn't physically completed yet.
    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index,
        "durable_index must not advance before fsync completes"
    );

    assert_eq!(
        raft_log.last_entry_id(),
        pre_last_entry_id + size,
        "in-memory tail should still advance even while fsync is stalled"
    );

    // One follower already matched index 2; the other is still behind at 0 — asymmetric
    // on purpose. If the leader's own un-fsynced entry counted (the old MemFirst
    // behavior), 2 out of 3 voters would reach index 2 — but RPO=0 requires the
    // leader's own copy to be durable first, so this must return None while fsync
    // is still gated.
    let result = raft_log.calculate_majority_matched_index(1, 1, vec![2, 0]);
    assert_eq!(
        result, None,
        "RPO=0: the leader's own un-fsynced entry must not count toward quorum, even \
         when a follower already reports it"
    );

    // Release the gate — flush() returns, notify_fsync_completed(2, 1) fires.
    flush_gate.send(()).unwrap();

    // Pick a polling/backoff strategy instead of a fixed sleep,
    // to avoid flakiness under CI load.
    tokio::time::sleep(Duration::from_millis(50)).await;
    drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);

    assert_eq!(
        raft_log.durable_index(),
        pre_write_durable_index + size,
        "durable_index must reach the expected index after fsync completes"
    );

    // Now the leader's own contribution is durable (2), so the same call must succeed.
    let result_after_fsync = raft_log.calculate_majority_matched_index(1, 1, vec![2, 0]);
    assert_eq!(
        result_after_fsync,
        Some(2),
        "once the leader's own entry is durable, majority index must advance to 2"
    );
}

/// `entry_term()` returns the correct term during high-concurrency writes
/// with artificially delayed fsyncs.
///
/// Term correctness is a memory-only invariant (TermSegments / SkipMap). Fsync
/// timing must not affect it. Run 10 concurrent writers with a 5ms fsync delay,
/// then verify every entry's term matches what was written.
///
/// Expected:
///   - 10 concurrent writers each append entries with a known term (pick terms
///     that exercise a term boundary too, e.g. entries 1-5 at term 1, entries
///     6-10 at term 2 — not just one flat term for all of them).
///   - `entry_term(index)` returns the correct term for every index, checked
///     BOTH while fsync is still delayed (in flight) and after it completes —
///     the answer must not depend on fsync having finished.
#[tokio::test]
async fn test_entry_term_correct_during_concurrent_fsync_delay() {
    // Gate closed: the first flush() call blocks until we send () on `flush_gate`.
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "entry_term_correct_during_concurrent_fsync_delay".into(),
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

    // 10 concurrent writers, each appending one entry. Term boundary at 5/6
    // exercises TermSegments::on_append's "new term" branch, not just the
    // flat single-term hot path.
    let handles: Vec<_> = (1u64..=10)
        .map(|index| {
            let raft_log = raft_log.clone();
            let term = if index <= 5 { 1 } else { 2 };
            tokio::spawn(async move {
                raft_log
                    .append_entries(vec![Entry {
                        index,
                        term,
                        payload: None,
                    }])
                    .await
                    .unwrap();
            })
        })
        .collect();
    futures::future::join_all(handles).await;

    let expected_term = |index: u64| if index <= 5 { 1 } else { 2 };

    // While the gate is closed (fsync still in flight / not even started for
    // some entries), term lookups must already be correct — TermSegments/SkipMap
    // are populated on append, independent of fsync completion.
    for index in 1u64..=10 {
        assert_eq!(
            raft_log.entry_term(index),
            Some(expected_term(index)),
            "entry {index} term must be correct while fsync is still gated"
        );
    }

    // Release the gate and let fsync complete.
    flush_gate.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Term lookups must remain correct after fsync completes too — fsync
    // timing must not affect this memory-only invariant either way.
    for index in 1u64..=10 {
        assert_eq!(
            raft_log.entry_term(index),
            Some(expected_term(index)),
            "entry {index} term must be correct after fsync completes"
        );
    }
}

// ── Logic correctness ─────────────────────────────────────────────────────────

/// `try_advance_durable_index` is monotonic: a late-arriving lower index is a no-op.
///
/// Directly call `try_advance_durable_index(150, 1)`, then `try_advance_durable_index(100, 1)`.
/// Assert:
///   - final `durable_index() == 150` (not 100)
///   - the 150 call returns `Some(150)` (it fired), the 100 call returns `None` (no-op)
///
/// Verifies the `fetch_max` invariant that makes out-of-order concurrent fsyncs safe.
///
/// Test changed from #446/#447's original (which checked an `InternalEvent::LogFlushed`
/// on a channel): `try_advance_durable_index` no longer sends that notification itself —
/// the caller (raft.rs's `FsyncCompleted` handler) decides whether to fire
/// `handle_log_flushed`, based on this method's `Option<u64>` return value. So "fired
/// exactly once, only for 150" is now asserted directly on the return values instead of
/// on a channel — same intent, moved to match where the behavior actually lives now.
#[tokio::test]
async fn test_durable_index_monotonic_when_fsyncs_complete_out_of_order() {
    // Storage engine choice doesn't matter here — try_advance_durable_index is
    // called directly, bypassing the real fsync pipeline entirely.
    let storage = Arc::new(MockStorageEngine::with_id(
        "durable_index_monotonic_when_fsyncs_complete_out_of_order".into(),
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
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // try_advance_durable_index() content-validates against entry_term(index) —
    // needs real entries in memory, not just a raw max_index poke.
    let entries: Vec<Entry> = (1..=150)
        .map(|index| Entry {
            index,
            term: 1,
            payload: None,
        })
        .collect();
    raft_log.append_entries(entries).await.unwrap();

    // Simulates a fsync task completing with index 150, then a second, older
    // fsync task (dispatched earlier, finishing later) completing with 100.
    let result_150 = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 150,
    });
    let result_100 = raft_log.try_advance_durable_index(LogId {
        term: 1,
        index: 100,
    });

    assert_eq!(
        result_150,
        Some(150),
        "the 150 call must fire — it's the first advance"
    );
    assert_eq!(
        result_100, None,
        "the later, lower 100 call must be a no-op (None), not a regression"
    );
    assert_eq!(
        raft_log.durable_index(),
        150,
        "durable_index must reflect the highest index seen (150), not the \
         later-arriving lower one (100)"
    );
}

/// A `flush()` caller receives `Ok(())` only after its batch is physically on disk.
///
/// Use a MockLogStore with a release-gate on flush(). Call `flush()`, verify the future
/// is still pending while the gate is closed, open the gate, verify the future resolves
/// to `Ok(())`.
///
/// Core contract of `FsyncCoordinator::run_until_caught_up`: the reply is sent
/// inside the `spawn_blocking` task, after `log_store.flush()` returns — never
/// before.
///
/// Expected:
///   - While the gate is closed: polling the `flush()` future (e.g. with a
///     short `tokio::time::timeout`) shows it still pending — it must NOT
///     resolve before the gate opens.
///   - After releasing the gate: the SAME future resolves to `Ok(())`.
#[tokio::test]
async fn test_flush_caller_blocked_until_fsync_completes() {
    // ── Setup (given) ──────────────────────────────────────────────────────
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "flush_caller_blocked_until_fsync_completes".into(),
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

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    let raft_log_clone = raft_log.clone();
    let flush_handle = tokio::spawn(async move { raft_log_clone.flush().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !flush_handle.is_finished(),
        "flush() must not resolve while the fsync gate is closed"
    );

    flush_gate.send(()).unwrap();

    let result = flush_handle.await.unwrap();
    assert!(
        result.is_ok(),
        "flush() must resolve to Ok(()) after the fsync gate opens"
    );
}

/// `flush()` callers whose requests arrive WHILE a fsync task is already running
/// on the blocking pool are coalesced into that same in-flight task — not into a
/// second, competing `spawn_blocking` call.
///
/// This is `FsyncCoordinator`'s actual new capability over the old inline-fsync
/// design (which could only coalesce callers that happened to land in the same
/// drain window, before the IO thread blocked on `flush()`). Now: `submit()`
/// records new work into `pending_max`/`pending_replies` and returns immediately
/// if `inflight` is already `true`; `run_until_caught_up` picks that accumulated
/// work up on its next loop iteration, before clearing `inflight`.
///
/// Configure a MockLogStore with a flush call counter and a release-gate on the
/// first `flush()` call. Append one entry — `write_notify` wakes the IO loop,
/// which submits a fsync round on its own (round 1), winning the CAS and
/// blocking on the gate. While round 1 is gated, call `raft_log.flush()` three
/// times (A, B, C) — none of them can win the CAS (round 1 is still in
/// flight), so all three just extend `pending_max`/`pending_replies` and wait.
/// Release the gate and assert: round 1 finishes and immediately picks up A/B/C
/// as a single round 2 (not one `spawn_blocking` call per caller), so
/// `flush_call_count` is 2, not 4 — and all three futures resolve to `Ok(())`.
///
/// Expected:
///   - `flush_call_count == 2`: round 1 (the append's own automatic fsync,
///     already in flight when A/B/C arrive) plus round 2 (A, B, and C served
///     together) — NOT 4 (one per append + one per explicit caller).
///   - All three `flush()` futures resolve to `Ok(())`.
#[tokio::test]
async fn test_flush_callers_arriving_during_inflight_fsync_are_coalesced() {
    let (storage, flush_gate, flush_call_count) =
        MockStorageEngine::not_durable_gated_flush_counted(
            "flush_callers_arriving_during_inflight_fsync_are_coalesced".into(),
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

    // Append one entry: write_notify wakes the IO loop, which submits its own
    // fsync round (round 1) and wins the CAS — flush() itself early-returns
    // Ok(()) with nothing to do while max_index is still 0, so a real write
    // is needed to get a round in flight for A/B/C to arrive during.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();

    // Give the IO loop time to submit round 1 and block on the gate before
    // A/B/C are dispatched — otherwise they could race the automatic round
    // for the CAS instead of deterministically losing it.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // A, B, C: all arrive while round 1 is still gated — none can win the
    // CAS, so all three just extend pending_max/pending_replies and wait.
    let raft_log_a = raft_log.clone();
    let a = tokio::spawn(async move { raft_log_a.flush().await });
    let raft_log_b = raft_log.clone();
    let b = tokio::spawn(async move { raft_log_b.flush().await });
    let raft_log_c = raft_log.clone();
    let c = tokio::spawn(async move { raft_log_c.flush().await });

    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !a.is_finished() && !b.is_finished() && !c.is_finished(),
        "all three flush() calls must still be pending while the gate is closed"
    );

    flush_gate.send(()).unwrap();

    let (ra, rb, rc) = tokio::join!(a, b, c);
    assert!(ra.unwrap().is_ok(), "A's flush() must resolve to Ok(())");
    assert!(rb.unwrap().is_ok(), "B's flush() must resolve to Ok(())");
    assert!(rc.unwrap().is_ok(), "C's flush() must resolve to Ok(())");

    assert_eq!(
        flush_call_count.load(std::sync::atomic::Ordering::Acquire),
        2,
        "expected exactly 2 physical flush() calls: round 1 (append's own \
         automatic fsync) plus round 2 (A, B, and C served together) — not 4 \
         (one per append + one per explicit caller)"
    );
}

/// A `flush()` caller queued behind an already in-flight fsync round still
/// gets a real reply after `close()` gives up waiting — not a silent
/// channel-closed error.
///
/// `close()`'s wait is bounded by `shutdown_timeout_ms` (a short value here so
/// the test doesn't burn real wall-clock seconds); it does not cancel the
/// in-flight `FsyncCoordinator` task, which keeps running on the blocking pool
/// and eventually serves any callers queued behind it, independent of whether
/// `close()` has already returned.
///
/// Expected:
///   - `close()` returns within roughly `shutdown_timeout_ms` even though the
///     fsync round is still gated — not hanging indefinitely.
///   - The queued caller's `flush()` future resolves to `Ok(())` once the
///     gate opens — even though `close()` already returned before that.
#[tokio::test]
async fn test_shutdown_with_pending_flush_caller_still_receives_ok_reply() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "shutdown_with_pending_flush_caller_still_receives_ok_reply".into(),
    );
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 100,
        },
        Arc::new(storage),
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // X arrives while round 1 is gated — loses the CAS, gets queued into
    // FsyncCoordinator's own pending_replies (not into batch_processor's
    // shutdown path).
    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !x.is_finished(),
        "X's flush() must still be pending before shutdown"
    );

    // The gate stays closed here: close() must give up after
    // shutdown_timeout_ms rather than hanging forever.
    tokio::time::timeout(Duration::from_secs(1), raft_log.close())
        .await
        .expect("close() must return within shutdown_timeout_ms, not hang");
    assert!(
        !x.is_finished(),
        "X must still be pending right after close() times out"
    );

    // Release the gate: round 1 completes on its own, loops back, and picks
    // up X's queued reply as round 2 — independent of close() having already
    // returned.
    flush_gate.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("X must not hang")
        .unwrap();
    assert!(
        result.is_ok(),
        "X must receive a real Ok(()) reply, not a channel-closed error, \
         even though close() already returned"
    );
}

/// Same as above, but the queued fsync round fails once unblocked — the
/// caller must receive the real `Err`, not a silent channel-closed error.
///
/// Expected:
///   - The queued caller's `flush()` future resolves to `Err(..)` carrying
///     the fsync failure after the gate opens.
#[tokio::test]
async fn test_shutdown_with_pending_flush_caller_still_receives_err_reply() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush_failing(
        "shutdown_with_pending_flush_caller_still_receives_err_reply".into(),
    );
    let (raft_log, receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 100,
        },
        Arc::new(storage),
    );
    let raft_log = raft_log.start(receiver, None);
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !x.is_finished(),
        "X's flush() must still be pending before shutdown"
    );

    // The gate stays closed here: close() must give up after
    // shutdown_timeout_ms rather than hanging forever.
    tokio::time::timeout(Duration::from_secs(1), raft_log.close())
        .await
        .expect("close() must return within shutdown_timeout_ms, not hang");
    assert!(
        !x.is_finished(),
        "X must still be pending right after close() times out"
    );

    flush_gate.send(()).unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("X must not hang")
        .unwrap();
    assert!(
        result.is_err(),
        "X must receive the real fsync failure, not a channel-closed error"
    );
}

// ── Distributed / concurrency ─────────────────────────────────────────────────

/// An in-flight fsync task completing AFTER `reset()` must not "resurrect" the
/// pre-reset `durable_index`, and must not silently report success to any
/// caller waiting on that stale round.
///
/// Scenario:
///   1. Write entry at index 1..100, gate the physical `flush()` call so the
///      round stays in flight (mirrors `not_durable_gated_flush` pattern used
///      throughout this file).
///   2. While gated, call `reset()` — bumps `FsyncCoordinator`'s fence
///      generation, then clears `durable_index`/`max_index`/entries to 0.
///   3. Release the gate — the stale round's `flush()` returns, but its
///      generation no longer matches; it must discard its result instead of
///      calling `advance_durable_and_notify(100)`.
///
/// Expected:
///   - `durable_index()` stays `0` after the gate releases — the stale round
///     must not resurrect the pre-reset value, not even transiently.
///   - If the stale round was carrying a `flush()` caller's reply, that
///     caller's future resolves to `Err(..)` — not a resurrected `Ok(())`,
///     and not a silently dropped channel.
#[tokio::test]
async fn test_reset_during_inflight_fsync_does_not_resurrect_stale_durable_index() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "reset_during_inflight_fsync_does_not_resurrect_stale_durable_index".into(),
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

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // X arrives while round 1 is gated — queues behind it in FsyncCoordinator.
    let raft_log_x = raft_log.clone();
    let x = tokio::spawn(async move { raft_log_x.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;

    // reset() while round 1 is still gated: bumps the fence generation, then
    // clears durable_index/max_index/entries to 0.
    raft_log.reset().await.unwrap();

    // Release the gate: round 1's flush() call returns, but its generation no
    // longer matches — it must discard instead of resurrecting durable_index.
    flush_gate.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), x)
        .await
        .expect("x must not hang")
        .unwrap();
    assert!(
        result.is_err(),
        "X's flush() must receive Err — its data was wiped by reset, not a \
         resurrected Ok(())"
    );

    assert_eq!(
        raft_log.durable_index(),
        0,
        "durable_index must stay 0 — the stale round must not resurrect the \
         pre-reset value, not even transiently"
    );
}

/// The reset fence must not over-trigger: writes that happen AFTER `reset()`
/// (in a fresh round, not the stale one) must still advance `durable_index`
/// normally — the fence only discards the round that was in flight *before*
/// the reset, not everything that comes after it.
///
/// Scenario:
///   1. Same setup as above: gate a round, call `reset()` while it's stalled.
///   2. Before releasing the gate, append new entries and call `flush()` —
///      this new round queues behind the still-gated stale round.
///   3. Release the gate — the stale round discards itself (per the test
///      above); `run_until_caught_up` loops back, captures a FRESH generation
///      at the top of the next iteration, and processes the new round.
///
/// Expected:
///   - The new (post-reset) `flush()` caller's future resolves to `Ok(())`.
///   - `durable_index()` advances to reflect the post-reset entries — the
///     fence must not discard legitimate work just because a reset happened
///     at some point in the task's lifetime; only the round that was
///     in-flight *at the moment of reset* is stale.
#[tokio::test]
async fn test_post_reset_writes_are_not_discarded_by_stale_fence() {
    let (storage, flush_gate) = MockStorageEngine::not_durable_gated_flush(
        "post_reset_writes_are_not_discarded_by_stale_fence".into(),
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
    let (log_flush_tx, mut log_flush_rx) = mpsc::unbounded_channel();
    let raft_log = raft_log.start(receiver, Some(log_flush_tx));
    std::thread::sleep(Duration::from_millis(10)); // ensure IO thread is ready

    // Append triggers the automatic round (round 1), which wins the CAS and
    // blocks on the gate.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    // reset() while round 1 is still gated: bumps the fence generation and
    // drains anything already queued (fence_reset()).
    raft_log.reset().await.unwrap();

    // New, post-reset entry — a fresh write, unrelated to the stale round.
    raft_log
        .append_entries(vec![Entry {
            index: 1,
            term: 1,
            payload: None,
        }])
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;

    let raft_log_y = raft_log.clone();
    let y = tokio::spawn(async move { raft_log_y.flush().await });
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        !y.is_finished(),
        "Y must still be pending behind the still-gated stale round"
    );

    // Release the gate: the stale round discards itself (generation
    // mismatch, per the test above); run_until_caught_up loops back,
    // captures a fresh generation, and processes Y's post-reset round.
    flush_gate.send(()).unwrap();

    let result = tokio::time::timeout(Duration::from_secs(1), y)
        .await
        .expect("y must not hang")
        .unwrap();
    assert!(
        result.is_ok(),
        "Y's flush() must succeed — its data was written after reset, not stale"
    );
    drain_and_apply_fsync_completions(&raft_log, &mut log_flush_rx);
    assert_eq!(
        raft_log.durable_index(),
        1,
        "durable_index must advance to reflect the post-reset entry"
    );
}
