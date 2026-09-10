//! Direct, isolated unit tests for `FsyncCoordinator`.
//!
//! This module is a child of `fsync_coordinator` (see the `#[path = ...]
//! mod tests;` declaration at the bottom of `fsync_coordinator.rs`), so it can
//! construct `FsyncCoordinator` directly and inspect its private fields
//! (`inflight`/`pending_max`/`pending_replies`/`generation`) without going
//! through `BufferedRaftLog::append_entries`/`write_notify`/`batch_processor`
//! at all — no IO thread, no `raft_log.start(...)`, most tests need no
//! `tokio` runtime either (`run_until_caught_up` is a plain sync fn).
//!
//! Scope: protocol/logic correctness of `FsyncCoordinator`'s own state
//! machine. Not performance — see `benches/` for throughput regression
//! guards.

use super::*;
use crate::FlushPolicy;
use crate::InternalEvent;
use crate::MockLogStore;
use crate::MockMetaStore;
use crate::MockStorageEngine;
use crate::MockTypeConfig;
use crate::PersistenceConfig;
use crate::RaftLog; // try_advance_durable_index is a RaftLog trait method
use crate::Result;
use d_engine_proto::common::Entry;
use d_engine_proto::common::LogId;
use std::sync::Arc;
use tokio::sync::mpsc;

/// Build a `BufferedRaftLog` for direct `FsyncCoordinator` method calls —
/// never `.start()`-ed, no IO thread, no channel plumbing. Only `log_store`/
/// `durable_index`/`advance_durable_and_notify` are ever touched by the
/// methods under test here.
fn minimal_raft_log(storage: MockStorageEngine) -> Arc<BufferedRaftLog<MockTypeConfig>> {
    let (raft_log, _receiver) = BufferedRaftLog::<MockTypeConfig>::new(
        1,
        PersistenceConfig {
            flush_policy: FlushPolicy::Batch {
                idle_flush_interval_ms: 60_000,
            },
            shutdown_timeout_ms: 5000,
        },
        Arc::new(storage),
    );
    Arc::new(raft_log)
}

// ── Initial state ──────────────────────────────────────────────────────────

/// `FsyncCoordinator::new()` starts with no work pending and no fence armed.
///
/// Expected:
///   - `inflight == false`, `pending_max == 0`, `pending_replies` empty,
///     `generation == 0`.
#[test]
fn test_new_initializes_empty_state() {
    let coord = FsyncCoordinator::new();

    assert!(
        !coord.inflight.load(Ordering::Acquire),
        "inflight must start false"
    );
    assert_eq!(
        coord.pending_max.lock().index,
        0,
        "pending_max must start at 0"
    );
    assert!(
        coord.pending_replies.lock().is_empty(),
        "pending_replies must start empty"
    );
    assert_eq!(
        coord.generation.load(Ordering::Acquire),
        0,
        "generation must start at 0"
    );
}

// ── fence_reset() ────────────────────────────────────────────────────────────

/// `fence_reset()` zeroes `pending_max` — any batch size recorded before
/// reset must not leak into a post-reset round.
///
/// Expected:
///   - After manually setting `pending_max` to a non-zero value and calling
///     `fence_reset()`, `pending_max` reads back as `0`.
#[test]
fn test_fence_reset_zeroes_pending_max() {
    let coord = FsyncCoordinator::new();
    // Directly seed pending_max — no need to go through submit() (which would
    // spawn a real background task and race with fence_reset() below).
    *coord.pending_max.lock() = LogId { term: 1, index: 10 };

    coord.fence_reset();

    assert_eq!(
        coord.pending_max.lock().index,
        0,
        "fence_reset() must zero pending_max"
    );
}

/// `fence_reset()` drains `pending_replies` and answers each with `Err` —
/// callers queued before reset must not be silently dropped nor receive a
/// stale `Ok(())`.
///
/// Expected:
///   - Manually push several oneshot senders into `pending_replies`, call
///     `fence_reset()`, assert every corresponding receiver resolves to
///     `Err(..)`.
///   - `pending_replies` is empty after the call.
#[test]
fn test_fence_reset_drains_pending_replies_with_err() {
    let coord = FsyncCoordinator::new();

    let (tx1, mut rx1) = oneshot::channel();
    let (tx2, mut rx2) = oneshot::channel();
    coord.pending_replies.lock().extend([tx1, tx2]);

    coord.fence_reset();

    assert!(
        coord.pending_replies.lock().is_empty(),
        "pending_replies must be empty after fence_reset()"
    );
    assert!(
        rx1.try_recv()
            .expect("tx1 must have been answered, not silently dropped")
            .is_err(),
        "queued reply must receive Err, not a silent drop or stale Ok"
    );
    assert!(
        rx2.try_recv()
            .expect("tx2 must have been answered, not silently dropped")
            .is_err(),
        "queued reply must receive Err, not a silent drop or stale Ok"
    );
}

/// `fence_reset()` increments `generation` — this is the fence itself; any
/// round whose `gen_at_start` predates this call must be recognized as stale.
///
/// Expected:
///   - `generation` after the call is exactly one more than before.
///   - Calling `fence_reset()` twice in a row increments it twice (no
///     accidental no-op / debounce).
#[test]
fn test_fence_reset_increments_generation() {
    let coord = FsyncCoordinator::new();

    coord.fence_reset();
    assert_eq!(
        coord.generation.load(Ordering::Acquire),
        1,
        "first fence_reset() must bump generation to 1"
    );
    coord.fence_reset();
    assert_eq!(
        coord.generation.load(Ordering::Acquire),
        2,
        "second fence_reset() must bump generation to 2"
    );
}

/// `fence_reset()` is safe to call with nothing pending (no in-flight round,
/// no queued replies) — e.g. `reset()` called on a freshly created log that
/// never wrote anything.
///
/// Expected:
///   - No panic. `generation` still increments. `pending_max` stays `0`.
#[test]
fn test_fence_reset_is_safe_with_nothing_pending() {
    let coord = FsyncCoordinator::new();

    // No panic expected from this call — nothing pending to drain/zero.
    coord.fence_reset();

    assert_eq!(
        coord.generation.load(Ordering::Acquire),
        1,
        "generation must still increment even with nothing pending"
    );
    assert_eq!(coord.pending_max.lock().index, 0, "pending_max must stay 0");
    assert!(
        coord.pending_replies.lock().is_empty(),
        "pending_replies must stay empty"
    );
}

// ── submit() ─────────────────────────────────────────────────────────────

/// `submit()` records `max_index` via `fetch_max`, not last-write-wins — a
/// smaller, later `max_index` must not regress the recorded high-water mark.
///
/// Expected:
///   - `submit(.., 100, vec![])` then `submit(.., 50, vec![])` leaves
///     `pending_max == 100`, not `50`.
#[test]
fn test_submit_pending_max_uses_fetch_max_not_last_write() {
    let coord = Arc::new(FsyncCoordinator::new());
    let raft_log = minimal_raft_log(MockStorageEngine::new());

    // Pretend a round is already in flight so submit() just records state
    // instead of actually spawning a background task — keeps this test
    // deterministic, no real concurrency needed to verify fetch_max order.
    coord.inflight.store(true, Ordering::Release);

    coord.submit(
        &raft_log,
        LogId {
            term: 1,
            index: 100,
        },
        vec![],
    );
    coord.submit(&raft_log, LogId { term: 1, index: 50 }, vec![]);

    assert_eq!(
        coord.pending_max.lock().index,
        100,
        "pending_max must stay at the high-water mark (100), not regress to \
         a later, smaller submit() value (50)"
    );
}

/// The first `submit()` call (no round in flight) wins the CAS and flips
/// `inflight` to `true` synchronously — the caller doesn't need to wait for
/// the spawned task to observe this.
///
/// Expected:
///   - Immediately after the first `submit()` call returns, `inflight` reads
///     `true`.
#[tokio::test]
async fn test_submit_first_call_sets_inflight_true() {
    // Gated so the spawned task stays stuck in flush() — guarantees it can't
    // race ahead and clear `inflight` again before our assertion below runs.
    let (storage, _flush_gate) =
        MockStorageEngine::not_durable_gated_flush("submit_first_call_sets_inflight_true".into());
    let coord = Arc::new(FsyncCoordinator::new());
    let raft_log = minimal_raft_log(storage);

    coord.submit(&raft_log, LogId { term: 1, index: 1 }, vec![]);

    assert!(
        coord.inflight.load(Ordering::Acquire),
        "inflight must be true immediately after the first submit() call wins the CAS"
    );
    // _flush_gate is dropped here without ever being sent — the spawned
    // task's gate.recv() returns Err (sender dropped) and its flush() call
    // proceeds to return Ok(()), so nothing hangs at test teardown.
}

/// A `submit()` call while a round is already in flight must not spawn a
/// second competing task — it only records `pending_max`/`pending_replies`
/// for the running task to pick up.
///
/// Expected:
///   - With `inflight` manually pre-set to `true`, calling `submit()` updates
///     `pending_max`/`pending_replies` as normal.
///   - The underlying mock's `flush()` call count does not increase (no new
///     physical fsync was triggered by this call).
#[test]
fn test_submit_second_call_does_not_spawn_second_task_while_inflight() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "submit_second_call_does_not_spawn_second_task_while_inflight".into(),
    );
    let coord = Arc::new(FsyncCoordinator::new());
    let raft_log = minimal_raft_log(storage);

    // Manually pre-set inflight — simulates "a round is already running", so
    // this submit() call must lose the CAS and only record state, matching
    // the doc comment's scenario directly without needing a real first round
    // to actually be dispatched.
    coord.inflight.store(true, Ordering::Release);

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    coord.submit(&raft_log, LogId { term: 1, index: 1 }, vec![tx]);

    assert_eq!(
        coord.pending_max.lock().index,
        1,
        "submit() must still record pending_max even though it lost the CAS"
    );
    assert_eq!(
        coord.pending_replies.lock().len(),
        1,
        "submit() must still queue the reply even though it lost the CAS"
    );
    assert!(
        rx.try_recv().is_err(),
        "the queued reply must still be pending — nobody has processed it yet"
    );
    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        0,
        "no physical flush() call should have been triggered — losing the CAS \
         must not spawn a competing task"
    );
}

// ── run_until_caught_up() ────────────────────────────────────────────────

/// With nothing pending, `run_until_caught_up` clears `inflight` and returns
/// immediately — no physical `flush()` call.
///
/// Expected:
///   - `inflight` was `true` (simulating a just-won CAS with no work),
///     `pending_max == 0`, `pending_replies` empty.
///   - After the call: `inflight == false`. The mock's `flush()` is never
///     called.
#[test]
fn test_run_until_caught_up_returns_immediately_when_nothing_pending() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_returns_immediately_when_nothing_pending".into(),
    );
    let coord = FsyncCoordinator::new();
    let raft_log = minimal_raft_log(storage);

    // Simulate having just won the CAS in submit() — pending_max/pending_replies
    // stay at their default empty state, matching the "nothing pending" scenario.
    coord.inflight.store(true, Ordering::Release);

    coord.run_until_caught_up(&raft_log);

    assert!(
        !coord.inflight.load(Ordering::Acquire),
        "inflight must be cleared when there's nothing to do"
    );
    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        0,
        "no physical flush() call should happen when nothing is pending"
    );
}

/// A successful physical flush advances `durable_index` to `max_index`.
///
/// Expected:
///   - After manually seeding `pending_max`/`inflight` and calling
///     `run_until_caught_up` against a mock whose `flush()` returns `Ok(())`,
///     `raft_log.durable_index()` equals the seeded `max_index`.
#[test]
fn test_run_until_caught_up_advances_durable_index_on_success() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_advances_durable_index_on_success".into(),
    );
    let coord = FsyncCoordinator::new();
    // run_until_caught_up() now only *reports* completion (notify_fsync_completed) —
    // durable_index only moves once something (normally raft.rs) drains that
    // report and calls try_advance_durable_index, which content-validates
    // against entry_term(index). Needs a real backing entry (not just
    // set_memory_max_index_for_test's raw atomic poke) and a registered
    // log_flush_tx to receive the report — minimal_raft_log() has neither, so
    // this test builds its own instead of using it.
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
    raft_log.entries.write().insert(
        5,
        Entry {
            index: 5,
            term: 1,
            payload: None,
        },
    );
    raft_log.set_memory_max_index_for_test(5);

    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 5 };

    coord.run_until_caught_up(&raft_log);

    // Stand in for raft.rs's InternalEvent::FsyncCompleted handler.
    while let Ok(InternalEvent::FsyncCompleted(mark)) = log_flush_rx.try_recv() {
        raft_log.try_advance_durable_index(mark);
    }

    assert_eq!(
        raft_log.durable_index.load(Ordering::Acquire),
        5,
        "a successful flush must advance durable_index to the round's max_index"
    );
}

/// A failed physical flush must NOT advance `durable_index` — the data was
/// never confirmed durable.
///
/// Expected:
///   - With a mock `flush()` returning `Err(..)`, `durable_index()` stays at
///     its pre-call value after `run_until_caught_up` returns.
#[test]
fn test_run_until_caught_up_does_not_advance_durable_index_on_flush_failure() {
    let storage = MockStorageEngine::not_durable_always_failing_flush(
        "run_until_caught_up_does_not_advance_durable_index_on_flush_failure".into(),
    );
    let coord = FsyncCoordinator::new();
    let raft_log = minimal_raft_log(storage);
    let pre = raft_log.durable_index.load(Ordering::Acquire);

    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 5 };

    coord.run_until_caught_up(&raft_log);

    assert_eq!(
        raft_log.durable_index.load(Ordering::Acquire),
        pre,
        "a failed flush must not advance durable_index"
    );
}

/// A failed physical flush sends `Err` (not a hang, not `Ok`) to every
/// queued reply.
///
/// Expected:
///   - Every oneshot receiver corresponding to the queued replies resolves
///     to `Err(..)`.
#[test]
fn test_run_until_caught_up_sends_err_to_replies_on_flush_failure() {
    let storage = MockStorageEngine::not_durable_always_failing_flush(
        "run_until_caught_up_sends_err_to_replies_on_flush_failure".into(),
    );
    let coord = FsyncCoordinator::new();
    let raft_log = minimal_raft_log(storage);

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 5 };
    coord.pending_replies.lock().push(tx);

    coord.run_until_caught_up(&raft_log);

    assert!(
        rx.try_recv()
            .expect("reply must have been answered, not silently dropped")
            .is_err(),
        "a failed flush must send Err to queued replies, not hang or Ok"
    );
}

/// The reset fence: if `generation` no longer matches what it was when this
/// round started, the physical flush's result must be discarded — not
/// applied to `durable_index`, not reported as `Ok` to callers.
///
/// Uses a mock `flush()` that bumps the coordinator's `generation` as a side
/// effect of being called — a deterministic, thread-free way to simulate
/// "a reset happened while this batch was physically flushing", instead of
/// racing real threads against a gate.
///
/// Expected:
///   - `durable_index()` does not advance to the round's `max_index`.
///   - The round's queued replies resolve to `Err(..)`, not `Ok(())`.
#[test]
fn test_run_until_caught_up_discards_stale_generation_result_without_advancing() {
    let coord = Arc::new(FsyncCoordinator::new());
    let coord_in_mock = Arc::clone(&coord);

    let mut mock_log_store = MockLogStore::new();
    let mock_meta_store = MockMetaStore::new();
    mock_log_store.expect_last_index().returning(|| 0);
    mock_log_store.expect_load_purge_boundary().returning(|| Ok(None));
    mock_log_store.expect_is_write_durable().returning(|| false);
    mock_log_store.expect_flush().returning(move || {
        // Simulates "a reset happened while this batch was physically
        // flushing" — deterministic, no real concurrency/gate needed.
        coord_in_mock.generation.fetch_add(1, Ordering::AcqRel);
        Ok(())
    });
    let storage = MockStorageEngine::from(mock_log_store, mock_meta_store);
    let raft_log = minimal_raft_log(storage);

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 5 };
    coord.pending_replies.lock().push(tx);

    coord.run_until_caught_up(&raft_log);

    assert_eq!(
        raft_log.durable_index.load(Ordering::Acquire),
        0,
        "a stale-generation result must not advance durable_index"
    );
    assert!(
        rx.try_recv()
            .expect("reply must have been answered, not silently dropped")
            .is_err(),
        "a stale-generation result must send Err, not a resurrected Ok(())"
    );
}

/// The other half of the fence: when `generation` at completion still
/// matches `generation` at the round's start (nothing fenced it while the
/// physical flush was running), the result must be accepted normally —
/// `durable_index` advances and queued replies resolve to `Ok`.
///
/// Bumps `generation` twice before the round starts (so this isn't just
/// "stays at the default 0"), proving it's the *match*, not the specific
/// value, that matters.
///
/// Expected:
///   - `durable_index()` advances to the round's `max_index`.
///   - The queued reply resolves to `Ok(())`.
#[test]
fn test_run_until_caught_up_accepts_result_when_generation_unchanged() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_accepts_result_when_generation_unchanged".into(),
    );
    let coord = FsyncCoordinator::new();
    // See test_run_until_caught_up_advances_durable_index_on_success for why
    // this doesn't use minimal_raft_log(): needs a real backing entry for
    // try_advance_durable_index's content check, and a registered
    // log_flush_tx to receive the FsyncCompleted report.
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
    raft_log.entries.write().insert(
        5,
        Entry {
            index: 5,
            term: 1,
            payload: None,
        },
    );
    raft_log.set_memory_max_index_for_test(5);

    // Two unrelated fences happened earlier — generation is 2, not 0 — before
    // this round is even recorded as in flight.
    coord.fence_reset();
    coord.fence_reset();
    assert_eq!(coord.generation.load(Ordering::Acquire), 2);

    let (tx, mut rx) = oneshot::channel::<Result<()>>();
    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 5 };
    coord.pending_replies.lock().push(tx);

    // Nothing fences this round while it runs — generation stays at 2.
    coord.run_until_caught_up(&raft_log);

    // Stand in for raft.rs's InternalEvent::FsyncCompleted handler.
    while let Ok(InternalEvent::FsyncCompleted(mark)) = log_flush_rx.try_recv() {
        raft_log.try_advance_durable_index(mark);
    }

    assert_eq!(
        raft_log.durable_index.load(Ordering::Acquire),
        5,
        "a matching generation must let the result advance durable_index normally"
    );
    assert!(
        rx.try_recv().expect("reply must have been answered").is_ok(),
        "a matching generation must resolve queued replies as Ok, not Err"
    );
}

/// Multiple `submit()` calls made while a round is in flight are coalesced
/// into a single subsequent physical `flush()` call by the same task — not
/// one physical flush per `submit()` call.
///
/// Expected:
///   - Two `submit()` calls queued behind one manually-simulated in-flight
///     round, followed by one `run_until_caught_up` pass, result in exactly
///     one additional physical `flush()` call serving both.
#[test]
fn test_run_until_caught_up_coalesces_queued_submits_into_one_flush() {
    let (storage, flush_call_count) = MockStorageEngine::not_durable(
        "run_until_caught_up_coalesces_queued_submits_into_one_flush".into(),
    );
    let coord = FsyncCoordinator::new();
    let raft_log = minimal_raft_log(storage);
    // advance_durable_and_notify() clamps against max_index — matches pending_max below.
    raft_log.set_memory_max_index_for_test(10);

    // Simulate two submit() calls that both lost the CAS while a round was
    // in flight — both just accumulated into the same pending state.
    let (tx1, mut rx1) = oneshot::channel::<Result<()>>();
    let (tx2, mut rx2) = oneshot::channel::<Result<()>>();
    coord.inflight.store(true, Ordering::Release);
    *coord.pending_max.lock() = LogId { term: 1, index: 10 };
    coord.pending_replies.lock().extend([tx1, tx2]);

    coord.run_until_caught_up(&raft_log);

    assert_eq!(
        flush_call_count.load(Ordering::Acquire),
        1,
        "two queued submissions must be served by exactly one physical flush() call"
    );
    assert!(
        rx1.try_recv().expect("tx1 must have been answered").is_ok(),
        "both queued replies must resolve to Ok"
    );
    assert!(
        rx2.try_recv().expect("tx2 must have been answered").is_ok(),
        "both queued replies must resolve to Ok"
    );
}

/// Two `submit()` calls race into the same pending round: a stale
/// pre-truncation persist carrying `(term 1, index 10)` (its captured entries
/// were all term 1, and index 10 has since been truncated away) and the valid
/// post-truncation `ReplaceRange` mark `(term 2, index 2)`.
///
/// Term-first ordering must keep `(term 2, index 2)` — a newer term wins over
/// an older term's higher index. Before this fix `pending_max` was a bare
/// `fetch_max` on the index: `10` swallowed `2`, the round reported the stale
/// `10`, content-validation rejected it, and `durable_index` never reached 2.
///
/// Deterministic, IO-thread-free repro of
/// `test_stale_persist_after_truncation_does_not_advance_durable_index`.
#[test]
fn test_submit_term_first_keeps_valid_mark_over_stale_higher_index() {
    let (storage, _flush_call_count) = MockStorageEngine::not_durable(
        "submit_term_first_keeps_valid_mark_over_stale_higher_index".into(),
    );
    let coord = Arc::new(FsyncCoordinator::new());
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

    // Post-truncation log: entry 1 (term 1) survived; entry 2 is the new
    // leader's replacement (term 2). Index 10 is gone.
    {
        let e = raft_log.entries.write();
        e.insert(
            1,
            Entry {
                index: 1,
                term: 1,
                payload: None,
            },
        );
        e.insert(
            2,
            Entry {
                index: 2,
                term: 2,
                payload: None,
            },
        );
    }
    raft_log.set_memory_max_index_for_test(2);

    // Pretend a round is in flight so submit() only records state.
    coord.inflight.store(true, Ordering::Release);
    coord.submit(&raft_log, LogId { term: 1, index: 10 }, vec![]); // stale
    coord.submit(&raft_log, LogId { term: 2, index: 2 }, vec![]); // valid

    assert_eq!(
        *coord.pending_max.lock(),
        LogId { term: 2, index: 2 },
        "term-first: the newer-term mark must win over the stale higher index"
    );

    coord.run_until_caught_up(&raft_log);

    while let Ok(InternalEvent::FsyncCompleted(mark)) = log_flush_rx.try_recv() {
        raft_log.try_advance_durable_index(mark);
    }

    assert_eq!(
        raft_log.durable_index.load(Ordering::Acquire),
        2,
        "durable_index must reach the valid post-truncation tail (2), not stall \
         because a stale batch top swallowed it"
    );
}
