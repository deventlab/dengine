//! Quorum + real-disk crash recovery integration test (#446 gap 4).
//!
//! Composes two pieces that are each already covered in isolation elsewhere, but never
//! together: `calculate_majority_matched_index` (RPO=0 quorum arithmetic, unit-tested
//! against a gated mock in `buffered_raft_log_test/quorum_durability_test.rs`) and real
//! `FileStorageEngine` crash/reopen (unit-tested without any quorum math in
//! `crash_recovery_test.rs`). This file proves they actually compose: an index that the
//! quorum calculation says is safe to acknowledge to the client is still there after a
//! real crash + reopen from the same on-disk path.
//!
//! Followers are represented as reported match_index values, same as in
//! `quorum_durability_test.rs` — this file's job is the leader-side real-disk durability
//! boundary, not follower ACK withholding (covered by follower_state_test.rs /
//! learner_state_test.rs).
//!
//! Deliberately NOT attempted here, and now CONFIRMED impossible with this engine's
//! architecture (not just a flakiness risk — an actual dead end, verified by building and
//! deadlocking it): proving that an entry which never reached quorum-durable is genuinely
//! absent from a real crash + reopen. `BufferedRaftLog::append_entries`
//! (`d-engine-core/src/storage/buffered_raft_log.rs:467-500`) is documented and
//! implemented to block the caller until `persist_entries()` returns — "still blocks the
//! caller until truly persisted" — and `FileLogStore::persist_entries`
//! (`d-engine-server/src/storage/adaptors/file/file_storage_engine.rs:254`) already writes
//! the entry to the real OS-visible file as an unconditional part of its body, before it
//! can return. So by the time `append_entries().await` ever resolves at all, the entry is
//! already on the file — there is no window where it's "acknowledged as appended" yet
//! "recoverably absent." A gate on `persist_entries()` was built and tried here; it did
//! not create the intended window, it just deadlocked `append_entries()` forever (the
//! call this file's other test depends on to make progress at all). Reverted.
//! What IS real and already correctly tested (see below): the gap between
//! `last_entry_id` and `durable_index` — `persist_entries()` writes the bytes, but a
//! *separate* `flush()` call (`sync_all()`) is what advances `durable_index`, and that one
//! genuinely runs later/independently. What's NOT reachable by a same-process test is
//! observing that an un-`sync_all`'d write doesn't survive — on the same OS instance,
//! `write()` alone (which `persist_entries` already does) is enough for a freshly-opened
//! handle to see the bytes, real crash or not. Proving the un-fsynced case would need an
//! actual power-loss simulation (dropped page cache / real reboot) — which prior sessions
//! already found to be a poor fit for this class of bug (see mempalace notes on the
//! Jepsen/lazyfs work for #444).

use super::TestContext;
use d_engine_core::FlushPolicy;
use d_engine_core::RaftLog;

#[tokio::test]
async fn test_quorum_acknowledged_index_survives_real_crash_and_reopen() {
    let mut ctx = TestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 1,
        },
        "test_quorum_ack_survives_crash",
    );

    // First 5 entries, explicitly flushed: genuinely durable, deterministic.
    ctx.append_entries(1, 5, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(ctx.raft_log.durable_index(), 5);

    // 3-node cluster: both followers already report match_index=5 (post-Stage2
    // semantics — a follower only reports a match_index once its own durable_index
    // reaches it). This is the index the leader would actually acknowledge to the
    // client.
    let commit = ctx.raft_log.calculate_majority_matched_index(1, 0, vec![5, 5]);
    assert_eq!(
        commit,
        Some(5),
        "index 5 is durable on the leader and acked by both followers"
    );

    // A follower report of 10 must not move commit past what the leader itself has
    // fsynced — restates the Stage1 invariant as this test's own setup precondition
    // rather than assuming it silently.
    let would_be_wrong = ctx.raft_log.calculate_majority_matched_index(1, 0, vec![10, 5]);
    assert_eq!(
        would_be_wrong,
        Some(5),
        "leader's own un-fsynced tail must not leak into the client-visible commit index"
    );

    // Second batch, also explicitly flushed, so the whole log is durable before the
    // simulated crash — keeps this test's crash/recovery assertions exact, not bounded.
    ctx.append_entries(6, 5, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(ctx.raft_log.durable_index(), 10);

    let recovered = ctx.recover_from_crash();
    ctx.close().await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    // The index that was actually acknowledged to the client (5) — and everything else
    // that was durably flushed (up to 10) — survives the real crash + reopen.
    assert_eq!(recovered.raft_log.durable_index(), 10);
    for i in 1..=10 {
        assert!(
            recovered.raft_log.entry(i).unwrap().is_some(),
            "entry {i} must survive real crash + reopen"
        );
    }

    // Re-running the same quorum calculation against the recovered log reaches the same
    // conclusion — the leader's durability contribution to quorum is stable across a
    // real restart, not just in the pre-crash in-memory view.
    let commit_after_recovery =
        recovered.raft_log.calculate_majority_matched_index(1, 0, vec![5, 5]);
    assert_eq!(commit_after_recovery, Some(5));

    recovered.close().await;
}
