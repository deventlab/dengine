//! `IOTask::ReplaceRange` (term-conflict truncation, see
//! `filter_out_conflicts_and_append`'s slow path) writes to the storage engine
//! synchronously and bumps `pending_max`, but is dispatched through the
//! `receiver.recv()` => `cmd => { run_storage_tasks(...) }` arm of the IO
//! thread's select loop — a branch that, unlike `run_batch_turn`, never calls
//! `fsync_coordinator.submit()`. If no further `append_entries()` call arrives
//! afterward (which would separately trigger a `run_batch_turn` via
//! `write_notify`), the replaced entries sit "written but never fsync-submitted"
//! indefinitely — nothing but the idle-timer safety net would ever flush them.
//!
//! This test pins down that gap: it disables the safety net (a very long
//! `idle_flush_interval_ms`) so only the normal notify-driven path could
//! possibly advance `durable_index`, then proves it never does after a
//! term-conflict truncation with no subsequent append.

use std::time::Duration;

use d_engine_proto::common::Entry;

use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;

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

/// A term-conflict truncation (`ReplaceRange`) must eventually become durable
/// even if no `append_entries()` call follows it.
///
/// Today it does not: `ReplaceRange` is handled outside `run_batch_turn`, so
/// nothing submits fsync for it. Only the idle-timer safety net would catch
/// this — and this test disables that timer (60s interval, well beyond the
/// test's wait window) to isolate the notify-driven path from the safety net.
///
/// RED (today): `durable_index()` never reaches `last_entry_id()` after the
/// truncation, because the replaced entries' fsync was never submitted.
#[tokio::test]
async fn test_replace_range_becomes_durable_without_a_following_append() {
    let mut ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 60_000, // effectively disabled for this test's timeframe
        },
        "replace_range_becomes_durable_without_a_following_append",
    );

    // Arrange: log [1,2,3] all term=1, explicitly flushed durable.
    ctx.append_entries(1, 3, 1).await;
    ctx.raft_log.flush().await.unwrap();
    ctx.drain_fsync_completions();
    assert_eq!(ctx.raft_log.durable_index(), 3, "baseline must be durable");

    // Act: leader (term=2) sends entries that conflict at index=2 and extend
    // the log to index=4. filter_out_conflicts_and_append's slow path detects
    // the term mismatch at index=2, truncates [2,3], and replaces with
    // [2,3,4] (term=2) via IOTask::ReplaceRange — with no append_entries()
    // call afterward.
    let result = ctx
        .raft_log
        .filter_out_conflicts_and_append(1, 1, vec![entry(2, 2), entry(3, 2), entry(4, 2)])
        .await
        .unwrap();
    assert_eq!(result.unwrap().index, 4);
    assert_eq!(
        ctx.raft_log.last_entry_id(),
        4,
        "memory must reflect the replace"
    );

    // Give the IO thread ample time to have submitted fsync, if anything
    // besides the (disabled) safety net were going to do it.
    tokio::time::sleep(Duration::from_millis(200)).await;
    ctx.drain_fsync_completions();

    // FIXED: ReplaceRange's handler now submits fsync directly instead of
    // relying on a following append/notify or the safety net.
    assert_eq!(
        ctx.raft_log.durable_index(),
        4,
        "ReplaceRange must submit fsync itself, without needing a following append"
    );
}
