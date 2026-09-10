use std::time::Duration;

use crate::FlushPolicy;
use crate::storage::raft_log::RaftLog;
use crate::test_utils::BufferedRaftLogTestContext;

/// Verifies that the flush worker continues operating normally after processing a large number
/// of flush tasks — the worker does not exit or become unresponsive under sustained load.
#[tokio::test]
async fn test_flush_worker_sustains_throughput_under_load() {
    let ctx = BufferedRaftLogTestContext::new(
        FlushPolicy::Batch {
            idle_flush_interval_ms: 50,
        },
        "test_flush_worker_sustains_throughput",
    );

    for i in 1..=100 {
        ctx.append_entries(i, 1, 1).await;
    }

    tokio::time::sleep(Duration::from_millis(200)).await;

    // Verify the worker is still alive by confirming continued processing
    ctx.append_entries(101, 50, 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    assert_eq!(ctx.raft_log.last_entry_id(), 150);
}
