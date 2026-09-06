//! Snapshot policy based on Raft log size.
//! Triggers a snapshot when the number of log entries exceeds a configured threshold.

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tracing::error;
use tracing::trace;
use tracing::warn;

use super::SnapshotContext;
use super::SnapshotPolicy;

#[derive(Debug)]
pub struct LogSizePolicy {
    threshold: AtomicU64,    // e.g. 5000 log entries
    is_checking: AtomicBool, // CAS lock for concurrent checks
}

impl SnapshotPolicy for LogSizePolicy {
    #[inline]
    fn should_trigger(
        &self,
        ctx: &SnapshotContext,
    ) -> bool {
        if ctx.current_term < ctx.last_included.term {
            return false;
        }

        // CAS lock to prevent concurrent checks
        if self
            .is_checking
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Relaxed)
            .is_err()
        {
            return false;
        }

        let lag = self.calculate_lag(ctx);
        let threshold = self.threshold.load(Ordering::Relaxed);

        metrics::gauge!("core.raft.snapshot.log_lag").set(lag as f64);

        // The in-memory Raft log grows until a snapshot purges it. If snapshot
        // creation can't keep up with the write rate this climbs unbounded and
        // eventually OOMs the node — make it loud well before that.
        if threshold > 0 {
            if lag >= threshold.saturating_mul(50) {
                error!(
                    lag,
                    threshold,
                    "Raft log lag is 50x the snapshot threshold — snapshot creation is \
                    NOT keeping up with writes; the in-memory log is growing unbounded \
                    and will OOM this node. Check snapshot/apply throughput."
                );
            } else if lag >= threshold.saturating_mul(10) {
                warn!(
                    lag,
                    threshold,
                    "Raft log lag exceeds 10x the snapshot threshold — snapshots may \
                            not be keeping up"
                );
            }
        }

        let should_trigger = lag >= threshold;
        self.is_checking.store(false, Ordering::Release);

        should_trigger
    }

    #[allow(unused)]
    /// For sized based policy, no need to use this function.
    fn mark_snapshot_created(&mut self) {}
}

impl LogSizePolicy {
    pub fn new(threshold: u64) -> Self {
        LogSizePolicy {
            threshold: AtomicU64::new(threshold),
            is_checking: AtomicBool::new(false),
        }
    }

    #[inline]
    pub(crate) fn calculate_lag(
        &self,
        ctx: &SnapshotContext,
    ) -> u64 {
        let lag = ctx.last_applied.index.saturating_sub(ctx.last_included.index);
        trace!("calculate_lag: {}", lag);
        lag
    }

    #[allow(unused)]
    pub(crate) fn update_threshold(
        &self,
        new_val: u64,
    ) {
        self.threshold.store(new_val, Ordering::Relaxed);
    }
}
