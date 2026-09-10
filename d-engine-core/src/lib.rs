//! # d-engine-core
//!
//! Pure Raft consensus algorithm - for building custom Raft-based systems
//!
//! ## ⚠️ Internal Crate - Not Ready for Standalone Use
//!
//! **Use [`d-engine`](https://crates.io/crates/d-engine) instead.**
//!
//! This crate contains the pure Raft consensus algorithm used internally by d-engine.
//! The API is unstable before v1.0.
//!
//! ```toml
//! # ❌ Don't use this directly
//! [dependencies]
//! d-engine-core = "0.2"
//!
//! # ✅ Use this instead
//! [dependencies]
//! d-engine = "0.2"
//! ```
//!
//! ## For Contributors
//!
//! ## What this crate provides
//!
//! This crate focuses solely on the Raft consensus algorithm:
//!
//! - **Leader Election** - Automatic leader election with randomized timeouts
//! - **Log Replication** - Reliable log replication to followers
//! - **Membership Changes** - Dynamic cluster membership changes
//! - **Snapshot Support** - Log compaction via snapshots
//!
//! Storage, networking, and state machine implementation are **your responsibility**.
//!
//! **Reference integration**: See how [d-engine-server](https://github.com/deventlab/d-engine/tree/main/d-engine-server) uses this crate.
//!
//! ## Future Vision
//!
//! **Post-1.0 goal**: Become a standalone Raft library with stable API.
//!
//! **Current status**: Internal to d-engine, API may change between minor versions.
//!
//! ## Key Traits
//!
//! - [`StorageEngine`] - Persistent storage for Raft logs
//! - [`StateMachine`] - Application-specific state transitions
//! - [`LogStore`] - Log entry persistence
//! - [`MetaStore`] - Metadata persistence (term, voted_for)
//!
//! ## Documentation
//!
//! For comprehensive guides:
//! - [Customize Storage Engine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_storage_engine/index.html)
//! - [Customize State Machine](https://docs.rs/d-engine/latest/d_engine/docs/server_guide/customize_state_machine/index.html)

pub mod client;
pub mod command;
mod commit_handler;
pub mod config;
mod election;
mod errors;
mod event;
mod maybe_clone_oneshot;
mod membership;
mod network;
mod purge;
mod raft;
mod raft_context;
mod raft_role;
mod replication;
mod state_machine_handler;
mod timer;
mod type_config;
mod utils;

pub mod storage;

#[cfg(feature = "watch")]
pub mod watch;

// ── User-facing public API ──────────────────────────────────────────────────
pub use client::*;
pub use command::*;
pub use config::*;
pub use errors::*;
pub use storage::*;
#[cfg(feature = "watch")]
pub use watch::*;

// Stable extension points — types developers need when implementing custom
// storage engines, state machines, or transport layers.
pub use membership::Membership;
pub use network::Transport;
pub use purge::PurgeExecutor;
pub use raft::{LeaderInfo, Raft, SignalParams};
pub use state_machine_handler::{SnapshotPolicy, StateMachineHandler};
pub use type_config::TypeConfig;

// ── Internal implementation details (not part of public API) ───────────────
// These remain accessible to d-engine-server but are hidden from cargo doc.
// Do not depend on these from external crates.
#[doc(hidden)]
pub use commit_handler::*;
#[doc(hidden)]
pub use election::*;
#[doc(hidden)]
pub use event::*;
#[doc(hidden)]
pub use maybe_clone_oneshot::*;
#[doc(hidden)]
pub use membership::*;
#[doc(hidden)]
pub use network::*;
#[doc(hidden)]
pub use purge::*;
#[doc(hidden)]
pub use raft_context::*;
#[doc(hidden)]
pub use raft_role::*;
#[doc(hidden)]
pub use replication::*;
#[doc(hidden)]
pub use state_machine_handler::*;
#[doc(hidden)]
pub use type_config::*;
#[doc(hidden)]
pub use utils::*;

pub(crate) use timer::*;

#[cfg(test)]
mod maybe_clone_oneshot_test;

#[cfg(test)]
mod errors_test;
#[cfg(test)]
mod raft_oneshot_test;

/// Internal test utilities - not part of public API
#[cfg(any(test, feature = "__test_support"))]
#[doc(hidden)]
pub mod test_utils;

#[cfg(any(test, feature = "__test_support"))]
pub use test_utils::*;

/// In raft, during any Leader to Peer communication,
///     if received response term is bigger than Leader's,
///     the current Leader need downgrade to follower
///     and update its term to higher one.
///
/// e.g. Append Entries RPC
/// e.g. Election: receive VoteResponse
/// e.g. Sync cluster membership configure
/// @return: true - found higher term;
pub(crate) fn if_higher_term_found(
    my_current_term: u64,
    term: u64,
    is_learner: bool,
) -> bool {
    //means I am fake leader or we should ignore learner's response
    if !is_learner && my_current_term < term {
        tracing::warn!("my_current_term: {} < term: {} ?", my_current_term, term);
        return true;
    }

    false
}

/// Raft paper: 5.4.1 Election restriction
///
/// Raft determines which of two logs is more up-to-date by comparing the index and term of the last
/// entries in the  logs. If the logs have last entries with different terms, then the log with the
/// later term is more up-to-date. If the logs end with the same term, then whichever log is longer
/// is more up-to-date.
///
/// #446: callers must pass the in-memory last-log-id (last_entry_id), never durable_index.
/// A node with an un-fsynced tail must still be able to reject a candidate whose log is
/// genuinely less up to date — voting eligibility and commit-durability are separate
/// concerns and must not share the same index source.
pub(crate) fn is_target_log_more_recent(
    my_last_log_index: u64,
    my_last_log_term: u64,
    target_last_log_index: u64,
    target_last_log_term: u64,
) -> bool {
    (target_last_log_term > my_last_log_term)
        || (target_last_log_term == my_last_log_term && target_last_log_index >= my_last_log_index)
}
