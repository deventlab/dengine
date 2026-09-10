//! Watch mechanism for monitoring key changes
//!
//! This module provides a high-performance, lock-free watch system that allows
//! clients to monitor specific keys for changes with minimal overhead on the write path.
//!
//! # Architecture Overview
//!
//! The watch system is composed of two main components created explicitly in NodeBuilder:
//!
//! 1. **WatchRegistry** (Shared State): Lock-free registration via DashMap
//! 2. **WatchDispatcher** (Background Task): Spawned explicitly in Builder, distributes events
//!
//! ```text
//! ┌─────────────┐
//! │ StateMachine│
//! │  apply()    │
//! └──────┬──────┘
//!        │ notify_change() [~10ns]
//!        ▼
//! ┌─────────────────┐
//! │  Event Queue    │ (tokio::sync::broadcast)
//! │  (bounded 10240)│
//! └──────┬──────────┘
//!        │
//!        ▼
//! ┌─────────────────┐
//! │   Dispatcher    │ (tokio task)
//! │   Task          │
//! └──────┬──────────┘
//!        │ lookup in DashMap (lock-free)
//!        ▼
//! ┌─────────────────┐
//! │ Per-Watcher     │ (tokio mpsc, bounded 10)
//! │ Channels        │
//! └──────┬──────────┘
//!        │
//!        ▼
//! ┌─────────────────┐
//! │ gRPC Stream     │
//! │ to Client       │
//! └─────────────────┘
//! ```
//!
//! # Performance Characteristics
//!
//! - **Write overhead**: < 0.01% with 100+ watchers (target: <10ns per notify)
//! - **Event latency**: Typically < 100μs end-to-end
//! - **Memory per watcher**: ~2.4KB with default buffer size (10)
//! - **Scalability**: Tested with 1000+ concurrent watchers
//!
//! # Usage Example
//!
//! ```ignore
//! use d_engine_core::watch::{WatchRegistry, WatchDispatcher};
//! use bytes::Bytes;
//!
//! # tokio::runtime::Runtime::new().unwrap().block_on(async {
//! // Create components (typically done in NodeBuilder)
//! let (unregister_tx, unregister_rx) = mpsc::unbounded_channel();
//! let registry = Arc::new(WatchRegistry::new(10, unregister_tx));
//! let dispatcher = WatchDispatcher::new(registry.clone(), broadcast_rx, unregister_rx);
//!
//! // Explicitly spawn dispatcher (visible resource allocation)
//! tokio::spawn(async move {
//!     dispatcher.run().await;
//! });
//!
//! // Register a watcher
//! let key = Bytes::from("mykey");
//! let handle = registry.register(key);
//! # });
//! ```
//!
//! # Configuration
//!
//! The watch system is configurable via [`WatchConfig`]:
//!
//! - `event_queue_size`: Global event queue buffer (default: 10240)
//! - `watcher_buffer_size`: Per-watcher channel buffer (default: 10)
//! - `enable_metrics`: Enable detailed logging and metrics (default: false)
//!
//! ```
//! use d_engine_core::watch::WatchConfig;
//!
//! let config = WatchConfig {
//!     event_queue_size: 2000,
//!     watcher_buffer_size: 256,
//!     enable_metrics: true,
//!     max_watcher_count: 5000,
//!     heartbeat_interval_ms: 30_000,
//! };
//! ```
//!
//! # Error Handling
//!
//! The watch system prioritizes write performance over guaranteed event delivery:
//!
//! - When the global event queue is full, new events are **dropped**
//! - When a per-watcher channel is full, events for that watcher are **dropped**
//! - Clients should use the Read API to re-sync if they detect missing events
//!
//! This design ensures that a slow or stuck watcher never blocks the write path.
//!
//! # Thread Safety
//!
//! All types in this module are thread-safe and can be safely shared across threads:
//!
//! - [`WatchRegistry`] is shared via `Arc` between Node and Dispatcher
//! - All operations are lock-free (DashMap + AtomicU64)
//! - The dispatcher runs as an independent tokio task
//!
//! # Design Principles
//!
//! - **No hidden resource allocation**: All tokio::spawn calls are explicit in Builder
//! - **Minimal abstraction**: Only essential data structures, no unnecessary wrappers
//! - **Composable**: Registry and Dispatcher are independent, composed in Builder
//! - Uses `DashMap` for the watcher registry (lock-free concurrent HashMap)
//! - Uses `tokio::sync::mpsc` for per-watcher channels (async-friendly)
//! - Watchers are automatically cleaned up when dropped (RAII pattern)

mod manager;

#[cfg(test)]
mod manager_test;

pub use manager::WatchDispatcher;
pub use manager::WatchError;
pub use manager::WatchEvent;
pub use manager::WatchEventType;
pub use manager::WatchRegistry;
pub use manager::WatcherHandle;

// Re-export WatchConfig from the unified config system
pub use crate::config::WatchConfig;

/// Build a CANCELED sentinel event for a given key.
///
/// Sent to a watcher's channel immediately before it is unregistered due to
/// buffer overflow.  The client receives this as the last event on the stream
/// and should treat it as a signal to re-sync via the Read API and re-register.
pub(crate) fn make_cancel_event(key: bytes::Bytes) -> WatchEvent {
    WatchEvent {
        key,
        value: bytes::Bytes::new(),
        prev_value: None,
        event_type: WatchEventType::Canceled,
        revision: 0,
    }
}
