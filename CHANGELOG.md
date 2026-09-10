# Changelog

All notable changes to this project will be documented in this file.

---

## [v0.2.5] - 2026-XX-XX

### Added

- **AppendEntries coalescing + speculative replication** (#407): Consecutive same-term `AppendEntries` in the receive buffer are merged before dispatch (heartbeats absorbed via `max(leader_commit_index)`); leader advances `next_index` speculatively on send, eliminating stop-and-wait per replication round.

- **ReadActor fast path for Eventual/LeaseRead** (#392): Dedicated `ReadActor` serves `EventualConsistency` and `LeaseRead` off the Raft loop, eliminating channel contention under high read concurrency. Lease Read +10.9%, Eventual Read +13.1% vs v0.2.4 (100 concurrent clients, local embedded).

- **`Command::Batch` — atomic multi-key writes** (#415): New `client.batch(ops)` API commits multiple Insert/Delete operations as a single Raft entry. All ops in a batch succeed or fail together. Library wrappers (e.g. service registration, distributed locks) no longer need workarounds like reserved-key encoding.

- **`start_node()` public API** (#415): Both `EmbeddedEngine` and `StandaloneEngine` now expose `start_node(config, storage, sm)` — start an engine with a programmatic `RaftNodeConfig`, no config file required. For library wrappers that build their own config layer in Rust.

- **`start_with()` accepts `impl AsRef<Path>`** (#415): Callers can now pass `&str`, `String`, `&Path`, or `PathBuf` — backward compatible, no migration needed.

### Fixed

- **fix(ttl) #398**: Removed `lease.enabled` flag — TTL expiration is always active. Fixes fatal crash when calling `put_with_ttl` without setting the (now-removed) `lease.enabled = true`.

- **🛑 Snapshot data corruption — label/data mismatch** (#418): `last_included` in snapshot metadata was computed by subtracting `retained_log_entries` from `last_applied`, but the snapshot data already reflected the full `last_applied` state. Followers installing such snapshots would re-apply entries 91–100 after restoring state through index 100, causing double-application and permanent cluster state divergence. Also fixes a fabricated `LogId` where `last_included.term` was copied from `last_applied.term` instead of queried from the log. Snapshot label is now always truthful: `last_included == last_applied`.

- **`ClusterConfig::default()` now consistent with serde default** (#415): `Default::default()` previously produced an empty `initial_cluster`, but `#[serde(default)]` returned `[{id:1, ...}]`. `RaftNodeConfig::new()` uses the Rust `Default` impl, so programmatic configs always got an empty cluster. Fixed.

- **WAL fsync serialization regression (#422)**: #407 enabled per-batch `flush_wal(true)` (Level 3 durability),
  but the IO thread blocked inline on fsync — every entry paid the full fsync penalty individually under
  moderate load. Introduced `FsyncCoordinator`: fsync is dispatched to `spawn_blocking`, the IO thread
  returns immediately, and entries arriving during an in-flight fsync are coalesced into the same physical
  disk flush. Storage-level group commit is restored without artificial batching windows.

- **🛑 Client-acknowledged writes could be lost on correlated power loss (#446)**: Raft commit quorum
  counted the leader's own log contribution using its in-memory tail (`last_entry_id()`), not its
  fsync-confirmed position (`durable_index()`) — a write could reach a majority-looking commit index,
  and be acknowledged to the client, before enough replicas had actually synced it to disk. If those
  nodes then lost power before their next fsync, the acknowledged write was gone. Fixed: leader quorum
  calculation, follower `AppendEntries` ACK timing (a follower now withholds its response until its own
  `durable_index` reaches the acknowledged entry), and single-voter clusters (previously exempted from
  this class of fix, see #329) all gate on `durable_index`. RPO=0 for acknowledged writes is now a
  mandatory invariant. Net effect: write acknowledgment latency now includes fsync time on a quorum of
  replicas — see [Throughput Optimization Guide](./d-engine/src/docs/performance/throughput-optimization-guide.md)
  for tuning `idle_flush_interval_ms`.

### Changed

- **MSRV raised to Rust 1.89**: The `data_dir` startup lock (prevents two node processes from
  sharing the same directory) uses `std::fs::File::try_lock()`, stable since 1.89.0. CI and
  `rust-toolchain.toml` were already pinned to 1.89.0 — only the `rust-version` field in
  `Cargo.toml` needed to catch up. If you're building on 1.88, upgrade your toolchain.

- `[raft.read_actor]` replaces the previous flat `read_actor_channel_capacity` / `read_actor_max_drain` fields in `[raft]`. Update existing config files accordingly.

- **⚠️ `StateMachine::entry_term()` removed from trait** (#418): Term lookup belongs to the log layer, not the state machine. Custom `StateMachine` implementations must delete this method — it is no longer part of the trait. See [Migration Guide](./MIGRATION_GUIDE.md) for details.

- **⚠️ `StateMachine::apply_snapshot_from_file` return type changed** (#436): Was `Result<()>`, now `Result<SnapshotApplyResult>`. The three outcomes (`Applied`/`IgnoredStale`/`IgnoredDuplicate`) are now explicit instead of every call looking identical on success. Custom `StateMachine` implementations must update the signature and return the matching variant. See [Migration Guide](./MIGRATION_GUIDE.md) for details.

- **⚠️ Learner initial-snapshot PULL path removed** (#436): `Transport::request_snapshot_from_leader` and `StateMachineHandler::apply_snapshot_stream_from_leader` are removed. New learners catch up via the leader's PUSH replication loop (AppendEntries / InstallSnapshot) instead of eagerly pulling a snapshot on startup. See [Migration Guide](./MIGRATION_GUIDE.md) for details.

- **KV encoding moved out of `d-engine-core`** (#415): `ClientWriteRequest.command` is now `Option<Bytes>` (pre-serialized). Serialization happens in the server transport layer (embedded/standalone/gRPC handler). Core is encoding-agnostic.

- **Metrics renamed with `core.` namespace prefix**: all `d-engine-core` metrics now use the `core.*`
  prefix (e.g. `core.raft.fsync.duration_ms`, `core.raft.write.propose_to_apply_ms`,
  `core.state_machine.apply_chunk.duration_ms`). The `[raft.metrics]` config section (`MetricsConfig`)
  has been removed — metrics are always-on, disabled only by not installing a recorder.

- **New `shutdown_timeout_ms` in `[raft.persistence]`** (default `5000`): bounds `close()` wait
  against a stuck fsync task. The task itself continues running in the background.

- **`data_dir` removed from `ClusterConfig`** — it is now a required explicit argument to all engine constructors (`EmbeddedEngine::start(data_dir)`, `StandaloneEngine::run(data_dir, shutdown_rx)`, etc.). Remove `[cluster] data_dir` / `[cluster] db_root_dir` from all config files; the constructor argument always wins and the config field is silently ignored.

- **`cluster.log_dir` removed** — was validated but never consumed.

- **`snapshots_dir` is no longer configurable** — always `data_dir/snapshots`. Fixes cross-node snapshot corruption when running multiple nodes on one machine (was a shared `/tmp/snapshots`).

- **`NodeBuilder` is no longer public** — use `EmbeddedEngine::start_custom`/`StandaloneEngine::run_custom` to plug in a custom storage engine or state machine. See [Migration Guide](./MIGRATION_GUIDE.md) for details.

- **⚠️ `[raft] ordered_channel_capacity` renamed to `max_pending_append_responses`** (#446): Follows the
  gRPC `AppendEntries` forwarder rewrite (`FuturesUnordered`-based, no longer strict-FIFO) that shipped
  alongside the durability fix above. Old field name is silently ignored, not an error — update existing
  configs to the new name to keep the setting in effect.

- **⚠️ `[raft.persistence] strategy` removed** (#446): `PersistenceStrategy` was a single-variant enum
  (`MemFirst`) left over from #268; its only meaning now lives in whether an entry has reached
  `durable_index`, which is no longer a configurable choice. Existing configs setting `strategy =
  "MemFirst"` or `"DiskFirst"` are silently ignored, not an error — remove the field, `flush_policy`
  is the only persistence knob now.

---

## [v0.2.4] - 2026-05-23

### Added

- **Linearizable read lease fast path** (#390): When the leader holds a valid lease, `LinearizableRead` is served locally without a consensus round-trip. Raft lease clock fixed from `SystemTime` (wall-clock drift) to `Instant` (monotonic).

- **Watch: `prev_kv` + progress heartbeat** (#379): `WatchEvent.prev_kv` carries the pre-change value. Idle streams receive periodic `WatchEventType::Progress` events to confirm liveness without a key change.

- **`scan_prefix` API** (#378): `ClientApi::scan_prefix(prefix)` returns all KV pairs matching a prefix in a single read — intended for zero-race-window state re-sync after watch reconnection.

- **Async IO architecture — pipeline replication** (#334, #341, #342, #343, #345, #349, #350, #351):
  The Inbound event loop is now fully non-blocking under write load
  - `BufferedRaftLog` runs on a dedicated OS thread — WAL writes never steal tokio worker threads
  - `StateMachineWorker::apply_batch` is fully async — RocksDB apply no longer blocks the event loop
  - AppendEntries uses a **persistent bidirectional gRPC stream per peer** — eliminates per-batch connection overhead
  - Replication is pipelined: leader sends to all followers concurrently without stop-and-wait
  - Truncate and purge are offloaded from the Raft hot path

- **Cluster membership streaming** (#327, #328): Subscribe to real-time membership changes
  - `EmbeddedEngine::watch_membership()` — in-process `watch::Receiver<MembershipSnapshot>` (embedded mode)
  - `GrpcClient::watch_membership()` — gRPC server-side stream of `MembershipSnapshot` (standalone mode)
  - Stream delivers the current snapshot immediately on connect (no need to wait for the next change)
  - Each committed ConfChange (AddNode, Promote, Remove) pushes a new snapshot to all subscribers
  - Stream terminates with `UNAVAILABLE` on server shutdown — callers reconnect and resubscribe
  - `MembershipSnapshot` carries `members`, `learners`, and `committed_index` (idempotency key for deduplication after reconnect)

- **Watch: prefix watch and watcher limits** (#300):
  - `watch_prefix(prefix)` API for both embedded and gRPC clients — one watcher covers an entire key namespace (e.g. `/services/payment/`) without registering a per-key watcher
  - Prefix semantics: prefix must start and end with `/`; matching uses slash-boundary decomposition so `/services/payment/node1` matches `/services/payment/` but not `/services/payment`
  - `WatchConfig.max_watcher_count` — hard cap on total active watchers (exact + prefix); `register()` / `watch_prefix()` return `LimitExceeded` error once reached (default: effectively unlimited)
  - `WatchConfig.watcher_buffer_size` default raised from 10 → 256, reducing spurious `CANCELED` events under burst load
  - Every `WatchEvent` carries `revision: u64` (Raft applied index) — use it to detect missed events after reconnection
  - Architecture: two separate `DashMap`s (exact and prefix) — O(1) exact lookup, O(depth) prefix dispatch; no linear scan
  - Proto: added `prefix: bool` to `WatchRequest`; added `revision: uint64` to `WatchResponse`

- **Unified RocksDB option** (#295): opt-in via `storage.unified_db = true`.
  Uses a single RocksDB instance with 4 column families instead of two separate instances.
  - Reduces memory RSS and file descriptor usage
  - **Experimental**: both paths are supported in v0.2.4; a future release will standardize on one
  - When enabled: data path changes to `db_root_dir/db/` (⚠️ existing `storage/` data not migrated automatically)

- **Simplified startup API** (#303): Pass a data directory directly — no config file required for common cases
  - `EmbeddedEngine::start(data_dir)` replaces the config-file-only constructor
  - `StandaloneEngine::run(data_dir, shutdown_rx)` for standalone deployments
  - Config file path still accepted via existing constructors; new API uses sensible defaults

### 🟡 Important Fixes

- **fix(crash-safety) #329**: Leader now commits against durable index, not in-memory index
  - Previously, entries could be marked committed before being flushed to disk, risking data loss on leader crash
  - All acknowledged writes are now guaranteed durable before commit advances

- **fix(raft) #340**: Candidate correctly steps down on same-term AppendEntries with log conflict
  - Eliminates election churn when recovering or newly-joined nodes have lagged logs
  - Aligns with Raft §5.2 (term check takes priority over log matching)

- **fix(snapshot) #315**: Snapshot disk I/O isolated from Inbound event loop via `spawn_blocking`
  - Previously, snapshot transfer competed with consensus and caused `ProposeFailed` (4006) under load

- **fix(snapshot) #308**: Snapshot install success is now driven by the follower's apply confirmation, not the transfer ACK
  - Prevents leader from advancing `match_index` before the follower has actually applied the snapshot

- **fix(snapshot) #253**: Snapshot stale-file corruption on retry eliminated; configurable chunk transfer timeout added

- **fix(snapshot) #290**: Snapshot cooldown reduced from 1 hour to a practical default; adaptive throttling added
  - WAL log compaction now triggers reliably in long-running deployments

- **fix(watch) #294**: Watch buffer overflow now sends a `CANCELED` sentinel instead of silently dropping events
  - Per-watcher channel capacity is `watcher_buffer_size + 1`; the +1 slot is reserved exclusively for the CANCELED event
  - Clients receive `event_type = CANCELED` with `error = WATCH_BUFFER_OVERFLOW (5001)` and must re-sync via Read API then re-register
  - `TrySendError::Closed` (receiver dropped) is now handled separately — silent cleanup, no CANCELED sent
  - Proto: added `WATCH_EVENT_TYPE_CANCELED = 2` and `ErrorCode::WATCH_BUFFER_OVERFLOW = 5001`

- **fix(read) #381**: Linearizable reads in multi-voter clusters now require quorum before serving. Previously a partitioned leader could serve stale reads.

- **fix(cas) #371**: CAS operations in the same `apply_chunk` no longer read stale values
  - Root cause: batch apply used a plain `ReadOptions` snapshot taken before the batch started; concurrent CAS ops targeting the same key within one chunk would overwrite each other
  - Fixed with `WriteBatchWithIndex` — each CAS op in a batch reads its own preceding writes

- **fix(client) #323**: `ClientApi` trait is now correctly exposed in embedded mode without the `client` feature flag

### Changed

- **`StateMachine::apply_chunk` signature** (#388): parameter changed from `Vec<Entry>` to `&[ApplyEntry]`. Custom state machine implementors must update their `impl`. `ApplyEntry` carries the decoded key/value/TTL directly — no proto parsing needed.

- No runtime behavior change for existing deployments — `unified_db` defaults to `false`

- **Zombie detection no longer auto-removes unreachable nodes** (#365):
  Previously, a node that failed to connect N times was automatically removed
  via `BatchRemove`. This was too aggressive and could evict temporarily
  restarting nodes. Detection now emits a `warn` log only — removal remains
  a deliberate operator or upper-layer decision.

### ⚠️ Breaking Change — Snapshot Format

The internal snapshot format changed from RocksDB **checkpoint** (v0.2.3) to
**CF export** (v0.2.4). Existing v0.2.3 snapshots cannot be loaded by v0.2.4.

**Migration**: Before upgrading, delete the `snapshot/` directory under `db_root_dir`.
d-engine will replay from WAL on first start and auto-create a new snapshot once
the log size threshold is reached.

> Note: d-engine does not currently provide a manual snapshot trigger API.
> Snapshots are created automatically based on the configure: e.g. `log_size_threshold`.

### ⚙️ Operational Notes

- **Minimum recommended CPU: 2 cores per node.**
  Each node runs a dedicated WAL IO thread (`buffered_raft_log`) plus tokio async workers
  (defaults to `num_cpus`). On single-core machines all threads share one CPU and latency
  degrades significantly under write load.

### ⚠️ Migration Note — WAL Purge After Snapshot

v0.2.4 purges WAL files after each successful snapshot. If you upgrade from v0.2.3 and
the node starts replaying from WAL before a new snapshot is taken, ensure sufficient
disk space is available for WAL replay. Nodes that have fallen significantly behind may
trigger a snapshot install from the leader instead of WAL replay.

---

## [v0.2.3] - 2026-02-21 [✅ Released]

### Added

- **CompareAndSwap (CAS) Operation** (#258): Atomic compare-and-swap primitive for distributed coordination
  - Use cases: Distributed locks, leader election, optimistic updates
  - API: `client.compare_and_swap(key, expected_value, new_value)`
  - Performance: No additional protocol round-trips; latency is comparable to regular writes in most workloads
- **Client: `Client::refresh()`** (#278): Rediscover cluster and rebuild connections after leader failover;
  blocks until a noop-committed leader is found or `cluster_ready_timeout` elapses
- **Client: `ClientBuilder::cluster_ready_timeout()`** / **`ClientConfig::cluster_ready_timeout`** (#278):
  Controls how long `build()` / `refresh()` waits for leader readiness (default: 5s)

### Changed

- **⚠️ BREAKING: Protobuf Enum Value Shifts** (#279): All protobuf enums now follow buf lint standards with `UNSPECIFIED = 0`
  - **NodeRole**: Old (Follower=0, Candidate=1, Leader=2, Learner=3) → New (UNSPECIFIED=0, Follower=1, Candidate=2, Leader=3, Learner=4)
  - **NodeStatus**: Old (Promotable=0, ReadOnly=1, Active=2) → New (UNSPECIFIED=0, Promotable=1, ReadOnly=2, Active=3)
  - **ErrorCode**: Old (None=0, NotLeader=1, ...) → New (UNSPECIFIED=0, NotLeader=1, ...)
  - All enum values now have proper prefixes (`NODE_ROLE_*`, `NODE_STATUS_*`, `ERROR_CODE_*`)
  - All fields now use snake_case naming (`leader_id`, `prev_log_index`, etc.)
  - **⚠️ Wire protocol incompatibility**: v0.2.3 cannot communicate with v0.2.2 or earlier
  - **Migration Required**: Update all TOML config files, upgrade all cluster nodes simultaneously (no rolling upgrade), upgrade client SDKs to v0.2.3

- **Unified Client API** (#258): Merged KV and cluster operations into single `ClientApi` trait
  - **Breaking**: `KvClient` → `ClientApi`, `KvError` → `ClientApiError`
  - Simplifies developer experience (single trait for all operations)
  - Both `GrpcClient` and `EmbeddedClient` implement unified interface

- **WriteResult Message** (#258): Replaced `bool succeeded` with `WriteResult` message
  - Improves API extensibility (reserved fields for version tracking)
  - Better type safety for future features

- **Simplified Error Handling** (#258): Removed `LocalClientError`, use `ClientApiError` directly
  - Unified error type across embedded and standalone clients
  - Less boilerplate (no intermediate error type conversion)
  - Clearer error semantics for users

- **Drain-based batch architecture** (#266): Replaced timeout-driven batching with drain-on-arrival pattern
  - Low load: near-zero wait (eliminated ~1ms timeout penalty)
  - High load: natural batching, significant throughput improvement
  - Embedded: linearizable read +92%, lease/eventual read +62% vs v0.2.2
  - See [bench report v0.2.3](benches/reports/v0.2.3/bench_report_v0.2.3.md)

- **Default PersistenceStrategy changed: `MemFirst` → `DiskFirst`** (#268): Raft protocol compliance
  - **Breaking**: Add `persistence_strategy = "MemFirst"` to `[raft.persistence]` config to restore prior behavior

### Fixed

- **Eliminated TOCTOU race in connection pool** (#278): Leader probe and TCP connect now share
  one retry loop under `cluster_ready_timeout`; a leader crash between probe success and connect
  no longer causes a silent failure — it is transparently retried

### Migration Notes

#### Protobuf Breaking Changes

- **⚠️ Wire protocol incompatible with v0.2.2**: All cluster nodes must upgrade simultaneously
- Update configuration files: Change `role` and `status` enum values (e.g., `role = 0` → `role = 1` for Follower)
- Upgrade all client SDKs to v0.2.3 before connecting to upgraded cluster
- See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md#-for-v022-users-protobuf-enum-breaking-changes-in-v023) for detailed migration steps

#### API Changes

- Replace `KvClient` with `ClientApi` in trait bounds
- Replace `KvError` with `ClientApiError` in error handling
- Update imports: `use d_engine::client::ClientApi;`

---

## [v0.2.2] - 2026-01-12 [✅ Released]

### 🎯 Key Improvements

- **ReadIndex Batching** - 440% linearizable read performance improvement (#236)
- **Embedded Mode Benchmarks** - Zero-copy performance validated (#233)
- **Cluster State APIs** - HA support (#234)
  - New: `is_leader()`, `node_id()`, `current_term()`, `wait_ready()`
  - Use case: Load balancers, health checks, leader discovery

---

### 🐛 Critical Fixes

- **Inconsistent Reads** (#228): Single-node mode returning stale data
- **Learner Promotion** (#212): Promotion stuck due to voter count bugs
- **Data Loss** (#242): Storage layer durability bugs
- **Snapshot Purge** (#235): Single-node cluster NoPeersAvailable error
- **Startup Race** (#209): `wait_ready()` timeout race condition

---

### ⚠️ Breaking Changes

**None** - All changes are backward compatible

---

## [v0.2.1] - 2026-01-01 [✅ Released]

### 🎯 Highlights for Developers

#### Workspace Structure - Modular Dependencies

**Problem**: v0.1.x pulled all dependencies even for client-only usage  
**Solution**: Feature flags `client`/`server`/`full` - depend only on what you need

```toml
# Client-only (lightweight)
d-engine = { version = "0.2", features = ["client"] }

# Embedded server (full engine)
d-engine = { version = "0.2", features = ["server"] }
```

**Impact**: Faster builds, smaller binaries

#### TTL/Lease - Automatic Key Expiration

**Use Case**: Distributed locks, session management, temporary state  
**API**: `client.put_with_ttl("session:123", data, Duration::from_secs(60))`  
**Feature**: Crash-safe (survives restart via absolute expiration time)

#### Watch API - Real-Time Key Monitoring

**Use Case**: Config change notifications, service discovery  
**Example**:

```rust,ignore
let mut watcher = client.watch("config/").await?;
while let Some(event) = watcher.next().await {
    println!("Changed: {:?}", event);
}
```

**Performance**: Lock-free, <0.1ms notification latency

#### StandaloneEngine - One-Line Deployment

**Use Case**: Independent server process (production deployment)  
**API**: `run(shutdown_rx)` uses env config, `run_with(config_path, shutdown_rx)` uses explicit config  
**Benefit**: Blocks until shutdown, no manual lifecycle management

#### EmbeddedEngine - In-Process Integration

**Use Case**: Embed d-engine in your Rust application  
**API**: `start()` uses env config, `start_with(config_path)` uses explicit config, `start_custom(...)` for advanced usage  
**Benefit**: Zero gRPC overhead via EmbeddedClient (<0.1ms latency)

#### EmbeddedClient - Zero-Overhead Embedded Access

**When**: Your app and d-engine in same process  
**Benefit**: Skip gRPC serialization, direct memory access (<0.1ms)  
**Example**: See `examples/service-discovery-embedded/`

---

### 📚 New Examples

- `examples/quick-start/` - 5-minute single-node setup
- `examples/single-node-expansion/` - Dynamic 1→3 node scaling
- `examples/service-discovery-embedded/` - EmbeddedClient zero-overhead access
- `examples/service-discovery-standalone/` - Watch API pattern

---

### ⚠️ Breaking Changes

**File-based State Machine WAL Format**

- WAL now uses absolute expiration time (not relative TTL)
- **Action Required**: See [MIGRATION_GUIDE.md](./MIGRATION_GUIDE.md) if upgrading from v0.1.x
- **New users**: No action needed

---

### 🚀 Performance & Quality

- Watch API: Lock-free, tested with 1000+ concurrent watchers, <0.1ms notification latency
- TTL cleanup: Lazy (read-time check) + Background (scheduled task), <1% CPU overhead
- 1000+ new integration tests covering edge cases
- Zero clippy warnings across entire codebase

---

## [v0.1.4] - 2025-10-12 [✅ Released]

### Features

- **Read Consistency Policies**: Implemented three-tier read consistency model with LeaseRead, LinearizableRead, and EventualConsistency support (#142)
- **Lease-Based Read Optimization**: Added leader-local reads with lease validation for improved read performance without sacrificing strong consistency (#142)

### Performance

- **Write Path Optimization**: Optimized RocksDB write path and Raft log loop for reduced latency (#141)
- **Zero-Copy Proto**: Migrated proto bytes fields to `bytes::Bytes` for zero-copy serialization (#140)
- **gRPC Compression**: Refactored gRPC compression configuration for Raft transport layer (#143)
- **Long-lived Peer Connections**: Optimized AppendEntries network layer with persistent peer task pools (#138)
- **Dedicated Read Thread Pool**: Offloaded state machine read operations to separate thread pool to improve throughput (#135)

### Testing

- **Multi-node Deployment**: Conducted comprehensive multi-node deployment testing for throughput validation (#137)
- **100K QPS Benchmark**: Achieved sustained 100,000+ QPS in high concurrency scenarios

---

## [v0.1.3] - 2025-09-XX [✅ Released]

### Features

- **Learner Join Flow**: Revised learner join process with promotion/fail semantics (#101)
- **Node Removal**: Automatic Node Removal (#102)
- **Learner Discovery**: Added auto-discovery support for new learner nodes (#89)
- **Snapshot Feature**: Implemented snapshot feature (#79)
- **Snapshot Compression**: Refactored compression logic from StateMachine to StateMachineHandler (#122)
- **Log Conflict Resolution**: Implemented first/last index for term to resolve replication conflicts (#45)
- **RocksDB Feature Flag**: Made RocksDB adapter optional via feature flag (#125)
- **Peer Connection Cache**: Enable RPC connection cache (#109)

### Fixes

- **Leaership Confirmation** Retry leadership noop confirmation until timeout (#106)

### Refactors

- **StateMachine API**: Made StateMachine trait more developer-friendly (#120)
- **StorageEngine API**: Made StorageEngine trait more developer-friendly (#119)

---

## [v0.1.2] - 2025-04-20 [✅ Released]

### Features

- **Benchmarking**: Added etcd v3.5 benchmarking on Mac Mini M2 (#59)
- **Client Example**: Created new crate client usage example (#71)
- **Raft Protocol**: Leader now sends empty log entry after election (#43)

### Fixes

- **Logging**: Replaced `log` crate with `tracing` implementation (#68)
- **Node Shutdown**: Fixed unexpected node termination after stress tests (#70)

### Refactors

- **Error Handling**: Separated protocol errors from system-level errors (#66)

---

## [v0.1.0] - 2025-04-11 [✅ Released]

### Added

- Initial implementation of core Raft consensus algorithm
  - Leader election
  - Log replication
  - State machine persistence
- Basic cluster communication layer using gRPC
  - Node-to-node heartbeat mechanism
  - AppendEntries RPC implementation
- Minimal working example demonstrating 3-node cluster setup

---

[//]: # "Version Links"
[v0.1.0]: https://github.com/deventlab/d-engine/releases/tag/v0.1.0
[v0.1.2]: https://github.com/deventlab/d-engine/releases/tag/v0.1.2
[v0.1.3]: https://github.com/deventlab/d-engine/releases/tag/v0.1.3
