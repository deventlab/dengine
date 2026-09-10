# Raft Throughput Optimization Guide for Tonic gRPC in Rust

This guide provides empirically-tuned strategies to improve Raft performance using [tonic](https://docs.rs/tonic/latest/tonic/) with connection type isolation and configurable persistence. These optimizations address critical bottlenecks in consensus systems where network transport and disk I/O impact throughput and latency.

## Connection Type Strategy

We implement three distinct connection types to prevent head-of-line blocking:

| **Type**  | **Purpose**          | **Critical Operations**           | **Performance Profile**    |
| --------- | -------------------- | --------------------------------- | -------------------------- |
| `Control` | Consensus operations | Heartbeats, Votes, Config changes | Latency-sensitive (sub-ms) |
| `Data`    | Log replication      | AppendEntries, Log writes         | Throughput-optimized       |
| `Bulk`    | Large transfers      | Snapshot streaming                | Bandwidth-intensive        |

```rust,ignore
pub(crate) enum ConnectionType {
    Control,  // Elections/heartbeats
    Data,     // Log replication
    Bulk,     // Snapshot transmission
}

```

## Persistence Strategy & Throughput/Latency Trade-offs

- **Write Path**: Entries are written to OS page cache via `db.write()` / `file.write()`; the IO
  thread batches them and calls fsync (`flush_wal(true)` / `sync_all()`) before advancing
  `durable_index`. Raft only counts an entry toward quorum after fsync completes.
- **Durability**:
  - _Process crash_: OS page cache survives a process restart → full recovery via WAL replay. ✅
  - _Power loss (single node)_: In a multi-node cluster, Raft quorum ensures committed data
    survives a single-node power failure — the other quorum members retain the data. ✅
  - _Power loss (majority of nodes simultaneously)_: Entries in the current unflushed batch
    (written since the last fsync) may be lost. This window is bounded by
    `idle_flush_interval_ms`. Raft has not yet counted these entries toward quorum, so no
    client-acknowledged write is lost — the leader will re-replicate after re-election. ⚠️
- **Throughput**: High. Multiple writes share a single fsync; no per-write fsync overhead.

### `FlushPolicy` Tuning

- **`Batch { idle_flush_interval_ms }`**: Flush (fsync) after this many milliseconds of idle time.
  - Lower values reduce the unflushed batch window but increase IO pressure.
  - Default `1000` ms is suitable for most workloads.

> **Note**: Writes are batched into a single fsync, reducing IO overhead while still providing
> disk-level durability for all client-acknowledged (committed) writes.

## Batching Configuration

The `max_batch_size` controls how many commands are drained per Raft loop iteration.

```toml
[raft.batching]
max_batch_size = 200  # default, suitable for most deployments
```

| Deployment                      | Recommended | Rationale                                                                         |
| ------------------------------- | ----------- | --------------------------------------------------------------------------------- |
| Embedded 3-node                 | **200**     | Matches typical concurrent client counts; higher values yield diminishing returns |
| Standalone 3-node               | **200**     | Network RTT dominates; batch size has limited impact                              |
| High concurrency (500+ clients) | **500**     | Increase if HC Write throughput plateaus                                          |

> **Rule of thumb**: For embedded mode, optimal `max_batch_size ≈ concurrent_client_count`. For standalone, keep at 200 unless profiling shows cmd_rx consistently saturated.

## Read Fast Path Tuning (v0.2.5+)

`Eventual` and `LeaseRead` bypass `cmd_rx` entirely — they never touch `max_batch_size` or `time_threshold_ms`. Two dedicated knobs:

```toml
[raft.read_actor]
channel_capacity = 512   # default
max_drain        = 100   # default
```

| Parameter          | What it controls                                              | Tune up when…                                                                              |
| ------------------ | ------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| `channel_capacity` | mpsc buffer depth between client and ReadActor                | read latency spikes under high concurrent Eventual/LeaseRead (channel full → backpressure) |
| `max_drain`        | reads batched per ReadActor wakeup (mirrors `max_batch_size`) | read throughput plateaus but CPU is not saturated                                          |

Same tradeoff as write batching: higher `max_drain` → better throughput, higher tail latency. Start with defaults; only tune if profiling shows a bottleneck here.

---

## Configuration Tuning

### Control Plane (`[network.control]`)

```toml
connection_window_size = 2_097_152  # 2MB
http2_keep_alive_timeout_in_secs = 20  # Aggressive timeout

```

**Why it matters:**

- Ensures election timeouts aren't missed during load
- Prevents heartbeat delays that cause unnecessary leader changes
- Uses smaller windows for faster roundtrips

### Data Plane (`[network.data]`)

```toml
connection_window_size = 6_291_456  # 6MB
request_timeout_in_ms = 200          # Batch-friendly

```

**Optimization rationale:**

- Larger windows accommodate log batches
- Timeout tuned for batch processing, not individual entries

### Bulk Plane (`[network.bulk]` - Recommended)

```toml
# SNAPSHOT-SPECIFIC SETTINGS (EXAMPLE)
connect_timeout_in_ms = 1000         # Slow-start connections
request_timeout_in_ms = 30000        # 30s for large transfers
connection_window_size = 33_554_432  # 32MB window

```

**Snapshot considerations:**

- Requires 10-100x larger windows than data plane
- Higher timeouts for GB-range transfers
- Compression essential (Gzip enabled in implementation)

## Critical Code Implementation

### Connection Type Routing

```rust,ignore
// Control operations
membership.get_peer_channel(peer_id, ConnectionType::Control)

// Data operations
membership.get_peer_channel(peer_id, ConnectionType::Data)

// Snapshot transfers
membership.get_peer_channel(leader_id, ConnectionType::Bulk)

```

### gRPC Server Tuning

The server side has its own `[network.server]` profile, separate from the
client-side `control`/`data`/`bulk` planes above — one listener accepts every
RPC type uniformly, so there's no per-plane timeout to apply. In particular,
the server must **not** apply a blanket request timeout: a single connection
carries both fast heartbeats and multi-minute snapshot transfers, so any
transport-level timeout would either be too tight for snapshots or too loose
for elections. Per-RPC deadlines are instead the client's responsibility
(`control_config.request_timeout_in_ms`, etc., enforced via `Endpoint::timeout()`
on the client side).

```rust,ignore
tonic::transport::Server::builder()
    .concurrency_limit_per_connection(server_config.concurrency_limit_per_connection)
    .max_concurrent_streams(server_config.max_concurrent_streams)
    .http2_max_pending_accept_reset_streams(Some(server_config.max_pending_accept_reset_streams))
    .http2_keepalive_interval(Some(Duration::from_secs(server_config.http2_keepalive_interval_in_secs)))
    .http2_keepalive_timeout(Some(Duration::from_secs(server_config.http2_keepalive_timeout_in_secs)))
    .initial_stream_window_size(server_config.initial_stream_window_size)
    .initial_connection_window_size(server_config.initial_connection_window_size)

```

Inbound message size is the one setting that _is_ per-service rather than
transport-wide, so it's applied on each `XxxServiceServer` individually:

```rust,ignore
RaftReplicationServiceServer::from_arc(node.clone())
    .max_decoding_message_size(server_config.max_decoding_message_size)

```

## Performance Results (Optimization Impact)

| **Metric**    | **Before**  | **After**   | **Delta**  |
| ------------- | ----------- | ----------- | ---------- |
| Throughput    | 368 ops/sec | 373 ops/sec | +1.3%      |
| p99 Latency   | 5543 µs     | 4703 µs     | **-15.2%** |
| p99.9 Latency | 14015 µs    | 11279 µs    | -19.5%     |

> **Key improvement**: 15% reduction in tail latency - critical for consensus stability  
> **Note**: These metrics show the impact of connection pooling optimization. These results can be further improved by tuning `FlushPolicy` for your specific workload.
>
> For absolute performance benchmarks, see [v0.2.4 Performance Report](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.4/bench_report_v0.2.4.md)

## Operational Recommendations

1. **Pre-warm connections** during node initialization
2. **Monitor connection types separately**:

   ```bash
   # Control plane
   netstat -an | grep ":9081" | grep ESTABLISHED | wc -l

   # Data plane
   netstat -an | grep ":9082" | grep ESTABLISHED

   ```

3. **Size bulk windows** for snapshot sizes:

   ```rust,ignore
   connection_window_size = max_snapshot_size * 1.2

   ```

4. **Compress snapshots**:

   ```rust,ignore
   .send_compressed(CompressionEncoding::Gzip)
   .accept_compressed(CompressionEncoding::Gzip)

   ```

5. **Monitor Flush Lag**: Monitor the difference between `last_log_index` and `durable_index`. Raft only counts an entry toward quorum and acknowledges it to the client after fsync — so a growing gap does not put acknowledged writes at risk. It does mean client-facing write latency is growing, and (if the gap keeps growing) the amount of work an unflushed batch would need to redo on restart is growing too.

## Anti-Patterns to Avoid

```rust,ignore
// DON'T: Use same connection for control and data
get_peer_channel(peer_id, ConnectionType::Data).await?;
client.request_vote(...)  // Control operation on data channel

// DO: Strict separation
get_peer_channel(peer_id, ConnectionType::Control).await?;
client.request_vote(...)

// DON'T: Set idle_flush_interval_ms too low — defeats batching.
flush_policy = { Batch = { idle_flush_interval_ms = 1 } } // Near-synchronous; low throughput

// DO: Use a generous idle interval to amortize disk I/O cost.
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }

```

## Why Connection Isolation and Strategy Choice Matters

1. **Prevents head-of-line blocking**
   Large snapshots won't delay heartbeats
2. **Enables targeted tuning**
   Control: Low latency ↔ Data: High throughput ↔ Bulk: Bandwidth
3. **Improves fault containment**
   Connection issues affect only one operation type
4. **Decouples Performance from Ack Latency**
   Client-acknowledged writes are always fsync-durable — that's not tunable. `idle_flush_interval_ms` lets you balance write throughput against how long a client waits for that fsync.

## Reference Deployment Configurations

Below are example configurations for different deployment scenarios.
Adjust values based on snapshot size, log append rate, and cluster size.

### 1. Single Node (Local Dev / Testing)

- CPU: 4 cores
  • Memory: 8 GB
  • Network: Localhost

```toml
[raft.persistence]
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }

[network.control]
connection_window_size = 1_048_576   # 1MB

[network.data]
connection_window_size = 2_097_152   # 2MB

[network.bulk]
connection_window_size = 8_388_608   # 8MB
request_timeout_in_ms = 10_000       # 10s

[network.server]
concurrency_limit_per_connection = 20
max_concurrent_streams = 128

```

**Tip**: Single-node setups focus on low resource usage; bulk window size can be smaller since snapshots are local.

### 2. 3-Node Public Cloud Cluster (Medium Durability)

- Instance Type: 4 vCPU / 16 GB RAM (e.g., AWS m6i.large, GCP n2-standard-4)
  • Network: 10 Gbps
  • Priority: Balanced throughput and durability

```toml
[raft.persistence]
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }

[network.control]
connection_window_size = 2_097_152   # 2MB

[network.data]
connection_window_size = 4_194_304   # 4MB

[network.bulk]
connection_window_size = 33_554_432  # 32MB
request_timeout_in_ms = 30_000       # 30s for multi-GB snapshots

[network.server]
concurrency_limit_per_connection = 30
max_concurrent_streams = 256

```

**Tip**: For public cloud, moderate concurrency and 32MB bulk windows ensure stable snapshot streaming without affecting heartbeats. The batch policy is tuned for high throughput; acknowledged writes are never at risk regardless of the interval, only ack latency and unflushed-batch replay time on restart scale with it.

### 3. 5-Node High-Durability Cluster (Production)

- Instance Type: 8 vCPU / 32 GB RAM (e.g., AWS m6i.xlarge)
  • Network: 25 Gbps
  • Priority: Data Integrity over Write Latency

```toml
[raft.persistence]
flush_policy = { Batch = { idle_flush_interval_ms = 100 } }  # More frequent flush, shorter ack latency

[network.control]
connection_window_size = 4_194_304   # 4MB

[network.data]
connection_window_size = 8_388_608   # 8MB

[network.bulk]
connection_window_size = 67_108_864  # 64MB
request_timeout_in_ms = 60_000       # 60s for large snapshots

[network.server]
concurrency_limit_per_connection = 80
max_concurrent_streams = 512

```

**Tip**: Lower `idle_flush_interval_ms` (e.g., 100ms) shortens client-facing write latency and shrinks the unflushed-batch window an IO thread has to redo on restart. Acknowledged writes are power-loss safe regardless of this setting — Raft only counts an entry toward quorum, and acknowledges it to the client, after fsync completes.

## Network Environment Tuning Recommendations

These parameters are primarily **network-dependent**, not CPU/memory dependent.

Adjust them based on latency, packet loss, and connection stability.

| **Environment**                  | **tcp_keepalive_in_secs** | **http2_keep_alive_interval_in_secs** | **http2_keep_alive_timeout_in_secs** | **Notes**                                             |
| -------------------------------- | ------------------------- | ------------------------------------- | ------------------------------------ | ----------------------------------------------------- |
| **Local / In-Cluster (LAN)**     | 60                        | 10                                    | 5                                    | Low latency & stable; defaults are fine               |
| **Cross-Region / Stable WAN**    | 60                        | 15                                    | 8                                    | Slightly longer keep-alive to avoid false disconnects |
| **Public Cloud / Moderate Loss** | 60                        | 20                                    | 10                                   | Higher interval & timeout for lossy links             |
| **High Latency / Unstable WAN**  | 120                       | 30                                    | 15                                   | Longer timeouts prevent spurious drops                |

**Guidelines:**

1. Keep-alive interval ≈ 1/3 of timeout.
2. High-latency WAN: favor fewer reconnects over aggressive failure detection.
3. These settings are independent of CPU and memory; focus on network RTT and stability.

### RPC Timeout Guidance

`connect_timeout_in_ms` and `request_timeout_in_ms` depend on **network latency and I/O**, not CPU or memory.

| **Environment**                  | **connect_timeout_in_ms** | **request_timeout_in_ms** | **Notes**                                             |
| -------------------------------- | ------------------------- | ------------------------- | ----------------------------------------------------- |
| **Local / In-Cluster (LAN)**     | 50–100                    | 100–300                   | Very low RTT; fast retries                            |
| **Cross-Region / Stable WAN**    | 200–500                   | 300–1000                  | Higher RTT, moderate batch sizes                      |
| **Public Cloud / Moderate Loss** | 500–1000                  | 1000–5000                 | Compensate for packet loss and I/O latency            |
| **High Latency / Unstable WAN**  | 1000+                     | 5000+                     | Favor fewer reconnects; allow large batch replication |

**Tips:**

1. `connect_timeout_in_ms` covers TCP+TLS+gRPC handshake; increase for high-latency links.
2. `request_timeout_in_ms` should accommodate log batches and disk write delays on followers.
3. Timeouts mainly depend on **network RTT** and **disk I/O**, not hardware compute.

## Optimizing gRPC Compression for Performance

The d-engine now supports granular control of gRPC compression settings per service type, allowing you to fine-tune your deployment for optimal performance based on your specific environment.

### Granular Compression Control

```toml
# Example configuration for AWS VPC environment
[raft.rpc_compression]
replication_response = false  # High-frequency, disable for CPU optimization
election_response = true      # Low-frequency, minimal CPU impact
snapshot_response = true      # Large data volume, benefits from compression
cluster_response = true       # Configuration data, benefits from compression
client_response = false       # Improves client read/write performance
```

### Performance Impact

Our benchmarks show that disabling compression for high-frequency operations (replication and client requests) can yield significant performance improvements in low-latency environments:

| Scenario     | CPU Savings | Throughput Improvement |
| ------------ | ----------- | ---------------------- |
| Same-AZ VPC  | 15-20%      | 30-40%                 |
| Cross-AZ VPC | 5-10%       | 10-15%                 |
| Cross-Region | -5% to -10% | -20% to -30%           |

Note: In cross-region deployments, enabling compression for all traffic types is generally beneficial due to bandwidth constraints.

### Read Consistency and Compression

The ReadConsistencyPolicy (`LeaseRead`, `LinearizableRead`, `EventualConsistency`) works in conjunction with compression settings. For maximum performance:

1. Use `EventualConsistency` when possible for non-critical reads
2. Combine with `client_response = false` for lowest latency
3. Longer `lease_duration_ms` with `LeaseRead` reduces network round-trips

Example configuration for high-throughput read operations:

```toml
[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 500  # Longer lease duration = better performance
allow_client_override = true

[raft.rpc_compression]
client_response = false  # Optimize client read performance

```

This combination provides strong consistency with minimal network overhead and no compression CPU penalty.

---

### Linearizable Read Batching Configuration

LinearizableRead batches requests to amortize consensus overhead. The key principle: **`size_threshold` should trigger quickly under load; `time_threshold` is the safety net for low concurrency.**

#### Configuration Parameters

```toml
[raft.read_consistency.read_batching]
size_threshold = 100      # Trigger point for high concurrency
time_threshold_ms = 50    # Fallback timeout for low concurrency
```

#### Core Tuning Principle

**`size_threshold` = "Large enough to batch effectively, small enough to trigger frequently"**

- **Too small (10-50)**: Batches too often → wasted overhead
- **Sweet spot (100-200)**: Triggers within 1-2ms under load → optimal
- **Too large (300+)**: Rarely reached → all batches wait for timeout → performance collapse

#### Quick Start Guide

**Step 1: Set `time_threshold_ms` based on network latency**

| Environment         | RTT    | Recommended `time_threshold_ms` |
| ------------------- | ------ | ------------------------------- |
| Local/LAN           | <1ms   | 10-20                           |
| AWS Same-Region VPC | 1-2ms  | 40-60                           |
| Cross-Region/WAN    | 5-10ms | 80-120                          |

**Step 2: Keep `size_threshold` in safe range**

```toml
size_threshold = 100-150  # Works for most workloads
```

**Step 3: Validate with benchmarks**

**Good configuration**: Most batches flush via `size_threshold`, not timeout  
❌ **Bad configuration**: P99 latency approaches `time_threshold_ms` (all batches timing out)

#### Validated Configurations

**AWS EC2 c5.2xlarge, 3-node cluster, 1000 concurrent clients:**

| Config     | Throughput   | P99 Latency | Result                              |
| ---------- | ------------ | ----------- | ----------------------------------- |
| `100/50ms` | 141K ops/sec | 1.09ms      | Optimal                             |
| `100/10ms` | 138K ops/sec | 1.15ms      | Good for low-latency networks       |
| `120/50ms` | 1.9K ops/sec | 52ms        | ❌ Threshold too high, all timeouts |
| `10/5ms`   | 24K ops/sec  | 5.4ms       | ❌ Window too short                 |

#### Troubleshooting

**Throughput drops to <10K ops/sec**  
→ `size_threshold` too high, reduce to 100-150

**P99 latency ≈ `time_threshold_ms`**  
→ Batches timing out instead of filling, increase timeout or decrease threshold

---

### Implementation Recommendations

1. **For your specific AWS VPC environment**: I recommend disabling compression for `replication_response` and `client_response` as you've already done. This is optimal for same-VPC deployments where network latency is negligible.

2. **Accept Compressed vs Send Compressed**: Always keep `accept_compressed` enabled for all services to maintain compatibility with clients. This has minimal performance impact when no compressed data is received.

3. **Granular Control**: The design allows you to optimize based on actual data patterns - snapshot transfers benefit from compression regardless of environment, while high-frequency operations like replication and client responses perform better without compression in low-latency networks.

This implementation provides a clean, configurable approach that follows Rust best practices and provides clear documentation for users.

---

## Diagnosing Bottlenecks with Metrics

d-engine emits Prometheus-compatible metrics at every stage of the write pipeline via the
[`metrics`](https://docs.rs/metrics) crate. Install any compatible recorder at application
startup and the metrics flow automatically — no d-engine configuration required.

```rust,ignore
// Example: install a Prometheus recorder in your application
metrics_exporter_prometheus::PrometheusBuilder::new()
    .install()
    .expect("failed to install recorder");

// Then start d-engine as normal — metrics are emitted automatically
let engine = DefaultEmbeddedEngine::new(config).await?;
```

When no recorder is installed, all metric calls are no-ops (~2ns overhead). There is no
d-engine-side on/off switch — the recorder controls everything.

### Write Pipeline Overview

```text
Client write
  → [propose buffer]    ← core.raft.buffer.length{buffer="propose"}
  → AppendEntries RPC   ← server.raft.replicate.rtt_ms{peer}        (planned)
  → Follower WAL fsync  ← core.raft.fsync.duration_ms
                          core.raft.fsync.batch_entries
                          core.raft.fsync.inflight
                          core.raft.fsync.busy_nanos_total         (utilization)
  → commit → SM apply   ← core.raft.buffer.length{buffer="linearizable"}
                          core.state_machine.apply_chunk.duration_ms
                          core.state_machine.apply_chunk.batch_size
                          core.state_machine.apply.busy_nanos_total (utilization)
  → Client response     ← core.raft.write.propose_to_apply_ms     (end-to-end)
                          core.raft.write.propose_to_commit_ms
```

### Metrics Reference

| Metric                                            | Type        | Answers                                |
| ------------------------------------------------- | ----------- | -------------------------------------- |
| `core.raft.buffer.length{buffer="propose"}`       | Gauge       | Is the proposal channel backlogged?    |
| `core.raft.fsync.duration_ms`                     | Histogram   | How long does each fsync take?         |
| `core.raft.fsync.batch_entries`                   | Histogram   | Is FsyncCoordinator coalescing writes? |
| `core.raft.fsync.inflight`                        | Gauge (0/1) | Is a fsync task currently running?     |
| `core.raft.fsync.busy_nanos_total`                | Counter     | fsync thread utilization               |
| `core.state_machine.apply_chunk.duration_ms`      | Histogram   | SM apply latency                       |
| `core.state_machine.apply_chunk.batch_size`       | Histogram   | Entries applied per chunk              |
| `core.state_machine.apply.busy_nanos_total`       | Counter     | SM apply utilization                   |
| `core.raft.write.propose_to_apply_ms`             | Histogram   | End-to-end write latency               |
| `core.raft.write.propose_to_commit_ms`            | Histogram   | Propose-to-commit latency              |
| `core.raft.backpressure.rejections{node_id,type}` | Counter     | Rejected requests (write/read)         |

### Finding the Bottleneck: Utilization Ratio

For any serialized stage (fsync, SM apply), compute utilization over a time window:

```promql
# fsync thread utilization (0.0 = idle, 1.0 = saturated)
rate(core_raft_fsync_busy_nanos_total[1m]) / 1e9

# SM apply utilization
rate(core_state_machine_apply_busy_nanos_total[1m]) / 1e9
```

**The stage whose utilization approaches 1.0 is the bottleneck.** No tuning elsewhere will
help until that stage is relieved. This is the USE method (Utilization, Saturation, Errors)
applied to d-engine's pipeline — no hardware-specific theoretical model needed.

Example interpretation:

| fsync utilization | SM apply utilization | Conclusion                                                            |
| ----------------- | -------------------- | --------------------------------------------------------------------- |
| 0.95              | 0.20                 | WAL fsync is saturated — tune batch window or use faster storage      |
| 0.30              | 0.85                 | SM apply is the bottleneck — check RocksDB compaction pressure        |
| 0.20              | 0.20                 | Neither stage saturated — bottleneck is elsewhere (network, upstream) |

### Interpreting Common Patterns

**`fsync.batch_entries` p50 = 1 under high write load**

FsyncCoordinator is not coalescing. Each proposal triggers its own fsync. Under a single-
client benchmark this is expected and correct — batching requires concurrent writers. Under
multi-client load, if batch_entries stays at 1, investigate whether proposals are arriving
in rapid bursts or at a steady trickle.

**`fsync.duration_ms` p99 >> p50 (high tail latency)**

Occasional long fsyncs (disk GC, cloud volume throttling). Check storage I/O metrics.
Increasing `idle_flush_interval_ms` allows larger batches that amortize these spikes.

**End-to-end latency high, both utilization metrics low**

The bottleneck is upstream (network RTT, proposal channel) or downstream (apply notify
latency). Check `propose_to_commit_ms` vs `propose_to_apply_ms` — the gap between them
is the commit-to-apply delay.

### Grafana Dashboard Setup

Two panels cover 80% of diagnostic needs:

**Panel 1 — End-to-end write latency (verification)**

```promql
histogram_quantile(0.99, rate(core_raft_write_propose_to_apply_ms_bucket[1m]))
histogram_quantile(0.50, rate(core_raft_write_propose_to_apply_ms_bucket[1m]))
```

**Panel 2 — Stage utilization (bottleneck locator)**

```promql
rate(core_raft_fsync_busy_nanos_total[1m]) / 1e9
rate(core_state_machine_apply_busy_nanos_total[1m]) / 1e9
```

When Panel 1 shows elevated p99 and Panel 2 shows one stage near 1.0, that stage is the
bottleneck. When both utilization values are well below 1.0, the bottleneck is elsewhere.

### Histogram Bucket Note

d-engine's storage operations span three orders of magnitude under load (100µs to seconds).
Configure your recorder with exponential buckets to preserve this range:

```rust,ignore
metrics_exporter_prometheus::PrometheusBuilder::new()
    .set_buckets_for_metric(
        metrics_exporter_prometheus::Matcher::Prefix("core.raft.fsync".to_string()),
        &[0.0001, 0.0002, 0.0004, 0.0008, 0.0016, 0.003, 0.006, 0.012,
          0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 3.3],
    )
    .unwrap()
    .install()
    .unwrap();
```

Default Prometheus buckets (5ms minimum) collapse the 100µs–5ms region where healthy
operations live, making percentile calculations meaningless for this use case.
