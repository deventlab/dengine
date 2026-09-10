# Three-Node Cluster Example

## Overview

This example demonstrates a **production-ready three-node d-engine cluster** with automatic failover and data replication. All benchmark data in [benches/reports/v0.2.3/](https://github.com/deventlab/d-engine/tree/main/benches/reports/v0.2.3) is generated using this configuration.

**Key characteristics:**

- **High availability**: Automatic failover on node failure
- **Data safety**: Replicated across all nodes
- **Fault tolerance**: Tolerates 1 node failure (quorum = 2/3)
- **Performance baseline**: Reference configuration for all published benchmarks

## When to Use

Use this example for:

- **Production deployments**: Mission-critical applications requiring high availability
- **Performance benchmarking**: Comparing d-engine against other distributed systems
- **Load testing**: Validating cluster behavior under high throughput
- **Development**: Testing multi-node features (replication, failover, leader election)

## Quick Start

```bash
cd examples/three-nodes-standalone

# Build and start cluster
make start-cluster

# Monitor logs (in separate terminals)
tail -f logs/1/d.log
tail -f logs/2/d.log
tail -f logs/3/d.log
```

The cluster starts with all three nodes simultaneously:

- **Node 1** at `0.0.0.0:9081` (metrics: 8081)
- **Node 2** at `0.0.0.0:9082` (metrics: 8082)
- **Node 3** at `0.0.0.0:9083` (metrics: 8083)

## Configuration

Each node uses an identical configuration structure in `config/n{1,2,3}.toml`:

```toml
[cluster]
node_id = 1
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 1, status = 2 },  # Follower
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 2 },  # Follower
    { id = 3, address = "0.0.0.0:9083", role = 1, status = 2 },  # Follower
]

[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 500

[raft.persistence]
flush_policy = { Batch = { idle_flush_interval_ms = 1000 } }
```

**Key differences from single-node expansion:**

- All nodes start with `role = 1` (Follower) and `status = 2` (Active)
- No `initial_cluster_size = 1` field (normal 3-node cluster start)
- Leader election happens automatically via Raft

## Testing Cluster Behavior

### Verify Leader Election

```bash
# Check which node is leader (look for "Became leader" in logs)
grep -r "Became leader" logs/
```

### Test Write Operations

```bash
# Write to cluster (automatically routes to leader)
curl -X POST http://localhost:9081/put \
  -H "Content-Type: application/json" \
  -d '{"key": "test-key", "value": "test-value"}'
```

### Test Failover

```bash
# 1. Find current leader
grep -r "Became leader" logs/

# 2. Stop leader node (e.g., if Node 1 is leader)
pkill -f "CONFIG_PATH=config/n1"

# 3. Verify new leader election (within 1-2 seconds)
grep -r "Became leader" logs/
```

Expected behavior:

- **Election timeout**: 1-2 seconds (configured via `election_timeout_min/max`)
- **New leader emerges**: One of the remaining 2 nodes becomes leader
- **Cluster remains operational**: Accepts writes with 2/3 nodes available

### Test Read Consistency

```bash
# LeaseRead (default): Fast reads from leader (500ms lease)
curl http://localhost:9081/get?key=test-key

# LinearizableRead: Strongest consistency (requires quorum confirmation)
curl http://localhost:9081/get?key=test-key&consistency=linearizable
```

## Benchmark Configuration Reference

All performance reports in `/benches/standalone-bench/reports` use this exact configuration:

**Raft settings:**

- Persistence: batched fsync (Level 3, fdatasync) with 1000ms idle flush interval
- Read consistency: `LeaseRead` (500ms lease duration)
- Replication: Batched append entries (5000 threshold, 0ms delay)
- Network: Tuned for high throughput (see `config/n1.toml` for details)

**Test environment:**

- 3-node cluster on localhost
- Benchmark client connects to all 3 nodes (ports 9081/9082/9083)
- Workload: Mixed read/write operations under various concurrency levels

See [benches/standalone-bench/README.md](../../../../benches/standalone-bench/README.md) for benchmark reproduction steps.

## Advanced Features

### Performance Profiling

Profile cluster under load using [Samply](https://github.com/mstange/samply):

```bash
# Profile all 3 nodes simultaneously
make perf-cluster

# View flame graphs (open in Firefox)
firefox samply_reports/1.profile.json
```

### Tokio Console Monitoring

Monitor async runtime behavior:

```bash
# Terminal 1: Start cluster with tokio-console enabled
make tokio-console-cluster

# Terminal 2-4: Connect to each node
tokio-console http://127.0.0.1:6670  # Node 1
tokio-console http://127.0.0.1:6671  # Node 2
tokio-console http://127.0.0.1:6672  # Node 3
```

### Expand to 5-Node Cluster

Add learner nodes for additional fault tolerance:

```bash
# Start 3-node cluster first
make start-cluster

# Add learner nodes (in separate terminals)
make join-node4
make join-node5

# Promote learners to voters (via API)
curl -X POST http://localhost:9081/promote -d '{"node_id": 4}'
curl -X POST http://localhost:9081/promote -d '{"node_id": 5}'
```

5-node cluster tolerates 2 simultaneous node failures (quorum = 3/5).

## Common Operations

### Clean Environment

```bash
# Remove all build artifacts, logs, and databases
make clean

# Remove only logs and databases (keep build)
make clean-log-db
```

### Manual Node Control

Start nodes individually (useful for debugging):

```bash
# Terminal 1
make start-node1

# Terminal 2
make start-node2

# Terminal 3
make start-node3
```

### Change Log Level

```bash
# Run with debug logs
LOG_LEVEL=debug make start-cluster

# Run with info logs (default: warn)
LOG_LEVEL=info make start-node1
```

## Troubleshooting

**Cluster won't start:**

- Verify `target/release/demo` exists (run `make build`)
- Check port availability: `lsof -i :9081` (9082, 9083)
- Ensure no stale processes: `pkill -f demo`

**Leader election fails:**

- Verify all 3 nodes are running: `ps aux | grep demo`
- Check network connectivity in logs: `grep "connection" logs/*/d.log`
- Confirm config files have matching `initial_cluster` entries

**Write operations timeout:**

- Verify at least 2/3 nodes are running (quorum requirement)
- Check leader exists: `grep "Became leader" logs/*/d.log`
- Monitor replication lag: `grep "append_entries" logs/*/d.log`

## Next Steps

- **Single-node expansion**: See [single-node-expansion.md](single-node-expansion.md) for dynamic 1→3 scaling
- **Run benchmarks**: See [../../../../benches/standalone-bench/README.md](../../../../benches/standalone-bench/README.md) to reproduce performance tests
