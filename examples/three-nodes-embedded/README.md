# Three-Node Embedded Cluster with HTTP API

Production-ready example demonstrating high-availability deployment with embedded d-engine, custom HTTP API, and health check endpoints for load balancer integration.

## What This Is

- **3-node embedded cluster**: Raft consensus with automatic failover
- **Custom HTTP API**: Simple KV service with PUT/GET operations
- **Health check endpoints**: `/primary` and `/replica` for load balancer integration
- **Production-ready**: Run 3 application instances, each with embedded d-engine

**Use this pattern when:**

- Your application is written in Rust
- You need HA with sub-millisecond local reads
- You want to deploy behind a load balancer (HAProxy/Nginx/Envoy)

## Quick Start

```bash
# Build and start 3-node cluster
make start-cluster
```

This starts three nodes in parallel:

- **Node 1**: HTTP API on `8081`, health checks on `10001`
- **Node 2**: HTTP API on `8082`, health checks on `10002`
- **Node 3**: HTTP API on `8083`, health checks on `10003`

Each node runs:

- Business API server (KV operations)
- Health check server (leader detection)
- Embedded d-engine (Raft consensus)

## Architecture

```
┌─────────────────────────────────────────────────┐
│              Load Balancer                      │
│  (HAProxy/Nginx/Envoy - optional)               │
│                                                  │
│  Writes → Leader   Reads → Round-robin          │
└──────┬──────────────────┬───────────────────┬───┘
       │                  │                   │
       ▼                  ▼                   ▼
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│   Node 1    │    │   Node 2    │    │   Node 3    │
│             │    │             │    │             │
│ HTTP: 8081  │◄──►│ HTTP: 8082  │◄──►│ HTTP: 8083  │
│ Health:10001│    │ Health:10002│    │ Health:10003│
│             │    │             │    │             │
│ ┌─────────┐ │    │ ┌─────────┐ │    │ ┌─────────┐ │
│ │ Axum API│ │    │ │ Axum API│ │    │ │ Axum API│ │
│ └────┬────┘ │    │ └────┬────┘ │    │ └────┬────┘ │
│      │      │    │      │      │    │      │      │
│ ┌────▼────┐ │    │ ┌────▼────┐ │    │ ┌────▼────┐ │
│ │ d-engine│ │    │ │ d-engine│ │    │ │ d-engine│ │
│ │(embedded)│◄┼────┼►│(embedded)│◄┼────┼►│(embedded)│ │
│ └─────────┘ │    │ └─────────┘ │    │ └─────────┘ │
│   (Raft protocol)  │             │    │             │
└─────────────┘    └─────────────┘    └─────────────┘
```

## Configuration

### Node Configuration

Each node uses identical configuration structure in `config/n{1,2,3}.toml`:

```toml
[cluster]
node_id = 1  # 2, 3 for other nodes
listen_address = "0.0.0.0:9081"
initial_cluster = [
    { id = 1, address = "0.0.0.0:9081", role = 1, status = 2 },
    { id = 2, address = "0.0.0.0:9082", role = 1, status = 2 },
    { id = 3, address = "0.0.0.0:9083", role = 1, status = 2 },
]

[raft.read_consistency]
default_policy = "LeaseRead"
lease_duration_ms = 500

[raft.persistence]
flush_policy = { Batch = { threshold = 100, interval_ms = 20 } }
```

**Key settings:**

- All nodes start as Followers (`role = 1`)
- Leader election happens automatically
- LeaseRead for fast local reads (500ms lease)
- MemFirst persistence with batched flush

### Port Configuration

Ports are customizable via CLI arguments:

```bash
# Custom ports
./target/release/three-nodes-embedded \
  --port 8081 \
  --health-port 10001 \
  --config-path config/n1
```

## API Reference

### Business API

**Writes must go to the leader.** Find it first via the health check endpoints:

```bash
# Find the leader (health port returns 200 only on the leader node)
for port in 10001 10002 10003; do
  code=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary)
  echo "health:$port → $code"
done
# health:10001 → 503
# health:10002 → 200  ← leader is on business port 8082
# health:10003 → 503
```

**Write operation (to the leader):**

```bash
curl -X POST http://localhost:8082/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "username", "value": "alice"}'
# Response: {}
```

Writing to a follower returns `503 Service Unavailable` with `{"error":"not_leader"}`.

**Read operation (any node):**

```bash
curl http://localhost:8081/kv/username
# Response: {"value":"alice"}
```

Reads use `get_eventual()` — they serve from the local state machine. A freshly
replicated write may take a few milliseconds to appear on follower nodes.

### Health Check API

**Leader check:**

```bash
curl -i http://localhost:10001/primary
# 200 OK if leader, 503 if follower
```

**Follower check:**

```bash
curl -i http://localhost:10001/replica
# 200 OK if follower, 503 if leader
```

These endpoints are designed for load balancer health checks.

## Logs

By default, logs output to terminal for quick debugging.

To save logs to files, redirect output:

```bash
mkdir -p logs
make start-node1 > logs/node1.log 2>&1 &
make start-node2 > logs/node2.log 2>&1 &
make start-node3 > logs/node3.log 2>&1 &
```

## Testing the Cluster

### Verify Leader Election

```bash
# Test health endpoints to find leader
for port in 10001 10002 10003; do
  echo -n "Port $port: "
  curl -s -o /dev/null -w "%{http_code}" http://localhost:$port/primary
  echo
done
# One port returns 200 (leader), others return 503 (followers)
```

### Test Write Operations

```bash
# Must write to leader — follower writes return 503 {"error":"not_leader"}
# Use GET /primary on port 8091/8092/8093 to find the current leader first.
curl -X POST http://localhost:8081/kv \
  -H "Content-Type: application/json" \
  -d '{"key": "test", "value": "hello"}'
```

### Test Read Consistency

```bash
# Read from different nodes
curl http://localhost:8081/kv/test
curl http://localhost:8082/kv/test
curl http://localhost:8083/kv/test
```

All nodes return the same value due to Raft consensus.

## Load Balancer Integration

This example is designed to work with any load balancer. See the [HA Deployment Guide](../../../d-engine/src/docs/examples/ha-deployment-load-balancing.md) for complete setup with HAProxy (reference implementation).

**Key integration points:**

- Health check port separate from business port
- `/primary` endpoint returns 200 only on leader
- `/replica` endpoint returns 200 only on followers
- Support for read/write routing strategies

## Running Individual Nodes

For debugging or custom deployment:

```bash
# Terminal 1
make start-node1

# Terminal 2
make start-node2

# Terminal 3
make start-node3
```

## Cleanup

```bash
# Remove logs and databases
make clean
```

## Key Features

✅ **Zero-overhead reads**: Sub-millisecond local reads via `get_eventual()`  
✅ **Leader detection**: Health check endpoints for load balancers  
✅ **Automatic failover**: New leader elected in 1-2 seconds  
✅ **Production-ready**: MemFirst persistence with batched flush  
✅ **Customizable**: All ports configurable via CLI

## Next Steps

- **Load balancer setup**: See [HA Deployment Guide](../../../d-engine/src/docs/examples/ha-deployment-load-balancing.md)
- **Scale to 5 nodes**: Add more nodes to tolerate 2 failures
- **Monitoring**: Add Prometheus metrics to track cluster health
- **Custom state machine**: Replace KV API with your business logic

## Implementation Notes

This example demonstrates the recommended pattern for embedded mode HA deployments:

1. Each application instance embeds d-engine
2. Application provides custom HTTP API (not forced to use gRPC)
3. Health check endpoints enable external load balancing
4. Leader detection happens at application layer
5. Business logic decides read/write routing strategy

**This is the Rust equivalent of running 3 application servers with embedded databases, but with Raft consensus for strong consistency.**
