# Makefile for Single-Node to Multi-Node Dynamic Expansion Testing

.PHONY: build start-node1 join-node2 join-node3 clean clean-logs help
.DEFAULT_GOAL := help

# ===============================
# Global Variables
# ===============================
LOG_LEVEL ?= debug


# On macOS with Homebrew: auto-detect compression lib paths to skip bundled C++
# compilation of RocksDB dependencies, which fails under macOS 26 + Xcode 26
# (Clang 16 lacks __builtin_ctzg/__builtin_clzg from LLVM 18+ SDK headers).
# brew --prefix resolves correctly on both Apple Silicon (/opt/homebrew) and
# Intel Mac (/usr/local). Silently no-ops when brew or a lib is absent.
SNAPPY_PREFIX    := $(shell brew --prefix snappy 2>/dev/null)
LZ4_PREFIX       := $(shell brew --prefix lz4    2>/dev/null)
ZSTD_PREFIX      := $(shell brew --prefix zstd   2>/dev/null)
BREW_ROCKSDB_ENV :=

ifneq ($(SNAPPY_PREFIX),)
ifneq ($(wildcard $(SNAPPY_PREFIX)/lib),)
  BREW_ROCKSDB_ENV += SNAPPY_LIB_DIR=$(SNAPPY_PREFIX)/lib
endif
endif
ifneq ($(LZ4_PREFIX),)
  BREW_ROCKSDB_ENV += LZ4_LIB_DIR=$(LZ4_PREFIX)/lib
endif
ifneq ($(ZSTD_PREFIX),)
  BREW_ROCKSDB_ENV += ZSTD_LIB_DIR=$(ZSTD_PREFIX)/lib
endif

# ===============================
# Build Targets
# ===============================
build:
	@echo "Building release binary..."
	$(BREW_ROCKSDB_ENV) cargo build --release --jobs 4

# ===============================
# Single Node Bootstrap
# ===============================
start-node1:
	@echo "=== Starting Node 1 (Single-Node Bootstrap) ==="
	@echo "This node will start as a standalone leader "
	@echo ""
	CONFIG_PATH=config/n1 \
	DB_PATH="./db/1" \
	LOG_DIR="./logs/1" \
	METRICS_PORT=8081 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo

# ===============================
# Dynamic Expansion - Add Node 2
# ===============================
join-node2:
	@echo "=== Joining Node 2 (Dynamic Expansion) ==="
	@echo "This node will join as Learner and auto-promote to Voter when caught up"
	@echo "Expected flow:"
	@echo "  1. Node 2 discovers Node 1 (leader)"
	@echo "  2. Node 2 sends JoinRequest to Node 1"
	@echo "  3. Node 1 commits AddNode config change"
	@echo "  4. Node 2 syncs via AppendEntries (raft log replication)"
	@echo "  5. Node 2 auto-promotes to Voter when match_index catches up"
	@echo ""
	CONFIG_PATH=config/n2 \
	DB_PATH="./db/2" \
	LOG_DIR="./logs/2" \
	METRICS_PORT=8082 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo

# ===============================
# Dynamic Expansion - Add Node 3
# ===============================
join-node3:
	@echo "=== Joining Node 3 (Dynamic Expansion) ==="
	@echo "This node will join after Node 2 has been promoted"
	@echo "Expected flow:"
	@echo "  1. Node 3 discovers Node 1 (leader)"
	@echo "  2. Node 3 sends JoinRequest to Node 1"
	@echo "  3. Node 1 commits AddNode config change (requires 2/3 quorum)"
	@echo "  4. Node 3 syncs via AppendEntries"
	@echo "  5. Node 3 auto-promotes to Voter when caught up"
	@echo ""
	CONFIG_PATH=config/n3 \
	DB_PATH="./db/3" \
	LOG_DIR="./logs/3" \
	METRICS_PORT=8083 \
	RUST_LOG=demo=$(LOG_LEVEL),d_engine=$(LOG_LEVEL),timing=$(LOG_LEVEL) \
	RUST_BACKTRACE=1 \
	./target/release/demo

# ===============================
# Log Monitoring
# ===============================
tail-node1:
	@echo "Watching Node 1 logs..."
	tail -f logs/1/d.log

tail-node2:
	@echo "Watching Node 2 logs..."
	tail -f logs/2/d.log

tail-node3:
	@echo "Watching Node 3 logs..."
	tail -f logs/3/d.log

# ===============================
# Utilities
# ===============================
clean:
	@echo "Cleaning all artifacts..."
	cargo clean
	rm -rf logs/*
	rm -rf db/*

clean-logs:
	@echo "Cleaning logs and database only..."
	rm -rf logs/*
	rm -rf db/*

help:
	@echo "Single-Node Dynamic Expansion Testing Commands:"
	@echo ""
	@echo "Build:"
	@echo "  make build              - Build release binary"
	@echo ""
	@echo "Testing Workflow (run in separate terminals):"
	@echo "  make start-node1        - Step 1: Start single-node cluster (leader)"
	@echo "  make join-node2         - Step 2: Join node 2 as learner (auto-promote)"
	@echo "  make join-node3         - Step 3: Join node 3 as learner (auto-promote)"
	@echo ""
	@echo "Log Monitoring:"
	@echo "  make tail-node1         - Watch node 1 logs"
	@echo "  make tail-node2         - Watch node 2 logs"
	@echo "  make tail-node3         - Watch node 3 logs"
	@echo ""
	@echo "Cleanup:"
	@echo "  make clean              - Remove all build artifacts and data"
	@echo "  make clean-logs         - Remove only logs and database"
	@echo ""
	@echo "Testing Steps:"
	@echo "  1. Open 3 terminal windows"
	@echo "  2. Terminal 1: make build && make start-node1"
	@echo "  3. Wait for Node 1 to become leader (check logs)"
	@echo "  4. Terminal 2: make join-node2"
	@echo "  5. Watch Node 2 join as Learner and promote to Voter"
	@echo "  6. Terminal 3: make join-node3"
	@echo "  7. Watch Node 3 join and promote"
	@echo ""
	@echo "Key Log Messages to Watch:"
	@echo "  - Node 1: 'Node X successfully added as learner'"
	@echo "  - Node 2/3: 'Learner state joined cluster successfully'"
	@echo "  - Node 1: 'Promoting learner X to voter'"
	@echo "  - Node 2/3: 'Transitioned from Learner to Follower'"
