# ------------------------
# Builder Stage (Optimized for ARM64)
# ------------------------
FROM rust:slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    pkg-config \
    libzstd-dev \
    protobuf-compiler \
    build-essential \
    ca-certificates \
    curl \
    clang \
    libclang-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

# Build context is the d-engine workspace root.
# Run: docker build -t demo:v0.2.4 -f examples/three-nodes-standalone/docker/Dockerfile .
WORKDIR /build

# Disable sccache: .cargo/config.toml sets rustc-wrapper=sccache for local dev,
# but sccache is not available in CI/Docker.
ENV RUSTC_WRAPPER=""

# Copy workspace source (.dockerignore excludes **/target/)
COPY . .

# Build the three-nodes-standalone demo binary.
# Cache mounts persist the cargo registry and compiled artifacts across builds:
# - First build: ~10 min (RocksDB compiles from source)
# - Subsequent builds: ~30s (only changed Rust code recompiles)
WORKDIR /build/examples/three-nodes-standalone
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/examples/three-nodes-standalone/target \
    cargo build --release && \
    cp target/release/demo /usr/local/bin/demo

# ------------------------
# Runtime Stage (ARM64 Optimized)
# ------------------------
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openssh-server \
    ca-certificates \
    curl \
    wget \
    unzip \
    sudo \
    iptables \
    iproute2 \
    libc6 \
    libfuse3-3 \
    fuse3 \
    tzdata && \
    rm -rf /var/lib/apt/lists/* && \
    mkdir -p /var/run/sshd && \
    ssh-keygen -A

# Configure system
RUN echo 'root:root' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config

# Set timezone
ENV TZ=Asia/Shanghai
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

# Install ARM64 Promtail
RUN curl -LO "https://github.com/grafana/loki/releases/download/v3.0.0/promtail-linux-arm64.zip" && \
    unzip promtail-linux-arm64.zip && \
    mv promtail-linux-arm64 /usr/local/bin/promtail && \
    chmod +x /usr/local/bin/promtail && \
    rm promtail-linux-arm64.zip

COPY --from=builder /usr/local/bin/demo /usr/local/bin/demo

# Create directories
RUN mkdir -p /app/{logs,db,snapshots} /etc/promtail

# Promtail config path relative to workspace root
COPY examples/three-nodes-standalone/docker/monitoring/promtail/config.yml /etc/promtail/config.yml

WORKDIR /app

CMD ["sh", "-c", "/usr/sbin/sshd -D & CONFIG_PATH=$CONFIG_PATH LOG_DIR=$LOG_DIR METRICS_PORT=$METRICS_PORT DB_PATH=/app/db/$ID RUST_LOG=demo=$LOG_LEVEL,d_engine=$LOG_LEVEL,hyper=warn,sled=warn demo & promtail --config.file=/etc/promtail/config.yml > /app/logs/promtail.log 2>&1"]
