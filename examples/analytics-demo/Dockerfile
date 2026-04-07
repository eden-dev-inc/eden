# Multi-stage development Dockerfile for better caching
FROM rust:1.90 as dependencies

# Configurable Cargo features for Redis command types, such as "redis-string"
# (default) or "redis-json".
ARG CARGO_FEATURES="redis-string"

WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml ./

# Create dummy source and build dependencies
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/bin/traffic-client.rs && \
    echo "fn main() {}" > src/bin/runtime-validator.rs
RUN cargo build --release --features "${CARGO_FEATURES}"
RUN rm src/main.rs src/bin/traffic-client.rs src/bin/runtime-validator.rs

# Final stage
FROM rust:1.90

ARG CARGO_FEATURES="redis-string"

WORKDIR /usr/src/app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency artifacts from previous stage
COPY --from=dependencies /usr/src/app/target target
COPY --from=dependencies /usr/local/cargo /usr/local/cargo
COPY Cargo.toml ./

# Copy source code
COPY src ./src

# Build application (dependencies already cached)
RUN cargo build --release --features "${CARGO_FEATURES}"

EXPOSE 3000 3100

# Create non-root user for security
RUN useradd -r -s /bin/false appuser
RUN chown -R appuser:appuser /usr/src/app

USER appuser

CMD ["./target/release/analytics-server"]
