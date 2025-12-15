# Multi-stage development Dockerfile for better caching
FROM rust:1.90 as dependencies

WORKDIR /usr/src/app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml Cargo.lock ./

# Create dummy source and build dependencies
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release
RUN rm src/main.rs

# Final stage
FROM rust:1.90

WORKDIR /usr/src/app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency artifacts from previous stage
COPY --from=dependencies /usr/src/app/target target
COPY --from=dependencies /usr/local/cargo /usr/local/cargo
COPY Cargo.toml Cargo.lock ./

# Copy source code
COPY src ./src

# Build application (dependencies already cached)
RUN cargo build --release

EXPOSE 3000

# Create non-root user for security
RUN useradd -r -s /bin/false appuser
RUN chown -R appuser:appuser /usr/src/app

USER appuser

CMD ["./target/release/analytics-demo"]