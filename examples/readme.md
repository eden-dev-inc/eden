# Eden Examples

A collection of practical examples demonstrating how to build tools and integrations with the Eden platform.

## What is Eden?

Eden is a unified data infrastructure platform that provides seamless access to databases, APIs, and AI services through a powerful orchestration layer. It offers enterprise-grade security, scalability, and observability while enabling developers to build sophisticated data operations with ease.

## Examples

### [adam-demo](./adam-demo/)

A cross-database AI inference demo for Eden's ADAM feature. Spins up PostgreSQL, MongoDB, Redis, ClickHouse, and Qdrant via Docker Compose, then automatically populates them with enterprise datasets from Hugging Face.

**Features:**
- Five databases running locally with Docker
- Auto-initialization with realistic enterprise data (e-commerce events, product catalog, financial transactions, support tickets)
- Vector embeddings for semantic search
- Pre-configured for Eden ADAM cross-database queries
- Includes a `bird` vertical for replaying validated BIRD benchmark SQL through Eden after importing SQLite into Postgres

**Usage:**
```bash
cd adam-demo
make up          # Start all databases and load data
make status      # Check data counts across all DBs
```

---

### [analytics-demo](./analytics-demo/)

A high-performance Redis migration demo capable of 10K+ queries per second, built with Rust.

**Features:**
- Redis-only hot path for maximum throughput
- Simulates multi-tenant analytics workloads with configurable organizations and users
- Query and event simulation with realistic data patterns
- Prometheus metrics endpoint for monitoring
- Configurable QPS, event rates, and worker pools

**Usage:**
```bash
cd analytics-demo
cargo build --release
./target/release/analytics-demo --redis-url redis://localhost:6379
```

**Configuration:**
| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--redis-url` | `REDIS_URL` | `redis://localhost:6370` | Redis connection URL |
| `--bind-address` | `BIND_ADDRESS` | `0.0.0.0:3000` | HTTP server address |
| `--queries-per-second` | `QUERIES_PER_SECOND` | `10000` | Target QPS |
| `--events-per-second` | `EVENTS_PER_SECOND` | `1000` | Events to simulate per second |
| `--organizations` | `ORGANIZATIONS` | `500` | Number of tenant orgs |
| `--users-per-org` | `USERS_PER_ORG` | `1000` | Users per organization |
| `--max-workers` | `MAX_WORKERS` | `500` | Maximum query workers |
| `--redis-pool-size` | `REDIS_POOL_SIZE` | `100` | Redis connection pool size |

**Endpoints:**
- `/metrics` - Prometheus metrics
- `/health` - Health check

---

### [redis-migrator](./redis-migrator/)

A unified Redis migration toolkit with Eden setup, bulk population, workload generation, and an integrated observer TUI.

**Features:**
- Configure Eden organizations, endpoints, interlays, and migrations
- Bulk-load Redis data through the interlay
- Run sustained read/write workloads
- Monitor source and destination Redis in an integrated TUI
- Launch population directly from the observer panel
- Support local Docker setups and TLS/password cloud Redis
- Consolidates the former split `redis-populator` and `redis-observer` tools

**Usage:**
```bash
cd redis-migrator
cargo run --release -- observe \
  --source-url redis://localhost:6378 \
  --dest-url redis://localhost:6377 \
  --api-url http://localhost:8000 \
  --eden-source-url redis://host.docker.internal:6378 \
  --eden-dest-url redis://host.docker.internal:6377 \
  --interlay-port 5731
```

## Getting Started

1. **Clone this repository**:
   ```bash
   git clone https://github.com/your-org/eden.git
   cd eden/examples
   ```

2. **Navigate to an example** and follow its specific instructions.

3. **Configure your Eden credentials** (if needed):
   ```bash
   export EDEN_API_URL="https://api.eden.com"
   export EDEN_ORG_ID="your-organization-id"
   export EDEN_JWT_TOKEN="your-jwt-token"
   ```

## Resources

- **[Eden Platform Docs](https://docs.eden.com)**: Complete platform documentation
- **[API Reference](https://api.eden.com/docs)**: Comprehensive API documentation
- **[GitHub Issues](https://github.com/your-org/eden/issues)**: Bug reports and feature requests

## License

This project is licensed under the MIT License - see the [LICENSE](../LICENSE) file for details.
