# Eden Examples

A collection of practical examples demonstrating how to build tools and integrations with the Eden platform.

## What is Eden?

Eden is a unified data infrastructure platform that provides seamless access to databases, APIs, and AI services through a powerful orchestration layer. It offers enterprise-grade security, scalability, and observability while enabling developers to build sophisticated data operations with ease.

## Examples

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

### [redis-observer](./redis-observer/)

A terminal dashboard for monitoring multiple Redis instances in real-time with integrated migration support, built with Rust.

**Features:**
- Monitor key counts, ops/sec, and connected clients across Redis instances
- Visual coverage analysis comparing key distribution between databases
- Live charts showing historical trends for keys and operations
- Automatic coverage checks every 15 seconds
- **Migration support**: Configure and trigger Redis migrations via Eden API
- Auto-setup of migration infrastructure (endpoints, interlays, migrations)

**Usage:**
```bash
cd redis-observer
cargo run -- <source> <dest> [api_endpoint]
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `source` | Source Redis as `host:port` or just `port` (default host: `172.24.2.218`) |
| `dest` | Destination Redis as `host:port` or just `port` (default host: `172.24.2.218`) |
| `api_endpoint` | Eden API endpoint (default: `http://localhost:8000`) |

**Examples:**
```bash
cargo run -- 6379 6380                           # Both use default host
cargo run -- 192.168.1.10:6379 192.168.1.20:6380 # Different hosts
cargo run -- 6379 10.0.0.5:6380                  # Mixed
```

**Controls:**
| Key | Action |
|-----|--------|
| `q` | Quit |
| `c` | Force coverage check |
| `v` | Toggle ops/sec chart |
| `s` | Start migration setup (connect to Eden API) |
| `m` | Trigger migration |
| `r` | Refresh migration status |

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
