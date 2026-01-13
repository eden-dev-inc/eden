# Analytics Demo - Redis Migration Showcase

A high-performance Rust application that simulates realistic analytics workloads to demonstrate Redis migration capabilities with 10K+ QPS support.

## Features

- **Self-contained**: Runs entirely with Docker - no external dependencies
- **High Throughput**: Supports 10K+ queries per second
- **Configurable Load**: Adjust events/queries per second via environment variables
- **Realistic Patterns**: Simulates real analytics platform with proper caching
- **Full Telemetry**: Prometheus metrics + Grafana dashboards
- **Production-Ready**: Connection pooling, proper error handling, structured logging

## Quick Start

```bash
# Start the full stack
make run

# Check that everything is healthy
make health
```

Access the services:
- **Grafana Dashboard**: http://localhost:3001 (admin/admin)
- **Prometheus**: http://localhost:9090
- **App Metrics**: http://localhost:3000/metrics
- **Health Check**: http://localhost:3000/health

## Configuration

Configure load patterns via environment variables:

```bash
# Ultra-high load scenario (10k+ req/s total)
make run-ultra-load
```
```bash
# High load scenario (1000+ req/s total)
make run-high-load
```
```bash
# Medium load (good for demos)
make run-demo-load
```
```bash
# Low load scenario for testing
make run-low-load
```

Available configuration options:

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_PER_SECOND` | 1000 | Events generated per second (Redis INCR operations) |
| `QUERIES_PER_SECOND` | 10000 | Analytics queries per second |
| `ORGANIZATIONS` | 500 | Number of tenant organizations to simulate |
| `USERS_PER_ORG` | 1000 | Users per organization |
| `CACHE_HIT_TARGET` | 95 | Target cache hit ratio % |
| `MAX_WORKERS` | 500 | Maximum number of query workers |
| `REDIS_POOL_SIZE` | 100 | Redis connection pool size |
| `CACHE_TTL` | 300 | Default cache TTL in seconds |
| `WARMUP_INTERVAL` | 300 | Cache warmup/refresh interval in seconds |
| `TIME_BUCKETS` | 24 | Number of time buckets for hourly analytics |

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Rust App      │────│   PostgreSQL    │    │     Redis       │
│  (Analytics)    │    │   (Primary DB)  │    │    (Cache)      │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │
         ▼
┌─────────────────┐    ┌─────────────────┐
│   Prometheus    │────│    Grafana      │
│   (Metrics)     │    │  (Dashboard)    │
└─────────────────┘    └─────────────────┘
```

## Data Model

Simulates a SaaS analytics platform:

- **Organizations**: Multi-tenant companies
- **Users**: Users within each organization
- **Events**: User activity (page views, clicks, conversions)
- **Analytics**: Real-time aggregations and reports

## Load Patterns

**Event Distribution**:
- 60% Page Views
- 28% Clicks
- 10% Conversions
- 1.5% Sign-ups
- 0.5% Purchases

**Query Patterns**:
- 70% Dashboard overview (high cache hit)
- 20% Filtered queries (moderate cache hit)
- 10% Real-time stats (never cached)

## Metrics

Key metrics exposed for monitoring migration impact:

**Performance**:
- Query duration percentiles (p50, p95, p99)
- Redis connection pool utilization
- Cache hit/miss ratios
- Events and queries per second

**Business**:
- Active organizations
- Conversion rates
- User engagement

**Infrastructure**:
- Redis operation latency
- Database query performance
- Memory and CPU utilization

## Migration Demo Usage

1. **Start Baseline Load**:
   ```bash
   make run
   # Observe steady state metrics in Grafana
   ```

2. **Simulate Migration**:
   - Start your Redis migration tool
   - Point to new Redis instance
   - Observe metrics during transition

3. **Key Observations**:
   - Query latency during migration
   - Cache invalidation patterns
   - Zero-downtime migration success
   - Performance recovery post-migration

## Development

### Local Development

```bash
# Install dependencies (requires local Postgres + Redis)
cargo build

# Run with custom config
cargo run -- --events-per-second 50 --queries-per-second 100

# Docker development
make dev
```

### Customization

Modify workload patterns in `src/generators.rs`:
- Event type distributions
- Cache key strategies
- Query complexity

Add new metrics in `src/metrics.rs`:
- Custom business metrics
- Infrastructure monitoring
- Performance counters

## Commands

```bash
make build          # Build Docker images
make run            # Start full stack (high load by default)
make run-ultra-load # Ultra-high load (2000 events/s, 8000 queries/s)
make run-high-load  # High load (300 events/s, 700 queries/s)
make run-demo-load  # Demo load (50 events/s, 150 queries/s)
make run-low-load   # Low load (10 events/s, 20 queries/s)
make stop           # Stop all services
make clean          # Remove volumes and images
make logs           # View application logs
make logs-all       # View logs from all services
make health         # Check service health
make metrics        # Show sample metrics
make stats          # Show Docker stats
make grafana        # Open Grafana in browser
make prometheus     # Open Prometheus in browser
make dev-local      # Run locally with external DB
make test           # Run tests
make fmt            # Format code
make lint           # Run clippy linter
```

## Monitoring

Default Grafana dashboards show:
- Request latency and throughput
- Redis performance metrics
- Cache efficiency
- Business KPIs
- System resource usage

Login to Grafana (admin/admin) and explore the pre-configured Analytics Dashboard to see real-time metrics.

## License

MIT License - feel free to adapt for your migration demos!
