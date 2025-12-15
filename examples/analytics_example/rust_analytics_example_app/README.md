# Analytics Demo - Database Migration Showcase

A high-performance Rust application that simulates realistic analytics workloads to demonstrate database migration capabilities under load.

## Features

- **Self-contained**: Runs entirely with Docker - no external dependencies
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
EVENTS_PER_SECOND=2000 QUERIES_PER_SECOND=8000 docker-compose up -d
```
```bash
# High load scenario (1000+ req/s total)
EVENTS_PER_SECOND=300 QUERIES_PER_SECOND=700 docker-compose up -d
```
```bash
# Medium load (good for demos)
EVENTS_PER_SECOND=50 QUERIES_PER_SECOND=150 docker-compose up -d
```
```bash
# Low load scenario for testing
EVENTS_PER_SECOND=10 QUERIES_PER_SECOND=20 docker-compose up -d
```

Available configuration options:

| Variable | Default | Description |
|----------|---------|-------------|
| `EVENTS_PER_SECOND` | 100 | Events generated per second |
| `QUERIES_PER_SECOND` | 200 | Analytics queries per second |
| `ORGANIZATIONS` | 50 | Number of organizations |
| `USERS_PER_ORG` | 1000 | Users per organization |
| `CACHE_HIT_TARGET` | 85 | Target cache hit ratio % |
| `READ_WRITE_RATIO` | 80 | Read percentage (80% reads, 20% writes) |

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
- Database connection pool utilization
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
   - Start your database migration tool
   - Point to new database instance
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
make build      # Build Docker images
make run        # Start full stack
make stop       # Stop all services  
make clean      # Remove volumes and images
make logs       # View application logs
make health     # Check service health
```

## Monitoring

Default Grafana dashboards show:
- Request latency and throughput
- Database performance metrics
- Cache efficiency
- Business KPIs
- System resource usage

Login to Grafana (admin/admin) and explore the pre-configured Analytics Dashboard to see real-time metrics.

## License

MIT License - feel free to adapt for your migration demos!