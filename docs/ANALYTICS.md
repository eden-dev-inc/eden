# Analytics Architecture

This distribution uses a single analytics surface backed by the telemetry store and endpoint metadata polling.

What this doc owns: data flow between telemetry producers, metadata pollers, ClickHouse storage, and dashboard/API queries.

What this doc does not own: collector implementations and capability gating (`POLLING_METADATA.md`), API request/response contracts (`api_docs.md`), metric names and dashboard queries (`METRICS.md`).

## Data Collection

Eden has two data sources that feed analytics:

1. Service and gateway telemetry. Request counters, latency histograms, connection gauges, traces, and logs are emitted through the shared telemetry wrapper. Exporters write these signals to the configured store.
2. Metadata polling. Scheduler-driven collectors query each database endpoint directly for server-side state such as Redis `INFO`, PostgreSQL `pg_stat`, MongoDB `serverStatus`, Oracle dynamic performance views, Cassandra system tables, and ClickHouse system tables. Poll snapshots are written to ClickHouse per collection cycle when poll export is enabled.

## Data Flow

```text
Client request
  -> Eden gateway/interlay
  -> telemetry counters, histograms, traces, logs
  -> telemetry exporter
  -> analytics tables
  -> dashboard and /api/v1/analytics/* queries

Metadata tick
  -> endpoint connection
  -> collector queries
  -> MetadataBatch
  -> Redis publication / service cache
  -> ClickHouse poll tables
  -> dashboard and /api/v1/endpoints/*/metadata/history
```

The metadata scheduler uses fixed intervals: High every 60 seconds, Medium every 30 minutes, and Low every 24 hours by default. Endpoint-specific capability checks decide which jobs run during a tick.

## Telemetry Store

All dashboard analytics are queried through Eden APIs. The API layer hides whether the runtime is backed by the embedded store or an external ClickHouse deployment.

### Generic Telemetry Tables

The telemetry exporter writes metric rows with a common schema to these tables:

| Table | What it stores |
|---|---|
| `analytics.proxy` | Gateway/interlay request and bridge metrics |
| `analytics.endpoint` | Endpoint runtime metrics |
| `analytics.eden` | Service-level metrics |
| `analytics.iam` | IAM/auth/session metrics |
| `analytics.metadata` | Metadata polling metrics |
| `analytics.snapshot` | Snapshot operation metrics |
| `analytics.workload` | Workload sizing and profile metrics |
| `analytics.validator` | Tool/command validation metrics |
| `analytics.analytics` | Analytics API and internal analytics metrics |
| `analytics.traces` | Exported spans |
| `analytics.logs` | Structured logs |

### Endpoint Poll Tables

Poll tables have one row per endpoint per frequency tier per collection cycle. They keep endpoint-state snapshots for 90 days.

| Table | What it stores |
|---|---|
| `redis_poll_metrics` | Memory, CPU, clients, replication, cluster, persistence, server info |
| `postgres_poll_metrics` | Connections, locks, cache/index hit ratios, replication, WAL, bgwriter |
| `mongo_poll_metrics` | Connections, lock queue, WiredTiger cache, replication lag, oplog, sharding |
| `oracle_poll_metrics` | Sessions, tablespace, SGA/PGA, wait events, redo log, ASM |
| `cassandra_poll_metrics` | Thread pools, compaction, key cache, read/write latency, streaming |
| `clickhouse_poll_metrics` | Queries, merges, parts, replication queue, memory, Keeper/ZooKeeper |

## Dashboard Query Model

Dashboard pages should query Eden service APIs, not endpoint databases directly.

- `/api/v1/analytics/series` returns time-bucketed metric series from the telemetry store.
- `/api/v1/analytics/export` returns raw metric, trace, and log rows with filters.
- `/api/v1/analytics/overview` returns a live in-process summary for fast dashboard loading.
- `/api/v1/endpoints/{endpoint}/metadata/history` returns stored poll history when available.

Verbose request-stream analytics are not included in this distribution. The compatibility routes return explicit unavailable responses so clients can distinguish a removed capability from a missing route.

## Source Of Truth Index

| Topic | Authoritative doc | What it covers |
|---|---|---|
| Collector implementations | `docs/POLLING_METADATA.md` | Per-DB-kind collectors, capability gating, backoff |
| Scheduler and job execution | `docs/POLLING_METADATA.md` | Tick loops, concurrency, timeouts, publishing |
| API request/response contracts | `docs/api_docs.md` | Query params, response shapes, RBAC requirements |
| Metric names for dashboards | `docs/METRICS.md` | OpenTelemetry metric catalog |
| ClickHouse DDL | `database/analytics_schema/sql/analytics/` | Canonical table definitions, materialized views, TTLs |
