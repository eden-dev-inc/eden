# eden_config

Centralized, type-safe configuration management for Eden MDBS using Figment for layered sources and ArcSwap for lock-free global access.

## Overview

The `eden_config` crate replaces scattered `std::env::var()` calls across the Eden codebase with a single, validated configuration system. It is used by:

- **`eden_service`** - Feature flags, analytics, snapshots, limits
- **Endpoint runtimes** - Network and database connection configuration
- **Telemetry exporters** - Export destinations and buffering behavior

## Architecture

### ArcSwap Global

Configuration is stored in a `Lazy<ArcSwap<EdenConfig>>` static. Readers get wait-free, lock-free access (~2-5ns per load). Updates swap the entire config atomically, so no partial state is ever visible.

### ConfigFieldRef Zero-Copy Pattern

Field accessors (e.g., `features()`, `analytics()`) return a `ConfigFieldRef<T>` smart pointer that holds an `Arc<EdenConfig>` and derefs directly to the requested field. No cloning is needed for read access.

### Figment 5-Layer Priority

Configuration is merged in priority order (lowest to highest):

1. **Compiled defaults**: `EdenConfig::default()`
2. **TOML config file**: `eden.toml` (or path from `--config`)
3. **Legacy flat env vars**: e.g., `EDEN_PORT` (52 mappings in `compat.rs`)
4. **Nested env vars**: `EDEN__` prefix with `__` separator (e.g., `EDEN__SERVICES__EDEN__PORT`)
5. **CLI arguments**: `--port`, `--log-level`, `--otlp-collector`

## Quick Start

### Initialization

The global config activates lazily on first accessor call. For explicit initialization with CLI args:

```rust
use eden_config::{CliArgs, load_config, install_config_with_args};
use clap::Parser;

let args = CliArgs::parse();
let config = load_config(Some(&args)).expect("config load failed");
install_config_with_args(config, Some(args));
```

### Reading Config

```rust
use eden_config::{features, limits, databases};

if features().analytics_enabled {
    println!("rate limit: {}ms", limits().rate_limit_ms);
    println!("redis: {}:{}", databases().redis.host, databases().redis.port);
}
```

### Runtime Updates

```rust
use eden_config::update_config;

// Mutate-and-swap (validates before committing)
update_config(|c| c.limits.rate_limit_ms = 200).expect("validation failed");

// Reload from file (preserves CLI overrides)
eden_config::reload_config().expect("reload failed");
```

## Configuration Reference

### [features]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `analytics_enabled` | `bool` | `true` | `EDEN_ANALYTICS_ENABLED` | Master switch for analytics data collection |
| `policy_enforcement_mode` | `string` | `"observe"` | `EDEN_POLICY_ENFORCEMENT_MODE` | Security policy mode: `observe`, `warn`, `block` |
| `redis_psync` | `bool` | `false` | `REDIS_PSYNC` | Enable Redis PSYNC at proxy layer |

### [limits]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `rate_limit_ms` | `u64` | `100` | `EDEN_RATE_LIMIT` | Rate limit interval in milliseconds |
| `jwt_expiry_secs` | `u64` | `900` | `EDEN_JWT_EXPIRY_S` | JWT token expiry in seconds |
| `redis_cache_ttl_secs` | `u64` | `3600` | `REDIS_CACHE_TTL` | Redis cache TTL in seconds |
| `clickhouse_pool_size` | `usize` | `8` | `CLICKHOUSE_POOL_SIZE` | ClickHouse connection pool size |
| `redis_pool_max_connections_cap` | `u32` | `256` | `EDEN_REDIS_POOL_MAX_CONNECTIONS_CAP` | Maximum Redis pool connections cap |
| `tools_service_timeout_secs` | `u64` | `10` | `TOOLS_SERVICE_TIMEOUT_SECS` | ATI service timeout in seconds |
| `redis_batch_count` | `usize` | `20000` | `REDIS_BATCH_COUNT` | Max keys per Redis bulk operation batch |
| `redis_batch_size_bytes` | `usize` | `1000000000` | `REDIS_BATCH_SIZE` | Max bytes per Redis bulk operation batch |

### [telemetry]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `otlp_collector` | `String` | `"http://localhost:4317"` | `EDEN_OTLP_COLLECTOR` | OTLP collector for Eden service |
| `otlp_traces_endpoint` | `String` | `"http://localhost:4318"` | `EDEN__TELEMETRY__OTLP_TRACES_ENDPOINT` | OTLP HTTP traces endpoint used when OTLP export is enabled |
| `otlp_export_enabled` | `bool` | `false` | `EDEN_OTLP_EXPORT_ENABLED` | Export tracing spans to the configured OTLP collector |
| `otlp_db_collector` | `String` | `""` | `EDEN_OTLP_DB_COLLECTOR` | OTLP collector for database telemetry |
| `engine_otlp_collector` | `String` | `"http://localhost:4317"` | `ENGINE_OTLP_COLLECTOR` | OTLP collector for Engine service |
| `log_level` | `String` | `"info"` | `EDEN_LOG_LEVEL` | Log level: trace, debug, info, warn, error |
| `dogstatsd_enabled` | `bool` | `false` | `EDEN_DOGSTATSD_ENABLED` | Export fast-telemetry metrics to DogStatsD |
| `dogstatsd_endpoint` | `String` | `""` | `EDEN_DOGSTATSD_ENDPOINT` | DogStatsD UDP endpoint used when DogStatsD export is enabled |
| `clickhouse_enabled` | `bool` | `true` | `EDEN_CLICKHOUSE_TELEMETRY_ENABLED` | Sync metrics, traces, and logs to the internal ClickHouse database |

### [services.eden]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `host` | `String` | `"localhost"` | `EDEN_HOST` | Eden service bind host |
| `port` | `u16` | `8000` | `EDEN_PORT` | Eden service bind port |
| `jwt_secret` | `Option<String>` | `None` | `EDEN_JWT_SECRET` | Base64-encoded JWT secret |
| `node_uuid` | `Option<String>` | `None` | `EDEN_NODE_UUID` | Node UUID for cluster identification |
| `new_org_token` | `Option<String>` | `None` | `EDEN_NEW_ORG_TOKEN` | Token for creating new organizations |
| `gateway_cpu_affinity` | `string` | `"auto"` | `EDEN_GATEWAY_CPU_AFFINITY` | Tokio gateway runtime affinity mode: `auto` = best-effort perf pinning, `off` = scheduler decides, `perf` = require detectable perf cores or exit |

### [services.engine]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `host` | `String` | `"localhost"` | `ENGINE_HOST` | Engine service host |
| `port` | `u16` | `8001` | `ENGINE_PORT` | Engine service port |

### [services.llm]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `provider` | `Option<String>` | `None` | `EDEN_INTERNAL_LLM_PROVIDER` | LLM provider name |
| `model` | `Option<String>` | `None` | `EDEN_INTERNAL_LLM_MODEL` | LLM model name |
| `api_key` | `Option<String>` | `None` | `EDEN_INTERNAL_LLM_API_KEY` | LLM API key |
| `base_url` | `Option<String>` | `None` | `EDEN_INTERNAL_LLM_BASE_URL` | LLM API base URL |
| `system_prompt` | `Option<String>` | `None` | `EDEN_INTERNAL_LLM_SYSTEM_PROMPT` | LLM system prompt |
| `temperature` | `Option<f32>` | `None` | `EDEN_INTERNAL_LLM_TEMPERATURE` | LLM temperature |
| `max_tokens` | `Option<u32>` | `None` | `EDEN_INTERNAL_LLM_MAX_TOKENS` | LLM max tokens |

### [databases.redis]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `host` | `String` | `"localhost"` | `REDIS_HOST` | Redis host |
| `port` | `u16` | `6379` | `REDIS_PORT` | Redis port |
| `username` | `String` | `""` | `REDIS_USER` | Redis username |
| `password` | `String` | `""` | `REDIS_PASSWORD` | Redis password |
| `db_number` | `u8` | `0` | `REDIS_DB_NUMBER` | Redis database number |

### [databases.postgres]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `host` | `String` | `"localhost"` | `POSTGRES_HOST` | PostgreSQL host |
| `port` | `u16` | `5432` | `POSTGRES_PORT` | PostgreSQL port |
| `username` | `String` | `"postgres"` | `POSTGRES_USER` | PostgreSQL username |
| `password` | `String` | `""` | `POSTGRES_PASSWORD` | PostgreSQL password |
| `database` | `String` | `"postgres"` | `POSTGRES_DB_NAME` | PostgreSQL database name |

### [databases.clickhouse]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `url` | `String` | `"http://localhost:8123"` | `CLICKHOUSE_URL` | ClickHouse HTTP URL |
| `username` | `String` | `""` | `CLICKHOUSE_USER` | ClickHouse username |
| `password` | `String` | `""` | `CLICKHOUSE_PASSWORD` | ClickHouse password |
| `database` | `Option<String>` | `None` | `CLICKHOUSE_DATABASE` / `CLICKHOUSE_DB` | ClickHouse database name |

ClickHouse telemetry uses the existing ClickHouse connection and writes grouped
metric rows to `analytics.proxy`, `analytics.endpoint`, `analytics.eden`,
`analytics.iam`, `analytics.metadata`, `analytics.migration`,
`analytics.snapshot`, `analytics.workload`, and `analytics.analytics`. Traces
and logs are written to `analytics.traces` and `analytics.logs`.

The service exposes these rows through
`GET /api/v1/analytics/telemetry/{metrics|traces|logs}` and the equivalent
`/api/v1/analytics/clickhouse/{metrics|traces|logs}` alias. Supported URL
filters include `from`, `to`, `range`, `limit`, `offset`, `order`, metric
`group`, common identity fields such as `service_name` and `node_uuid`, and map
filters such as `label.endpoint_uuid=...`, `tag.interlay_uuid=...`, or
`attr.http.method=...`. Metrics and logs resolve `label.*`, `labels.*`,
`tag.*`, and `tags.*` against the ClickHouse `labels` map. Traces resolve
`attr.*`, `attrs.*`, `attribute.*`, `attributes.*`, `tag.*`, and `tags.*`
against the ClickHouse `attributes` map.

Example:
`GET /api/v1/analytics/telemetry/metrics?group=proxy&metric_name=proxy_lane_pool_lanes_open&tag.interlay_uuid=...&range=15m`

### [snapshot]

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `path` | `Option<String>` | `None` | `EDEN_SNAPSHOT_PATH` | Path to snapshot file |
| `password` | `Option<String>` | `None` | `EDEN_SNAPSHOT_PASSWORD` | Snapshot encryption password |
| `dir` | `Option<String>` | `None` | `EDEN_SNAPSHOT_DIR` | Snapshot storage directory |

### [licensing]

Legacy section name for optional external metering compatibility. Open-source
deployments do not require these fields.

| Field | Type | Default | Legacy Env Var | Description |
|-------|------|---------|----------------|-------------|
| `license_key` | `Option<String>` | `None` | `EDEN_LICENSE_KEY` | Optional signed entitlement token |
| `cluster_uid` | `Option<String>` | `None` | `EDEN_CLUSTER_UID` | Optional cluster unique identifier |
| `phone_home_url` | `String` | `""` | `EDEN_PHONE_HOME_URL` | Optional external metering base URL |
| `heartbeat_interval_secs` | `u64` | `86400` | `EDEN_HEARTBEAT_INTERVAL_SECS` | External sync interval (seconds) |
| `disabled` | `bool` | `false` | `EDEN_PHONE_HOME_DISABLED` | Disable external compatibility exports |
| `metering_enabled` | `bool` | `false` | `EDEN_METERING_ENABLED` | Enable metering exporter |
| `metering_endpoint_url` | `Option<String>` | `None` | `EDEN_METERING_ENDPOINT_URL` | Full metering endpoint URL override |
| `metering_ingest_api_key` | `Option<String>` | `None` | `EDEN_METERING_INGEST_API_KEY` | Dedicated bearer token for metering ingestion (`metering_enabled` has no effect if this is unset) |
| `metering_flush_interval_secs` | `u64` | `60` | `EDEN_METERING_FLUSH_INTERVAL_SECS` | Metering flush/window interval |
| `metering_max_batch_size` | `usize` | `100` | `EDEN_METERING_MAX_BATCH_SIZE` | Max events per metering batch |
| `metering_retry_max_attempts` | `u32` | `5` | `EDEN_METERING_RETRY_MAX_ATTEMPTS` | Retry attempts for transient metering failures |
| `metering_retry_base_delay_ms` | `u64` | `500` | `EDEN_METERING_RETRY_BASE_DELAY_MS` | Base retry backoff |
| `metering_retry_max_delay_ms` | `u64` | `30000` | `EDEN_METERING_RETRY_MAX_DELAY_MS` | Max retry backoff |

### [analytics]

The analytics section contains nested sub-sections for sampling, audit, stream, ingestion, metadata collection, and Redis-specific analytics. See `eden.example.toml` at the workspace root for the full structure with defaults and descriptions.

Key sub-sections:
- `[analytics.sampling.burst]`: Anomaly-triggered burst capture windows
- `[analytics.sampling.always_on]`: Always-active analysis (PII, dangerous commands, slow queries)
- `[analytics.sampling.force_sample]`: Force-sample triggers (errors, slow queries, dangerous/write commands)
- `[analytics.sampling.discovery]`: Timer-triggered discovery windows for template reverse-engineering
- `[analytics.sampling.divergence]`: JS divergence anomaly detection
- `[analytics.audit]`: Audit trail (commands, services, flush interval)
- `[analytics.stream]`: Real-time analytics stream
- `[analytics.ingestion]`: Ingestion loop settings (rollup, live flush timeout, shutdown, blocked commands)
- `[analytics.metadata]`: Metadata collection scheduler (intervals, timeouts, backoff)
- `[analytics.redis.anti_patterns]`: Aggregate anti-pattern detection thresholds
- `[analytics.redis.recommendations]`: Recommendation engine thresholds

## CLI Arguments

| Argument | Type | Default | Maps To |
|----------|------|---------|---------|
| `--config`, `-c` | `PathBuf` | `eden.toml` | Config file path |
| `--port` | `Option<u16>` | (none) | `services.eden.port` |
| `--log-level` | `Option<String>` | (none) | `telemetry.log_level` |
| `--otlp-collector` | `Option<String>` | (none) | `telemetry.otlp_collector` |

## API Reference

### Config Loading

- `load_config(args: Option<&CliArgs>) -> Result<EdenConfig, ConfigError>`: Load config from file with optional CLI overrides
- `install_config(config: EdenConfig)`: Install pre-loaded config into global store
- `install_config_with_args(config: EdenConfig, args: Option<CliArgs>)`: Install config and CLI args (recommended for main.rs)
- `install_default_config()`: Install default config (testing only)

### Accessors

All accessors return smart pointers that deref to the config type (zero-copy):

- `config() -> Guard<Arc<EdenConfig>>`: Full config
- `features() -> impl Deref<Target = FeatureFlags>`
- `analytics() -> impl Deref<Target = AnalyticsConfig>`
- `limits() -> impl Deref<Target = LimitsConfig>`
- `telemetry() -> impl Deref<Target = TelemetryConfig>`
- `databases() -> impl Deref<Target = DatabasesConfig>`
- `services() -> impl Deref<Target = ServicesConfig>`
- `snapshot() -> impl Deref<Target = SnapshotConfig>`
- `licensing() -> impl Deref<Target = LicensingClientConfig>`

### Mutation

- `update_config(mutator: impl FnOnce(&mut EdenConfig)) -> Result<(), ConfigError>`: Clone-mutate-validate-swap
- `reload_config() -> Result<(), ConfigError>`: Reload from file, preserving CLI overrides

### Convenience Setters

- `EdenConfig::set_analytics_enabled(bool)`: Toggle analytics
- `EdenConfig::set_rate_limit_ms(u64)`: Update rate limit
- `EdenConfig::set_log_level(String)`: Update log level

## Validation Rules

- `services.eden.port` and `services.engine.port` must be non-zero
- `limits.clickhouse_pool_size` must be > 0
- `analytics.sampling.burst.*`: window, cooldown, max_requests must be > 0 when enabled
- `analytics.sampling.discovery.*`: window, interval, cooldown, max_requests must be > 0 when enabled
- `analytics.sampling.divergence`: ewma_alpha in (0,1), dirichlet_beta > 0, warn <= critical threshold, multipliers >= 1.0
- `analytics.audit.flush_interval_secs` must be > 0
- `analytics.stream.interval_secs` must be > 0 when enabled
- `analytics.redis.anti_patterns`: large_response_bytes and high_fanout_threshold > 0, rates in [0,1]
- `analytics.redis.recommendations`: min_observation_windows and min_total_requests > 0, ratios in [0,1], stale_days > 0, oversized_value_bytes > 0, ram_price >= 0
- `analytics.ingestion`: flush timeouts > 0, total shutdown timeout >= shutdown flush timeout, blocked_command_max > 0
- `analytics.metadata`: all intervals and timeouts > 0, backoff_factor > 0
- `licensing`: `license_key` and `cluster_uid` must be set together when used, external sync interval > 0, metering flush/batch/retry values > 0, and retry max delay >= base delay
- `analytics.sampling.always_on.pii_detection` requires `features.analytics_enabled`

## Migration Guide

**Before** (scattered env vars):
```rust
let port: u16 = std::env::var("EDEN_PORT")
    .unwrap_or_else(|_| "8000".to_string())
    .parse()
    .unwrap();
```

**After** (eden_config):
```rust
let port = eden_config::services().eden.port;
```

## Error Handling

`ConfigError` has three variants:

- `LoadError(String)`: General config loading failure
- `InvalidValue(String)`: Validation constraint violation
- `Figment(figment::Error)`: Figment deserialization/merge error

On startup, the global `CONFIG` lazy static panics if loading fails, ensuring misconfigurations are caught immediately.
