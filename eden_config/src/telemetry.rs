//! Telemetry and observability configuration.
//!
//! Maps to the `[telemetry]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Telemetry and observability configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct TelemetryConfig {
    /// OTLP collector endpoint for Eden service (gRPC, port 4317).
    pub otlp_collector: String,
    /// OTLP HTTP endpoint for trace export (HTTP/protobuf, port 4318).
    /// If empty, defaults to `otlp_collector` with port 4317 replaced by 4318.
    pub otlp_traces_endpoint: String,
    /// OTLP collector endpoint for database telemetry.
    pub otlp_db_collector: String,
    /// OTLP collector endpoint for Engine service.
    pub engine_otlp_collector: String,
    /// Log level (trace, debug, info, warn, error).
    pub log_level: String,
    /// Export tracing spans to the configured OTLP collector.
    pub otlp_export_enabled: bool,
    /// Export fast-telemetry metrics to DogStatsD.
    pub dogstatsd_enabled: bool,
    /// DogStatsD UDP endpoint, for example `127.0.0.1:8125`.
    pub dogstatsd_endpoint: String,
    /// Export Eden metrics, traces, and logs to the internal ClickHouse database.
    pub clickhouse_enabled: bool,
    /// Embedded DuckDB analytics storage used by `embedded-db` builds.
    pub duckdb: DuckDbTelemetryConfig,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_collector: "http://localhost:4317".to_string(),
            otlp_traces_endpoint: "http://localhost:4318".to_string(),
            otlp_db_collector: String::new(),
            engine_otlp_collector: "http://localhost:4317".to_string(),
            log_level: "info".to_string(),
            otlp_export_enabled: false,
            dogstatsd_enabled: false,
            dogstatsd_endpoint: String::new(),
            clickhouse_enabled: true,
            duckdb: DuckDbTelemetryConfig::default(),
        }
    }
}

impl TelemetryConfig {
    /// Get the OTLP HTTP traces endpoint.
    ///
    /// If `otlp_traces_endpoint` is set, returns it directly.
    /// Otherwise, derives from `otlp_collector` by replacing port 4317 with 4318.
    pub fn traces_endpoint(&self) -> String {
        if !self.otlp_traces_endpoint.is_empty() {
            return self.otlp_traces_endpoint.clone();
        }
        // Derive from gRPC endpoint by replacing port
        self.otlp_collector.replace(":4317", ":4318")
    }
}

/// Embedded analytics storage configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct DuckDbTelemetryConfig {
    /// DuckDB database file path. `~` is resolved against `$HOME`.
    pub path: String,
    /// DuckDB memory cap, for example `512MB`.
    pub memory_limit: String,
    /// DuckDB temporary spill directory. `~` is resolved against `$HOME`.
    pub temp_directory: String,
    /// Maximum temporary spill space.
    pub max_temp_directory_size: String,
    /// DuckDB checkpoint threshold.
    pub checkpoint_threshold: String,
    /// Background checkpoint cadence.
    pub checkpoint_interval_secs: u64,
    /// Retention for metric/request analytics tables.
    pub analytics_retention_days: u32,
    /// Retention for telemetry log rows.
    pub logs_retention_days: u32,
    /// Retention for telemetry trace rows.
    pub traces_retention_days: u32,
}

impl Default for DuckDbTelemetryConfig {
    fn default() -> Self {
        Self {
            path: "~/.eden/telemetry.duckdb".to_string(),
            memory_limit: "512MB".to_string(),
            temp_directory: "~/.eden/duckdb-tmp".to_string(),
            max_temp_directory_size: "2GB".to_string(),
            checkpoint_threshold: "64MB".to_string(),
            checkpoint_interval_secs: 60,
            analytics_retention_days: 30,
            logs_retention_days: 14,
            traces_retention_days: 14,
        }
    }
}
