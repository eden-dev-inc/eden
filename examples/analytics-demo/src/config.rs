// Configuration Management
//
// Runtime-selectable backend configuration for analytics simulation.

use clap::Parser;

use crate::telemetry::{parse_telemetry_provider, TelemetryOptions, TelemetryProvider};

fn parse_bool(value: &str) -> Result<bool, String> {
    value
        .parse::<bool>()
        .map_err(|error| format!("expected true or false, got '{value}': {error}"))
}

/// Command line and environment variable configuration for analytics-server
#[derive(Parser, Debug, Clone)]
#[clap(name = "analytics-server")]
#[clap(
    about = "A high-performance analytics server with runtime-selectable Redis/PostgreSQL backends"
)]
pub struct Config {
    /// Whether the Redis workload backend should be started
    #[clap(
        long,
        env = "REDIS_ENABLED",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    pub redis_enabled: bool,

    /// Redis connection URL for caching layer
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6370")]
    pub redis_url: String,

    /// Whether the PostgreSQL workload backend should be started
    #[clap(
        long,
        env = "POSTGRES_ENABLED",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    pub postgres_enabled: bool,

    /// Test-only escape hatch that allows the server to run without Redis or PostgreSQL.
    #[clap(
        long,
        env = "ALLOW_NO_BACKEND",
        hide = true,
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    pub allow_no_backend: bool,

    /// HTTP server bind address for metrics and health endpoints
    #[clap(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3000")]
    pub bind_address: String,

    /// Number of Redis operations per second (writes + reads)
    #[clap(long, env = "EVENTS_PER_SECOND", default_value = "1000")]
    pub events_per_second: u64,

    /// Target number of unique Redis keys to write before stabilizing at min write ratio
    #[clap(long, env = "REDIS_TARGET_KEYS", default_value = "1000000")]
    pub redis_target_keys: u64,

    /// Number of analytics queries to execute per second (10K+ supported)
    #[clap(long, env = "QUERIES_PER_SECOND", default_value = "10000")]
    pub queries_per_second: u64,

    /// Whether the server should generate its own internal workload
    #[clap(
        long,
        env = "INTERNAL_WORKLOAD_ENABLED",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    pub internal_workload_enabled: bool,

    /// Number of tenant organizations to simulate
    #[clap(long, env = "ORGANIZATIONS", default_value = "100")]
    pub organizations: u32,

    /// Number of users per organization for realistic data distribution
    #[clap(long, env = "USERS_PER_ORG", default_value = "1000")]
    pub users_per_org: u32,

    /// Target cache hit ratio as a percentage (0-100)
    #[clap(long, env = "CACHE_HIT_TARGET", default_value = "95")]
    pub cache_hit_target: u8,

    /// Maximum number of query workers to spawn
    #[clap(long, env = "MAX_WORKERS", default_value = "500")]
    pub max_workers: usize,

    /// Redis connection pool size for high concurrency
    #[clap(long, env = "REDIS_POOL_SIZE", default_value = "100")]
    pub redis_pool_size: u32,

    /// Default cache TTL in seconds for most queries
    #[clap(long, env = "CACHE_TTL", default_value = "300")]
    pub cache_ttl: u64,

    /// Cache warmup/refresh interval in seconds
    #[clap(long, env = "WARMUP_INTERVAL", default_value = "300")]
    pub warmup_interval: u64,

    /// Number of time buckets for hourly analytics (24 hours = 24 buckets)
    #[clap(long, env = "TIME_BUCKETS", default_value = "24")]
    pub time_buckets: u32,

    /// Telemetry provider used by the exporter runtime
    #[clap(
        long = "telemetry-provider",
        alias = "datadog-provider",
        env = "TELEMETRY_PROVIDER",
        default_value = "datadog",
        parse(try_from_str = parse_telemetry_provider)
    )]
    pub telemetry_provider: TelemetryProvider,

    /// Enable telemetry export mode for logs and activity collection
    #[clap(
        long = "telemetry-enabled",
        alias = "datadog-enabled",
        env = "TELEMETRY_ENABLED",
        default_value = "false",
        parse(try_from_str = parse_bool)
    )]
    pub telemetry_enabled: bool,

    /// Telemetry service name attached to exported payloads
    #[clap(
        long = "telemetry-service",
        alias = "datadog-service",
        env = "TELEMETRY_SERVICE",
        default_value = "analytics-server"
    )]
    pub telemetry_service: String,

    /// Telemetry environment tag attached to exported payloads
    #[clap(
        long = "telemetry-env",
        alias = "datadog-env",
        env = "TELEMETRY_ENV",
        default_value = "demo"
    )]
    pub telemetry_env: String,

    /// Telemetry version tag attached to exported payloads
    #[clap(
        long = "telemetry-version",
        alias = "datadog-version",
        env = "TELEMETRY_VERSION",
        default_value = "0.1.0"
    )]
    pub telemetry_version: String,

    /// Telemetry site used by the downstream exporter or agent
    #[clap(
        long = "telemetry-site",
        alias = "datadog-site",
        env = "TELEMETRY_SITE",
        default_value = "datadoghq.com"
    )]
    pub telemetry_site: String,

    /// Optional Datadog API key for a direct Datadog exporter path
    #[clap(
        long = "telemetry-datadog-api-key",
        alias = "datadog-api-key",
        env = "TELEMETRY_DATADOG_API_KEY"
    )]
    pub telemetry_datadog_api_key: Option<String>,

    /// Optional DogStatsD endpoint for fast-telemetry metric export (host:port)
    #[clap(
        long = "telemetry-dogstatsd-endpoint",
        env = "TELEMETRY_DOGSTATSD_ENDPOINT"
    )]
    pub telemetry_dogstatsd_endpoint: Option<String>,

    /// Export interval in seconds for fast-telemetry DogStatsD metrics
    #[clap(
        long = "telemetry-export-interval-seconds",
        env = "TELEMETRY_EXPORT_INTERVAL_SECONDS",
        default_value = "10"
    )]
    pub telemetry_export_interval_seconds: u64,

    /// Optional OpenTelemetry endpoint for OTLP span export
    #[clap(
        long = "telemetry-opentelemetry-endpoint",
        alias = "telemetry-otlp-traces-endpoint",
        env = "TELEMETRY_OPENTELEMETRY_ENDPOINT"
    )]
    pub telemetry_opentelemetry_endpoint: Option<String>,

    /// Export interval in seconds for OpenTelemetry OTLP span export
    #[clap(
        long = "telemetry-otlp-export-interval-seconds",
        env = "TELEMETRY_OTLP_EXPORT_INTERVAL_SECONDS",
        default_value = "10"
    )]
    pub telemetry_otlp_export_interval_seconds: u64,

    /// Request timeout in seconds for OTLP span export
    #[clap(
        long = "telemetry-otlp-timeout-seconds",
        env = "TELEMETRY_OTLP_TIMEOUT_SECONDS",
        default_value = "5"
    )]
    pub telemetry_otlp_timeout_seconds: u64,

    /// Export one query payload every N queries (misses are always exported)
    #[clap(
        long = "telemetry-query-log-every",
        alias = "datadog-query-log-every",
        env = "TELEMETRY_QUERY_LOG_EVERY",
        default_value = "50"
    )]
    pub telemetry_query_log_every: u64,

    /// Number of representative records to include in each exported event batch
    #[clap(
        long = "telemetry-event-sample-size",
        alias = "datadog-event-sample-size",
        env = "TELEMETRY_EVENT_SAMPLE_SIZE",
        default_value = "6"
    )]
    pub telemetry_event_sample_size: usize,

    /// Whether telemetry export should include query payloads
    #[clap(
        long = "telemetry-capture-query-payloads",
        alias = "datadog-capture-query-payloads",
        env = "TELEMETRY_CAPTURE_QUERY_PAYLOADS",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    pub telemetry_capture_query_payloads: bool,

    /// Whether telemetry export should include representative event payloads
    #[clap(
        long = "telemetry-capture-event-payloads",
        alias = "datadog-capture-event-payloads",
        env = "TELEMETRY_CAPTURE_EVENT_PAYLOADS",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    pub telemetry_capture_event_payloads: bool,

    /// Whether telemetry export should include periodic system snapshots
    #[clap(
        long = "telemetry-capture-system-snapshots",
        alias = "datadog-capture-system-snapshots",
        env = "TELEMETRY_CAPTURE_SYSTEM_SNAPSHOTS",
        default_value = "true",
        parse(try_from_str = parse_bool)
    )]
    pub telemetry_capture_system_snapshots: bool,

    /// PostgreSQL host (matches Eden's POSTGRES_HOST env var)
    #[clap(long, env = "POSTGRES_HOST", default_value = "localhost")]
    pub postgres_host: String,

    /// PostgreSQL port (matches Eden's POSTGRES_PORT env var)
    #[clap(long, env = "POSTGRES_PORT", default_value = "5432")]
    pub postgres_port: u16,

    /// PostgreSQL username (matches Eden's POSTGRES_USER env var)
    #[clap(long, env = "POSTGRES_USER", default_value = "postgres")]
    pub postgres_user: String,

    /// PostgreSQL password (matches Eden's POSTGRES_PASSWORD env var)
    #[clap(long, env = "POSTGRES_PASSWORD", default_value = "postgres")]
    pub postgres_password: String,

    /// PostgreSQL database name (matches Eden's POSTGRES_DB_NAME env var)
    #[clap(long, env = "POSTGRES_DB_NAME", default_value = "analytics")]
    pub postgres_database: String,

    /// Full PostgreSQL URL override. If set, takes precedence over individual
    /// POSTGRES_HOST/PORT/USER/PASSWORD/DB_NAME fields.
    /// Format matches Eden's PostgresConnection.url: postgresql://user:pass@host:port/db
    #[clap(long, env = "DATABASE_URL")]
    pub database_url: Option<String>,

    /// PostgreSQL connection pool size (used when POSTGRES_ENABLED=true)
    #[clap(long, env = "DB_POOL_SIZE", default_value = "50")]
    pub db_pool_size: u32,

    /// Number of PostgreSQL query workers (used when POSTGRES_ENABLED=true)
    #[clap(long, env = "PG_QUERY_WORKERS", default_value = "100")]
    pub pg_query_workers: usize,

    /// PostgreSQL events per second for write workers (used when POSTGRES_ENABLED=true)
    #[clap(long, env = "PG_EVENTS_PER_SECOND", default_value = "500")]
    pub pg_events_per_second: u64,
}

impl Config {
    pub fn validate(&self) -> Result<(), String> {
        if !self.redis_enabled && !self.postgres_enabled && !self.allow_no_backend {
            return Err(
                "at least one backend must be enabled: set REDIS_ENABLED=true or POSTGRES_ENABLED=true"
                    .to_string(),
            );
        }

        Ok(())
    }

    pub fn backend_mode_label(&self) -> &'static str {
        match (self.redis_enabled, self.postgres_enabled) {
            (true, true) => "Redis + PostgreSQL",
            (true, false) => "Redis-only",
            (false, true) => "PostgreSQL-only",
            (false, false) => "No backend",
        }
    }

    /// Build the PostgreSQL connection URL.
    /// Uses DATABASE_URL if provided, otherwise constructs from individual fields
    /// (POSTGRES_HOST, POSTGRES_PORT, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DB_NAME).
    /// Output format matches Eden's PostgresConnection.url: postgresql://user:pass@host:port/db
    pub fn postgres_url(&self) -> String {
        if let Some(ref url) = self.database_url {
            url.clone()
        } else {
            format!(
                "postgresql://{}:{}@{}:{}/{}",
                self.postgres_user,
                self.postgres_password,
                self.postgres_host,
                self.postgres_port,
                self.postgres_database,
            )
        }
    }

    pub fn telemetry_options(&self) -> TelemetryOptions {
        TelemetryOptions {
            provider: self.telemetry_provider,
            enabled: self.telemetry_enabled,
            service: self.telemetry_service.clone(),
            environment: self.telemetry_env.clone(),
            version: self.telemetry_version.clone(),
            site: self.telemetry_site.clone(),
            datadog_api_key: self.telemetry_datadog_api_key.clone(),
            dogstatsd_endpoint: self.telemetry_dogstatsd_endpoint.clone(),
            dogstatsd_export_interval_seconds: self.telemetry_export_interval_seconds,
            opentelemetry_endpoint: self.telemetry_opentelemetry_endpoint.clone(),
            otlp_export_interval_seconds: self.telemetry_otlp_export_interval_seconds,
            otlp_export_timeout_seconds: self.telemetry_otlp_timeout_seconds,
            query_log_every: self.telemetry_query_log_every,
            event_sample_size: self.telemetry_event_sample_size,
            capture_query_payloads: self.telemetry_capture_query_payloads,
            capture_event_payloads: self.telemetry_capture_event_payloads,
            capture_system_snapshots: self.telemetry_capture_system_snapshots,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Config;
    use crate::telemetry::TelemetryProvider;

    fn base_config() -> Config {
        Config {
            redis_enabled: true,
            redis_url: "redis://localhost:6379".to_string(),
            postgres_enabled: false,
            allow_no_backend: false,
            bind_address: "127.0.0.1:3000".to_string(),
            events_per_second: 100,
            redis_target_keys: 1000,
            queries_per_second: 250,
            internal_workload_enabled: false,
            organizations: 10,
            users_per_org: 50,
            cache_hit_target: 95,
            max_workers: 16,
            redis_pool_size: 8,
            cache_ttl: 300,
            warmup_interval: 300,
            time_buckets: 24,
            telemetry_provider: TelemetryProvider::Datadog,
            telemetry_enabled: false,
            telemetry_service: "analytics-server".to_string(),
            telemetry_env: "test".to_string(),
            telemetry_version: "0.0.0-test".to_string(),
            telemetry_site: "datadoghq.com".to_string(),
            telemetry_datadog_api_key: None,
            telemetry_dogstatsd_endpoint: None,
            telemetry_export_interval_seconds: 10,
            telemetry_opentelemetry_endpoint: None,
            telemetry_otlp_export_interval_seconds: 10,
            telemetry_otlp_timeout_seconds: 5,
            telemetry_query_log_every: 10,
            telemetry_event_sample_size: 2,
            telemetry_capture_query_payloads: true,
            telemetry_capture_event_payloads: true,
            telemetry_capture_system_snapshots: false,
            postgres_host: "localhost".to_string(),
            postgres_port: 5432,
            postgres_user: "postgres".to_string(),
            postgres_password: "postgres".to_string(),
            postgres_database: "analytics".to_string(),
            database_url: None,
            db_pool_size: 5,
            pg_query_workers: 2,
            pg_events_per_second: 10,
        }
    }

    #[test]
    fn validate_rejects_when_all_backends_are_disabled() {
        let mut config = base_config();
        config.redis_enabled = false;

        let error = config.validate().expect_err("config should be invalid");
        assert!(error.contains("at least one backend must be enabled"));
    }

    #[test]
    fn postgres_url_prefers_database_url_override() {
        let mut config = base_config();
        config.database_url = Some("postgresql://override:pass@db.example:5432/demo".to_string());

        assert_eq!(
            config.postgres_url(),
            "postgresql://override:pass@db.example:5432/demo"
        );
    }
}
