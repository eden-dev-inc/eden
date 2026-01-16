// Configuration Management
//
// Simplified configuration for Redis-only analytics simulation.
// Postgres configuration retained but not used in hot path.

use clap::Parser;

/// Command line and environment variable configuration for the analytics demo
#[derive(Parser, Debug, Clone)]
#[clap(name = "analytics-demo")]
#[clap(about = "A high-performance Redis migration demo with 10K+ QPS")]
pub struct Config {
    /// Redis connection URL for caching layer
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6370")]
    pub redis_url: String,

    /// HTTP server bind address for metrics and health endpoints
    #[clap(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3000")]
    pub bind_address: String,

    /// Number of events to simulate per second (Redis INCR operations)
    #[clap(long, env = "EVENTS_PER_SECOND", default_value = "1000")]
    pub events_per_second: u64,

    /// Number of analytics queries to execute per second (10K+ supported)
    #[clap(long, env = "QUERIES_PER_SECOND", default_value = "10000")]
    pub queries_per_second: u64,

    /// Number of tenant organizations to simulate
    #[clap(long, env = "ORGANIZATIONS", default_value = "500")]
    pub organizations: u32,

    /// Number of users per organization for realistic data distribution
    #[clap(long, env = "USERS_PER_ORG", default_value = "1000")]
    pub users_per_org: u32,

    /// Target cache hit ratio as a percentage (0-100)
    #[clap(long, env = "CACHE_HIT_TARGET", default_value = "95")]
    pub cache_hit_target: u8,

    /// Maximum number of query workers to spawn
    #[clap(long, env = "MAX_WORKERS", default_value = "50")]
    pub max_workers: usize,

    /// Redis connection pool size for high concurrency
    #[clap(long, env = "REDIS_POOL_SIZE", default_value = "10")]
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

    /// Data validation sample rate (0.0 to 1.0). At high load, only this fraction
    /// of operations will be validated. Set to 1.0 for full validation, 0.01 for 1%.
    #[clap(long, env = "VALIDATION_SAMPLE_RATE", default_value = "0.01")]
    pub validation_sample_rate: f64,
}