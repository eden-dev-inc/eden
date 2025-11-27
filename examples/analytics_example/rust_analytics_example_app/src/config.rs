// Configuration Management
//
// Enhanced configuration for high-throughput analytics simulation with 10K+ QPS.
// Includes Redis pooling, worker tuning, and cache strategy configuration.

use clap::Parser;

/// Command line and environment variable configuration for the analytics demo
#[derive(Parser, Debug, Clone)]
#[clap(name = "analytics-demo")]
#[clap(about = "A high-performance database migration demo with 10K+ QPS")]
pub struct Config {
    /// PostgreSQL connection URL with connection pooling
    #[clap(long, env = "DATABASE_URL", default_value = "postgresql://postgres:postgres@localhost:5434/analytics")]
    pub database_url: String,

    /// Redis connection URL for caching layer
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6370")]
    pub redis_url: String,

    /// HTTP server bind address for metrics and health endpoints
    #[clap(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3000")]
    pub bind_address: String,

    /// Number of events to generate per second
    #[clap(long, env = "EVENTS_PER_SECOND", default_value = "1000")]
    pub events_per_second: u64,

    /// Number of analytics queries to execute per second (10K+ supported)
    #[clap(long, env = "QUERIES_PER_SECOND", default_value = "10000")]
    pub queries_per_second: u64,

    /// Number of tenant organizations to simulate
    #[clap(long, env = "ORGANIZATIONS", default_value = "100")]
    pub organizations: u32,

    /// Number of users per organization for realistic data distribution
    #[clap(long, env = "USERS_PER_ORG", default_value = "5000")]
    pub users_per_org: u32,

    /// Target cache hit ratio as a percentage (0-100)
    #[clap(long, env = "CACHE_HIT_TARGET", default_value = "95")]
    pub cache_hit_target: u8,

    /// Read to write ratio percentage (e.g., 80 = 80% reads, 20% writes)
    #[clap(long, env = "READ_WRITE_RATIO", default_value = "80")]
    pub read_write_ratio: u8,

    /// Maximum number of query workers to spawn (removed cap for 10K+ QPS)
    #[clap(long, env = "MAX_WORKERS", default_value = "500")]
    pub max_workers: usize,

    /// Redis connection pool size for high concurrency
    #[clap(long, env = "REDIS_POOL_SIZE", default_value = "100")]
    pub redis_pool_size: u32,

    /// PostgreSQL connection pool size
    #[clap(long, env = "DB_POOL_SIZE", default_value = "100")]
    pub db_pool_size: u32,

    /// Default cache TTL in seconds for most queries
    #[clap(long, env = "CACHE_TTL", default_value = "900")]
    pub cache_ttl: u64,

    /// Cache warmup interval in seconds
    #[clap(long, env = "WARMUP_INTERVAL", default_value = "60")]
    pub warmup_interval: u64,

    /// Enable granular time-series caching (hourly/daily metrics)
    #[clap(long, env = "ENABLE_TIMESERIES_CACHE", default_value = "true")]
    pub enable_timeseries_cache: bool,

    /// Enable per-user analytics caching
    #[clap(long, env = "ENABLE_USER_CACHE", default_value = "true")]
    pub enable_user_cache: bool,

    /// Enable page-level performance caching
    #[clap(long, env = "ENABLE_PAGE_CACHE", default_value = "true")]
    pub enable_page_cache: bool,

    /// Enable migration simulation features
    #[clap(long, env = "ENABLE_MIGRATION_SIM")]
    pub enable_migration_sim: bool,

    /// Delay in seconds before triggering migration simulation
    #[clap(long, env = "MIGRATION_DELAY", default_value = "30")]
    pub migration_delay: u64,

    /// Enable Redis pipelining for batch operations
    #[clap(long, env = "ENABLE_REDIS_PIPELINE", default_value = "true")]
    pub enable_redis_pipeline: bool,

    /// Number of time buckets for hourly analytics (24 hours = 24 buckets)
    #[clap(long, env = "TIME_BUCKETS", default_value = "24")]
    pub time_buckets: u32,
}