// Configuration Management
//
// This module handles all configuration options for the analytics demo.
// Configuration can be provided via command line arguments or environment variables.
// This allows for easy runtime customization of load patterns and database connections.

use clap::Parser;

/// Command line and environment variable configuration for the analytics demo
///
/// All fields can be set via command line flags (--events-per-second) or
/// environment variables (EVENTS_PER_SECOND). Environment variables take precedence.
#[derive(Parser, Debug, Clone)]
#[clap(name = "analytics-demo")]
#[clap(about = "A database migration demo with realistic load patterns")]
pub struct Config {
    /// PostgreSQL connection URL with connection pooling
    /// Format: postgresql://user:password@host:port/database
    #[clap(long, env = "DATABASE_URL", default_value = "postgresql://postgres:postgres@localhost:5432/analytics")]
    pub database_url: String,

    /// Redis connection URL for caching layer
    /// Format: redis://host:port or redis://host:port/db_number
    #[clap(long, env = "REDIS_URL", default_value = "redis://localhost:6379")]
    pub redis_url: String,

    /// HTTP server bind address for metrics and health endpoints
    /// Use 0.0.0.0 for Docker containers, 127.0.0.1 for local development
    #[clap(long, env = "BIND_ADDRESS", default_value = "0.0.0.0:3000")]
    pub bind_address: String,

    /// Number of events to generate per second
    /// Higher values create more write load and cache invalidation
    #[clap(long, env = "EVENTS_PER_SECOND", default_value = "1000")]
    pub events_per_second: u64,

    /// Number of analytics queries to execute per second
    /// This represents dashboard loads, reports, and real-time queries
    #[clap(long, env = "QUERIES_PER_SECOND", default_value = "2000")]
    pub queries_per_second: u64,

    /// Number of tenant organizations to simulate
    /// More organizations create more diverse query patterns
    #[clap(long, env = "ORGANIZATIONS", default_value = "100")]
    pub organizations: u32,

    /// Number of users per organization for realistic data distribution
    /// Higher values create more unique user activity patterns
    #[clap(long, env = "USERS_PER_ORG", default_value = "5000")]
    pub users_per_org: u32,

    /// Target cache hit ratio as a percentage (0-100)
    /// Used by cache warmup worker to maintain realistic performance
    #[clap(long, env = "CACHE_HIT_TARGET", default_value = "85")]
    pub cache_hit_target: u8,

    /// Read to write ratio percentage (e.g., 80 = 80% reads, 20% writes)
    /// Reflects typical analytics workload patterns
    #[clap(long, env = "READ_WRITE_RATIO", default_value = "80")]
    pub read_write_ratio: u8,

    /// Enable migration simulation features (planned for future enhancement)
    /// When enabled, simulates database failover and performance degradation
    #[clap(long, env = "ENABLE_MIGRATION_SIM")]
    pub enable_migration_sim: bool,

    /// Delay in seconds before triggering migration simulation
    /// Allows baseline metrics to be established before disruption
    #[clap(long, env = "MIGRATION_DELAY", default_value = "30")]
    pub migration_delay: u64,
}