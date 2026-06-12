//! Database connection configuration (Redis, PostgreSQL, ClickHouse).
//!
//! Maps to the `[databases]` section in `eden.toml`.

mod clickhouse;
mod postgres;
mod redis;

pub use clickhouse::InternalClickhouseConfig;
pub use postgres::InternalPostgresConfig;
pub use redis::InternalRedisConfig;

use serde::{Deserialize, Serialize};

/// Aggregated database configurations.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct DatabasesConfig {
    pub redis: InternalRedisConfig,
    pub postgres: InternalPostgresConfig,
    pub clickhouse: InternalClickhouseConfig,
}
