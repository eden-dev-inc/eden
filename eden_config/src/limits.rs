//! System-wide limits and thresholds.
//!
//! Maps to the `[limits]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// System-wide limits and thresholds.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LimitsConfig {
    /// Rate limit in milliseconds (add 1 request to quota every N ms).
    /// Example: 100 = allow 10 requests per second, but not more than 1 in 100ms.
    /// Set to 0 to disable rate limiting.
    pub rate_limit_ms: u64,
    /// JWT token expiry time in seconds.
    pub jwt_expiry_secs: u64,
    /// Redis cache TTL in seconds.
    pub redis_cache_ttl_secs: u64,
    /// ClickHouse connection pool size.
    pub clickhouse_pool_size: usize,
    /// Maximum Redis pool connections cap.
    pub redis_pool_max_connections_cap: u32,
    /// Tools service timeout in seconds.
    pub tools_service_timeout_secs: u64,
    /// Maximum number of keys in a Redis bulk operation batch.
    pub redis_batch_count: usize,
    /// Maximum number of bytes in a Redis bulk operation batch.
    pub redis_batch_size_bytes: usize,
}

impl Default for LimitsConfig {
    fn default() -> Self {
        Self {
            rate_limit_ms: 100,
            jwt_expiry_secs: 900,
            redis_cache_ttl_secs: 3600,
            clickhouse_pool_size: 8,
            redis_pool_max_connections_cap: 64,
            tools_service_timeout_secs: 10,
            redis_batch_count: 20_000,
            redis_batch_size_bytes: 1_000_000_000,
        }
    }
}
