//! RBAC Redis-to-Postgres sync worker configuration.
//!
//! Maps to the `[rbac_pg_sync]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// RBAC PG sync worker tuning configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RbacPgSyncConfig {
    /// Number of stream entries read in one call.
    pub batch_size: usize,
    /// Maximum block timeout for stream reads in milliseconds.
    pub block_ms: usize,
    /// Delay before retrying after worker failure in milliseconds.
    pub retry_delay_ms: u64,
    /// Prefix used when generating the consumer name.
    pub consumer_prefix: String,
    /// How many days to keep RBAC tombstone rows before purging them.
    /// Set to 0 to disable automatic cleanup.
    pub tombstone_retention_days: u32,
}

impl Default for RbacPgSyncConfig {
    fn default() -> Self {
        Self {
            batch_size: 128,
            block_ms: 5_000,
            retry_delay_ms: 1_000,
            consumer_prefix: "eden".to_string(),
            tombstone_retention_days: 90,
        }
    }
}
