use std::time::Duration;

/// Scheduler intervals for metadata collection.
#[derive(Debug, Clone)]
pub struct SchedulerIntervals {
    pub high: Duration,
    pub medium: Duration,
    pub low: Duration,
}

/// Backoff configuration for repeated scheduler failures.
#[derive(Debug, Clone)]
pub struct BackoffConfig {
    pub base: Duration,
    pub factor: u32,
    pub max: Duration,
}

/// Metadata collection configuration.
///
/// Constructed by the caller from `eden_config::MetadataCollectionConfig`
/// (this crate intentionally does not depend on `eden_config`).
#[derive(Debug, Clone)]
pub struct MetadataConfig {
    pub intervals: SchedulerIntervals,
    pub job_timeout: Duration,
    /// Per-endpoint wall-clock timeout. Caps the total time a single
    /// `process_endpoint` call can take across all its jobs.
    pub endpoint_timeout: Duration,
    /// Maximum number of endpoints processed concurrently per tick.
    pub max_concurrent_endpoints: usize,
    pub backoff: BackoffConfig,
    pub redis_prefix: String,
    /// Optional default query timeout collectors can reuse.
    pub collector_query_timeout: Duration,
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            intervals: SchedulerIntervals {
                high: Duration::from_secs(60),
                medium: Duration::from_secs(1800),
                low: Duration::from_secs(86400),
            },
            job_timeout: Duration::from_secs(60),
            endpoint_timeout: Duration::from_secs(120),
            max_concurrent_endpoints: 4,
            backoff: BackoffConfig {
                base: Duration::from_secs(30),
                factor: 2,
                max: Duration::from_secs(900),
            },
            redis_prefix: "metadata:".to_string(),
            collector_query_timeout: Duration::from_secs(5),
        }
    }
}
