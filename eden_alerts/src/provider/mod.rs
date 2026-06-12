//! Data providers for fetching analytics data from storage backends.
//!
//! This module defines the `AnalyticsProvider` trait and implementations
//! for querying ClickHouse analytics tables.

#[path = "clickhouse.rs"]
pub mod clickhouse_provider;
mod types;

pub use clickhouse_provider::{ClickhouseConfig, ClickhouseProvider};
pub use types::{AlertSnapshot, AntiPatternRow, EndpointHealth, HourlyRollup, SignalRow};

use ::clickhouse::Row;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

/// Error type for provider operations.
#[derive(Debug, thiserror::Error)]
pub enum ProviderError {
    #[error("clickhouse error: {0}")]
    Clickhouse(String),
    #[error("query error: {0}")]
    Query(String),
    #[error("connection error: {0}")]
    Connection(String),
}

impl From<::clickhouse::error::Error> for ProviderError {
    fn from(err: ::clickhouse::error::Error) -> Self {
        ProviderError::Clickhouse(err.to_string())
    }
}

/// Time window for queries.
#[derive(Debug, Clone)]
pub struct TimeWindow {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

impl TimeWindow {
    pub fn new(start: DateTime<Utc>, end: DateTime<Utc>) -> Self {
        Self { start, end }
    }

    /// Create a window from now going back the specified duration.
    pub fn last_minutes(minutes: i64) -> Self {
        let end = Utc::now();
        let start = end - chrono::Duration::minutes(minutes);
        Self { start, end }
    }
}

/// Analytics data provider trait.
///
/// Implementations fetch data from storage backends (e.g., ClickHouse)
/// for alert rule evaluation.
#[async_trait]
pub trait AnalyticsProvider: Send + Sync {
    /// Fetch endpoint health metrics for the given window.
    async fn fetch_endpoint_health(&self, window: &TimeWindow) -> Result<Vec<EndpointHealth>, ProviderError>;

    /// Fetch anti-pattern occurrences for the given window.
    async fn fetch_anti_patterns(&self, window: &TimeWindow) -> Result<Vec<AntiPatternRow>, ProviderError>;

    /// Fetch hourly rollup data for the given window.
    async fn fetch_hourly_rollups(&self, window: &TimeWindow) -> Result<Vec<HourlyRollup>, ProviderError>;

    /// Fetch signal events (errors, slow queries, etc.) for the given window.
    async fn fetch_signals(&self, window: &TimeWindow) -> Result<Vec<SignalRow>, ProviderError>;

    /// Fetch hot keys detected in the given window.
    async fn fetch_hot_keys(&self, window: &TimeWindow, min_hits: u64) -> Result<Vec<HotKeyRow>, ProviderError>;

    /// Fetch error spikes in the given window.
    async fn fetch_error_spikes(&self, window: &TimeWindow, min_errors: u64) -> Result<Vec<ErrorSpikeRow>, ProviderError>;
}

/// Hot key detection result.
#[derive(Debug, Clone, Row, serde::Serialize, serde::Deserialize)]
pub struct HotKeyRow {
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub key_pattern: String,
    pub hit_count: u64,
    pub window_start: DateTime<Utc>,
}

/// Error spike detection result.
#[derive(Debug, Clone, Row, serde::Serialize, serde::Deserialize)]
pub struct ErrorSpikeRow {
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub error_count: u64,
    pub total_requests: u64,
    pub error_rate: f64,
    pub window_start: DateTime<Utc>,
}
