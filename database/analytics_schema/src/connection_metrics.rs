//! Connection metrics row types for ClickHouse analytics.
//!
//! Periodic snapshots of endpoint, proxy, and client connection counts.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Serialize;

/// Table name for connection metrics.
pub const CONNECTION_METRICS_TABLE: &str = "analytics.connection_metrics";

/// Row for analytics.connection_metrics.
///
/// Note: `Vec<(String, i64)>` is used for the Map columns because
/// clickhouse-rs RowBinary serializer does not support `HashMap`.
#[derive(Debug, Clone, Serialize, Row)]
pub struct ConnectionMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_connections_total: i64,
    pub endpoint_connections_by_type: Vec<(String, i64)>,
    pub endpoint_connections_by_uuid: Vec<(String, i64)>,
    pub endpoint_connections_in_use: i64,
    pub endpoint_connections_in_use_by_uuid: Vec<(String, i64)>,
    pub proxy_connections_total: i64,
    pub proxy_connections_by_endpoint: Vec<(String, i64)>,
    /// Proxy sessions broken down by client IP — key is `"client_ip|interlay_id"`
    /// so one client talking through multiple interlays stays disambiguated.
    pub proxy_connections_by_client: Vec<(String, i64)>,
    pub active_requests: i64,
}
