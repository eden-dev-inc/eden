//! ClickHouse row types for Eden telemetry sync.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::{Deserialize, Serialize};

pub mod tables {
    pub const PROXY: &str = "analytics.proxy";
    pub const ENDPOINT: &str = "analytics.endpoint";
    pub const EDEN: &str = "analytics.eden";
    pub const IAM: &str = "analytics.iam";
    pub const METADATA: &str = "analytics.metadata";
    pub const SNAPSHOT: &str = "analytics.snapshot";
    pub const WORKLOAD: &str = "analytics.workload";
    pub const VALIDATOR: &str = "analytics.validator";
    pub const ANALYTICS: &str = "analytics.analytics";
    pub const TRACES: &str = "analytics.traces";
    pub const LOGS: &str = "analytics.logs";
}

#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct MetricRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub organization_uuid: String,
    pub service_name: String,
    pub node_uuid: String,
    pub metric_name: String,
    pub metric_kind: String,
    pub value: Option<f64>,
    pub count: Option<u64>,
    pub sum: Option<f64>,
    pub bucket_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub labels: Vec<(String, String)>,
    pub scope: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct MetricExportRow {
    pub metric_group: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub organization_uuid: String,
    pub service_name: String,
    pub node_uuid: String,
    pub metric_name: String,
    pub metric_kind: String,
    pub value: Option<f64>,
    pub count: Option<u64>,
    pub sum: Option<f64>,
    pub bucket_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
    pub labels: Vec<(String, String)>,
    pub scope: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct TraceRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub organization_uuid: String,
    pub service_name: String,
    pub node_uuid: String,
    pub trace_id: String,
    pub span_id: String,
    pub parent_span_id: String,
    pub span_name: String,
    pub span_kind: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub start_time: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub end_time: DateTime<Utc>,
    pub duration_ns: u64,
    pub status: String,
    pub status_message: String,
    pub attributes: Vec<(String, String)>,
    pub events_json: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct LogRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub timestamp: DateTime<Utc>,
    pub service_name: String,
    pub node_uuid: String,
    pub level: String,
    pub audience: String,
    pub message: String,
    pub trace_id: String,
    pub span_id: String,
    pub feature: String,
    pub function: String,
    pub file: String,
    pub line: Option<u32>,
    pub eden_node_uuid: String,
    pub organization_uuid: String,
    pub organization_id: String,
    pub user_uuid: String,
    pub user_id: String,
    pub endpoint_uuid: String,
    pub endpoint_id: String,
    pub endpoint_kind: String,
    pub error_code: String,
    pub error_category: String,
    pub labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Row)]
pub struct CountRow {
    pub total: u64,
}
