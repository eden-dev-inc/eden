//! Wire analytics storage types for ClickHouse.
//!
//! Row types for the protocol-neutral analytics tables:
//! - command_rollups: Aggregated command metrics
//! - endpoint_metrics: Endpoint metrics snapshots
//! - target_pattern_rollups: Target-pattern cost rollups

use chrono::{DateTime, Utc};
use clickhouse::Row;
use serde::Serialize;

/// Row for analytics.command_rollups.
#[derive(Debug, Clone, Serialize, Row)]
pub struct CommandRollupRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub service: String,
    pub command_id: u16,
    pub command: String,
    pub category: String,
    pub request_count: u64,
    pub success_count: u64,
    pub error_count: u64,
    pub slow_count: u64,
    pub dangerous_count: u64,
    pub write_command_count: u64,
    pub latency_sum: u64,
    pub latency_sample_count: u64,
    pub latency_sample_sum_us: f64,
    pub latency_sample_sumsq_us2: f64,
    pub latency_min: u64,
    pub latency_max: u64,
    pub latency_histogram: Vec<u64>,
    pub request_bytes_sum: u64,
    pub response_bytes_sum: u64,
    pub request_size_histogram: Vec<u64>,
    pub response_size_histogram: Vec<u64>,
    pub target_count_sum: u64,
    pub cost_sum: u64,
    pub cache_hit_count: u64,
    pub cache_miss_count: u64,
    pub redirect_count: u64,
    pub server_error_count: u64,
    pub client_error_count: u64,
    /// Bandwidth cost: (request_bytes_sum + response_bytes_sum) / 1024.
    pub bandwidth_cost: u64,
}

/// Row for analytics.endpoint_metrics.
#[derive(Debug, Clone, Serialize, Row)]
pub struct EndpointMetricsRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub snapshot_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub source: String,
    pub ops_per_sec: Option<f64>,
    pub total_commands: Option<u64>,
    pub total_errors: Option<u64>,
    pub slow_query_count: Option<u64>,
    pub latency_p50_us: Option<u64>,
    pub latency_p95_us: Option<u64>,
    pub latency_p99_us: Option<u64>,
    pub latency_p999_us: Option<u64>,
    pub error_rate: Option<f64>,
    pub cache_hit_rate: Option<f64>,
    pub command_distribution: String,
    pub hot_keys: String,
    pub top_slow_commands: String,
    pub request_size_distribution: String,
    pub response_size_distribution: String,
    pub pipeline_depth_distribution: String,
    pub avg_pipeline_depth: Option<f64>,
    pub transactions_committed: Option<u64>,
    pub transactions_aborted: Option<u64>,
    pub avg_transaction_size: Option<f64>,
    pub transaction_size_distribution: String,
    pub ttl_distribution: String,
    pub keys_with_ttl_pct: Option<f64>,
    pub connections_opened: Option<u64>,
    pub connections_closed: Option<u64>,
    pub extra_metrics: String,

    // Redis INFO / poll metrics
    pub used_memory_bytes: Option<u64>,
    pub peak_memory_bytes: Option<u64>,
    pub mem_fragmentation_ratio: Option<f32>,
    pub connected_clients: Option<u32>,
    pub blocked_clients: Option<u32>,
    pub replication_role: Option<String>,
    pub used_cpu_sys: Option<f64>,
    pub used_cpu_user: Option<f64>,

    // Typed columns (dual-write alongside JSON strings above)
    /// Map(String, UInt64), same data as command_distribution JSON.
    pub command_distribution_map: Vec<(String, u64)>,
    /// Raw pipeline depth sample count (typed).
    pub pipeline_depth_samples: u64,
    /// Raw pipeline depth sum (typed). Named `_typed` to avoid CH column name clash.
    pub pipeline_depth_sum_typed: u64,
    /// TTL histogram: Array(UInt64) with 6 buckets [no_ttl, lt_1m, lt_1h, lt_1d, lt_7d, gte_7d].
    pub ttl_bucket_counts: Vec<u64>,
    /// Map(String, Float64), same data as extra_metrics JSON.
    pub extra_metrics_map: Vec<(String, f64)>,
}

/// Row for analytics.exemplars.
#[derive(Debug, Clone, Serialize, Row)]
pub struct ExemplarRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub exemplar_type: String,
    pub command_id: u16,
    pub command_name: String,
    pub latency_us: u64,
    pub key_pattern: String,
    pub redacted_args: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub sample_timestamp: DateTime<Utc>,
}

/// Row for analytics.anomaly_transitions.
#[derive(Debug, Clone, Serialize, Row)]
pub struct AnomalyTransitionRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub transition_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub detector: String,
    pub from_level: String,
    pub to_level: String,
}

impl EndpointMetricsRow {
    /// Create a minimal metrics row for backfill or testing.
    pub fn minimal(organization_uuid: String, endpoint_uuid: String, protocol: String, source: String) -> Self {
        Self {
            snapshot_time: Utc::now(),
            organization_uuid,
            endpoint_uuid,
            protocol,
            source,
            ops_per_sec: None,
            total_commands: None,
            total_errors: None,
            slow_query_count: None,
            latency_p50_us: None,
            latency_p95_us: None,
            latency_p99_us: None,
            latency_p999_us: None,
            error_rate: None,
            cache_hit_rate: None,
            command_distribution: "{}".to_string(),
            hot_keys: "[]".to_string(),
            top_slow_commands: "[]".to_string(),
            request_size_distribution: "{}".to_string(),
            response_size_distribution: "{}".to_string(),
            pipeline_depth_distribution: "{}".to_string(),
            avg_pipeline_depth: None,
            transactions_committed: None,
            transactions_aborted: None,
            avg_transaction_size: None,
            transaction_size_distribution: "{}".to_string(),
            ttl_distribution: "{}".to_string(),
            keys_with_ttl_pct: None,
            connections_opened: None,
            connections_closed: None,
            extra_metrics: "{}".to_string(),
            used_memory_bytes: None,
            peak_memory_bytes: None,
            mem_fragmentation_ratio: None,
            connected_clients: None,
            blocked_clients: None,
            replication_role: None,
            used_cpu_sys: None,
            used_cpu_user: None,
            command_distribution_map: Vec::new(),
            pipeline_depth_samples: 0,
            pipeline_depth_sum_typed: 0,
            ttl_bucket_counts: Vec::new(),
            extra_metrics_map: Vec::new(),
        }
    }
}

/// Row for analytics.target_pattern_rollups.
#[derive(Debug, Clone, Serialize, Row)]
pub struct TargetPatternRollupRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub service: String,
    pub target_pattern: String,
    pub command: String,
    pub request_count: u64,
    pub error_count: u64,
    pub cost_sum: u64,
    pub bandwidth_cost: u64,
    pub latency_sum: u64,
    pub read_count: u64,
    pub write_count: u64,
    pub ttl_present_count: u64,
    pub value_bytes_sum: u64,
}

/// Row for analytics.user_rollups.
/// Per-user aggregated metrics for "I/O by user" dashboard views.
#[derive(Debug, Clone, Serialize, Row)]
pub struct UserRollupRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub user_uuid: String,
    pub protocol: String,
    pub command: String,
    pub request_count: u64,
    pub error_count: u64,
    pub request_bytes_sum: u64,
    pub response_bytes_sum: u64,
    pub latency_sum: u64,
}

/// Row for analytics.mongo_shape_rollups.
#[derive(Debug, Clone, Serialize, Row)]
pub struct MongoShapeRollupRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub command: String,
    pub namespace: String,
    pub pipeline_stages: String,
    pub filter_shape: String,
    pub sort_fields: String,
    pub projection_fields: String,
    pub hint: String,
    pub skip_value: u64,
    pub max_time_ms: Option<u64>,
    pub has_javascript: u8,
    pub max_in_array_len: Option<u64>,
    pub read_concern: String,
    pub write_concern: String,
    pub latency_max: u64,
    pub count: u64,
    pub error_count: u64,
    pub total_latency_us: u64,
}

/// Table names for wire storage.
pub mod tables {
    pub const COMMAND_ROLLUPS: &str = "analytics.command_rollups";
    pub const ENDPOINT_METRICS: &str = "analytics.endpoint_metrics";
    pub const TARGET_PATTERN_ROLLUPS: &str = "analytics.target_pattern_rollups";
    pub const PII_AGGREGATE: &str = "analytics.pii_aggregate";
    pub const EXEMPLARS: &str = "analytics.exemplars";
    pub const ANOMALY_TRANSITIONS: &str = "analytics.anomaly_transitions";
    pub const MONGO_SHAPE_ROLLUPS: &str = "analytics.mongo_shape_rollups";
    pub const USER_ROLLUPS: &str = "analytics.user_rollups";
    pub const SESSION_HISTORY: &str = "analytics.session_history";
    pub const API_USAGE_HISTORY: &str = "analytics.api_usage_history";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_endpoint_metrics_minimal() {
        let row =
            EndpointMetricsRow::minimal("tenant1".to_string(), "endpoint1".to_string(), "redis".to_string(), "aggregator".to_string());

        assert_eq!(row.organization_uuid, "tenant1");
        assert_eq!(row.protocol, "redis");
        assert!(row.ops_per_sec.is_none());
    }
}
