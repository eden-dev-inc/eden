//! Data types returned by analytics providers.

use ::clickhouse::Row;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Endpoint health metrics aggregated over a time window.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct EndpointHealth {
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub requests: u64,
    pub errors: u64,
    pub slow_queries: u64,
    pub error_rate: f64,
    pub slow_rate: f64,
    pub avg_latency_us: f64,
    pub p95_latency_us: f64,
    pub max_latency_us: u64,
}

/// Anti-pattern occurrence row from ClickHouse.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct AntiPatternRow {
    pub detected_at: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub pattern_type: String,
    pub occurrence_count: u64,
    #[serde(default)]
    pub sample_key: Option<String>,
    #[serde(default)]
    pub sample_details: Option<String>,
}

/// Hourly rollup row from ClickHouse.
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct HourlyRollup {
    pub hour: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub protocol: String,
    pub command: String,
    pub pattern_hash: u64,
    pub requests: u64,
    pub errors: u64,
    pub slow_queries: u64,
    pub sum_latency_us: u64,
    pub max_latency_us: u64,
    pub avg_latency_us: f64,
}

/// Signal event row from ClickHouse (errors, slow queries, PII detections).
#[derive(Debug, Clone, Row, Serialize, Deserialize)]
pub struct SignalRow {
    pub event_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub signal_type: String,
    pub severity: String,
    #[serde(default)]
    pub details: Option<String>,
    pub latency_us: u64,
}

/// Snapshot of analytics data for alert evaluation.
#[derive(Debug, Clone)]
pub struct AlertSnapshot {
    pub generated_at: DateTime<Utc>,
    pub window_minutes: i64,
    pub endpoint_health: Vec<EndpointHealth>,
    pub anti_patterns: Vec<AntiPatternRow>,
    pub signals: Vec<SignalRow>,
}

impl AlertSnapshot {
    pub fn new(window_minutes: i64) -> Self {
        Self {
            generated_at: Utc::now(),
            window_minutes,
            endpoint_health: Vec::new(),
            anti_patterns: Vec::new(),
            signals: Vec::new(),
        }
    }

    pub fn with_health(mut self, health: Vec<EndpointHealth>) -> Self {
        self.endpoint_health = health;
        self
    }

    pub fn with_anti_patterns(mut self, patterns: Vec<AntiPatternRow>) -> Self {
        self.anti_patterns = patterns;
        self
    }

    pub fn with_signals(mut self, signals: Vec<SignalRow>) -> Self {
        self.signals = signals;
        self
    }
}
