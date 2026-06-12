//! Discovery template row type for ClickHouse analytics.

#[cfg(feature = "pipeline")]
use chrono::{DateTime, Utc};
#[cfg(feature = "pipeline")]
use clickhouse::Row;
#[cfg(feature = "pipeline")]
use serde::Serialize;

/// Row for analytics.discovery_templates.
#[cfg(feature = "pipeline")]
#[derive(Debug, Clone, Serialize, Row)]
pub struct DiscoveryTemplateRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::millis")]
    pub discovered_at: DateTime<Utc>,
    pub organization_uuid: String,
    pub endpoint_uuid: String,
    pub template_name: String,
    pub template_pattern: String,
    pub sample_count: u64,
    pub unique_commands: u32,
    pub cluster_id: u32,
    pub cluster_size: u32,
    pub representative_commands: String,
}
