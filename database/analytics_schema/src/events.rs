//! Sparse event row types for ClickHouse analytics.
//!
//! Anti-patterns, blocked commands, audit trail, PII aggregate rows,
//! session history, and API usage history.

use chrono::{DateTime, Utc};
use clickhouse::Row;
use eden_core::format::EndpointUuid;
use serde::{Deserialize, Serialize, Serializer};

fn serialize_endpoint_uuid<S>(endpoint_uuid: &EndpointUuid, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&endpoint_uuid.to_string())
}

/// Row for analytics.anti_patterns.
#[derive(Debug, Clone, Serialize, Row)]
pub struct AntiPatternRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub detected_at: DateTime<Utc>,
    pub organization_uuid: String,
    #[serde(serialize_with = "serialize_endpoint_uuid")]
    pub endpoint_uuid: EndpointUuid,
    pub protocol: String,
    pub pattern_type: String,
    pub details: String,
    pub connection_id: u64,
    pub occurrence_count: u32,
}

/// Row for analytics.blocked_commands.
#[derive(Debug, Clone, Serialize, Row)]
pub struct BlockedCommandRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub event_time: DateTime<Utc>,
    pub organization_uuid: String,
    #[serde(serialize_with = "serialize_endpoint_uuid")]
    pub endpoint_uuid: EndpointUuid,
    pub command: String,
    pub reason: String,
    pub severity: u8,
    pub service: String,
    pub client_ip: Option<String>,
}

/// Row for analytics.audit_trail.
#[derive(Debug, Clone, Serialize, Row)]
pub struct AuditEventRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub event_time: DateTime<Utc>,
    pub organization_uuid: String,
    #[serde(serialize_with = "serialize_endpoint_uuid")]
    pub endpoint_uuid: EndpointUuid,
    pub service: String,
    pub command: String,
    pub key: Option<String>,
    pub args_hash: u64,
    pub latency_us: u64,
    pub success: u8,
    pub client_ip: Option<String>,
    pub connection_id: u64,
}

/// Row for analytics.pii_aggregate.
#[derive(Debug, Clone, Serialize, Row)]
pub struct PiiAggregateRow {
    #[serde(with = "clickhouse::serde::chrono::datetime")]
    pub window_start: DateTime<Utc>,
    pub window_secs: u16,
    pub organization_uuid: String,
    #[serde(serialize_with = "serialize_endpoint_uuid")]
    pub endpoint_uuid: EndpointUuid,
    pub pii_type: String,
    pub detection_count: u64,
    pub representative_key_pattern: String,
    pub representative_redacted_sample: String,
}

/// Row for analytics.session_history.
/// Tracks user login sessions for security auditing and activity monitoring.
#[derive(Debug, Clone, Serialize, Row)]
pub struct SessionHistoryRow {
    pub session_uuid: String,
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub started_at: DateTime<Utc>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros::option")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub last_active_at: DateTime<Utc>,
    pub organization_uuid: String,
    pub user_uuid: String,
    pub user_id: String,
    pub device: String,
    pub user_agent: String,
    pub ip_address: String,
    pub auth_method: String,
    pub status: String,
    pub request_count: u64,
    pub error_count: u64,
}

/// Session status for analytics.session_history.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionStatus {
    Active,
    Expired,
    Revoked,
    LoggedOut,
}

impl SessionStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::LoggedOut => "logged_out",
        }
    }
}

impl std::fmt::Display for SessionStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Authentication method for session tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthMethod {
    Basic,
    Bearer,
    ApiKey,
}

impl AuthMethod {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Basic => "basic",
            Self::Bearer => "bearer",
            Self::ApiKey => "api_key",
        }
    }
}

impl std::fmt::Display for AuthMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Row for analytics.api_usage_history.
/// Tracks all API requests per user for usage monitoring and billing.
#[derive(Debug, Clone, Serialize, Row)]
pub struct ApiUsageHistoryRow {
    #[serde(with = "clickhouse::serde::chrono::datetime64::micros")]
    pub request_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub user_uuid: String,
    pub user_id: String,
    pub session_uuid: Option<String>,
    pub request_id: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status: u16,
    pub endpoint_uuid: Option<String>,
    pub endpoint_id: Option<String>,
    pub latency_us: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub client_ip: String,
    pub user_agent: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}
