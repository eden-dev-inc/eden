use super::Row;
use crate::database::schema::FromRow;
use chrono::{DateTime, Utc};
use error::EpError;
use format::UserUuid;
use format::timestamp::DateTimeWrapper;
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use utoipa::ToSchema;
use uuid::Uuid;

/// Minimum allowed schedule interval (15 minutes).
pub const MIN_SNAPSHOT_INTERVAL_SECS: u64 = 900;

/// Schedule configuration for snapshots.
///
/// - **Recurring**: set `interval_secs` to the repeat period (minimum 900 / 15 minutes).
/// - **One-time**: omit `interval_secs`. The snapshot runs once on the next scheduler poll
///   and is not rescheduled.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SnapshotSchedule {
    /// Interval in seconds between snapshot runs. Minimum 900 (15 minutes).
    /// Omit for a one-time scheduled snapshot.
    pub interval_secs: Option<u64>,
    /// Whether the schedule is currently enabled.
    pub enabled: bool,
}

/// Source mode for a snapshot: batch scan or real-time CDC.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum SourceMode {
    /// Batch data movement using scan/dump (existing behavior).
    #[default]
    Scan,
    /// Real-time change data capture via Postgres WAL logical replication.
    Cdc,
}

impl std::fmt::Display for SourceMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scan => write!(f, "scan"),
            Self::Cdc => write!(f, "cdc"),
        }
    }
}

impl std::str::FromStr for SourceMode {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "scan" => Ok(Self::Scan),
            "cdc" => Ok(Self::Cdc),
            other => Err(EpError::parse(format!("Unknown source_mode: {other}"))),
        }
    }
}

/// Lifecycle status for a standalone snapshot run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "PascalCase")]
pub enum SnapshotStatus {
    #[default]
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

impl std::fmt::Display for SnapshotStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "Pending"),
            Self::Running => write!(f, "Running"),
            Self::Paused => write!(f, "Paused"),
            Self::Completed => write!(f, "Completed"),
            Self::Failed => write!(f, "Failed"),
        }
    }
}

impl std::str::FromStr for SnapshotStatus {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(EpError::parse(format!("Unknown snapshot status: {other}"))),
        }
    }
}

/// CDC-specific configuration for real-time change data capture.
///
/// Common fields (`tables`, `batch_size`, etc.) apply to all database types.
/// Database-specific parameters live in [`source_params`]. For backward
/// compatibility, Postgres-specific fields (`slot_name`, `publication_name`)
/// are also accepted at the top level.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct CdcConfig {
    /// Tables/collections to replicate (e.g., `["public.orders", "public.items"]`).
    pub tables: Vec<String>,
    /// Maximum rows per write batch before flushing.
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Maximum milliseconds between flushes.
    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
    /// Whether to propagate DELETE events to the destination.
    #[serde(default = "default_include_deletes")]
    pub include_deletes: bool,
    /// Database-specific source parameters.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_params: Option<CdcSourceParams>,
    /// Replication slot name (Postgres-specific, kept for backward compatibility).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub slot_name: Option<String>,
    /// Publication name (Postgres-specific, kept for backward compatibility).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publication_name: Option<String>,
}

/// Database-specific CDC source parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum CdcSourceParams {
    /// PostgreSQL logical replication parameters.
    Postgres {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        slot_name: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        publication_name: Option<String>,
    },
    /// MySQL binlog replication parameters.
    Mysql {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        server_id: Option<u32>,
    },
    /// MongoDB change stream parameters.
    Mongo {},
}

impl CdcConfig {
    /// Resolve the effective slot name, checking `source_params` first then the legacy top-level field.
    pub fn effective_slot_name(&self) -> Option<&str> {
        if let Some(CdcSourceParams::Postgres { slot_name: Some(ref s), .. }) = self.source_params {
            return Some(s);
        }
        self.slot_name.as_deref()
    }

    /// Resolve the effective publication name, checking `source_params` first then the legacy top-level field.
    pub fn effective_publication_name(&self) -> Option<&str> {
        if let Some(CdcSourceParams::Postgres { publication_name: Some(ref p), .. }) = self.source_params {
            return Some(p);
        }
        self.publication_name.as_deref()
    }
}

fn default_batch_size() -> u32 {
    1000
}

fn default_flush_interval_ms() -> u64 {
    5000
}

fn default_include_deletes() -> bool {
    true
}

/// Schema for a standalone data movement snapshot between two endpoints.
///
/// A snapshot copies data from a source endpoint to a target endpoint
/// without requiring interlays or traffic routing changes.
///
/// Supports two source modes:
/// - **Scan**: batch data movement on a schedule (with optional SQL WHERE filter).
/// - **CDC**: real-time change data capture via Postgres WAL logical replication
///   with SQL WHERE filtering on each row change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct SnapshotSchema {
    id: String,
    uuid: Uuid,
    description: Option<String>,
    #[serde(default)]
    status: SnapshotStatus,
    source_endpoint: Uuid,
    target_endpoint: Uuid,
    #[serde(default = "default_snapshot_data")]
    data: Value,
    #[serde(default = "default_preserve_ttl")]
    preserve_ttl: bool,
    schedule: Option<SnapshotSchedule>,
    last_run_at: Option<DateTimeWrapper>,
    next_run_at: Option<DateTimeWrapper>,
    job_uuid: Option<Uuid>,
    #[serde(default)]
    source_mode: SourceMode,
    /// SQL WHERE clause to filter source data. Applied natively in scan mode,
    /// evaluated in-process against WAL events in CDC mode.
    filter: Option<String>,
    /// CDC-specific configuration. Required when `source_mode` is `Cdc`.
    cdc_config: Option<CdcConfig>,
    /// Last confirmed Postgres LSN for CDC resume.
    last_lsn: Option<String>,
    /// Write template for destination writes (required for CDC).
    write_template_uuid: Option<Uuid>,
    /// Read template for backfill/selection queries (required for CDC).
    read_template_uuid: Option<Uuid>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

fn default_preserve_ttl() -> bool {
    true
}

fn default_snapshot_data() -> Value {
    json!({})
}

impl SnapshotSchema {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        description: Option<String>,
        source_endpoint: Uuid,
        target_endpoint: Uuid,
        data: Option<Value>,
        preserve_ttl: Option<bool>,
        schedule: Option<SnapshotSchedule>,
        created_by: UserUuid,
        source_mode: Option<SourceMode>,
        filter: Option<String>,
        cdc_config: Option<CdcConfig>,
        write_template_uuid: Option<Uuid>,
        read_template_uuid: Option<Uuid>,
    ) -> Self {
        let now = DateTimeWrapper::now();
        let source_mode = source_mode.unwrap_or_default();

        // If a schedule is provided and enabled, set next_run_at so the scheduler picks it up.
        // For recurring snapshots, first run is after one interval. For one-time, run on next poll.
        // CDC mode does not use schedule-based polling.
        let next_run_at = match source_mode {
            SourceMode::Cdc => None,
            SourceMode::Scan => match &schedule {
                Some(s) if s.enabled => match s.interval_secs {
                    Some(interval) => {
                        let next = chrono::Utc::now() + chrono::Duration::seconds(interval as i64);
                        Some(DateTimeWrapper::from(next))
                    }
                    None => Some(now.clone()), // one-time: run on next poll
                },
                _ => None,
            },
        };

        // Auto-generate CDC slot/publication names from UUID if not provided.
        let uuid = Uuid::new_v4();
        let cdc_config = cdc_config.map(|mut c| {
            if c.slot_name.is_none() {
                c.slot_name = Some(format!("eden_pipeline_{}", uuid.as_simple()));
            }
            if c.publication_name.is_none() {
                c.publication_name = Some(format!("eden_pub_{}", uuid.as_simple()));
            }
            c
        });

        Self {
            id,
            uuid,
            description,
            status: SnapshotStatus::Pending,
            source_endpoint,
            target_endpoint,
            data: data.unwrap_or_else(default_snapshot_data),
            preserve_ttl: preserve_ttl.unwrap_or(true),
            schedule,
            last_run_at: None,
            next_run_at,
            job_uuid: None,
            source_mode,
            filter,
            cdc_config,
            last_lsn: None,
            write_template_uuid,
            read_template_uuid,
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn uuid(&self) -> &Uuid {
        &self.uuid
    }

    pub fn description(&self) -> &Option<String> {
        &self.description
    }

    pub fn status(&self) -> &SnapshotStatus {
        &self.status
    }

    pub fn data(&self) -> &Value {
        &self.data
    }

    pub fn source_endpoint(&self) -> &Uuid {
        &self.source_endpoint
    }

    pub fn target_endpoint(&self) -> &Uuid {
        &self.target_endpoint
    }

    pub fn preserve_ttl(&self) -> bool {
        self.preserve_ttl
    }

    pub fn schedule(&self) -> &Option<SnapshotSchedule> {
        &self.schedule
    }

    pub fn last_run_at(&self) -> &Option<DateTimeWrapper> {
        &self.last_run_at
    }

    pub fn next_run_at(&self) -> &Option<DateTimeWrapper> {
        &self.next_run_at
    }

    pub fn job_uuid(&self) -> &Option<Uuid> {
        &self.job_uuid
    }

    pub fn set_job_uuid(&mut self, job_uuid: Option<Uuid>) {
        self.job_uuid = job_uuid;
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }

    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }

    pub fn created_at(&self) -> &DateTimeWrapper {
        &self.created_at
    }

    pub fn updated_at(&self) -> &DateTimeWrapper {
        &self.updated_at
    }

    pub fn source_mode(&self) -> &SourceMode {
        &self.source_mode
    }

    pub fn filter(&self) -> &Option<String> {
        &self.filter
    }

    pub fn cdc_config(&self) -> &Option<CdcConfig> {
        &self.cdc_config
    }

    pub fn last_lsn(&self) -> &Option<String> {
        &self.last_lsn
    }

    pub fn set_last_lsn(&mut self, lsn: Option<String>) {
        self.last_lsn = lsn;
        self.updated_at = DateTimeWrapper::now();
    }

    pub fn write_template_uuid(&self) -> &Option<Uuid> {
        &self.write_template_uuid
    }

    pub fn read_template_uuid(&self) -> &Option<Uuid> {
        &self.read_template_uuid
    }

    pub fn is_cdc(&self) -> bool {
        matches!(self.source_mode, SourceMode::Cdc)
    }

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }
}

impl FromRow for SnapshotSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let source_mode = row
            .try_get::<&str, Option<String>>("source_mode")
            .map_err(EpError::database)?
            .map(|s| s.parse::<SourceMode>())
            .transpose()?
            .unwrap_or_default();

        let cdc_config = row
            .try_get::<&str, Option<serde_json::Value>>("cdc_config")
            .map_err(EpError::database)?
            .and_then(|v| serde_json::from_value(v).ok());

        let status = row
            .try_get::<&str, Option<String>>("status")
            .map_err(EpError::database)?
            .map(|s| s.parse::<SnapshotStatus>())
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            status,
            source_endpoint: row.try_get("source_endpoint").map_err(EpError::database)?,
            target_endpoint: row.try_get("target_endpoint").map_err(EpError::database)?,
            data: row.try_get::<&str, Option<Value>>("data").map_err(EpError::database)?.unwrap_or_else(default_snapshot_data),
            preserve_ttl: row.try_get::<&str, Option<bool>>("preserve_ttl").map_err(EpError::database)?.unwrap_or(true),
            schedule: row.try_get::<&str, Option<serde_json::Value>>("schedule").map_err(EpError::database)?.and_then(|v| {
                serde_json::from_value(v.clone())
                    .map_err(|e| {
                        eprintln!("WARNING: failed to deserialize snapshot schedule from database, treating as None: {e}");
                        e
                    })
                    .ok()
            }),
            last_run_at: row.try_get::<_, Option<DateTime<Utc>>>("last_run_at").map_err(EpError::database)?.map(DateTimeWrapper::from),
            next_run_at: row.try_get::<_, Option<DateTime<Utc>>>("next_run_at").map_err(EpError::database)?.map(DateTimeWrapper::from),
            job_uuid: row.try_get("job_uuid").map_err(EpError::database)?,
            source_mode,
            filter: row.try_get("filter").map_err(EpError::database)?,
            cdc_config,
            last_lsn: row.try_get("last_lsn").map_err(EpError::database)?,
            write_template_uuid: row.try_get("write_template_uuid").map_err(EpError::database)?,
            read_template_uuid: row.try_get("read_template_uuid").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for SnapshotSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // serde_json::to_vec on a well-typed struct should not fail in practice,
        // but we fall back to an empty JSON object rather than an empty byte array
        // so that FromRedisValue deserialization produces a clear error.
        let serialized = serde_json::to_vec(self).unwrap_or_else(|_| b"{}".to_vec());
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for SnapshotSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((redis::ErrorKind::ResponseError, "Invalid response type"))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_schema() -> SnapshotSchema {
        SnapshotSchema::new(
            "test-snap".to_string(),
            Some("description".to_string()),
            Uuid::new_v4(),
            Uuid::new_v4(),
            None,
            None,
            Some(SnapshotSchedule { interval_secs: Some(3600), enabled: true }),
            UserUuid::from(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        )
    }

    #[test]
    fn snapshot_schema_redis_roundtrip_preserves_data() {
        let schema = make_test_schema();

        // Serialize via ToRedisArgs
        let mut buf: Vec<Vec<u8>> = Vec::new();
        schema.write_redis_args(&mut buf);
        assert!(!buf.is_empty(), "serialization should produce non-empty output");

        // Deserialize via FromRedisValue
        let redis_value = redis::Value::BulkString(buf[0].clone());
        let deserialized = SnapshotSchema::from_redis_value(&redis_value).expect("should deserialize");

        assert_eq!(deserialized.id(), schema.id());
        assert_eq!(deserialized.schedule(), schema.schedule());
        assert_eq!(deserialized.preserve_ttl(), schema.preserve_ttl());
    }

    #[test]
    fn snapshot_schedule_serde_roundtrip() {
        let schedule = SnapshotSchedule { interval_secs: Some(900), enabled: true };
        let json = serde_json::to_value(&schedule).expect("serialize");
        let deserialized: SnapshotSchedule = serde_json::from_value(json).expect("deserialize");
        assert_eq!(deserialized, schedule);
    }

    #[test]
    fn snapshot_schema_new_sets_next_run_at_for_enabled_schedule() {
        let schema = make_test_schema();
        assert!(schema.next_run_at().is_some(), "enabled schedule should set next_run_at");
    }

    #[test]
    fn snapshot_schema_new_no_next_run_for_disabled_schedule() {
        let schema = SnapshotSchema::new(
            "test".to_string(),
            None,
            Uuid::new_v4(),
            Uuid::new_v4(),
            None,
            None,
            Some(SnapshotSchedule { interval_secs: Some(3600), enabled: false }),
            UserUuid::from(Uuid::new_v4()),
            None,
            None,
            None,
            None,
            None,
        );
        assert!(schema.next_run_at().is_none(), "disabled schedule should not set next_run_at");
    }
}

/// Constructor input for creating a new snapshot via the API.
#[derive(Debug, Deserialize, ToSchema)]
pub struct SnapshotConstructor {
    pub id: String,
    pub description: Option<String>,
    pub source_endpoint: String,
    pub target_endpoint: String,
    pub data: Option<Value>,
    pub preserve_ttl: Option<bool>,
    pub schedule: Option<SnapshotSchedule>,
    /// Source mode: `scan` (batch, default) or `cdc` (real-time WAL streaming).
    pub source_mode: Option<SourceMode>,
    /// SQL WHERE clause to filter source data.
    pub filter: Option<String>,
    /// CDC-specific configuration. Required when `source_mode` is `cdc`.
    pub cdc_config: Option<CdcConfig>,
    /// Write template UUID for destination writes.
    pub write_template_uuid: Option<Uuid>,
    /// Read template UUID for backfill/selection queries.
    pub read_template_uuid: Option<Uuid>,
}
