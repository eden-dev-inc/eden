use super::Row;
use crate::database::schema::FromRow;
use crate::database::schema::snapshot::CdcConfig;
use chrono::{DateTime, Utc};
use error::EpError;
use format::UserUuid;
use format::timestamp::DateTimeWrapper;
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Lifecycle status for a continuous CDC pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "PascalCase")]
pub enum PipelineStatus {
    #[default]
    Pending,
    Running,
    Paused,
    Completed,
    Failed,
}

impl std::fmt::Display for PipelineStatus {
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

impl std::str::FromStr for PipelineStatus {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "paused" => Ok(Self::Paused),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            other => Err(EpError::parse(format!("Unknown pipeline status: {other}"))),
        }
    }
}

/// Schema for a CDC pipeline — a real-time, filtered data sync between two endpoints.
///
/// Pipelines are always CDC-based (Postgres WAL logical replication). Unlike snapshots,
/// they have no schedule or batch scan mode — they run continuously until paused or deleted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct PipelineSchema {
    id: String,
    uuid: Uuid,
    description: Option<String>,
    status: PipelineStatus,
    source_endpoint: Uuid,
    target_endpoint: Uuid,
    /// SQL WHERE clause to filter source data (evaluated in-process against WAL events).
    filter: Option<String>,
    /// CDC-specific configuration (required).
    cdc_config: CdcConfig,
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

impl PipelineSchema {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        description: Option<String>,
        source_endpoint: Uuid,
        target_endpoint: Uuid,
        filter: Option<String>,
        cdc_config: CdcConfig,
        write_template_uuid: Option<Uuid>,
        read_template_uuid: Option<Uuid>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        let uuid = Uuid::new_v4();

        // Auto-generate CDC slot/publication names from UUID if not provided.
        let mut cdc_config = cdc_config;
        if cdc_config.slot_name.is_none() {
            cdc_config.slot_name = Some(format!("eden_pipeline_{}", uuid.as_simple()));
        }
        if cdc_config.publication_name.is_none() {
            cdc_config.publication_name = Some(format!("eden_pub_{}", uuid.as_simple()));
        }

        Self {
            id,
            uuid,
            description,
            status: PipelineStatus::default(),
            source_endpoint,
            target_endpoint,
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

    pub fn status(&self) -> &PipelineStatus {
        &self.status
    }

    pub fn source_endpoint(&self) -> &Uuid {
        &self.source_endpoint
    }

    pub fn target_endpoint(&self) -> &Uuid {
        &self.target_endpoint
    }

    pub fn filter(&self) -> &Option<String> {
        &self.filter
    }

    pub fn cdc_config(&self) -> &CdcConfig {
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

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }
}

impl FromRow for PipelineSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let cdc_config: CdcConfig = row
            .try_get::<&str, serde_json::Value>("cdc_config")
            .map_err(EpError::database)
            .and_then(|v| serde_json::from_value(v).map_err(|e| EpError::parse(format!("Invalid cdc_config: {e}"))))?;

        let status = row
            .try_get::<_, Option<String>>("status")
            .map_err(EpError::database)?
            .map(|value| value.parse::<PipelineStatus>())
            .transpose()?
            .unwrap_or_default();

        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            status,
            source_endpoint: row.try_get("source_endpoint").map_err(EpError::database)?,
            target_endpoint: row.try_get("target_endpoint").map_err(EpError::database)?,
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

impl ToRedisArgs for PipelineSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for PipelineSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((redis::ErrorKind::ResponseError, "Invalid response type"))),
        }
    }
}

/// Constructor input for creating a new pipeline via the API.
#[derive(Debug, Deserialize, ToSchema)]
pub struct PipelineConstructor {
    pub id: String,
    pub description: Option<String>,
    pub source_endpoint: String,
    pub target_endpoint: String,
    /// SQL WHERE clause to filter source data.
    pub filter: Option<String>,
    /// CDC-specific configuration (required).
    pub cdc_config: CdcConfig,
    /// Write template UUID for destination writes.
    pub write_template_uuid: Option<Uuid>,
    /// Read template UUID for backfill/selection queries.
    pub read_template_uuid: Option<Uuid>,
}
