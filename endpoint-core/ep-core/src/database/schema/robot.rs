use super::Row;
use crate::database::schema::{FromRow, Table};
use auth::ApiKey;
use chrono::{DateTime, Utc};
use error::EpError;
pub use format::rbac::ControlPerms;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, OrganizationUuid, RobotId, RobotUuid, UserUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use utoipa::ToSchema;

/// Client-supplied input for creating a new robot (machine-account).
///
/// This struct carries only caller-provided fields; derived and audit fields
/// live on [`RobotSchema`]:
///
/// - **`ttl_sec`** is an optional lifetime in *seconds*. When present,
///   [`RobotSchema::new`] computes `expires_at = now + ttl_sec` at creation
///   time. `RobotInput` never carries `expires_at` directly.
/// - **`created_by` / `updated_by`** are not part of this input. They are
///   resolved from the authenticated caller and injected when converting to
///   [`RobotSchema`] (see the `From<(RobotInput, OrganizationUuid, ApiKey,
///   UserUuid)>` impl).
/// - **`perms`** is optional on the wire but wrapped as `Some` in
///   [`RobotInput::new`] so programmatic callers always supply one.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct RobotInput {
    username: String,
    description: Option<String>,
    perms: Option<ControlPerms>,
    ttl_sec: Option<i64>,
}

impl RobotInput {
    pub fn new(username: String, description: Option<String>, perms: ControlPerms, ttl: Option<i64>) -> Self {
        Self { username, description, perms: Some(perms), ttl_sec: ttl }
    }

    pub fn username(&self) -> &str {
        &self.username
    }

    pub fn description(&self) -> Option<String> {
        self.description.clone()
    }

    pub fn perms(&self) -> Option<ControlPerms> {
        self.perms
    }

    pub fn ttl(&self) -> Option<i64> {
        self.ttl_sec
    }
}

/// Persisted robot (machine-account) record.
///
/// Invariants:
/// - `username` is unique per organization (`(username, organization_uuid)`).
/// - `ttl` is stored as seconds; when present, `expires_at` is derived from `now + ttl`.
/// - `created_by` is immutable creator identity; `updated_by` tracks the most recent mutating actor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct RobotSchema {
    uuid: RobotUuid,
    username: RobotId,
    api_key: ApiKey,
    organization_uuid: OrganizationUuid,
    description: Option<String>,
    ttl_sec: Option<i64>,
    expires_at: Option<DateTimeWrapper>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl RobotSchema {
    pub fn new(
        username: RobotId,
        api_key: ApiKey,
        organization_uuid: OrganizationUuid,
        description: Option<String>,
        ttl: Option<i64>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        let expires_at = ttl.map(|seconds| DateTimeWrapper::from(Utc::now() + chrono::Duration::seconds(seconds)));
        Self {
            uuid: RobotUuid::new_uuid(),
            username,
            api_key,
            organization_uuid,
            description,
            ttl_sec: ttl,
            expires_at,
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }

    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn username(&self) -> &RobotId {
        &self.username
    }

    pub fn api_key(&self) -> &ApiKey {
        &self.api_key
    }

    pub fn organization_uuid(&self) -> &OrganizationUuid {
        &self.organization_uuid
    }

    pub fn ttl(&self) -> Option<i64> {
        self.ttl_sec
    }

    pub fn expires_at(&self) -> Option<DateTime<Utc>> {
        self.expires_at.as_ref().map(|dt| dt.as_datetime())
    }

    pub fn is_expired(&self) -> bool {
        self.expires_at.as_ref().is_some_and(|dt| dt.as_datetime() < Utc::now())
    }

    pub fn verify_api_key(&self, plaintext: &str) -> bool {
        self.api_key.verify(plaintext)
    }

    pub fn update_description(&mut self, description: String) -> Option<String> {
        let old = self.description.replace(description);
        self.update_timestamp();
        old
    }

    pub fn update_api_key(&mut self, api_key: ApiKey) {
        self.api_key = api_key;
        self.update_timestamp();
    }

    pub fn update_ttl(&mut self, ttl: Option<i64>) {
        self.ttl_sec = ttl;
        self.expires_at = ttl.map(|seconds| DateTimeWrapper::from(Utc::now() + chrono::Duration::seconds(seconds)));
        self.update_timestamp();
    }
}

impl From<(RobotInput, OrganizationUuid, ApiKey, UserUuid)> for RobotSchema {
    fn from(input: (RobotInput, OrganizationUuid, ApiKey, UserUuid)) -> Self {
        Self::new(RobotId::from(input.0.username), input.2, input.1, input.0.description, input.0.ttl_sec, input.3)
    }
}

impl Table for RobotSchema {
    type I = RobotId;
    type U = RobotUuid;

    fn id(&self) -> RobotId {
        RobotId::new(self.username.to_string())
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.username.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> RobotUuid {
        self.uuid.to_owned()
    }
    fn description(&self) -> Option<String> {
        self.description.clone()
    }
    fn update_description(&mut self, description: String) -> Option<String> {
        let out = self.description.replace(description);
        self.update_timestamp();
        out
    }
    fn created_at(&self) -> DateTime<Utc> {
        self.created_at.as_datetime()
    }
    fn updated_at(&self) -> DateTime<Utc> {
        self.updated_at.as_datetime()
    }
    fn update_timestamp(&mut self) {
        self.updated_at = DateTimeWrapper::now();
    }
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for RobotSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            username: row.try_get("username").map_err(EpError::database)?,
            api_key: {
                #[cfg(not(embedded_db))]
                {
                    row.try_get::<&str, postgres_types::Json<ApiKey>>("api_key").map_err(EpError::database)?.0
                }
                #[cfg(embedded_db)]
                {
                    row.try_get_json::<&str, ApiKey>("api_key").map_err(EpError::database)?
                }
            },
            organization_uuid: row.try_get("organization_uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            ttl_sec: row.try_get("ttl").map_err(EpError::database)?,
            expires_at: row.try_get::<_, Option<DateTime<Utc>>>("expires_at").map_err(EpError::database)?.map(DateTimeWrapper::from),
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for RobotSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for RobotSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting RobotSchema",
            ))),
        }
    }
}
