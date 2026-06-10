use super::Row;
use crate::database::schema::{AuthType, FromRow, Table};
use chrono::{DateTime, Utc};
use error::EpError;
use format::timestamp::DateTimeWrapper;
use format::{AuthId, AuthUuid, EdenId, EdenUuid, EndpointUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthSchema {
    id: AuthId,
    uuid: AuthUuid,
    auth: AuthType,
    endpoint_uuid: EndpointUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl AuthSchema {
    pub fn new(id: String, auth: AuthType, endpoint_uuid: EndpointUuid) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id: AuthId::new(id),
            uuid: AuthUuid::new_uuid(),
            auth,
            endpoint_uuid,
            created_at: now.clone(),
            updated_at: now,
        }
    }
    pub fn auth(&self) -> AuthType {
        self.auth
    }
    pub fn endpoint_uuid(&self) -> Uuid {
        self.endpoint_uuid.uuid()
    }
    pub fn update_auth(&mut self, new_auth: AuthType) {
        self.auth = new_auth;
        self.update_timestamp();
    }
}

impl Table for AuthSchema {
    type I = AuthId;
    type U = AuthUuid;

    fn id(&self) -> AuthId {
        self.id.to_owned()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> AuthUuid {
        self.uuid.to_owned()
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
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for AuthSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            auth: row.try_get("auth").map_err(EpError::database)?,
            endpoint_uuid: row.try_get("endpoint_uuid").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for AuthSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the AuthSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for AuthSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting AuthSchema",
            ))),
        }
    }
}
