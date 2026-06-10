use super::Row;
use crate::database::schema::{FromRow, Table};
use chrono::{DateTime, Utc};
use error::EpError;
use format::endpoint::EpKind;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, EndpointGroupId, EndpointGroupUuid, EndpointUuid, UserUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use utoipa::ToSchema;

/// An endpoint group is a named collection of endpoints that share the same `EpKind`.
/// Templates can bind to a group, allowing the same template to be executed against
/// any member endpoint (e.g. dev, staging, prod instances of the same database).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct EndpointGroupSchema {
    id: EndpointGroupId,
    uuid: EndpointGroupUuid,
    description: Option<String>,
    ep_kind: EpKind,
    default_endpoint: Option<EndpointUuid>,
    members: Vec<EndpointUuid>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl EndpointGroupSchema {
    pub fn new(
        id: EndpointGroupId,
        description: Option<String>,
        ep_kind: EpKind,
        default_endpoint: Option<EndpointUuid>,
        members: Vec<EndpointUuid>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id,
            uuid: EndpointGroupUuid::new_uuid(),
            description,
            ep_kind,
            default_endpoint,
            members,
            updated_by: created_by.clone(),
            created_by,
            created_at: now.clone(),
            updated_at: now,
        }
    }

    pub fn ep_kind(&self) -> EpKind {
        self.ep_kind
    }

    pub fn default_endpoint(&self) -> Option<&EndpointUuid> {
        self.default_endpoint.as_ref()
    }

    pub fn set_default_endpoint(&mut self, endpoint: Option<EndpointUuid>) {
        self.default_endpoint = endpoint;
        self.update_timestamp();
    }

    pub fn members(&self) -> &[EndpointUuid] {
        &self.members
    }

    pub fn set_members(&mut self, members: Vec<EndpointUuid>) {
        self.members = members;
    }

    pub fn add_member(&mut self, endpoint: EndpointUuid) {
        if !self.members.contains(&endpoint) {
            self.members.push(endpoint);
            self.update_timestamp();
        }
    }

    pub fn remove_member(&mut self, endpoint: &EndpointUuid) -> bool {
        let initial_len = self.members.len();
        self.members.retain(|e| e != endpoint);
        if self.members.len() < initial_len {
            // Clear default if the removed endpoint was the default
            if self.default_endpoint.as_ref() == Some(endpoint) {
                self.default_endpoint = self.members.first().cloned();
            }
            self.update_timestamp();
            true
        } else {
            false
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
}

impl Table for EndpointGroupSchema {
    type U = EndpointGroupUuid;
    type I = EndpointGroupId;

    fn id(&self) -> EndpointGroupId {
        self.id.clone()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> EndpointGroupUuid {
        self.uuid.clone()
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
        self.updated_at = DateTimeWrapper::now()
    }
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for EndpointGroupSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            ep_kind: row.try_get("ep_kind").map_err(EpError::database)?,
            default_endpoint: row.try_get("default_endpoint").map_err(EpError::database)?,
            members: Vec::new(), // Members are loaded separately via join query
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for EndpointGroupSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for EndpointGroupSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting EndpointGroupSchema",
            ))),
        }
    }
}

/// Lightweight view for listing endpoint groups.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct EndpointGroupSchemaIds {
    id: EndpointGroupId,
    uuid: EndpointGroupUuid,
    description: Option<String>,
    ep_kind: String,
    default_endpoint: Option<EndpointUuid>,
    members: Vec<EndpointUuid>,
    created_by: UserUuid,
    updated_by: UserUuid,
}

impl FromRow for EndpointGroupSchemaIds {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            ep_kind: row.try_get("ep_kind").map_err(EpError::database)?,
            default_endpoint: row.try_get("default_endpoint").map_err(EpError::database)?,
            members: Vec::new(),
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
        })
    }
}

impl ToRedisArgs for EndpointGroupSchemaIds {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        let serialized = serde_json::to_vec(self).unwrap_or_default();
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for EndpointGroupSchemaIds {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting EndpointGroupSchemaIds",
            ))),
        }
    }
}

/// Input for creating a new endpoint group.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct EndpointGroupBuilder {
    pub id: String,
    pub description: Option<String>,
    pub ep_kind: EpKind,
    pub default_endpoint: Option<String>,
    pub members: Vec<String>,
}

/// Input for updating an existing endpoint group.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateEndpointGroupSchema {
    id: Option<EndpointGroupId>,
    description: Option<String>,
    default_endpoint: Option<EndpointUuid>,
}

impl UpdateEndpointGroupSchema {
    pub fn id(&self) -> Option<&EndpointGroupId> {
        self.id.as_ref()
    }
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn default_endpoint(&self) -> Option<&EndpointUuid> {
        self.default_endpoint.as_ref()
    }
    pub fn update(&self, schema: &mut EndpointGroupSchema) {
        if let Some(id) = self.id() {
            schema.update_id(id.to_string());
        }
        if let Some(description) = self.description() {
            schema.update_description(description.to_string());
        }
        if let Some(endpoint) = self.default_endpoint() {
            schema.set_default_endpoint(Some(endpoint.clone()));
        }
    }
}
