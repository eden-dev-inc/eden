use super::Row;
use crate::database::schema::{FromRow, Table, UuidArrayOperations, UuidArrayOps};
use chrono::{DateTime, Utc};
use error::EpError;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, EdenNodeId, EdenNodeUuid, EndpointUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;

#[derive(Debug, Clone, Serialize, Deserialize, PartialOrd, PartialEq)]
pub struct EdenNodeSchema {
    id: EdenNodeId,
    uuid: EdenNodeUuid,
    endpoint_uuids: Vec<EndpointUuid>,
    // info: Value, //todo define information for each eden node
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl EdenNodeSchema {
    pub fn new(id: String, eden_node_uuid: EdenNodeUuid, endpoint_uuids: Vec<EndpointUuid>, _info: Value) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id: EdenNodeId::new(id),
            uuid: eden_node_uuid,
            endpoint_uuids,
            // info,
            created_at: now.clone(),
            updated_at: now,
        }
    }
    pub fn endpoint_uuids(&self) -> &[EndpointUuid] {
        &self.endpoint_uuids
    }
    // pub fn info(&self) -> &Value {
    //     &self.info
    // }
    // pub fn update_info(&mut self, info: Value) -> Value {
    //     let old = self.info.clone();
    //     self.info = info;
    //     self.update_timestamp();
    //     old
    // }
    pub fn add_endpoint_uuid(&mut self, uuids: EndpointUuid) {
        self.endpoint_uuids.push(uuids);
        self.update_timestamp();
    }
    pub fn add_endpoint_uuids(&mut self, uuids: Vec<EndpointUuid>) {
        self.endpoint_uuids.extend(uuids);
        self.update_timestamp();
    }
    pub fn remove_endpoint_uuid(&mut self, uuid: EndpointUuid) {
        if UuidArrayOperations::remove_uuid(&mut self.endpoint_uuids, uuid) {
            self.update_timestamp()
        }
    }
    pub fn remove_endpoint_uuids(&mut self, uuids: Vec<EndpointUuid>) {
        if UuidArrayOperations::remove_uuids(&mut self.endpoint_uuids, &uuids) {
            self.update_timestamp()
        }
    }
}

impl Table for EdenNodeSchema {
    type I = EdenNodeId;
    type U = EdenNodeUuid;
    fn id(&self) -> EdenNodeId {
        self.id.to_owned()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> EdenNodeUuid {
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

impl FromRow for EdenNodeSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            endpoint_uuids: row.try_get("endpoint_uuids").map_err(EpError::database)?,
            // info: row.try_get("info").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for EdenNodeSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the EdenNodeSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for EdenNodeSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting EdenNodeSchema",
            ))),
        }
    }
}
