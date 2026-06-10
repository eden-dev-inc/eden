use super::Row;
use crate::database::schema::{FromRow, Table};
use crate::database::workflow::{Dag, Workflow};
use chrono::{DateTime, Utc};
use error::EpError;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, TemplateUuid, UserUuid, WorkflowId, WorkflowUuid};
use redis::FromRedisValue;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WorkflowSchema {
    id: WorkflowId,
    uuid: WorkflowUuid,
    template_uuids: Vec<TemplateUuid>,
    dag: Dag, // JSONB
    description: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl WorkflowSchema {
    pub fn new(workflow: Workflow, created_by: UserUuid) -> Self {
        let dag = workflow.dag();

        Self {
            id: workflow.id(),
            uuid: workflow.uuid(),
            template_uuids: dag.get_templates(),
            dag,
            description: Some(workflow.description()),
            updated_by: created_by.clone(),
            created_by,
            created_at: DateTimeWrapper::from(workflow.created_at()),
            updated_at: DateTimeWrapper::from(workflow.updated_at()),
        }
    }

    pub fn created_by(&self) -> &UserUuid {
        &self.created_by
    }

    pub fn set_created_by(&mut self, created_by: UserUuid) {
        self.created_by = created_by;
    }

    pub fn updated_by(&self) -> &UserUuid {
        &self.updated_by
    }

    pub fn set_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }

    pub fn template_uuids(&self) -> Vec<TemplateUuid> {
        self.template_uuids.clone()
    }

    pub fn dag(&self) -> Dag {
        self.dag.clone()
    }

    pub fn update_dag(&mut self, dag: Dag) {
        self.dag = dag
    }

    pub fn add_template_uuid(&mut self, uuid: TemplateUuid) {
        self.template_uuids.push(uuid);
        self.update_timestamp();
    }

    pub fn add_template_uuids(&mut self, uuids: Vec<TemplateUuid>) {
        self.template_uuids.extend(uuids);
        self.update_timestamp();
    }

    pub fn remove_template_uuid(&mut self, uuid: TemplateUuid) {
        if let Some(pos) = self.template_uuids.iter().position(|x| *x == uuid) {
            self.template_uuids.remove(pos);
            self.update_timestamp()
        }
    }

    pub fn remove_template_uuids(&mut self, uuids: Vec<TemplateUuid>) {
        for uuid in uuids {
            self.remove_template_uuid(uuid);
        }
    }
}

impl Table for WorkflowSchema {
    type I = WorkflowId;
    type U = WorkflowUuid;

    fn id(&self) -> WorkflowId {
        self.id.clone()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> WorkflowUuid {
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
        self.updated_at = DateTimeWrapper::now();
    }
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for WorkflowSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let dag_json: Value = row.try_get("dag").map_err(EpError::database)?;
        let dag: Dag = serde_json::from_value(dag_json).map_err(EpError::database)?;
        let template_uuids = dag.get_templates();
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            template_uuids,
            dag,
            description: row.try_get("description").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateWorkflowSchema {
    id: Option<WorkflowId>,
    description: Option<String>,
    dag: Option<Dag>,
}

impl UpdateWorkflowSchema {
    pub fn new(id: Option<WorkflowId>, description: Option<String>, dag: Option<Dag>) -> Self {
        Self { id, description, dag }
    }
    pub fn id(&self) -> Option<&WorkflowId> {
        self.id.as_ref()
    }
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn dag(&self) -> Option<&Dag> {
        self.dag.as_ref()
    }
    pub fn update(&self, schema: &mut WorkflowSchema) {
        if let Some(id) = self.id() {
            schema.update_id(id.to_string());
        }
        if let Some(description) = self.description() {
            schema.update_description(description.to_string());
        }
        if let Some(dag) = self.dag() {
            schema.update_dag(dag.to_owned());
        }
    }
}

impl redis::ToRedisArgs for WorkflowSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the WorkflowSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for WorkflowSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting WorkflowSchema",
            ))),
        }
    }
}
