use super::Row;
use crate::database::api::fields::FieldType;
use crate::database::cache::TemplateCache;
use crate::database::schema::{FromRow, Table};
use crate::database::template::handlebars::{ConditionalBlock, FieldRequirement};
use crate::database::template::wrapper::TemplateValue;
use crate::database::template::{JsonTemplate, TemplateKind};
use chrono::{DateTime, Utc};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use format::timestamp::DateTimeWrapper;
use format::{EdenId, EndpointUuid, TemplateId, TemplateUuid, UserUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::any::Any;
use utoipa::ToSchema;

/// Template definition with Handlebars templating and endpoint binding.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct TemplateSchema {
    id: TemplateId,
    uuid: TemplateUuid,
    template: JsonTemplate,
    description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    llm_recommendation: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl TryFrom<(TemplateBuilder, UserUuid)> for TemplateSchema {
    type Error = EpError;
    fn try_from((constructor, created_by): (TemplateBuilder, UserUuid)) -> ResultEP<Self> {
        let json_template = JsonTemplate::new(
            constructor.endpoint,
            constructor.kind,
            constructor.template,
            constructor.conditions,
            constructor.ep_kind,
            constructor.cache,
        )?;
        Ok(Self::new(
            constructor.id,
            json_template,
            constructor.description,
            constructor.llm_recommendation,
            created_by,
        ))
    }
}

impl TemplateSchema {
    pub fn new(
        id: TemplateId,
        template: JsonTemplate,
        description: Option<String>,
        llm_recommendation: Option<String>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id,
            uuid: TemplateUuid::new_uuid(),
            template,
            description,
            llm_recommendation,
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
    pub fn template_uuid(&self) -> &TemplateUuid {
        &self.uuid
    }
    pub fn template(&self) -> &JsonTemplate {
        &self.template
    }
    pub fn update_template(&mut self, template: JsonTemplate) {
        self.template = template;
        self.update_timestamp();
    }
    pub fn llm_recommendation(&self) -> Option<&String> {
        self.llm_recommendation.as_ref()
    }
    pub fn update_llm_recommendation(&mut self, recommendation: Option<String>) -> Option<String> {
        let previous = self.llm_recommendation.clone();
        self.llm_recommendation = recommendation;
        self.update_timestamp();
        previous
    }
}

impl Table for TemplateSchema {
    type U = TemplateUuid;
    type I = TemplateId;

    fn id(&self) -> TemplateId {
        self.id.clone()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> TemplateUuid {
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

impl FromRow for TemplateSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            template: row.try_get("template").map_err(EpError::database)?,
            // template: row.try_get("template").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            llm_recommendation: row.try_get("llm_recommendation").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateField {
    name: String,
    field_type: FieldType,
    description: String,
    required: bool,
    #[schema(value_type = Object, additional_properties)]
    default_value: Option<Value>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateCondition {
    trigger: String,
    #[schema(value_type = Object, additional_properties)]
    condition: Value,
    path: Option<String>,
}

impl From<ConditionalBlock> for TemplateCondition {
    fn from(conditional_block: ConditionalBlock) -> Self {
        Self {
            trigger: conditional_block.trigger_field,
            condition: conditional_block.template_addition,
            path: conditional_block.merge_at_path,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateHandlebars {
    #[schema(value_type = Object, additional_properties)]
    template: Value,
    fields: Vec<TemplateField>,
    conditions: Vec<TemplateCondition>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateContent {
    kind: String,
    endpoint_uuid: EndpointUuid,
    handlebars: TemplateHandlebars,
    cache: Option<TemplateCache>,
}
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateSchemaIds {
    id: TemplateId,
    uuid: TemplateUuid,
    description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    llm_recommendation: Option<String>,
    created_by: UserUuid,
    updated_by: UserUuid,
    template: TemplateContent,
}

impl FromRow for TemplateSchemaIds {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let template = row.try_get::<&str, JsonTemplate>("template").map_err(EpError::database)?;
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            llm_recommendation: row.try_get("llm_recommendation").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            template: TemplateContent {
                kind: String::default(),
                endpoint_uuid: template.endpoint_uuid().to_owned(),
                handlebars: TemplateHandlebars {
                    template: template.handlebars().template().to_owned(),
                    fields: template
                        .handlebars()
                        .fields()
                        .iter()
                        .map(|f| TemplateField {
                            name: f.name.to_owned(),
                            field_type: f.field_type.to_owned(),
                            description: String::default(),
                            required: f.requirement == FieldRequirement::Required,
                            default_value: f.default_value.to_owned(),
                        })
                        .collect(), // convert FieldInfo into TemplateField
                    conditions: template.handlebars().conditional_blocks().iter().map(|c| c.clone().into()).collect(), // convert from ConditionalBlock to TemplateCondition
                },
                cache: template.cache().clone(),
            },
        })
    }
}

impl ToRedisArgs for TemplateSchemaIds {
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

impl FromRedisValue for TemplateSchemaIds {
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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateTemplateSchema {
    id: Option<TemplateId>,
    description: Option<String>,
    template: Option<JsonTemplate>,
    #[serde(default)]
    llm_recommendation: Option<Option<String>>,
}

impl UpdateTemplateSchema {
    pub fn new(
        id: Option<TemplateId>,
        description: Option<String>,
        template: Option<JsonTemplate>,
        llm_recommendation: Option<Option<String>>,
    ) -> Self {
        Self { id, description, template, llm_recommendation }
    }
    pub fn id(&self) -> Option<&TemplateId> {
        self.id.as_ref()
    }
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn template(&self) -> Option<&JsonTemplate> {
        self.template.as_ref()
    }
    pub fn llm_recommendation(&self) -> Option<&Option<String>> {
        self.llm_recommendation.as_ref()
    }
    pub fn set_llm_recommendation(&mut self, recommendation: Option<String>) {
        self.llm_recommendation = Some(recommendation);
    }
    pub fn update(&self, schema: &mut TemplateSchema) {
        if let Some(id) = self.id() {
            schema.update_id(id.to_string());
        }
        if let Some(description) = self.description() {
            schema.update_description(description.to_string());
        }
        if let Some(template) = self.template() {
            schema.update_template(template.to_owned())
        }
        if let Some(recommendation) = &self.llm_recommendation {
            schema.update_llm_recommendation(recommendation.clone());
        }
    }
}

impl ToRedisArgs for TemplateSchema {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + redis::RedisWrite,
    {
        // Serialize the TemplateSchema to JSON
        let serialized = serde_json::to_vec(self).unwrap_or_default();

        // Write the serialized bytes to the Redis output
        out.write_arg(&serialized);
    }
}

impl FromRedisValue for TemplateSchema {
    fn from_redis_value(v: &redis::Value) -> Result<Self, redis::RedisError> {
        match v {
            // redis::Value::Data
            redis::Value::BulkString(bytes) => serde_json::from_slice(bytes)
                .map_err(|e| redis::RedisError::from((redis::ErrorKind::ParseError, "Failed to deserialize JSON", e.to_string()))),
            _ => Err(redis::RedisError::from((
                redis::ErrorKind::ResponseError,
                "Invalid response type when expecting TemplateSchema",
            ))),
        }
    }
}

/// Data structure for uploading templates from the client.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemplateBuilder {
    pub(crate) id: TemplateId,
    pub(crate) endpoint: EndpointUuid,
    pub(crate) ep_kind: EpKind,
    pub(crate) kind: TemplateKind,
    pub(crate) template: TemplateValue,
    pub(crate) conditions: Vec<ConditionalBlock>,
    pub(crate) description: Option<String>,
    pub(crate) cache: Option<TemplateCache>,
    #[serde(default)]
    pub(crate) llm_recommendation: Option<String>,
}

impl TemplateBuilder {
    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: impl Into<TemplateId>,
        endpoint: impl Into<EndpointUuid>,
        ep_kind: impl Into<EpKind>,
        kind: TemplateKind,
        template: TemplateValue,
        conditions: Vec<ConditionalBlock>,
        description: Option<String>,
        cache: Option<TemplateCache>,
    ) -> Self {
        Self {
            id: id.into(),
            endpoint: endpoint.into(),
            ep_kind: ep_kind.into(),
            kind,
            template,
            conditions,
            description,
            cache,
            llm_recommendation: None,
        }
    }

    pub fn id(&self) -> &TemplateId {
        &self.id
    }

    pub fn endpoint(&self) -> &EndpointUuid {
        &self.endpoint
    }

    pub fn ep_kind(&self) -> EpKind {
        self.ep_kind
    }

    pub fn kind(&self) -> &TemplateKind {
        &self.kind
    }

    pub fn template(&self) -> &TemplateValue {
        &self.template
    }

    pub fn description(&self) -> &Option<String> {
        &self.description
    }

    pub fn cache(&self) -> &Option<TemplateCache> {
        &self.cache
    }

    pub fn conditions(&self) -> &Vec<ConditionalBlock> {
        &self.conditions
    }

    pub fn llm_recommendation(&self) -> &Option<String> {
        &self.llm_recommendation
    }
}
