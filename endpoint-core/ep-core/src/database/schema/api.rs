use super::Row;
pub use crate::database::api::bindings::{Binding, BindingBuilder};
pub use crate::database::api::fields::{ApiFieldName, FieldSchema};
use crate::database::schema::template::TemplateSchema;
use crate::database::schema::{FromRow, Table};
use crate::database::template::wrapper::TemplateFieldName;
use chrono::{DateTime, Utc};
use error::EpError;
use format::timestamp::DateTimeWrapper;
use format::{ApiId, ApiUuid, EdenId, TemplateUuid, UserUuid};
use redis::{FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::any::Any;
use std::collections::HashSet;
use utoipa::ToSchema;

/// Define an Eden API, which includes multiple templates that are run in parallel as part of every request.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiSchema {
    id: ApiId,
    uuid: ApiUuid,
    description: Option<String>,
    /// Fields for the API to accept.
    fields: Vec<FieldSchema>,
    /// Bindings are a list of all input fields to the API, and the sub-templates that call them
    /// this is required for ensuring that we pass the correct order of fields to each template
    bindings: Vec<Binding>,
    response_logic: Option<serde_json::Value>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl ApiSchema {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        description: Option<String>,
        fields: Vec<FieldSchema>,
        bindings: Vec<Binding>,
        response_logic: Option<serde_json::Value>,
        created_by: UserUuid,
    ) -> Self {
        let now = DateTimeWrapper::now();
        Self {
            id: ApiId::new(id),
            uuid: ApiUuid::new_uuid(),
            description,
            fields,
            bindings,
            response_logic,
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

    pub fn get_template_bindings(&self, template_uuid: &TemplateUuid) -> Option<&[(TemplateFieldName, ApiFieldName)]> {
        self.bindings.iter().find(|binding| binding.template() == template_uuid).map(|binding| binding.fields())
    }

    pub fn fields(&self) -> &[FieldSchema] {
        &self.fields
    }
    pub fn bindings(&self) -> &[Binding] {
        &self.bindings
    }
    pub fn templates(&self) -> Vec<TemplateUuid> {
        self.bindings.iter().map(|binding| binding.template().clone()).collect::<Vec<TemplateUuid>>()
    }
    pub fn response_logic(&self) -> Option<&serde_json::Value> {
        self.response_logic.as_ref()
    }
    pub fn add_binding(&mut self, binding: Binding) {
        self.bindings.push(binding);
    }
    pub fn remove_binding(&mut self, template_uuid: &TemplateUuid) {
        self.bindings.retain(|b| b.template() != template_uuid);
    }
    /// Partial-update setters (used by `PATCH /apis/{api}`); each bumps `updated_at`.
    pub fn set_description(&mut self, description: Option<String>) {
        self.description = description;
        self.update_timestamp();
    }
    pub fn set_fields(&mut self, fields: Vec<FieldSchema>) {
        self.fields = fields;
        self.update_timestamp();
    }
    pub fn set_bindings(&mut self, bindings: Vec<Binding>) {
        self.bindings = bindings;
        self.update_timestamp();
    }
    pub fn set_response_logic(&mut self, response_logic: Option<serde_json::Value>) {
        self.response_logic = response_logic;
        self.update_timestamp();
    }
    /// Validates that the bindings are correct
    pub fn validate_bindings(&self, templates: &[TemplateSchema]) -> Result<(), EpError> {
        if self.bindings.is_empty() {
            return Ok(()); // No bindings to validate
        }

        // Pre-compute API fields for efficient lookup
        let api_fields: HashSet<String> = self.fields().iter().map(|f| f.name.clone()).collect();

        // Create a lookup map for templates by UUID for O(1) access
        let template_lookup: std::collections::HashMap<_, _> = templates.iter().map(|t| (t.template_uuid(), t)).collect();

        let mut validation_errors = Vec::new();

        for (binding_index, binding) in self.bindings.iter().enumerate() {
            let template_uuid = binding.template();

            let template = match template_lookup.get(&template_uuid) {
                Some(template) => template,
                None => {
                    validation_errors.push(format!("Binding {} references unknown template UUID: {}", binding_index, template_uuid));
                    continue; // Skip to next binding
                }
            };

            // Pre-compute template fields for this binding
            let template_fields: HashSet<String> = template.template().handlebars().fields().iter().map(|f| f.name.clone()).collect();

            // Validate each field mapping in this binding
            for (field_index, (template_binding, api_binding)) in binding.fields().iter().enumerate() {
                let template_field = template_binding.to_string();

                // Extract the root field name from the API binding path
                let api_field = match Self::extract_root_field_name(&api_binding.to_string()) {
                    Some(field) => field,
                    None => {
                        validation_errors.push(format!(
                            "Binding {} field {}: Invalid API binding format '{}'",
                            binding_index, field_index, api_binding
                        ));
                        continue;
                    }
                };

                // Validate template field exists
                if !template_fields.contains(&template_field) {
                    validation_errors.push(format!(
                        "Binding {} field {}: Template field '{}' not found in template {}",
                        binding_index, field_index, template_field, template_uuid
                    ));
                }

                // Validate API field exists
                if !api_fields.contains(&api_field) {
                    validation_errors.push(format!(
                        "Binding {} field {}: API field '{}' not found in API schema",
                        binding_index, field_index, api_field
                    ));
                }
            }
        }

        // Return accumulated errors or success
        if validation_errors.is_empty() {
            Ok(())
        } else {
            Err(EpError::api(format!(
                "Binding validation failed with {} error(s):\n{}",
                validation_errors.len(),
                validation_errors.join("\n")
            )))
        }
    }

    /// Extracts the root field name from a potentially dotted API binding path
    /// e.g., "user.profile.name" -> Some("user"), "field" -> Some("field"), "" -> None
    fn extract_root_field_name(api_binding: &str) -> Option<String> {
        if api_binding.is_empty() {
            return None;
        }

        api_binding.split('.').next().filter(|s| !s.is_empty()).map(|s| s.to_string())
    }
}

impl Table for ApiSchema {
    type I = ApiId;
    type U = ApiUuid;

    fn id(&self) -> ApiId {
        self.id.to_owned()
    }
    fn update_id(&mut self, id: String) -> Option<String> {
        let out = self.id.update(id);
        self.update_timestamp();
        Some(out)
    }
    fn uuid(&self) -> ApiUuid {
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
    fn update_updated_by(&mut self, updated_by: UserUuid) {
        self.updated_by = updated_by;
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl FromRow for ApiSchema {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get("description").map_err(EpError::database)?,
            fields: serde_json::from_value(row.try_get("fields").map_err(EpError::database)?).map_err(EpError::serde)?,
            bindings: serde_json::from_value(row.try_get("bindings").map_err(EpError::database)?).map_err(EpError::serde)?,
            response_logic: row.try_get("response_logic").map_err(EpError::database)?,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for ApiSchema {
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

impl FromRedisValue for ApiSchema {
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

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FieldMapping {
    template_field: String,
    api_field: String,
}

impl From<&(TemplateFieldName, ApiFieldName)> for FieldMapping {
    fn from((template_field, api_field): &(TemplateFieldName, ApiFieldName)) -> Self {
        Self {
            template_field: template_field.to_string(),
            api_field: api_field.to_string(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct TemplateBindingData {
    template_uuid: TemplateUuid,
    field_mappings: Vec<FieldMapping>,
}

impl From<Binding> for TemplateBindingData {
    fn from(binding: Binding) -> Self {
        Self {
            template_uuid: binding.template().clone(),
            field_mappings: binding.fields().iter().map(FieldMapping::from).collect(),
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiSchemaIds {
    id: ApiId,
    uuid: ApiUuid,
    description: String,
    fields: Vec<FieldSchema>,
    template_bindings: Vec<TemplateBindingData>,
    created_by: UserUuid,
    updated_by: UserUuid,
    created_at: DateTimeWrapper,
    updated_at: DateTimeWrapper,
}

impl From<ApiSchema> for ApiSchemaIds {
    fn from(schema: ApiSchema) -> Self {
        Self {
            id: schema.id,
            uuid: schema.uuid,
            description: schema.description.unwrap_or_default(),
            fields: schema.fields,
            template_bindings: schema.bindings.into_iter().map(TemplateBindingData::from).collect(),
            created_by: schema.created_by,
            updated_by: schema.updated_by,
            created_at: schema.created_at,
            updated_at: schema.updated_at,
        }
    }
}

impl FromRow for ApiSchemaIds {
    fn from_row(row: &Row) -> Result<Self, EpError> {
        let bindings: Vec<Binding> = serde_json::from_value(row.try_get("bindings").map_err(EpError::database)?).map_err(EpError::serde)?;
        // let migration = if let Some(migration_uuid) = row
        //     .try_get::<&str, Option<MigrationUuid>>("migration_uuid")
        //     .map_err(EpError::database)?
        // {
        //     Some(ApiMigration {
        //         uuid: migration_uuid,
        //         state: row.try_get("state").map_err(EpError::database)?,
        //         bindings: vec![], // TODO - fix SQL table, missing bindings in migrations serde_json::from_value(
        //         // row.try_get("migration_bindings")
        //         //     .map_err(EpError::database)?,
        //         // )
        //         // .map_err(EpError::serde)?,
        //         response_logic: None, // TODO - fix SQL
        //                               // row
        //                               //     .try_get("migration_response_logic")
        //                               //     .map_err(EpError::database)?,
        //     })
        // } else {
        //     None
        // };
        let template_bindings = bindings
            .iter()
            .map(|b| TemplateBindingData {
                template_uuid: b.template().to_owned(),
                field_mappings: b
                    .fields()
                    .iter()
                    .map(|f| FieldMapping { template_field: f.0.to_string(), api_field: f.1.to_string() })
                    .collect(),
            })
            .collect();
        Ok(Self {
            id: row.try_get("id").map_err(EpError::database)?,
            uuid: row.try_get("uuid").map_err(EpError::database)?,
            description: row.try_get::<&str, Option<String>>("description").map_err(EpError::database)?.unwrap_or_default(),
            fields: serde_json::from_value(row.try_get("fields").map_err(EpError::database)?).map_err(EpError::serde)?,
            template_bindings,
            created_by: row.try_get("created_by").map_err(EpError::database)?,
            updated_by: row.try_get("updated_by").map_err(EpError::database)?,
            created_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("created_at").map_err(EpError::database)?),
            updated_at: DateTimeWrapper::from(row.try_get::<_, DateTime<Utc>>("updated_at").map_err(EpError::database)?),
        })
    }
}

impl ToRedisArgs for ApiSchemaIds {
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

impl FromRedisValue for ApiSchemaIds {
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

/// Partial update for an API (`PATCH /apis/{api}`). Only the provided fields are
/// changed; `bindings` are supplied as `BindingBuilder`s (template id → uuid is
/// resolved server-side, like create).
#[derive(Debug, Clone, Default, Serialize, Deserialize, ToSchema)]
pub struct UpdateApiSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    fields: Option<Vec<FieldSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bindings: Option<Vec<BindingBuilder>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    response_logic: Option<serde_json::Value>,
}

impl UpdateApiSchema {
    pub fn description(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn fields(&self) -> Option<&Vec<FieldSchema>> {
        self.fields.as_ref()
    }
    pub fn bindings(&self) -> Option<&Vec<BindingBuilder>> {
        self.bindings.as_ref()
    }
    pub fn response_logic(&self) -> Option<&serde_json::Value> {
        self.response_logic.as_ref()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ApiBuilder {
    id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    fields: Vec<FieldSchema>,
    bindings: Vec<BindingBuilder>,
    #[serde(skip_serializing_if = "Option::is_none")]
    response_logic: Option<serde_json::Value>,
}

impl ApiBuilder {
    pub fn new(
        id: String,
        description: Option<String>,
        fields: Vec<FieldSchema>,
        bindings: Vec<BindingBuilder>,
        response_logic: Option<serde_json::Value>,
    ) -> Self {
        Self { id, description, fields, bindings, response_logic }
    }

    pub fn id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }
    pub fn id_ref(&self) -> &String {
        &self.id
    }
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }
    pub fn description_ref(&self) -> Option<&String> {
        self.description.as_ref()
    }
    pub fn fields(mut self, fields: Vec<FieldSchema>) -> Self {
        self.fields = fields;
        self
    }
    pub fn fields_ref(&self) -> &Vec<FieldSchema> {
        &self.fields
    }
    pub fn bindings(mut self, bindings: Vec<BindingBuilder>) -> Self {
        self.bindings = bindings;
        self
    }
    pub fn bindings_ref(&self) -> &Vec<BindingBuilder> {
        &self.bindings
    }
    pub fn response_logic(mut self, response_logic: Option<serde_json::Value>) -> Self {
        self.response_logic = response_logic;
        self
    }
    pub fn response_logic_ref(&self) -> Option<&serde_json::Value> {
        self.response_logic.as_ref()
    }
}
