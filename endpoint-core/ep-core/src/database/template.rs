pub mod handlebars;
pub mod registry;
pub mod wrapper;

use crate::database::cache::TemplateCache;
use crate::database::schema::Table;
use crate::database::schema::endpoint::{EndpointRequestInput, EndpointTransactionInput};
use crate::database::schema::template::TemplateSchema;
use crate::database::template::handlebars::{ConditionalBlock, Handlebars};
use crate::database::template::wrapper::TemplateValue;
use borsh::{BorshDeserialize, BorshSerialize};
use bytes::BytesMut;
use eden_logger_internal::{ctx_with_trace, log_debug};
use error::{EpError, ResultEP, SerdeError};
use format::endpoint::EpKind;
use format::{EndpointUuid, TemplateId};
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::{Debug, Display};
use std::ops::Deref;
use tokio_postgres::types::{FromSql, IsNull, ToSql, Type};
use utoipa::openapi::{ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize)]
pub struct ApiFields(Vec<(String, Value)>);

impl ApiFields {
    pub fn new(map: Vec<(String, Value)>) -> Self {
        Self(map)
    }
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
    pub fn map(&self) -> &Vec<(String, Value)> {
        &self.0
    }
    pub fn get(&self, name: String) -> Option<&Value> {
        self.0.iter().find(|(key, _)| key == &name).map(|(_, v)| v)
    }
    pub fn insert(&mut self, key: String, value: Value) {
        self.0.push((key, value));
    }
}

impl From<ApiFields> for Value {
    fn from(val: ApiFields) -> Self {
        let mut map = serde_json::Map::new();
        for (k, v) in &val.0 {
            map.insert(k.clone(), v.clone());
        }
        Value::Object(map)
    }
}

impl TryFrom<Value> for ApiFields {
    type Error = EpError;
    fn try_from(value: Value) -> ResultEP<Self> {
        match value {
            Value::Object(map) => {
                let mut data = Self::default();
                for (key, value) in map {
                    data.insert(key.clone(), value.clone());
                }
                Ok(data)
            }
            _ => Err(EpError::Serde(SerdeError::ExpectedJsonObject)),
        }
    }
}

#[derive(Debug, Default, PartialEq, Clone, Serialize, Deserialize, ToSchema)]
pub struct TemplateFields(Vec<(String, Value)>);

impl Deref for TemplateFields {
    type Target = [(String, Value)];

    fn deref(&self) -> &Self::Target {
        self.0.as_slice()
    }
}

impl TemplateFields {
    pub fn new(map: Vec<(String, Value)>) -> Self {
        Self(map)
    }
    pub fn contains_key(&self, key: &str) -> bool {
        self.0.iter().any(|(k, _)| k == key)
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }
    pub fn map(&self) -> &Vec<(String, Value)> {
        &self.0
    }
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.0.iter().find(|(key, _)| key == name).map(|(_, v)| v)
    }
    pub fn insert(&mut self, key: String, value: Value) {
        self.0.push((key, value));
    }
    pub fn extend(&mut self, iter: &TemplateFields) {
        self.0.extend(iter.map().to_owned());
    }
}

impl From<TemplateFields> for Value {
    fn from(val: TemplateFields) -> Self {
        let mut map = serde_json::Map::new();
        for (k, v) in &val.0 {
            map.insert(k.clone(), v.clone());
        }
        Value::Object(map)
    }
}

impl TryFrom<Value> for TemplateFields {
    type Error = EpError;
    fn try_from(value: Value) -> ResultEP<Self> {
        match value {
            Value::Object(map) => {
                let mut data = Self::default();
                for (key, value) in map {
                    data.insert(key.clone(), value.clone());
                }
                Ok(data)
            }
            _ => Err(EpError::Serde(SerdeError::ExpectedJsonObject)),
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
        id: Option<impl Into<TemplateId>>,
        description: Option<String>,
        template: Option<JsonTemplate>,
        llm_recommendation: Option<Option<String>>,
    ) -> Self {
        Self {
            id: id.map(Into::into),
            description,
            template,
            llm_recommendation,
        }
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

// #[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
// pub struct TemplateInput {
//     pub name: String,
//     pub template: TemplateKind,
// }

/// Data structure for storing Read and Write requests
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshDeserialize, BorshSerialize)]
pub struct EndpointRequestTemplate {
    endpoint_uuid: EndpointUuid,
    request: EndpointRequestInput,
}

impl EndpointRequestTemplate {
    pub fn new(endpoint_uuid: EndpointUuid, request: EndpointRequestInput) -> Self {
        Self { endpoint_uuid, request }
    }
    pub fn get_endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }
    pub fn get_request(&self) -> &EndpointRequestInput {
        &self.request
    }
    #[named]
    pub fn get_mut_request(&mut self) -> &mut EndpointRequestInput {
        let _ctx = ctx_with_trace!().with_feature("ep_core");
        log_debug!(
            _ctx,
            "EndpointRequestTemplate:get_mut_request",
            audience = eden_logger_internal::LogAudience::Internal,
            request = format!("{:?}", self.request)
        );
        &mut self.request
    }
}

impl ToSchema for EndpointRequestTemplate {}
impl PartialSchema for EndpointRequestTemplate {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("endpoint_uuid", EndpointUuid::schema())
                .property("request", EndpointRequestInput::schema())
                .required("endpoint_uuid")
                .required("request")
                .build(),
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshDeserialize, BorshSerialize, ToSchema)]
pub struct EndpointTransactionTemplate {
    endpoint_uuid: EndpointUuid,
    transaction: EndpointTransactionInput,
}

impl EndpointTransactionTemplate {
    pub fn new(endpoint_uuid: EndpointUuid, transaction: EndpointTransactionInput) -> Self {
        Self { endpoint_uuid, transaction }
    }
    pub fn get_endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }
    pub fn get_transaction(&self) -> &EndpointTransactionInput {
        &self.transaction
    }
}

#[derive(Debug, Default, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, PartialEq, Eq)]
pub enum TemplateKind {
    #[default]
    Read,
    Write,
    Transaction,
    TwoPhaseTransaction,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum TemplateOutput {
    Read(EndpointRequestTemplate),
    Write(EndpointRequestTemplate),
    Transaction(EndpointTransactionTemplate),
    TwoPhaseTransaction(EndpointTransactionTemplate),
}

// This assumes the type is stored as JSONB in Postgres
impl<'a> FromSql<'a> for TemplateOutput {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // First, ensure we're dealing with a JSONB type
        if *ty != Type::JSONB {
            return Err("Expected JSONB type for TemplateKind".into());
        }

        // Parse the JSONB from Postgres
        let json_value: serde_json::Value = serde_json::from_slice(
            // Skip the version byte that Postgres JSONB includes
            &raw[1..],
        )?;

        // Deserialize into our enum
        let template_kind = serde_json::from_value(json_value)?;

        Ok(template_kind)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

impl ToSql for TemplateOutput {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Verify we're writing to a JSONB column
        if *ty != Type::JSONB {
            return Err("TemplateKind can only be serialized to JSONB".into());
        }

        // Convert to JSON
        let json = serde_json::to_value(self)?;

        // Postgres JSONB needs a version byte (1) at the start
        out.extend_from_slice(&[1]);

        // Write the actual JSON data
        let json_bytes = serde_json::to_vec(&json)?;
        out.extend_from_slice(&json_bytes);

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

impl TemplateOutput {
    pub fn kind(&self) -> TemplateKind {
        match self {
            Self::Read(_) => TemplateKind::Read,
            Self::Write(_) => TemplateKind::Write,
            Self::Transaction(_) => TemplateKind::Transaction,
            Self::TwoPhaseTransaction(_) => TemplateKind::TwoPhaseTransaction,
        }
    }
}

impl Display for TemplateOutput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", serde_json::to_string(&self).unwrap_or_default())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct JsonTemplate {
    kind: TemplateKind,
    endpoint_uuid: EndpointUuid,
    handlebars: Handlebars,
    ep_kind: EpKind,
    cache: Option<TemplateCache>,
}

impl JsonTemplate {
    pub fn new(
        endpoint_uuid: EndpointUuid,
        kind: TemplateKind,
        template: TemplateValue,
        conditions: Vec<ConditionalBlock>,
        ep_kind: EpKind,
        cache: Option<TemplateCache>,
    ) -> ResultEP<Self> {
        Ok(JsonTemplate {
            endpoint_uuid,
            kind,
            handlebars: Handlebars::new_with_conditions(template.into(), conditions)?,
            ep_kind,
            cache,
        })
    }

    /// Constructor that uses pre-compiled Handlebars (for optimization).
    pub fn new_with_cached_handlebars(
        endpoint_uuid: EndpointUuid,
        kind: TemplateKind,
        handlebars: Handlebars,
        ep_kind: EpKind,
        cache: Option<TemplateCache>,
    ) -> Self {
        JsonTemplate { endpoint_uuid, kind, handlebars, ep_kind, cache }
    }

    /// High-performance rendering method
    pub fn render(&self, values: &TemplateFields) -> ResultEP<Value> {
        self.handlebars.render(values)
    }

    pub fn kind(&self) -> &TemplateKind {
        &self.kind
    }

    pub fn ep_kind(&self) -> EpKind {
        self.ep_kind
    }

    pub fn handlebars(&self) -> &Handlebars {
        &self.handlebars
    }

    pub fn endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }

    pub fn cache(&self) -> &Option<TemplateCache> {
        &self.cache
    }

    pub fn mut_cache(&mut self) -> &mut Option<TemplateCache> {
        &mut self.cache
    }
}

// This assumes the type is stored as JSONB in Postgres
impl<'a> FromSql<'a> for JsonTemplate {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // First, ensure we're dealing with a JSONB type
        if *ty != Type::JSONB {
            return Err("Expected JSONB type for TemplateKind".into());
        }

        let json_value: serde_json::Value = serde_json::from_slice(
            // Skip the version byte that Postgres JSONB includes
            &raw[1..],
        )?;

        let json_template = serde_json::from_value(json_value)?;

        Ok(json_template)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

impl ToSql for JsonTemplate {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Verify we're writing to a JSONB column
        if *ty != Type::JSONB {
            return Err("Template can only be serialized to JSONB".into());
        }

        let json = serde_json::to_value(self)?;

        // Postgres JSONB needs a version byte (1) at the start
        out.extend_from_slice(&[1]);

        let json_bytes = serde_json::to_vec(&json)?;
        out.extend_from_slice(&json_bytes);

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }

    fn to_sql_checked(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        self.to_sql(ty, out)
    }
}

#[cfg(test)]
mod tests {
    use crate::database::template::wrapper::TemplateValue;
    use bytes::BytesMut;
    use postgres_types::{FromSql, ToSql};
    use serde_json::{Value, json};
    use tokio_postgres::types::Type;

    #[test]
    fn test_template_value_creation() {
        // Test creation from a JSON Value
        let json_value = json!({"name": "test", "values": [1, 2, 3]});
        let template_value = TemplateValue::from(json_value.clone());

        // Verify the inner value matches the original
        assert_eq!(template_value.value(), &json_value);
    }

    #[test]
    fn test_template_value_serde() {
        // Create a complex JSON structure
        let json_value = json!({
            "template": "Hello {{name}}",
            "metadata": {
                "version": 1,
                "tags": ["greeting", "personalized"]
            },
            "options": {
                "escape": false,
                "strict": true
            }
        });

        // Create a TemplateValue
        let template_value = TemplateValue::from(json_value);

        // Serialize to JSON string
        let serialized = serde_json::to_string(&template_value).expect("Failed to serialize");

        // Deserialize back to TemplateValue
        let deserialized: TemplateValue = serde_json::from_str(&serialized).expect("Failed to deserialize");

        // Verify they match
        assert_eq!(template_value, deserialized);
    }

    #[test]
    fn test_to_sql_jsonb() {
        // Create a simple template
        let template_value = TemplateValue::from(json!({"greeting": "Hello World"}));
        let mut bytes = BytesMut::new();

        // Convert to SQL
        let result = template_value.to_sql(&Type::JSONB, &mut bytes);
        assert!(result.is_ok());

        // Check that the first byte is the JSONB version (1)
        assert!(!bytes.is_empty());
        assert_eq!(bytes[0], 1);

        // Parse the rest of the bytes and verify it matches our original json
        let parsed_json: Value = serde_json::from_slice(&bytes[1..]).expect("Failed to parse JSON");
        assert_eq!(parsed_json, *template_value.value());
    }

    #[test]
    fn test_to_sql_wrong_type() {
        // Try to convert to a non-JSONB type
        let template_value = TemplateValue::from(json!({"greeting": "Hello World"}));
        let mut bytes = BytesMut::new();

        // This should fail as we only support JSONB
        let result = template_value.to_sql(&Type::TEXT, &mut bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_valid_jsonb() {
        // Create valid JSONB data (version byte + JSON)
        let json_value = json!({"template": "{{variable}}", "required": true});
        let json_bytes = serde_json::to_vec(&json_value).expect("Failed to serialize JSON");

        // Create the raw JSONB data with version byte
        let mut raw_data = vec![1]; // Version byte
        raw_data.extend_from_slice(&json_bytes);

        // Parse from SQL
        let result = TemplateValue::from_sql(&Type::JSONB, &raw_data);
        assert!(result.is_ok());

        // Verify the parsed value
        let template_value = result.unwrap_or_default();
        assert_eq!(template_value.value(), &json_value);
    }

    #[test]
    fn test_from_sql_invalid_version() {
        // Create JSON data with incorrect version byte
        let json_value = json!({"template": "{{variable}}"});
        let json_bytes = serde_json::to_vec(&json_value).expect("Failed to serialize JSON");

        // Create the raw data with an invalid version byte
        let mut raw_data = vec![2]; // Invalid version byte (should be 1)
        raw_data.extend_from_slice(&json_bytes);

        // This should fail due to invalid version
        let result = TemplateValue::from_sql(&Type::JSONB, &raw_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_wrong_type() {
        // Try to parse from non-JSONB type
        let json_value = json!({"template": "{{variable}}"});
        let json_bytes = serde_json::to_vec(&json_value).expect("Failed to serialize JSON");

        let result = TemplateValue::from_sql(&Type::TEXT, &json_bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_from_sql_empty_data() {
        // Try to parse from empty data
        let empty_data: &[u8] = &[];
        let result = TemplateValue::from_sql(&Type::JSONB, empty_data);
        assert!(result.is_err());
    }

    #[test]
    fn test_to_sql_checked() {
        // Test that to_sql_checked calls to_sql
        let template_value = TemplateValue::from(json!({"greeting": "Hello World"}));
        let mut bytes1 = BytesMut::new();
        let mut bytes2 = BytesMut::new();

        // Call both methods
        let result1 = template_value.to_sql(&Type::JSONB, &mut bytes1);
        let result2 = template_value.to_sql_checked(&Type::JSONB, &mut bytes2);

        // Both should succeed
        assert!(result1.is_ok());
        assert!(result2.is_ok());

        // Both should produce the same output
        assert_eq!(bytes1, bytes2);
    }

    // #[test]
    // fn test_accepts_type() {
    //     // Test the type acceptance logic
    //     assert!(TemplateValue::accepts(&Type::JSONB));
    //     assert!(!TemplateValue::accepts(&Type::TEXT));
    //     assert!(!TemplateValue::accepts(&Type::INT4));
    // }

    #[test]
    fn test_roundtrip_conversion() {
        // Test full roundtrip: TemplateValue -> SQL bytes -> TemplateValue
        let original = TemplateValue::from(json!({
            "complex": {
                "nested": ["array", "of", "values"],
                "number": 42,
                "boolean": true,
                "null": null
            }
        }));

        // Convert to SQL bytes
        let mut bytes = BytesMut::new();
        let result = original.to_sql(&Type::JSONB, &mut bytes);
        assert!(result.is_ok());

        // Convert back to TemplateValue
        let recovered = TemplateValue::from_sql(&Type::JSONB, &bytes);
        assert!(recovered.is_ok());

        // Verify they match
        assert_eq!(original, recovered.unwrap_or_default());
    }

    #[test]
    fn test_complex_template_structure() {
        // Test a complex template structure that might be used for a real template
        let template_value = TemplateValue::from(json!({
            "kind": "Mongo",
            "type": "database.collection.aggregate",
            "database": "{{database}}",
            "collection": "{{collection}}",
            "pipeline": [
                { "$match": { "{{field}}": { "$gte": "{{min_value}}" } } },
                { "$group": { "_id": "${{group_by}}", "count": { "$sum": 1 } } },
                { "$sort": { "count": -1 } },
                { "$limit": "{{limit}}" }
            ],
            "options": {
                "allowDiskUse": true,
                "maxTimeMS": 5000
            }
        }));

        // Serialize and deserialize
        let serialized = serde_json::to_string(&template_value).expect("Failed to serialize");
        let deserialized: TemplateValue = serde_json::from_str(&serialized).expect("Failed to deserialize");

        assert_eq!(template_value, deserialized);

        // Convert to SQL and back
        let mut bytes = BytesMut::new();
        template_value.to_sql(&Type::JSONB, &mut bytes).expect("Failed to convert to SQL");

        let from_sql = TemplateValue::from_sql(&Type::JSONB, &bytes).expect("Failed to parse from SQL");
        assert_eq!(template_value, from_sql);
    }

    #[test]
    fn test_redis_template_structure() {
        // Test a Redis template structure
        let template_value = TemplateValue::from(json!({
            "kind": "Redis",
            "type": "GET",
            "key": "user:{{user_id}}:profile",
            "value": null
        }));

        // Deep check internal structure
        let value = template_value.value();
        assert_eq!(value["kind"], "Redis");
        assert_eq!(value["type"], "GET");
        assert_eq!(value["key"], "user:{{user_id}}:profile");
        assert_eq!(value["value"], Value::Null);

        // Test SQL conversion
        let mut bytes = BytesMut::new();
        template_value.to_sql(&Type::JSONB, &mut bytes).expect("Failed to convert to SQL");

        let from_sql = TemplateValue::from_sql(&Type::JSONB, &bytes).expect("Failed to parse from SQL");
        assert_eq!(template_value, from_sql);
    }

    #[test]
    fn test_transaction_template_structure() {
        // Test a transaction template with multiple operations
        let template_value = TemplateValue::from(json!({
            "operations": [
                {
                    "kind": "Redis",
                    "type": "HSET",
                    "key": "user:{{user_id}}",
                    "value": {
                        "name": "{{name}}",
                        "email": "{{email}}",
                        "created_at": "{{timestamp}}"
                    }
                },
                {
                    "kind": "Redis",
                    "type": "SADD",
                    "key": "users:active",
                    "value": ["{{user_id}}"]
                },
                {
                    "kind": "Mongo",
                    "type": "database_collection_insertOne",
                    "database": "users",
                    "collection": "profiles",
                    "document": {
                        "userId": "{{user_id}}",
                        "name": "{{name}}",
                        "email": "{{email}}",
                        "createdAt": "{{timestamp}}"
                    }
                }
            ]
        }));

        // Test structure access
        let value = template_value.value();
        let operations = value["operations"].as_array().expect("Operations should be an array");
        assert_eq!(operations.len(), 3);

        // Test the Redis HSET operation
        let op1 = &operations[0];
        assert_eq!(op1["kind"], "Redis");
        assert_eq!(op1["type"], "HSET");
        assert_eq!(op1["key"], "user:{{user_id}}");

        // Test the Redis SADD operation
        let op2 = &operations[1];
        assert_eq!(op2["kind"], "Redis");
        assert_eq!(op2["type"], "SADD");

        // Test the Mongo insertOne operation
        let op3 = &operations[2];
        assert_eq!(op3["kind"], "Mongo");
        assert_eq!(op3["type"], "database_collection_insertOne");
        assert_eq!(op3["database"], "users");

        // Test roundtrip conversion
        let serialized = serde_json::to_string(&template_value).expect("Failed to serialize");
        let deserialized: TemplateValue = serde_json::from_str(&serialized).expect("Failed to deserialize");
        assert_eq!(template_value, deserialized);
    }

    #[test]
    fn test_template_value_from_raw_json() {
        // Test creating a TemplateValue from raw JSON string
        let json_str = r#"{"kind":"Redis","type":"GET","key":"user:{{id}}","value":null}"#;

        // Parse JSON string to Value
        let json_value: Value = serde_json::from_str(json_str).expect("Failed to parse JSON");

        // Create TemplateValue
        let template_value = TemplateValue::from(json_value);

        // Verify structure
        assert_eq!(template_value.value()["kind"], "Redis");
        assert_eq!(template_value.value()["type"], "GET");
        assert_eq!(template_value.value()["key"], "user:{{id}}");
        assert_eq!(template_value.value()["value"], Value::Null);
    }

    #[test]
    fn test_template_with_array_values() {
        // Test template with array values
        let template_value = TemplateValue::from(json!({
            "kind": "Redis",
            "type": "LPUSH",
            "key": "list:{{list_id}}",
            "value": ["{{item1}}", "{{item2}}", "{{item3}}"]
        }));

        // Check array access
        let value = template_value.value();
        let array_value = value["value"].as_array().expect("Value should be an array");
        assert_eq!(array_value.len(), 3);
        assert_eq!(array_value[0], "{{item1}}");
        assert_eq!(array_value[1], "{{item2}}");
        assert_eq!(array_value[2], "{{item3}}");

        // Test roundtrip
        let mut bytes = BytesMut::new();
        template_value.to_sql(&Type::JSONB, &mut bytes).expect("Failed to convert to SQL");

        let from_sql = TemplateValue::from_sql(&Type::JSONB, &bytes).expect("Failed to parse from SQL");
        assert_eq!(template_value, from_sql);
    }

    #[test]
    fn test_complex_nested_document() {
        // Test with a complex nested document structure
        let template_value = TemplateValue::from(json!({
            "kind": "Mongo",
            "type": "database_collection_insertOne",
            "database": "{{database}}",
            "collection": "{{collection}}",
            "document": {
                "user": {
                    "id": "{{user_id}}",
                    "profile": {
                        "name": {
                            "first": "{{first_name}}",
                            "last": "{{last_name}}"
                        },
                        "contact": {
                            "email": "{{email}}",
                            "phone": "{{phone}}"
                        },
                        "preferences": {
                            "theme": "{{theme}}",
                            "notifications": {
                                "email": "{{email_notifications}}",
                                "push": "{{push_notifications}}",
                                "sms": "{{sms_notifications}}"
                            }
                        }
                    },
                    "metadata": {
                        "created_at": "{{timestamp}}",
                        "last_login": null,
                        "tags": ["{{tag1}}", "{{tag2}}"]
                    }
                }
            }
        }));

        // Check deep nested access
        let value = template_value.value();
        let document = &value["document"];
        let user = &document["user"];
        let profile = &user["profile"];
        let name = &profile["name"];

        assert_eq!(name["first"], "{{first_name}}");
        assert_eq!(name["last"], "{{last_name}}");

        let notifications = &profile["preferences"]["notifications"];
        assert_eq!(notifications["email"], "{{email_notifications}}");

        let tags = user["metadata"]["tags"].as_array().unwrap();
        assert_eq!(tags[0], "{{tag1}}");
        assert_eq!(tags[1], "{{tag2}}");

        // Test SQL roundtrip with complex structure
        let mut bytes = BytesMut::new();
        template_value.to_sql(&Type::JSONB, &mut bytes).expect("Failed to convert to SQL");

        let from_sql = TemplateValue::from_sql(&Type::JSONB, &bytes).expect("Failed to parse from SQL");
        assert_eq!(template_value, from_sql);
    }
}
