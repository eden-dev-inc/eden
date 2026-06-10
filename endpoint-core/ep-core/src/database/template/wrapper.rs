use bytes::BytesMut;
use postgres_types::{FromSql, IsNull, ToSql, Type};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::error::Error;
use std::fmt::Display;
use std::ops::Deref;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct TemplateFieldName(String);

impl Deref for TemplateFieldName {
    type Target = str;
    fn deref(&self) -> &Self::Target {
        self.0.as_str()
    }
}

impl TemplateFieldName {
    pub fn new(field_name: impl Into<String>) -> Self {
        TemplateFieldName(field_name.into())
    }
}

impl From<String> for TemplateFieldName {
    fn from(name: String) -> Self {
        TemplateFieldName::new(name)
    }
}

impl From<&str> for TemplateFieldName {
    fn from(name: &str) -> Self {
        TemplateFieldName::new(name)
    }
}

impl From<TemplateFieldName> for String {
    fn from(val: TemplateFieldName) -> Self {
        val.0
    }
}

impl From<&TemplateFieldName> for String {
    fn from(val: &TemplateFieldName) -> Self {
        val.0.clone()
    }
}

impl Display for TemplateFieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

/// A template value that can hold any valid JSON structure.
/// This is used for defining request/response templates with handlebars placeholders.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct TemplateValue(Value);

impl utoipa::ToSchema for TemplateValue {
    fn name() -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("TemplateValue")
    }
}
impl utoipa::PartialSchema for TemplateValue {
    fn schema() -> utoipa::openapi::RefOr<utoipa::openapi::schema::Schema> {
        utoipa::openapi::RefOr::T(utoipa::openapi::schema::Schema::Object(
            utoipa::openapi::schema::ObjectBuilder::new()
                .schema_type(utoipa::openapi::schema::Type::Object)
                .description(Some(
                    "JSON template value - can be any valid JSON object with handlebars placeholders like {{field_name}}",
                ))
                .additional_properties(Some(utoipa::openapi::schema::AdditionalProperties::FreeForm(true)))
                .build(),
        ))
    }
}

impl Display for TemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

impl From<TemplateValue> for Value {
    fn from(val: TemplateValue) -> Self {
        val.0
    }
}

impl TemplateValue {
    pub fn new(value: Value) -> Self {
        Self(value)
    }
    pub fn value(&self) -> &Value {
        &self.0
    }
}

impl From<Value> for TemplateValue {
    fn from(value: Value) -> Self {
        TemplateValue(value)
    }
}

// This assumes the type is stored as JSONB in Postgres
impl<'a> FromSql<'a> for TemplateValue {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        // Validate the type first
        if !<Self as FromSql>::accepts(ty) {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Expected JSONB type for TemplateValue, got {:?}", ty),
            )));
        }

        // Ensure we have at least one byte for the version
        if raw.is_empty() {
            return Err(Box::new(std::io::Error::new(std::io::ErrorKind::InvalidData, "Empty JSONB data")));
        }

        // Version byte for JSONB in Postgres should be 1
        if raw[0] != 1 {
            return Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Invalid JSONB version byte: {}", raw[0]),
            )));
        }

        // Parse the JSONB data, skipping the version byte
        serde_json::from_slice(&raw[1..]).map(Self).map_err(|e| Box::new(e) as Box<dyn Error + Sync + Send>)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB
    }
}

impl ToSql for TemplateValue {
    fn to_sql(&self, ty: &Type, out: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        // Verify we're writing to a JSONB column
        if *ty != Type::JSONB {
            return Err("HandlebarsValue can only be serialized to JSONB".into());
        }

        // Convert to JSON
        let json = self.0.to_owned();

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
