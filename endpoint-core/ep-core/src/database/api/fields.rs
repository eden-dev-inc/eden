use error::EpError;
use postgres_types::{FromSql, ToSql, Type};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt::Display;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, PartialEq)]
pub struct ApiFieldName(String);

impl ApiFieldName {
    pub fn new(field_name: impl Into<String>) -> Self {
        ApiFieldName(field_name.into())
    }
}

impl From<String> for ApiFieldName {
    fn from(name: String) -> Self {
        ApiFieldName::new(name)
    }
}

impl From<&str> for ApiFieldName {
    fn from(name: &str) -> Self {
        ApiFieldName::new(name)
    }
}

impl From<ApiFieldName> for String {
    fn from(val: ApiFieldName) -> Self {
        val.0
    }
}

impl From<&ApiFieldName> for String {
    fn from(val: &ApiFieldName) -> Self {
        val.0.clone()
    }
}

impl Display for ApiFieldName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.0, f)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
pub enum FieldType {
    String,
    Number,
    Boolean,
    Array,
    Object,
    #[default]
    Null,
}

impl Display for FieldType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FieldType::String => write!(f, "string"),
            FieldType::Number => write!(f, "number"),
            FieldType::Boolean => write!(f, "boolean"),
            FieldType::Array => write!(f, "array"),
            FieldType::Object => write!(f, "object"),
            FieldType::Null => write!(f, "null"),
        }
    }
}

impl TryFrom<&str> for FieldType {
    type Error = EpError;
    fn try_from(string: &str) -> Result<Self, Self::Error> {
        match string.to_lowercase().as_str() {
            "string" => Ok(FieldType::String),
            "number" => Ok(FieldType::Number),
            "boolean" => Ok(FieldType::Boolean),
            "array" => Ok(FieldType::Array),
            "object" => Ok(FieldType::Object),
            "null" => Ok(FieldType::Null),
            _ => Err(EpError::parse(format!("unknown field type: {}", string))),
        }
    }
}

impl From<&serde_json::Value> for FieldType {
    fn from(value: &serde_json::Value) -> Self {
        match value {
            serde_json::Value::String(_) => FieldType::String,
            serde_json::Value::Number(_) => FieldType::Number,
            serde_json::Value::Bool(_) => FieldType::Boolean,
            serde_json::Value::Array(_) => FieldType::Array,
            serde_json::Value::Object(_) => FieldType::Object,
            serde_json::Value::Null => FieldType::Null,
        }
    }
}

impl FieldType {
    pub fn matches(&self, value: &serde_json::Value) -> bool {
        FieldType::from(value) == *self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct FieldSchema {
    pub name: String,
    pub field_type: FieldType,
    pub description: String,
    pub required: bool,
}

impl FieldSchema {
    pub fn new(name: impl Into<String>, field_type: FieldType, description: impl Into<String>, required: bool) -> Self {
        Self {
            name: name.into(),
            field_type,
            description: description.into(),
            required,
        }
    }
}

impl<'a> FromSql<'a> for FieldSchema {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => {
                let json_str = std::str::from_utf8(raw)?;
                let field_schema: FieldSchema = serde_json::from_str(json_str)?;
                Ok(field_schema)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_str = std::str::from_utf8(raw)?;
                let field_schema: FieldSchema = serde_json::from_str(json_str)?;
                Ok(field_schema)
            }
            _ => Err(format!("cannot convert from SQL type {} to FieldSchema", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }
}

impl ToSql for FieldSchema {
    fn to_sql(&self, ty: &Type, out: &mut bytes::BytesMut) -> Result<postgres_types::IsNull, Box<dyn Error + Sync + Send>> {
        match *ty {
            Type::JSON | Type::JSONB => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            Type::TEXT | Type::VARCHAR => {
                let json_string = serde_json::to_string(self)?;
                json_string.to_sql(ty, out)
            }
            _ => Err(format!("cannot convert FieldSchema to SQL type {}", ty).into()),
        }
    }

    fn accepts(ty: &Type) -> bool {
        matches!(*ty, Type::JSON | Type::JSONB | Type::TEXT | Type::VARCHAR)
    }

    postgres_types::to_sql_checked!();
}
