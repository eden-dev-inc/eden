#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Response Types
//!
//! Response structures and serialization for Eve operations.
//!
//! ## Overview
//!
//! Defines standard response formats:
//! - Success responses
//! - Error responses
//! - Typed data responses
//!
//! ## Core Type
//!
//! [`EdenResponse<R>`] - Generic response envelope:
//! - `Ok(String)` - Success message
//! - `Err(String)` - Error message
//! - `Response(R)` - Typed data response

use error::EpError;
use serde::de::{self, Deserialize, DeserializeOwned, Deserializer};
use serde::{Serialize, Serializer};
use utoipa::ToSchema;

#[derive(Debug, Clone, PartialEq, ToSchema)]
pub enum EdenResponse<R: PartialEq> {
    Ok(String),
    Err(String),
    Response(R),
}

impl<R> Serialize for EdenResponse<R>
where
    R: Serialize + PartialEq,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;

        match self {
            EdenResponse::Ok(message) => {
                let mut state = serializer.serialize_struct("EdenResponse", 1)?;
                state.serialize_field("ok", message.to_lowercase().as_str())?;
                state.end()
            }
            EdenResponse::Err(message) => {
                let mut state = serializer.serialize_struct("EdenResponse", 1)?;
                state.serialize_field("error", message.to_lowercase().as_str())?;
                state.end()
            }
            EdenResponse::Response(response) => response.serialize(serializer),
        }
    }
}

impl<'de, R> Deserialize<'de> for EdenResponse<R>
where
    R: DeserializeOwned + PartialEq,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;

        if let serde_json::Value::Object(ref map) = value {
            if let Some(ok_val) = map.get("ok")
                && let serde_json::Value::String(s) = ok_val
            {
                return Ok(EdenResponse::Ok(s.clone()));
            }

            if let Some(error_val) = map.get("error")
                && let serde_json::Value::String(s) = error_val
            {
                return Ok(EdenResponse::Err(s.clone()));
            }
        }

        // Try to deserialize as R (Response variant)
        let response: R =
            serde_json::from_value(value).map_err(|e| de::Error::custom(format!("Failed to deserialize as response: {e}")))?;

        Ok(EdenResponse::Response(response))
    }
}

impl<R: PartialEq> EdenResponse<R> {
    pub fn ok(message: &str) -> Self {
        Self::Ok(message.to_lowercase())
    }

    pub fn error(message: &str) -> Self {
        Self::Err(message.to_lowercase())
    }

    pub fn response(response: R) -> Self {
        Self::Response(response)
    }
}

impl<R: Serialize + PartialEq> From<EpError> for EdenResponse<R> {
    fn from(error: EpError) -> Self {
        Self::Err(error.to_string().to_lowercase())
    }
}

impl<R: Serialize + PartialEq> From<String> for EdenResponse<R> {
    fn from(ok: String) -> Self {
        Self::Ok(ok.to_lowercase())
    }
}

impl<R: Serialize + PartialEq> From<&str> for EdenResponse<R> {
    fn from(ok: &str) -> Self {
        Self::Ok(ok.to_lowercase())
    }
}

impl<R: Serialize + PartialEq> From<EdenResponse<R>> for Result<actix_web::HttpResponse, actix_web::error::Error> {
    fn from(value: EdenResponse<R>) -> Self {
        match value {
            EdenResponse::Ok(ok) => Ok(actix_web::HttpResponse::Ok().json(ok)),
            EdenResponse::Err(error) => Err(actix_web::error::ErrorInternalServerError(error)),
            EdenResponse::Response(response) => Ok(actix_web::HttpResponse::Ok().json(response)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, Serialize, Clone, PartialEq)]
    struct TestData {
        id: u32,
        name: String,
    }

    #[test]
    fn test_ok_variant() {
        let response = EdenResponse::<TestData>::ok("Operation successful");

        match response {
            EdenResponse::Ok(msg) => assert_eq!(msg, "operation successful"),
            _ => panic!("Expected Ok variant"),
        }
    }

    #[test]
    fn test_error_variant() {
        let response = EdenResponse::<TestData>::error("Something went wrong");

        match response {
            EdenResponse::Err(msg) => assert_eq!(msg, "something went wrong"),
            _ => panic!("Expected Err variant"),
        }
    }

    #[test]
    fn test_response_variant() {
        let test_data = TestData { id: 42, name: "Test".to_string() };

        let response = EdenResponse::response(test_data.clone());

        match response {
            EdenResponse::Response(data) => assert_eq!(data, test_data),
            _ => panic!("Expected Response variant"),
        }
    }

    #[test]
    fn test_serialize_ok() {
        let response = EdenResponse::<TestData>::ok("Success");
        let json = serde_json::to_string(&response).unwrap_or_default();
        let expected = r#"{"ok":"success"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn test_serialize_error() {
        let response = EdenResponse::<TestData>::error("Failed");
        let json = serde_json::to_string(&response).unwrap_or_default();
        let expected = r#"{"error":"failed"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn test_serialize_response() {
        let test_data = TestData { id: 123, name: "Example".to_string() };
        let response = EdenResponse::response(test_data);
        let json = serde_json::to_string(&response).unwrap_or_default();
        let expected = r#"{"id":123,"name":"Example"}"#;
        assert_eq!(json, expected);
    }

    #[test]
    fn test_serialize_pretty() {
        let test_data = TestData { id: 456, name: "Pretty Test".to_string() };
        let response = EdenResponse::response(test_data);
        let json = serde_json::to_string_pretty(&response).unwrap_or_default();

        // Just verify it contains the expected fields
        assert!(json.contains("\"id\": 456"));
        assert!(json.contains("\"name\": \"Pretty Test\""));
    }

    #[test]
    fn test_debug_impl() {
        let response = EdenResponse::<TestData>::ok("Debug test");
        let debug_str = format!("{response:?}");
        assert!(debug_str.contains("Ok"));
        assert!(debug_str.contains("debug test"));
    }

    #[test]
    fn test_constructor_methods() {
        let ok_resp = EdenResponse::<()>::ok("ok message");
        let err_resp = EdenResponse::<()>::error("Error message");
        let data_resp = EdenResponse::response(42);

        // Test that constructors create the right variants
        assert!(matches!(ok_resp, EdenResponse::Ok(_)));
        assert!(matches!(err_resp, EdenResponse::Err(_)));
        assert!(matches!(data_resp, EdenResponse::Response(_)));
    }

    #[test]
    fn test_empty_messages() {
        let empty_ok = EdenResponse::<()>::Ok(String::new());
        let empty_err = EdenResponse::<()>::Err(String::new());

        let ok_json = serde_json::to_string(&empty_ok).unwrap_or_default();
        let err_json = serde_json::to_string(&empty_err).unwrap_or_default();

        assert_eq!(ok_json, r#"{"ok":""}"#);
        assert_eq!(err_json, r#"{"error":""}"#);
    }
}
