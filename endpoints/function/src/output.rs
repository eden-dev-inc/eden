use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use function_core::FunctionProvider;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "format", rename_all = "snake_case", content = "value")]
pub enum FunctionPayload {
    Empty,
    Json(Value),
    Text(String),
    Base64(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct FunctionInvokeOutput {
    pub provider: FunctionProvider,
    pub function_name: String,
    pub status_code: i32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub executed_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_result_base64: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    pub payload: FunctionPayload,
}

impl ToOutput for FunctionInvokeOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Function, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Err(EpError::Protocol(ProtocolError::NotImplemented))
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        let json = serde_json::to_string(self).map_err(EpError::serde)?;
        borsh::to_vec(&json).map_err(EpError::serde)
    }
}

pub fn normalize_payload(payload: Option<&[u8]>) -> FunctionPayload {
    let Some(bytes) = payload else {
        return FunctionPayload::Empty;
    };

    if bytes.is_empty() {
        return FunctionPayload::Empty;
    }

    if let Ok(value) = serde_json::from_slice::<Value>(bytes) {
        return FunctionPayload::Json(value);
    }

    if let Ok(value) = String::from_utf8(bytes.to_vec()) {
        return FunctionPayload::Text(value);
    }

    FunctionPayload::Base64(BASE64.encode(bytes))
}
