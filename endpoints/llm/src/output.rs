use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use llm_core::LlmChatResponse;

use error::{EpError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use utoipa::ToSchema;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
// TODO: Consider boxing to reduce size differences between variants.
#[allow(clippy::large_enum_variant)]
pub enum LlmOutput {
    Chat { response: LlmChatResponse },
    Message { message: String },
}

impl LlmOutput {
    pub fn chat(response: LlmChatResponse) -> Self {
        Self::Chat { response }
    }

    pub fn message(message: impl Into<String>) -> Self {
        Self::Message { message: message.into() }
    }
}

impl ToOutput for LlmOutput {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Llm, EndpointResponse::Response(self))
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
