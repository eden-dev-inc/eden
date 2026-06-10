use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClientGetredirInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientGetredir,
    "Returns the client ID to which the connection's tracking notifications are redirected",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT GETREDIR`
/// https://redis.io/docs/latest/commands/client-getredir/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientGetredirInput {}

impl Serialize for ClientGetredirInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientGetredirInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientGetredirInput, API_INFO);

impl RedisCommandInput for ClientGetredirInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            return Err(EpError::request(format!("CLIENT GETREDIR requires no arguments, given {}", args.len())));
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLIENT GETREDIR command
///
/// Returns the client ID to which tracking notifications are redirected.
/// Returns -1 if tracking is not enabled, 0 if enabled but not redirected.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientGetredirOutput {
    client_id: i64,
}

impl ClientGetredirOutput {
    pub fn new(client_id: i64) -> Self {
        Self { client_id }
    }

    /// Get the redirect client ID
    pub fn client_id(&self) -> i64 {
        self.client_id
    }

    /// Check if tracking is not enabled (-1)
    pub fn tracking_not_enabled(&self) -> bool {
        self.client_id == -1
    }

    /// Check if tracking is enabled but not redirected (0)
    pub fn not_redirected(&self) -> bool {
        self.client_id == 0
    }

    /// Check if tracking is redirected to another client (positive ID)
    pub fn is_redirected(&self) -> bool {
        self.client_id > 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let client_id = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { client_id })
    }
}

impl Serialize for ClientGetredirOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientGetredirOutput", 1)?;
        state.serialize_field("client_id", &self.client_id)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClientGetredirInput {};
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"GETREDIR"));
        }

        #[test]
        fn test_decode_not_enabled() {
            let output = ClientGetredirOutput::decode(b":-1\r\n").unwrap();
            assert!(output.tracking_not_enabled());
            assert_eq!(output.client_id(), -1);
        }

        #[test]
        fn test_decode_not_redirected() {
            let output = ClientGetredirOutput::decode(b":0\r\n").unwrap();
            assert!(output.not_redirected());
            assert!(!output.is_redirected());
        }

        #[test]
        fn test_decode_redirected() {
            let output = ClientGetredirOutput::decode(b":12345\r\n").unwrap();
            assert!(output.is_redirected());
            assert_eq!(output.client_id(), 12345);
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = ClientGetredirInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClientGetredir);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let err = ClientGetredirInput::decode(vec![RedisJsonValue::Integer(1)]).unwrap_err();
            assert!(err.to_string().contains("no arguments"));
        }

        #[test]
        fn test_keys_empty() {
            assert!(ClientGetredirInput {}.keys().is_empty());
        }
    }
}
