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

const API_INFO: ApiInfo<RedisApi, ClientUnpauseInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientUnpause,
    "Resumes processing commands from paused clients",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `CLIENT UNPAUSE`
/// https://redis.io/docs/latest/commands/client-unpause/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientUnpauseInput {}

impl Serialize for ClientUnpauseInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientUnpauseInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientUnpauseInput, API_INFO);

impl RedisCommandInput for ClientUnpauseInput {
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
            return Err(EpError::request(format!("CLIENT UNPAUSE requires no arguments, given {}", args.len())));
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLIENT UNPAUSE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientUnpauseOutput {
    result: String,
}

impl ClientUnpauseOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    pub fn result(&self) -> &str {
        &self.result
    }

    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientUnpauseOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientUnpauseOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = ClientUnpauseInput {};
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"UNPAUSE"));
        }

        #[test]
        fn test_decode_ok() {
            let output = ClientUnpauseOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = ClientUnpauseInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClientUnpause);
        }

        #[test]
        fn test_decode_input_with_args_fails() {
            let err = ClientUnpauseInput::decode(vec![RedisJsonValue::String("x".into())]).unwrap_err();
            assert!(err.to_string().contains("no arguments"));
        }

        #[test]
        fn test_keys_empty() {
            assert!(ClientUnpauseInput {}.keys().is_empty());
        }
    }
}
