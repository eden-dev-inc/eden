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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClientNoTouchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientNoTouch,
    "Controls whether the commands sent by the client affect the LRU/LFU of accessed keys",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `CLIENT NO-TOUCH`
/// https://redis.io/docs/latest/commands/client-no-touch/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientNoTouchInput {
    enabled: bool,
}

impl Serialize for ClientNoTouchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientNoTouchInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("enabled", &self.enabled)?;
        state.end()
    }
}

impl_redis_operation!(ClientNoTouchInput, API_INFO, { enabled });

impl RedisCommandInput for ClientNoTouchInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(if self.enabled { "ON" } else { "OFF" });
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("CLIENT NO-TOUCH requires 1 argument, given {}", args.len())));
        }

        let enabled = match &args[0] {
            RedisJsonValue::String(s) => s.to_uppercase() == "ON",
            _ => false,
        };

        Ok(Self { enabled })
    }
}

/// Output for Redis CLIENT NO-TOUCH command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientNoTouchOutput {
    result: String,
}

impl ClientNoTouchOutput {
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) => String::from_utf8(s).map_err(EpError::parse)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => String::from_utf8(data).map_err(EpError::parse)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };

        Ok(Self { result })
    }
}

impl Serialize for ClientNoTouchOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientNoTouchOutput", 1)?;
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
        fn test_encode_on() {
            let input = ClientNoTouchInput { enabled: true };
            assert!(input.command().windows(2).any(|w| w == b"ON"));
        }

        #[test]
        fn test_decode_ok() {
            assert!(ClientNoTouchOutput::decode(b"+OK\r\n").unwrap().is_ok());
        }

        #[test]
        fn test_keys_empty() {
            assert!(ClientNoTouchInput::default().keys().is_empty());
        }
    }
}
