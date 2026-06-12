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

const API_INFO: ApiInfo<RedisApi, ClientTrackinginfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClientTrackinginfo,
    "Returns information about server-assisted client-side caching for the connection",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `CLIENT TRACKINGINFO`
/// https://redis.io/docs/latest/commands/client-trackinginfo/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClientTrackinginfoInput {}

impl Serialize for ClientTrackinginfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClientTrackinginfoInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClientTrackinginfoInput, API_INFO);

impl RedisCommandInput for ClientTrackinginfoInput {
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
            return Err(EpError::request(format!("CLIENT TRACKINGINFO requires no arguments, given {}", args.len())));
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLIENT TRACKINGINFO command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClientTrackinginfoOutput {
    /// Raw tracking info
    info: Vec<RedisJsonValue>,
}

impl ClientTrackinginfoOutput {
    pub fn info(&self) -> &[RedisJsonValue] {
        &self.info
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let info = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(items)) => items.into_iter().map(Self::frame_to_value_resp2).collect(),
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.into_iter().map(Self::frame_to_value_resp3).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                data.into_iter().flat_map(|(k, v)| vec![Self::frame_to_value_resp3(k), Self::frame_to_value_resp3(v)]).collect()
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };

        Ok(Self { info })
    }

    fn frame_to_value_resp2(frame: Resp2Frame) -> RedisJsonValue {
        match frame {
            Resp2Frame::BulkString(b) => RedisJsonValue::String(String::from_utf8_lossy(&b).to_string()),
            Resp2Frame::SimpleString(s) => RedisJsonValue::String(String::from_utf8_lossy(&s).to_string()),
            Resp2Frame::Integer(n) => RedisJsonValue::Integer(n),
            _ => RedisJsonValue::Null,
        }
    }

    fn frame_to_value_resp3(frame: Resp3Frame) -> RedisJsonValue {
        match frame {
            Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(&data).to_string()),
            Resp3Frame::SimpleString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(&data).to_string()),
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(data),
            _ => RedisJsonValue::Null,
        }
    }
}

impl Serialize for ClientTrackinginfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClientTrackinginfoOutput", 1)?;
        state.serialize_field("info", &self.info)?;
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
            let input = ClientTrackinginfoInput {};
            let cmd = input.command();
            assert!(cmd.windows(12).any(|w| w == b"TRACKINGINFO"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = ClientTrackinginfoInput::decode(vec![]).unwrap();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClientTrackinginfo);
        }

        #[test]
        fn test_keys_empty() {
            assert!(ClientTrackinginfoInput {}.keys().is_empty());
        }
    }
}
