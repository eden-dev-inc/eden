use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClusterShardsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterShards,
    "Returns the mapping of cluster slots to shards",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER SHARDS`
/// https://redis.io/docs/latest/commands/cluster-shards/
///
/// Available since Redis 7.0.0
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterShardsInput {}

impl Serialize for ClusterShardsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterShardsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterShardsInput, API_INFO);

impl RedisCommandInput for ClusterShardsInput {
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
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER SHARDS expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER SHARDS command
///
/// Returns an array of shards, each containing slot ranges and node information.
/// The response format is complex nested data - we return it as raw JSON value.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterShardsOutput {
    /// Raw shard data from Redis
    shards: RedisJsonValue,
}

impl ClusterShardsOutput {
    pub fn new(shards: RedisJsonValue) -> Self {
        Self { shards }
    }

    pub fn shards(&self) -> &RedisJsonValue {
        &self.shards
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let shards = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => Self::resp2_array_to_json(arr)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SHARDS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::resp3_array_to_json(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SHARDS response: {:?}", other)));
                }
            },
        };

        Ok(Self { shards })
    }

    fn resp2_array_to_json(arr: Vec<Resp2Frame>) -> Result<RedisJsonValue, EpError> {
        let mut result = Vec::new();
        for frame in arr {
            result.push(Self::resp2_frame_to_json(frame)?);
        }
        Ok(RedisJsonValue::Array(result))
    }

    fn resp2_frame_to_json(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) => Ok(RedisJsonValue::String(String::from_utf8_lossy(&s).into())),
            Resp2Frame::Integer(i) => Ok(RedisJsonValue::Integer(i)),
            Resp2Frame::Array(arr) => Self::resp2_array_to_json(arr),
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
        }
    }

    fn resp3_array_to_json(arr: Vec<Resp3Frame>) -> Result<RedisJsonValue, EpError> {
        let mut result = Vec::new();
        for frame in arr {
            result.push(Self::resp3_frame_to_json(frame)?);
        }
        Ok(RedisJsonValue::Array(result))
    }

    fn resp3_frame_to_json(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } | Resp3Frame::BlobString { data, .. } => {
                Ok(RedisJsonValue::String(String::from_utf8_lossy(&data).into()))
            }
            Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
            Resp3Frame::Array { data, .. } => Self::resp3_array_to_json(data),
            Resp3Frame::Null => Ok(RedisJsonValue::Null),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            _ => Ok(RedisJsonValue::Null),
        }
    }
}

impl Serialize for ClusterShardsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterShardsOutput", 1)?;
        state.serialize_field("shards", &self.shards)?;
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
            let input = ClusterShardsInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("SHARDS"));
        }

        #[test]
        fn test_decode_empty_array() {
            // Empty shards: *0\r\n
            let output = ClusterShardsOutput::decode(b"*0\r\n").unwrap();
            assert!(matches!(output.shards(), RedisJsonValue::Array(arr) if arr.is_empty()));
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterShardsOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterShardsInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterShardsInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterShardsInput {};
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterShards);
        }
    }
}
