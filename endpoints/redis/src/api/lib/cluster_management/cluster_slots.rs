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

const API_INFO: ApiInfo<RedisApi, ClusterSlotsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterSlots,
    "Returns the mapping of cluster slots to nodes",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER SLOTS`
/// https://redis.io/docs/latest/commands/cluster-slots/
///
/// **Deprecated**: As of Redis 7.0, this command is deprecated.
/// Use `CLUSTER SHARDS` instead.
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterSlotsInput {}

impl Serialize for ClusterSlotsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSlotsInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterSlotsInput, API_INFO);

impl RedisCommandInput for ClusterSlotsInput {
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
                "CLUSTER SLOTS expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER SLOTS command
///
/// Returns an array of slot ranges with node information.
/// Each slot range entry contains: [start_slot, end_slot, master_node, ...replica_nodes]
/// The response format is complex nested data - we return it as raw JSON value.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterSlotsOutput {
    /// Raw slot mapping data from Redis
    slots: RedisJsonValue,
}

impl ClusterSlotsOutput {
    pub fn new(slots: RedisJsonValue) -> Self {
        Self { slots }
    }

    pub fn slots(&self) -> &RedisJsonValue {
        &self.slots
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let slots = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => Self::resp2_array_to_json(arr)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SLOTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::resp3_array_to_json(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SLOTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { slots })
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

impl Serialize for ClusterSlotsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSlotsOutput", 1)?;
        state.serialize_field("slots", &self.slots)?;
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
            let input = ClusterSlotsInput {};
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("CLUSTER"));
            assert!(cmd.contains("SLOTS"));
        }

        #[test]
        fn test_decode_empty_array() {
            // No slots (not in cluster mode or empty): *0\r\n
            let output = ClusterSlotsOutput::decode(b"*0\r\n").unwrap();
            assert!(matches!(output.slots(), RedisJsonValue::Array(arr) if arr.is_empty()));
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterSlotsOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterSlotsInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterSlotsInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterSlotsInput {};
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterSlots);
        }

        #[test]
        fn test_decode_slot_range() {
            // Simplified slot range: *1\r\n*3\r\n:0\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n
            // This represents slot 0-5460 on 127.0.0.1:7000
            let resp = b"*1\r\n*3\r\n:0\r\n:5460\r\n*2\r\n$9\r\n127.0.0.1\r\n:7000\r\n";
            let output = ClusterSlotsOutput::decode(resp).unwrap();

            if let RedisJsonValue::Array(slots) = output.slots() {
                assert_eq!(slots.len(), 1);
                if let RedisJsonValue::Array(range) = &slots[0] {
                    assert_eq!(range.len(), 3);
                    // Start slot
                    assert!(matches!(&range[0], RedisJsonValue::Integer(0)));
                    // End slot
                    assert!(matches!(&range[1], RedisJsonValue::Integer(5460)));
                }
            } else {
                panic!("Expected array");
            }
        }
    }
}
