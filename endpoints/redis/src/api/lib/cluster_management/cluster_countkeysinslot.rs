use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterCountkeysinslotInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterCountkeysinslot,
    "Returns the number of keys in a hash slot",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER COUNTKEYSINSLOT`
/// https://redis.io/docs/latest/commands/cluster-countkeysinslot/
///
/// Official example: `CLUSTER COUNTKEYSINSLOT 7000`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterCountkeysinslotInput {
    slot: RedisJsonValue,
}

impl Serialize for ClusterCountkeysinslotInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterCountkeysinslotInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slot", &self.slot)?;
        state.end()
    }
}

impl_redis_operation!(ClusterCountkeysinslotInput, API_INFO, { slot });

impl RedisCommandInput for ClusterCountkeysinslotInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.slot);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("CLUSTER COUNTKEYSINSLOT requires 1 argument, given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER COUNTKEYSINSLOT expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { slot: args[0].clone() })
    }
}

/// Output for Redis CLUSTER COUNTKEYSINSLOT command
///
/// Returns the number of keys in the specified hash slot.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterCountkeysinslotOutput {
    /// The number of keys in the slot
    count: i64,
}

impl ClusterCountkeysinslotOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of keys in the slot
    pub fn count(&self) -> i64 {
        self.count
    }
}

impl Serialize for ClusterCountkeysinslotOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterCountkeysinslotOutput", 1)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterCountkeysinslotOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterCountkeysinslot
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER COUNTKEYSINSLOT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER COUNTKEYSINSLOT response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterCountkeysinslotInput { slot: RedisJsonValue::Integer(7000) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("COUNTKEYSINSLOT"));
            assert!(cmd_str.contains("7000"));
        }

        #[test]
        fn test_encode_command_string_slot() {
            let input = ClusterCountkeysinslotInput { slot: RedisJsonValue::String("5000".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("5000"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterCountkeysinslotInput { slot: RedisJsonValue::Integer(0) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_valid_integer() {
            let args = vec![RedisJsonValue::Integer(7000)];
            let input = ClusterCountkeysinslotInput::decode(args).unwrap();
            assert_eq!(input.slot, RedisJsonValue::Integer(7000));
        }

        #[test]
        fn test_decode_input_valid_string() {
            let args = vec![RedisJsonValue::String("7000".into())];
            let input = ClusterCountkeysinslotInput::decode(args).unwrap();
            assert_eq!(input.slot, RedisJsonValue::String("7000".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterCountkeysinslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = ClusterCountkeysinslotOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.count(), 42);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ClusterCountkeysinslotOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_output_large_value() {
            let output = ClusterCountkeysinslotOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.count(), 1000000);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = ClusterCountkeysinslotOutput::decode(b"-ERR Invalid slot\r\n").unwrap_err();
            assert!(err.to_string().contains("Invalid slot"));
        }
    }
}
