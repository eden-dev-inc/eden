use crate::api::lib::RedisCommandOutput;
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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterGetkeysinslotInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterGetkeysinslot,
    "Returns the key names in a hash slot",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER GETKEYSINSLOT`
/// https://redis.io/docs/latest/commands/cluster-getkeysinslot/
///
/// Official example: `CLUSTER GETKEYSINSLOT 7000 3`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterGetkeysinslotInput {
    pub(crate) slot: RedisJsonValue,
    pub(crate) count: RedisJsonValue,
}

impl Serialize for ClusterGetkeysinslotInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterGetkeysinslotInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("slot", &self.slot)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

impl_redis_operation!(ClusterGetkeysinslotInput, API_INFO, { slot, count });

impl RedisCommandInput for ClusterGetkeysinslotInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.slot).arg(&self.count);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!(
                "CLUSTER GETKEYSINSLOT requires 2 arguments (slot, count), given {}",
                args.len()
            )));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER GETKEYSINSLOT expects 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { slot: args[0].clone(), count: args[1].clone() })
    }
}

/// Output for Redis CLUSTER GETKEYSINSLOT command
///
/// Returns an array of key names stored in the specified hash slot.
#[derive(Debug, Deserialize, Clone, Default, ToSchema, JsonSchema)]
pub struct ClusterGetkeysinslotOutput {
    /// Key names in the slot
    keys: Vec<String>,
}

impl ClusterGetkeysinslotOutput {
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Get the keys in the slot
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Check if any keys were found
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Get the number of keys returned
    pub fn len(&self) -> usize {
        self.keys.len()
    }
}

impl Serialize for ClusterGetkeysinslotOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterGetkeysinslotOutput", 1)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterGetkeysinslotOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterGetkeysinslot
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                            String::from_utf8(b).map_err(|e| EpError::parse(e.to_string()))
                        }
                        _ => Err(EpError::parse("expected string in keys array")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER GETKEYSINSLOT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            String::from_utf8(data).map_err(|e| EpError::parse(e.to_string()))
                        }
                        _ => Err(EpError::parse("expected string in keys array")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER GETKEYSINSLOT response: {:?}", other)));
                }
            },
        };

        Ok(Self { keys })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterGetkeysinslotInput {
                slot: RedisJsonValue::Integer(7000),
                count: RedisJsonValue::Integer(3),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("GETKEYSINSLOT"));
            assert!(cmd_str.contains("7000"));
            assert!(cmd_str.contains("3"));
        }

        #[test]
        fn test_encode_command_string_args() {
            let input = ClusterGetkeysinslotInput {
                slot: RedisJsonValue::String("7000".into()),
                count: RedisJsonValue::String("10".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("7000"));
            assert!(cmd_str.contains("10"));
        }

        #[test]
        fn test_decode_empty_array() {
            // Empty array: *0\r\n
            let output = ClusterGetkeysinslotOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_keys_array() {
            // Array with 3 keys: *3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n
            let resp = b"*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n";
            let output = ClusterGetkeysinslotOutput::decode(resp).unwrap();
            assert!(!output.is_empty());
            assert_eq!(output.len(), 3);
            assert_eq!(output.keys(), &["key1", "key2", "key3"]);
        }

        #[test]
        fn test_decode_single_key() {
            let resp = b"*1\r\n$6\r\nmykey1\r\n";
            let output = ClusterGetkeysinslotOutput::decode(resp).unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.keys(), &["mykey1"]);
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterGetkeysinslotOutput::decode(b"-ERR Invalid or out of range slot\r\n").unwrap_err();
            assert!(err.to_string().contains("Invalid"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::Integer(7000), RedisJsonValue::Integer(3)];
            let input = ClusterGetkeysinslotInput::decode(args).unwrap();
            assert_eq!(input.slot, RedisJsonValue::Integer(7000));
            assert_eq!(input.count, RedisJsonValue::Integer(3));
        }

        #[test]
        fn test_decode_input_string_args() {
            let args = vec![RedisJsonValue::String("7000".into()), RedisJsonValue::String("10".into())];
            let input = ClusterGetkeysinslotInput::decode(args).unwrap();
            assert_eq!(input.slot, RedisJsonValue::String("7000".into()));
            assert_eq!(input.count, RedisJsonValue::String("10".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(7000)];
            let err = ClusterGetkeysinslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterGetkeysinslotInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_extra_args_warns_but_succeeds() {
            let args = vec![
                RedisJsonValue::Integer(7000),
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("extra".into()),
            ];
            let input = ClusterGetkeysinslotInput::decode(args).unwrap();
            assert_eq!(input.slot, RedisJsonValue::Integer(7000));
            assert_eq!(input.count, RedisJsonValue::Integer(3));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterGetkeysinslotInput {
                slot: RedisJsonValue::Integer(0),
                count: RedisJsonValue::Integer(0),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterGetkeysinslotInput {
                slot: RedisJsonValue::Integer(0),
                count: RedisJsonValue::Integer(0),
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterGetkeysinslot);
        }

        #[test]
        fn test_output_serialization() {
            let output = ClusterGetkeysinslotOutput::new(vec!["key1".to_string(), "key2".to_string()]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("key1"));
            assert!(json.contains("key2"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_getkeysinslot_on_non_cluster_returns_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterGetkeysinslotInput {
                                slot: RedisJsonValue::Integer(0),
                                count: RedisJsonValue::Integer(10),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // On a non-cluster Redis, this should return an error
                    let output = ClusterGetkeysinslotOutput::decode(&result);
                    // Non-cluster Redis returns error for cluster commands
                    assert!(output.is_err());
                })
            })
            .await;
        }
    }
}
