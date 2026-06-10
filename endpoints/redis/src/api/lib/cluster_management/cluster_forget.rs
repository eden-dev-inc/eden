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

const API_INFO: ApiInfo<RedisApi, ClusterForgetInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterForget, "Removes a node from the nodes table", ReqType::Write, true);

/// See official Redis documentation for `CLUSTER FORGET`
/// https://redis.io/docs/latest/commands/cluster-forget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterForgetInput {
    pub(crate) node_id: RedisJsonValue,
}

impl Serialize for ClusterForgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterForgetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl_redis_operation!(ClusterForgetInput, API_INFO, { node_id });

impl RedisCommandInput for ClusterForgetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.node_id);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("CLUSTER FORGET requires 1 argument (node_id), given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER FORGET expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { node_id: args[0].clone() })
    }
}

/// Output for Redis CLUSTER FORGET command
///
/// Returns OK on success when the node is removed from the cluster.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterForgetOutput {
    /// "OK" on success
    status: String,
}

impl ClusterForgetOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }
}

impl Default for ClusterForgetOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterForgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterForgetOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterForgetOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterForget
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CLUSTER FORGET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER FORGET response: {:?}", other))),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterForgetInput {
                node_id: RedisJsonValue::String("e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("FORGET"));
            assert!(cmd_str.contains("e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterForgetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_unknown_node() {
            let err = ClusterForgetOutput::decode(b"-ERR Unknown node e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0e0\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown node"));
        }

        #[test]
        fn test_decode_error_cannot_forget_myself() {
            let err = ClusterForgetOutput::decode(b"-ERR I tried hard but I can't forget myself...\r\n").unwrap_err();
            assert!(err.to_string().contains("myself"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("1234567890123456789012345678901234567890".into())];
            let input = ClusterForgetInput::decode(args).unwrap();
            assert_eq!(input.node_id, RedisJsonValue::String("1234567890123456789012345678901234567890".into()));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterForgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_extra_args_warns_but_succeeds() {
            let args = vec![RedisJsonValue::String("node123".into()), RedisJsonValue::String("extra".into())];
            let input = ClusterForgetInput::decode(args).unwrap();
            assert_eq!(input.node_id, RedisJsonValue::String("node123".into()));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterForgetInput { node_id: RedisJsonValue::String("node".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterForgetInput { node_id: RedisJsonValue::String("node".into()) };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterForget);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_forget_on_non_cluster_returns_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterForgetInput {
                                node_id: RedisJsonValue::String("0000000000000000000000000000000000000000".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // On a non-cluster Redis, this should return an error
                    let output = ClusterForgetOutput::decode(&result);
                    assert!(output.is_err() || !output.unwrap().is_ok());
                })
            })
            .await;
        }
    }
}
