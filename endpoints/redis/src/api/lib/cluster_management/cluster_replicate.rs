use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ClusterReplicateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterReplicate,
    "Configure a node as replica of a master node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER REPLICATE`
/// https://redis.io/docs/latest/commands/cluster-replicate/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct ClusterReplicateInput {
    /// The 40-character node ID of the master to replicate
    node_id: RedisJsonValue,
}

impl Serialize for ClusterReplicateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterReplicateInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl_redis_operation!(ClusterReplicateInput, API_INFO, { node_id });

impl RedisCommandInput for ClusterReplicateInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("CLUSTER REPLICATE requires 1 argument, given {}", args.len())));
        }

        Ok(Self { node_id: args[0].clone() })
    }
}

/// Output for Redis CLUSTER REPLICATE command
///
/// Returns OK on success. An error is returned if the node ID is invalid
/// or if the operation cannot be performed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterReplicateOutput {
    /// Always "OK" on success
    status: String,
}

impl ClusterReplicateOutput {
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

impl Default for ClusterReplicateOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterReplicateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterReplicateOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterReplicateOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterReplicate
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
                other => Err(EpError::parse(format!("unexpected CLUSTER REPLICATE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER REPLICATE response: {:?}", other))),
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
            let input = ClusterReplicateInput {
                node_id: RedisJsonValue::String("e7d1eec13a0b59f67b5e3f2d4c7a8b9e0f1d2e3c".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("REPLICATE"));
            assert!(cmd_str.contains("e7d1eec13a0b59f67b5e3f2d4c7a8b9e0f1d2e3c"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterReplicateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterReplicateOutput::decode(b"-ERR Unknown node e7d1eec\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown node"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("e7d1eec13a0b59f67b5e3f2d4c7a8b9e0f1d2e3c".into())];
            let input = ClusterReplicateInput::decode(args).unwrap();
            assert_eq!(input.node_id, RedisJsonValue::String("e7d1eec13a0b59f67b5e3f2d4c7a8b9e0f1d2e3c".into()));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterReplicateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args_fails() {
            let args = vec![RedisJsonValue::String("node1".into()), RedisJsonValue::String("node2".into())];
            let err = ClusterReplicateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterReplicateInput { node_id: RedisJsonValue::String("nodeid".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization() {
            let input = ClusterReplicateInput { node_id: RedisJsonValue::String("abc123".into()) };
            let json = serde_json::to_value(&input).unwrap();
            assert!(json.get("type").is_some());
            assert!(json.get("node_id").is_some());
        }
    }
}
