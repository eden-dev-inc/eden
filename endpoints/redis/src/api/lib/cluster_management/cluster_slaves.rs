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

const API_INFO: ApiInfo<RedisApi, ClusterSlavesInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterSlaves,
    "Lists the replica nodes of a master node",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER SLAVES`
/// https://redis.io/docs/latest/commands/cluster-slaves/
///
/// **Deprecated**: As of Redis 5.0, this command is deprecated.
/// Use `CLUSTER REPLICAS` instead.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterSlavesInput {
    /// The node ID of the master node
    node_id: RedisJsonValue,
}

impl Serialize for ClusterSlavesInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSlavesInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl_redis_operation!(ClusterSlavesInput, API_INFO, { node_id });

impl RedisCommandInput for ClusterSlavesInput {
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
            return Err(EpError::request("CLUSTER SLAVES requires 1 argument (node-id), given none"));
        }
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER SLAVES expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self { node_id: args[0].clone() })
    }
}

/// Output for Redis CLUSTER SLAVES command
///
/// Returns an array of strings containing information about replica nodes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterSlavesOutput {
    /// List of replica node info strings
    replicas: Vec<String>,
}

impl ClusterSlavesOutput {
    pub fn new(replicas: Vec<String>) -> Self {
        Self { replicas }
    }

    pub fn replicas(&self) -> &[String] {
        &self.replicas
    }

    pub fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    pub fn count(&self) -> usize {
        self.replicas.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let replicas = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::BulkString(s) | Resp2Frame::SimpleString(s) => Ok(String::from_utf8_lossy(&s).into()),
                        _ => Err(EpError::parse("expected string in replicas array")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SLAVES response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            Ok(String::from_utf8_lossy(&data).into())
                        }
                        _ => Err(EpError::parse("expected string in replicas array")),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER SLAVES response: {:?}", other)));
                }
            },
        };

        Ok(Self { replicas })
    }
}

impl Serialize for ClusterSlavesOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterSlavesOutput", 1)?;
        state.serialize_field("replicas", &self.replicas)?;
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
            let input = ClusterSlavesInput { node_id: RedisJsonValue::String("abc123def456".to_string()) };
            let bytes = input.command();
            let cmd = String::from_utf8_lossy(&bytes);
            assert!(cmd.contains("CLUSTER"));
            assert!(cmd.contains("SLAVES"));
            assert!(cmd.contains("abc123def456"));
        }

        #[test]
        fn test_decode_empty_array() {
            // No replicas: *0\r\n
            let output = ClusterSlavesOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_single_replica() {
            // One replica: *1\r\n$10\r\nreplica123\r\n
            let resp = b"*1\r\n$10\r\nreplica123\r\n";
            let output = ClusterSlavesOutput::decode(resp).unwrap();
            assert_eq!(output.count(), 1);
            assert_eq!(output.replicas()[0], "replica123");
        }

        #[test]
        fn test_decode_multiple_replicas() {
            // Two replicas
            let resp = b"*2\r\n$8\r\nreplica1\r\n$8\r\nreplica2\r\n";
            let output = ClusterSlavesOutput::decode(resp).unwrap();
            assert_eq!(output.count(), 2);
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterSlavesOutput::decode(b"-ERR Unknown node abc123\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown node"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("node123".to_string())];
            let input = ClusterSlavesInput::decode(args).unwrap();
            assert!(matches!(input.node_id, RedisJsonValue::String(s) if s == "node123"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterSlavesInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterSlavesInput { node_id: RedisJsonValue::String("node123".to_string()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ClusterSlavesInput { node_id: RedisJsonValue::String("node123".to_string()) };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::ClusterSlaves);
        }
    }
}
