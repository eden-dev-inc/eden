use crate::api::lib::cluster_management::ReplicaInfo;
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

const API_INFO: ApiInfo<RedisApi, ClusterReplicasInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterReplicas,
    "Lists the replica nodes of a master node",
    ReqType::Read, // Fixed: was incorrectly Write
    true,
);

/// See official Redis documentation for `CLUSTER REPLICAS`
/// https://redis.io/docs/latest/commands/cluster-replicas/
///
/// Available since Redis 5.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterReplicasInput {
    node_id: RedisJsonValue,
}

impl Serialize for ClusterReplicasInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterReplicasInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl_redis_operation!(ClusterReplicasInput, API_INFO, { node_id });

impl RedisCommandInput for ClusterReplicasInput {
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
            return Err(EpError::request("CLUSTER REPLICAS requires 1 argument (node_id), given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER REPLICAS expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { node_id: args[0].clone() })
    }
}

/// Output for Redis CLUSTER REPLICAS command
///
/// Returns a list of replica nodes for the specified master node.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterReplicasOutput {
    /// Raw output string
    raw: String,
    /// Parsed replica information
    replicas: Vec<ReplicaInfo>,
}

impl ClusterReplicasOutput {
    pub fn new(raw: String) -> Self {
        let replicas = Self::parse_replicas(&raw);
        Self { raw, replicas }
    }

    /// Get the raw output string
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Get parsed replicas
    pub fn replicas(&self) -> &[ReplicaInfo] {
        &self.replicas
    }

    /// Check if any replicas exist
    pub fn has_replicas(&self) -> bool {
        !self.replicas.is_empty()
    }

    /// Get the count of replicas
    pub fn count(&self) -> usize {
        self.replicas.len()
    }

    /// Parse the CLUSTER REPLICAS output
    fn parse_replicas(raw: &str) -> Vec<ReplicaInfo> {
        let mut replicas = Vec::new();

        for line in raw.lines() {
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 8 {
                continue;
            }

            let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();

            replicas.push(ReplicaInfo {
                node_id: parts[0].to_string(),
                address: parts[1].to_string(),
                flags,
                master_id: parts[3].to_string(),
                ping_sent: parts[4].parse().unwrap_or(0),
                pong_recv: parts[5].parse().unwrap_or(0),
                config_epoch: parts[6].parse().unwrap_or(0),
                link_state: parts[7].to_string(),
            });
        }

        replicas
    }
}

impl Serialize for ClusterReplicasOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterReplicasOutput", 2)?;
        state.serialize_field("raw", &self.raw)?;
        state.serialize_field("replicas", &self.replicas)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterReplicasOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterReplicas
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let raw = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                // Empty array means no replicas
                Resp2Frame::Array(arr) if arr.is_empty() => String::new(),
                // Non-empty array - each element is a bulk string line
                Resp2Frame::Array(arr) => {
                    let mut lines = Vec::new();
                    for item in arr {
                        if let Resp2Frame::BulkString(data) = item {
                            lines.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                    }
                    lines.join("\n")
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER REPLICAS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                // Empty array means no replicas
                Resp3Frame::Array { data, .. } if data.is_empty() => String::new(),
                // Non-empty array
                Resp3Frame::Array { data, .. } => {
                    let mut lines = Vec::new();
                    for item in data {
                        if let Resp3Frame::BlobString { data, .. } = item {
                            lines.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                    }
                    lines.join("\n")
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER REPLICAS response: {:?}", other)));
                }
            },
        };

        Ok(Self::new(raw))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterReplicasInput {
                node_id: RedisJsonValue::String("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("REPLICAS"));
            assert!(cmd_str.contains("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca"));
        }

        #[test]
        fn test_decode_replicas_output() {
            let raw_output = "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1 127.0.0.1:7003@17003 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1234567892 1 connected\n\
                             c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1d2 127.0.0.1:7004@17004 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1234567893 1 connected\n";

            let resp = format!("${}\r\n{}\r\n", raw_output.len(), raw_output);
            let output = ClusterReplicasOutput::decode(resp.as_bytes()).unwrap();

            assert!(output.has_replicas());
            assert_eq!(output.count(), 2);

            let replica1 = &output.replicas()[0];
            assert_eq!(replica1.node_id, "b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1");
            assert!(replica1.flags.contains(&"slave".to_string()));
            assert_eq!(replica1.master_id, "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca");
            assert_eq!(replica1.link_state, "connected");
        }

        #[test]
        fn test_decode_empty_replicas() {
            // Empty array response
            let output = ClusterReplicasOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.has_replicas());
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_error_no_such_node() {
            let err = ClusterReplicasOutput::decode(b"-ERR Unknown node e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown node"));
        }

        #[test]
        fn test_decode_error_cluster_disabled() {
            let err = ClusterReplicasOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("nodeid123".into())];
            let input = ClusterReplicasInput::decode(args).unwrap();
            assert_eq!(input.node_id, RedisJsonValue::String("nodeid123".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterReplicasInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterReplicasInput { node_id: RedisJsonValue::String("nodeid".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_parse_empty_output() {
            let output = ClusterReplicasOutput::new(String::new());
            assert!(!output.has_replicas());
            assert_eq!(output.count(), 0);
        }
    }
}
