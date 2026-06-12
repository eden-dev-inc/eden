use crate::api::lib::{RedisApi, RedisCommandInput, RedisCommandOutput};
use crate::api::{ClusterNode, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterNodesInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterNodes,
    "Returns the cluster configuration for a node",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER NODES`
/// https://redis.io/docs/latest/commands/cluster-nodes/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterNodesInput {}

impl Serialize for ClusterNodesInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterNodesInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterNodesInput, API_INFO);

impl RedisCommandInput for ClusterNodesInput {
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
                "CLUSTER NODES expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER NODES command
///
/// Returns serialized cluster configuration as a string, with optional parsed nodes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterNodesOutput {
    /// Raw cluster nodes output string
    raw: String,
    /// Parsed node information (best-effort parsing)
    nodes: Vec<ClusterNode>,
}

impl ClusterNodesOutput {
    pub fn new(raw: String) -> Self {
        let nodes = Self::parse_nodes(&raw);
        Self { raw, nodes }
    }

    /// Get the raw output string
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Get parsed nodes
    pub fn nodes(&self) -> &[ClusterNode] {
        &self.nodes
    }

    /// Parse the CLUSTER NODES output into structured data
    fn parse_nodes(raw: &str) -> Vec<ClusterNode> {
        let mut nodes = Vec::new();

        for line in raw.lines() {
            if line.is_empty() {
                continue;
            }

            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 8 {
                continue;
            }

            let flags: Vec<String> = parts[2].split(',').map(|s| s.to_string()).collect();
            let master_id = if parts[3] == "-" { None } else { Some(parts[3].to_string()) };

            // Slots are from index 8 onwards
            let slots: Vec<String> = parts.get(8..).map_or(vec![], |s| s.iter().map(|&slot| slot.to_string()).collect());

            nodes.push(ClusterNode {
                node_id: parts[0].to_string(),
                address: parts[1].to_string(),
                flags,
                master_id,
                ping_sent: parts[4].parse().unwrap_or(0),
                pong_recv: parts[5].parse().unwrap_or(0),
                config_epoch: parts[6].parse().unwrap_or(0),
                link_state: parts[7].to_string(),
                slots,
            });
        }

        nodes
    }
}

impl Serialize for ClusterNodesOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterNodesOutput", 2)?;
        state.serialize_field("raw", &self.raw)?;
        state.serialize_field("nodes", &self.nodes)?;
        state.end()
    }
}

impl Serialize for ClusterNode {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterNode", 9)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.serialize_field("address", &self.address)?;
        state.serialize_field("flags", &self.flags)?;
        state.serialize_field("master_id", &self.master_id)?;
        state.serialize_field("ping_sent", &self.ping_sent)?;
        state.serialize_field("pong_recv", &self.pong_recv)?;
        state.serialize_field("config_epoch", &self.config_epoch)?;
        state.serialize_field("link_state", &self.link_state)?;
        state.serialize_field("slots", &self.slots)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterNodesOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterNodes
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let raw = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER NODES response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER NODES response: {:?}", other)));
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
            let input = ClusterNodesInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("NODES"));
        }

        #[test]
        fn test_decode_cluster_nodes_output() {
            let raw_output = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 127.0.0.1:7000@17000 myself,master - 0 1234567890 1 connected 0-5460\n\
                             a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0 127.0.0.1:7001@17001 master - 0 1234567891 2 connected 5461-10922\n\
                             b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a9b0c1 127.0.0.1:7002@17002 slave e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca 0 1234567892 1 connected\n";

            let resp = format!("${}\r\n{}\r\n", raw_output.len(), raw_output);
            let output = ClusterNodesOutput::decode(resp.as_bytes()).unwrap();

            assert_eq!(output.nodes().len(), 3);

            // Check first node (master with slots)
            let node1 = &output.nodes()[0];
            assert_eq!(node1.node_id, "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca");
            assert!(node1.flags.contains(&"myself".to_string()));
            assert!(node1.flags.contains(&"master".to_string()));
            assert!(node1.master_id.is_none());
            assert_eq!(node1.link_state, "connected");
            assert!(node1.slots.contains(&"0-5460".to_string()));

            // Check replica node
            let node3 = &output.nodes()[2];
            assert!(node3.flags.contains(&"slave".to_string()));
            assert_eq!(node3.master_id, Some("e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca".to_string()));
            assert!(node3.slots.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterNodesOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterNodesInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterNodesInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_parse_empty_output() {
            let output = ClusterNodesOutput::new(String::new());
            assert!(output.nodes().is_empty());
        }
    }
}
