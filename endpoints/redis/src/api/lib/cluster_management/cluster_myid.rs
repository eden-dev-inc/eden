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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClusterMyidInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterMyid, "Returns the ID of a node", ReqType::Read, true);

/// See official Redis documentation for `CLUSTER MYID`
/// https://redis.io/docs/latest/commands/cluster-myid/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterMyidInput {}

impl Serialize for ClusterMyidInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterMyidInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterMyidInput, API_INFO);

impl RedisCommandInput for ClusterMyidInput {
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
                "CLUSTER MYID expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER MYID command
///
/// Returns the node's 40-character unique identifier.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterMyidOutput {
    /// The 40-character node ID
    node_id: String,
}

impl ClusterMyidOutput {
    pub fn new(node_id: String) -> Self {
        Self { node_id }
    }

    /// Get the node ID
    pub fn node_id(&self) -> &str {
        &self.node_id
    }
}

impl Serialize for ClusterMyidOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterMyidOutput", 1)?;
        state.serialize_field("node_id", &self.node_id)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterMyidOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterMyid
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let node_id = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER MYID response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER MYID response: {:?}", other)));
                }
            },
        };

        Ok(Self { node_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterMyidInput {};
            // CLUSTER MYID -> *2\r\n$7\r\nCLUSTER\r\n$4\r\nMYID\r\n
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("MYID"));
        }

        #[test]
        fn test_decode_bulk_string() {
            // 40-char node ID as bulk string
            let node_id = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca";
            let resp = format!("${}\r\n{}\r\n", node_id.len(), node_id);
            let output = ClusterMyidOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.node_id(), node_id);
        }

        #[test]
        fn test_decode_simple_string() {
            let node_id = "e7d1eecce10fd6bb5eb35b9f99a514335d9ba9ca";
            let resp = format!("+{}\r\n", node_id);
            let output = ClusterMyidOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.node_id(), node_id);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterMyidOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterMyidInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterMyidInput {};
            assert!(input.keys().is_empty());
        }
    }

    // Integration tests require a Redis cluster which needs special setup
    // #[cfg(feature = "integration")]
    // mod integration { ... }
}
