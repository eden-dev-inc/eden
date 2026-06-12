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

const API_INFO: ApiInfo<RedisApi, ClusterMyshardidInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ClusterMyshardid, "Returns the shard ID of a node", ReqType::Read, true);

/// See official Redis documentation for `CLUSTER MYSHARDID`
/// https://redis.io/docs/latest/commands/cluster-myshardid/
///
/// Available since Redis 7.2.0
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterMyshardidInput {}

impl Serialize for ClusterMyshardidInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterMyshardidInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterMyshardidInput, API_INFO);

impl RedisCommandInput for ClusterMyshardidInput {
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
                "CLUSTER MYSHARDID expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER MYSHARDID command
///
/// Returns the shard ID of the node.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterMyshardidOutput {
    /// The shard ID
    shard_id: String,
}

impl ClusterMyshardidOutput {
    pub fn new(shard_id: String) -> Self {
        Self { shard_id }
    }

    /// Get the shard ID
    pub fn shard_id(&self) -> &str {
        &self.shard_id
    }
}

impl Serialize for ClusterMyshardidOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterMyshardidOutput", 1)?;
        state.serialize_field("shard_id", &self.shard_id)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterMyshardidOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterMyshardid
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let shard_id = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER MYSHARDID response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected CLUSTER MYSHARDID response: {:?}", other)));
                }
            },
        };

        Ok(Self { shard_id })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ClusterMyshardidInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("MYSHARDID"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let shard_id = "3f8a9b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a";
            let resp = format!("${}\r\n{}\r\n", shard_id.len(), shard_id);
            let output = ClusterMyshardidOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.shard_id(), shard_id);
        }

        #[test]
        fn test_decode_simple_string() {
            let shard_id = "3f8a9b2c1d4e5f6a7b8c9d0e1f2a3b4c5d6e7f8a";
            let resp = format!("+{}\r\n", shard_id);
            let output = ClusterMyshardidOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.shard_id(), shard_id);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ClusterMyshardidOutput::decode(b"-ERR This instance has cluster support disabled\r\n").unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterMyshardidInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterMyshardidInput {};
            assert!(input.keys().is_empty());
        }
    }

    // Integration tests require a Redis cluster which needs special setup
    // #[cfg(feature = "integration")]
    // mod integration { ... }
}
