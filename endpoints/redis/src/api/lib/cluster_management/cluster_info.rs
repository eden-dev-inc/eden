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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ClusterInfoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterInfo,
    "Returns information about the state of a node",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `CLUSTER INFO`
/// https://redis.io/docs/latest/commands/cluster-info/
///
/// Available since Redis 3.0.0
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ClusterInfoInput {}

impl Serialize for ClusterInfoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterInfoInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ClusterInfoInput, API_INFO);

impl RedisCommandInput for ClusterInfoInput {
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
                "CLUSTER INFO expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis CLUSTER INFO command
///
/// Returns cluster state information as key-value pairs.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterInfoOutput {
    /// Raw info string from Redis
    info: HashMap<String, String>,
}

impl ClusterInfoOutput {
    pub fn new(info: HashMap<String, String>) -> Self {
        Self { info }
    }

    /// Get the raw info map
    pub fn info(&self) -> &HashMap<String, String> {
        &self.info
    }

    /// Get cluster state (e.g., "ok" or "fail")
    pub fn cluster_state(&self) -> Option<&String> {
        self.info.get("cluster_state")
    }

    /// Get the number of known nodes
    pub fn cluster_known_nodes(&self) -> Option<u64> {
        self.info.get("cluster_known_nodes").and_then(|s| s.parse().ok())
    }

    /// Get the number of slots assigned
    pub fn cluster_slots_assigned(&self) -> Option<u64> {
        self.info.get("cluster_slots_assigned").and_then(|s| s.parse().ok())
    }

    /// Decode the Redis protocol response into a ClusterInfoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let info_str = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CLUSTER INFO response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected CLUSTER INFO response: {:?}", other)));
                }
            },
        };

        // Parse the info string into key-value pairs
        // Format: "key:value\r\nkey:value\r\n..."
        let mut info = HashMap::new();
        for line in info_str.lines() {
            if let Some((key, value)) = line.split_once(':') {
                info.insert(key.trim().to_string(), value.trim().to_string());
            }
        }

        Ok(Self { info })
    }
}

impl Serialize for ClusterInfoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterInfoOutput", 1)?;
        state.serialize_field("info", &self.info)?;
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
            let input = ClusterInfoInput {};
            let cmd = input.command();
            // CLUSTER INFO has no args: *1\r\n$12\r\nCLUSTER INFO\r\n
            // or *2\r\n$7\r\nCLUSTER\r\n$4\r\nINFO\r\n depending on implementation
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("INFO") || cmd_str.contains("CLUSTER INFO"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterInfoInput::decode(args).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_decode_input_with_extra_args_succeeds() {
            // Should succeed with warning, not error
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = ClusterInfoInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_decode_output_bulk_string() {
            // Simulated CLUSTER INFO response
            let response = b"$87\r\ncluster_state:ok\r\ncluster_slots_assigned:16384\r\ncluster_known_nodes:6\r\ncluster_size:3\r\n\r\n";
            let output = ClusterInfoOutput::decode(response).unwrap();
            assert_eq!(output.cluster_state(), Some(&"ok".to_string()));
            assert_eq!(output.cluster_slots_assigned(), Some(16384));
            assert_eq!(output.cluster_known_nodes(), Some(6));
        }

        #[test]
        fn test_decode_output_error_response() {
            let response = b"-ERR This instance has cluster support disabled\r\n";
            let err = ClusterInfoOutput::decode(response).unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterInfoInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization_roundtrip() {
            let input = ClusterInfoInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("CLUSTER INFO") || json.contains("ClusterInfo"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: These tests require a Redis cluster setup.
        // In standalone mode, CLUSTER INFO returns an error.
        // We test the error handling path for standalone instances.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_info_standalone_returns_error() {
            test_all_protocols_min_version("3.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterInfoInput {}.command()).await.expect("raw failed");

                    // Standalone Redis returns error for cluster commands
                    // This validates our error decoding works
                    let decode_result = ClusterInfoOutput::decode(&result);

                    // Either it's a proper error response, or if cluster is enabled, it works
                    match decode_result {
                        Ok(output) => {
                            // Cluster mode - should have state
                            assert!(output.cluster_state().is_some());
                        }
                        Err(e) => {
                            // Standalone mode - should mention cluster disabled
                            let err_msg = e.to_string().to_lowercase();
                            assert!(
                                err_msg.contains("cluster") || err_msg.contains("disabled"),
                                "Expected cluster-related error, got: {}",
                                e
                            );
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_info_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("3.0", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;
                let result = ctx.raw(&ClusterInfoInput {}.command()).await.expect("raw failed");

                // Should get either bulk string or error
                assert!(
                    result.starts_with(b"$") || result.starts_with(b"-"),
                    "Expected bulk string or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_info_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("6", version) {
                    continue; // RESP3 requires Redis 6+
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;
                let result = ctx.raw(&ClusterInfoInput {}.command()).await.expect("raw failed");

                // Should get either blob string or error
                // RESP3 blob string starts with $ or simple error with -
                assert!(
                    result.starts_with(b"$") || result.starts_with(b"-") || result.starts_with(b"!"),
                    "Expected blob string or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }
    }
}
