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

const API_INFO: ApiInfo<RedisApi, ClusterMeetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterMeet,
    "Forces a node to handshake with another node",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER MEET`
/// https://redis.io/docs/latest/commands/cluster-meet/
///
/// Available since Redis 3.0.0
///
/// The optional cluster_bus_port argument was added in Redis 4.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, Default)]
#[builder(default)]
pub struct ClusterMeetInput {
    /// IP address of the target node
    pub(crate) ip: RedisJsonValue,
    /// Port of the target node
    pub(crate) port: RedisJsonValue,
    /// Optional cluster bus port (Redis 4.0+)
    pub(crate) cluster_bus_port: Option<RedisJsonValue>,
}

impl Serialize for ClusterMeetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, ip, port
        if self.cluster_bus_port.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ClusterMeetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("ip", &self.ip)?;
        state.serialize_field("port", &self.port)?;
        if let Some(ref cluster_bus_port) = self.cluster_bus_port {
            state.serialize_field("cluster_bus_port", cluster_bus_port)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ClusterMeetInput,
    API_INFO,
    {ip, port, cluster_bus_port}
);

impl RedisCommandInput for ClusterMeetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.ip).arg(&self.port);

        // Only add cluster_bus_port if it's Some
        if let Some(ref cbp) = self.cluster_bus_port {
            command.arg(cbp);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!(
                "CLUSTER MEET requires at least 2 arguments (ip, port), given {}",
                args.len()
            )));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER MEET expects at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let ip = args[0].clone();
        let port = args[1].clone();
        let cluster_bus_port = if args.len() >= 3 { Some(args[2].clone()) } else { None };

        Ok(Self { ip, port, cluster_bus_port })
    }
}

/// Output for Redis CLUSTER MEET command
///
/// Returns OK if the command was accepted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterMeetOutput {
    /// Status message ("OK" on success)
    status: String,
}

impl ClusterMeetOutput {
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

    /// Decode the Redis protocol response into a ClusterMeetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CLUSTER MEET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER MEET response: {:?}", other))),
            },
        }
    }
}

impl Default for ClusterMeetOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterMeetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterMeetOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("127.0.0.1".into()),
                port: RedisJsonValue::Integer(6379),
                cluster_bus_port: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("MEET") || cmd_str.contains("CLUSTER MEET"));
            assert!(cmd_str.contains("127.0.0.1"));
            assert!(cmd_str.contains("6379"));
            // Should NOT contain any extra arguments
            assert!(!cmd_str.contains("None"));
        }

        #[test]
        fn test_encode_command_with_cluster_bus_port() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("10.0.0.1".into()),
                port: RedisJsonValue::Integer(6379),
                cluster_bus_port: Some(RedisJsonValue::Integer(16379)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("10.0.0.1"));
            assert!(cmd_str.contains("6379"));
            assert!(cmd_str.contains("16379"));
        }

        #[test]
        fn test_encode_command_without_cluster_bus_port_no_extra_args() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("192.168.1.1".into()),
                port: RedisJsonValue::String("7000".into()),
                cluster_bus_port: None,
            };
            let cmd = input.command();

            // Count the number of arguments in the RESP array
            // Format: *N\r\n where N is the number of elements
            let cmd_str = String::from_utf8_lossy(&cmd);

            // For CLUSTER MEET with no bus port, should be 3 elements:
            // CLUSTER, MEET, ip, port (or CLUSTER MEET, ip, port depending on impl)
            // The key point: should NOT include a null/empty fourth argument
            assert!(!cmd_str.contains("$-1\r\n")); // No null bulk strings
            assert!(!cmd_str.contains("$0\r\n\r\n")); // No empty strings from None
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("127.0.0.1".into()), RedisJsonValue::Integer(6379)];
            let input = ClusterMeetInput::decode(args).unwrap();
            assert_eq!(input.ip, RedisJsonValue::String("127.0.0.1".into()));
            assert_eq!(input.port, RedisJsonValue::Integer(6379));
            assert!(input.cluster_bus_port.is_none());
        }

        #[test]
        fn test_decode_input_with_cluster_bus_port() {
            let args = vec![
                RedisJsonValue::String("127.0.0.1".into()),
                RedisJsonValue::Integer(6379),
                RedisJsonValue::Integer(16379),
            ];
            let input = ClusterMeetInput::decode(args).unwrap();
            assert!(input.cluster_bus_port.is_some());
            assert_eq!(input.cluster_bus_port.unwrap(), RedisJsonValue::Integer(16379));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("127.0.0.1".into())];
            let err = ClusterMeetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ClusterMeetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let response = b"+OK\r\n";
            let output = ClusterMeetOutput::decode(response).unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_output_error() {
            let response = b"-ERR Invalid node address specified\r\n";
            let err = ClusterMeetOutput::decode(response).unwrap_err();
            assert!(err.to_string().contains("Invalid node address"));
        }

        #[test]
        fn test_decode_output_cluster_disabled() {
            let response = b"-ERR This instance has cluster support disabled\r\n";
            let err = ClusterMeetOutput::decode(response).unwrap_err();
            assert!(err.to_string().contains("cluster support disabled"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("127.0.0.1".into()),
                port: RedisJsonValue::Integer(6379),
                cluster_bus_port: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization_without_bus_port() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("127.0.0.1".into()),
                port: RedisJsonValue::Integer(6379),
                cluster_bus_port: None,
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("127.0.0.1"));
            assert!(json.contains("6379"));
            assert!(!json.contains("cluster_bus_port"));
        }

        #[test]
        fn test_serialization_with_bus_port() {
            let input = ClusterMeetInput {
                ip: RedisJsonValue::String("127.0.0.1".into()),
                port: RedisJsonValue::Integer(6379),
                cluster_bus_port: Some(RedisJsonValue::Integer(16379)),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("cluster_bus_port"));
            assert!(json.contains("16379"));
        }

        #[test]
        fn test_output_serialization() {
            let output = ClusterMeetOutput::new();
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"status\":\"OK\""));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_meet_standalone_returns_error() {
            test_all_protocols_min_version("3.0", |ctx| {
                Box::pin(async move {
                    // Try to meet a non-existent node
                    let result = ctx
                        .raw(
                            &ClusterMeetInput {
                                ip: RedisJsonValue::String("127.0.0.1".into()),
                                port: RedisJsonValue::Integer(9999),
                                cluster_bus_port: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let decode_result = ClusterMeetOutput::decode(&result);

                    match decode_result {
                        Ok(output) => {
                            // Cluster mode with successful meet (unlikely in test)
                            assert!(output.is_ok());
                        }
                        Err(e) => {
                            // Expected: either cluster disabled or invalid address
                            let err_msg = e.to_string().to_lowercase();
                            assert!(
                                err_msg.contains("cluster")
                                    || err_msg.contains("disabled")
                                    || err_msg.contains("invalid")
                                    || err_msg.contains("address"),
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
        async fn test_cluster_meet_with_bus_port() {
            // CLUSTER MEET with bus port requires Redis 4.0+
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ClusterMeetInput {
                                ip: RedisJsonValue::String("127.0.0.1".into()),
                                port: RedisJsonValue::Integer(9999),
                                cluster_bus_port: Some(RedisJsonValue::Integer(19999)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Should either succeed or return cluster-related error
                    let decode_result = ClusterMeetOutput::decode(&result);
                    match decode_result {
                        Ok(_) => {}
                        Err(e) => {
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
        async fn test_cluster_meet_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("3.0", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;
                let result = ctx
                    .raw(
                        &ClusterMeetInput {
                            ip: RedisJsonValue::String("127.0.0.1".into()),
                            port: RedisJsonValue::Integer(9999),
                            cluster_bus_port: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                // Should get either simple string OK or error
                assert!(
                    result.starts_with(b"+") || result.starts_with(b"-"),
                    "Expected simple string or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_meet_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("6", version) {
                    continue; // RESP3 requires Redis 6+
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;
                let result = ctx
                    .raw(
                        &ClusterMeetInput {
                            ip: RedisJsonValue::String("127.0.0.1".into()),
                            port: RedisJsonValue::Integer(9999),
                            cluster_bus_port: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                // RESP3 simple string or error
                assert!(
                    result.starts_with(b"+") || result.starts_with(b"-") || result.starts_with(b"!"),
                    "Expected simple string or error, got: {:?}",
                    String::from_utf8_lossy(&result)
                );

                ctx.stop().await;
            }
        }
    }
}
