use crate::api::lib::RedisCommandOutput;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Failover, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, ClusterFailoverInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ClusterFailover,
    "Forces a replica to perform a manual failover of its master",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `CLUSTER FAILOVER`
/// https://redis.io/docs/latest/commands/cluster-failover/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct ClusterFailoverInput {
    pub(crate) failover: Option<Failover>,
}

impl Serialize for ClusterFailoverInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.failover.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ClusterFailoverInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(failover) = &self.failover {
            state.serialize_field("failover", failover)?;
        }
        state.end()
    }
}

impl_redis_operation!(ClusterFailoverInput, API_INFO, { failover });

impl RedisCommandInput for ClusterFailoverInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(failover) = &self.failover {
            match failover {
                Failover::FORCE => command.arg("FORCE"),
                Failover::TAKEOVER => command.arg("TAKEOVER"),
            };
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "CLUSTER FAILOVER expects at most 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let failover = if !args.is_empty() {
            match &args[0] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "FORCE" => Some(Failover::FORCE),
                    "TAKEOVER" => Some(Failover::TAKEOVER),
                    _ => {
                        return Err(EpError::request(format!("CLUSTER FAILOVER unknown option: {}", s)));
                    }
                },
                _ => return Err(EpError::parse("CLUSTER FAILOVER option must be a string")),
            }
        } else {
            None
        };

        Ok(Self { failover })
    }
}

/// Output for Redis CLUSTER FAILOVER command
///
/// Returns OK on success when the failover is initiated.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ClusterFailoverOutput {
    /// "OK" on success
    status: String,
}

impl ClusterFailoverOutput {
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

impl Default for ClusterFailoverOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for ClusterFailoverOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ClusterFailoverOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

impl RedisCommandOutput for ClusterFailoverOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::ClusterFailover
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
                other => Err(EpError::parse(format!("unexpected CLUSTER FAILOVER response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CLUSTER FAILOVER response: {:?}", other))),
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
        fn test_encode_command_no_option() {
            let input = ClusterFailoverInput { failover: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("FAILOVER"));
            assert!(!cmd_str.contains("FORCE"));
            assert!(!cmd_str.contains("TAKEOVER"));
        }

        #[test]
        fn test_encode_command_force() {
            let input = ClusterFailoverInput { failover: Some(Failover::FORCE) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("FAILOVER"));
            assert!(cmd_str.contains("FORCE"));
        }

        #[test]
        fn test_encode_command_takeover() {
            let input = ClusterFailoverInput { failover: Some(Failover::TAKEOVER) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CLUSTER"));
            assert!(cmd_str.contains("FAILOVER"));
            assert!(cmd_str.contains("TAKEOVER"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = ClusterFailoverOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = ClusterFailoverOutput::decode(b"-ERR You should send CLUSTER FAILOVER to a replica\r\n").unwrap_err();
            assert!(err.to_string().contains("replica"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ClusterFailoverInput::decode(args).unwrap();
            assert!(input.failover.is_none());
        }

        #[test]
        fn test_decode_input_force() {
            let args = vec![RedisJsonValue::String("FORCE".into())];
            let input = ClusterFailoverInput::decode(args).unwrap();
            assert_eq!(input.failover, Some(Failover::FORCE));
        }

        #[test]
        fn test_decode_input_takeover() {
            let args = vec![RedisJsonValue::String("takeover".into())];
            let input = ClusterFailoverInput::decode(args).unwrap();
            assert_eq!(input.failover, Some(Failover::TAKEOVER));
        }

        #[test]
        fn test_decode_input_unknown_option_fails() {
            let args = vec![RedisJsonValue::String("INVALID".into())];
            let err = ClusterFailoverInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("unknown option"));
        }

        #[test]
        fn test_decode_input_non_string_fails() {
            let args = vec![RedisJsonValue::Integer(42)];
            let err = ClusterFailoverInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("must be a string"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ClusterFailoverInput::default();
            assert!(input.keys().is_empty());
        }
    }

    // Note: Integration tests for CLUSTER commands require a Redis Cluster setup
    // which is more complex than single-node tests. These would typically be
    // run in a separate cluster test suite.
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // CLUSTER FAILOVER requires a cluster environment with replicas
        // These tests verify command encoding works correctly against a real Redis
        // but the actual failover behavior requires cluster setup

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cluster_failover_on_non_cluster_returns_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ClusterFailoverInput::default().command()).await.expect("raw failed");

                    // On a non-cluster Redis, this should return an error
                    let output = ClusterFailoverOutput::decode(&result);
                    assert!(output.is_err() || !output.unwrap().is_ok());
                })
            })
            .await;
        }
    }
}
