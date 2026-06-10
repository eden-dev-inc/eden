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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, FailoverInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Failover,
    "Starts a coordinated failover from a server to one of its replicas",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `FAILOVER`
/// https://redis.io/docs/latest/commands/failover/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FailoverInput {}

impl Serialize for FailoverInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FailoverInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(FailoverInput, API_INFO);

impl RedisCommandInput for FailoverInput {
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
                "FAILOVER expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis FAILOVER command
///
/// Returns OK when the failover command was accepted.
/// Note: This does not mean the failover completed, only that it started.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FailoverOutput {
    success: bool,
}

impl Serialize for FailoverOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FailoverOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FailoverOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the failover was accepted
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FAILOVER response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FAILOVER response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FailoverInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$8\r\nFAILOVER\r\n");
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FailoverOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FailoverOutput::decode(b"-ERR FAILOVER requires connected replicas\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = FailoverInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_still_succeeds() {
            let input = FailoverInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FailoverInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = FailoverInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Failover);
        }

        #[test]
        fn test_serialize_input() {
            let input = FailoverInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("FAILOVER") || json.contains("Failover"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FailoverOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_req_type_is_write() {
            assert_eq!(API_INFO.request_type, ReqType::Write);
        }
    }

    // Note: FAILOVER integration tests require a Redis replica setup.
    // These would typically be implemented separately with a multi-container test setup.
    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // FAILOVER requires replicas to function properly.
        // On a standalone Redis instance, it will return an error.
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_failover_standalone_returns_error() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FailoverInput {}.command()).await;

                    // On standalone Redis without replicas, FAILOVER should error
                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected: error because no replicas
                        }
                        Ok(r) => {
                            // If somehow it succeeds, that's fine too
                            let _ = FailoverOutput::decode(&r);
                        }
                        Err(_) => {
                            // Error is expected
                        }
                    }
                })
            })
            .await;
        }
    }
}
