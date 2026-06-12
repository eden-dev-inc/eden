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

const API_INFO: ApiInfo<RedisApi, WaitInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Wait,
    "Blocks until the asynchronous replication of all preceding write commands sent by the connection is complete, or until the timeout is reached. Returns the number of replicas that acknowledged.",
    ReqType::Read, // WAIT doesn't modify data, it observes replication state
    false,
);

/// See official Redis documentation for `WAIT`
/// https://redis.io/docs/latest/commands/wait/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct WaitInput {
    /// Number of replicas to wait for
    pub(crate) num_replicas: i64,
    /// Timeout in milliseconds (0 means block indefinitely)
    pub(crate) timeout: i64,
}

impl Serialize for WaitInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("WaitInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("num_replicas", &self.num_replicas)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(WaitInput, API_INFO, { num_replicas, timeout });

impl RedisCommandInput for WaitInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![] // WAIT operates on connection state, not keys
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(self.num_replicas).arg(self.timeout);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("WAIT requires 2 arguments (num_replicas, timeout), given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "WAIT expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let num_replicas = parse_non_negative_integer(&args[0], "num_replicas")?;
        let timeout = parse_non_negative_integer(&args[1], "timeout")?;

        Ok(Self { num_replicas, timeout })
    }
}

fn parse_non_negative_integer(value: &RedisJsonValue, field_name: &str) -> Result<i64, EpError> {
    let parsed = match value {
        RedisJsonValue::Integer(i) => *i,
        RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse(format!("{} must be a valid integer", field_name)))?,
        _ => return Err(EpError::parse(format!("{} must be an integer", field_name))),
    };

    if parsed < 0 {
        return Err(EpError::parse(format!("{} must be non-negative", field_name)));
    }

    Ok(parsed)
}

/// Output for Redis WAIT command
///
/// Returns the number of replicas that acknowledged the write commands.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct WaitOutput {
    /// Number of replicas that acknowledged
    num_acknowledged: i64,
}

impl WaitOutput {
    pub fn new(num_acknowledged: i64) -> Self {
        Self { num_acknowledged }
    }

    /// Get the number of replicas that acknowledged
    pub fn num_acknowledged(&self) -> i64 {
        self.num_acknowledged
    }

    /// Check if the requested number of replicas acknowledged
    pub fn reached_target(&self, target: i64) -> bool {
        self.num_acknowledged >= target
    }

    /// Decode the Redis protocol response into a WaitOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let num_acknowledged = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected WAIT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected WAIT response: {:?}", other)));
                }
            },
        };

        Ok(Self { num_acknowledged })
    }
}

impl Serialize for WaitOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("WaitOutput", 1)?;
        state.serialize_field("num_acknowledged", &self.num_acknowledged)?;
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
            let input = WaitInput { num_replicas: 1, timeout: 1000 };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$4\r\n1000\r\n");
        }

        #[test]
        fn test_encode_command_zero_timeout() {
            let input = WaitInput { num_replicas: 2, timeout: 0 };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$4\r\nWAIT\r\n"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = WaitOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.num_acknowledged(), 3);
        }

        #[test]
        fn test_decode_zero_response() {
            let output = WaitOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.num_acknowledged(), 0);
            assert!(!output.reached_target(1));
        }

        #[test]
        fn test_reached_target() {
            let output = WaitOutput::new(2);
            assert!(output.reached_target(1));
            assert!(output.reached_target(2));
            assert!(!output.reached_target(3));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = WaitOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::Integer(2), RedisJsonValue::Integer(5000)];
            let input = WaitInput::decode(args).unwrap();
            assert_eq!(input.num_replicas, 2);
            assert_eq!(input.timeout, 5000);
        }

        #[test]
        fn test_decode_input_string_values() {
            let args = vec![RedisJsonValue::String("1".into()), RedisJsonValue::String("1000".into())];
            let input = WaitInput::decode(args).unwrap();
            assert_eq!(input.num_replicas, 1);
            assert_eq!(input.timeout, 1000);
        }

        #[test]
        fn test_decode_input_negative_replicas_fails() {
            let args = vec![RedisJsonValue::Integer(-1), RedisJsonValue::Integer(1000)];
            let err = WaitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_negative_timeout_fails() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(-100)];
            let err = WaitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(1)];
            let err = WaitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = WaitInput { num_replicas: 1, timeout: 1000 };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: WAIT returns 0 immediately on standalone Redis without replicas.
        // Meaningful replica testing requires a Redis cluster setup.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wait_no_replicas() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &WaitInput {
                                num_replicas: 1,
                                timeout: 100, // Short timeout
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = WaitOutput::decode(&result).expect("decode failed");
                    // Standalone Redis has no replicas, should return 0
                    assert_eq!(output.num_acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wait_zero_replicas_returns_immediately() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let start = std::time::Instant::now();
                    let result = ctx
                        .raw(
                            &WaitInput {
                                num_replicas: 0,
                                timeout: 10000, // Long timeout that shouldn't be reached
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let elapsed = start.elapsed();
                    let output = WaitOutput::decode(&result).expect("decode failed");

                    // Requesting 0 replicas should return immediately
                    assert!(elapsed.as_millis() < 1000, "WAIT 0 should return immediately");
                    assert_eq!(output.num_acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wait_timeout_respected() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let start = std::time::Instant::now();
                    let result = ctx.raw(&WaitInput { num_replicas: 1, timeout: 200 }.command()).await.expect("raw failed");

                    let elapsed = start.elapsed();
                    let output = WaitOutput::decode(&result).expect("decode failed");

                    // Should timeout after ~200ms since no replicas exist
                    assert!(elapsed.as_millis() >= 150, "Should wait approximately timeout duration");
                    assert_eq!(output.num_acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wait_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&WaitInput { num_replicas: 0, timeout: 0 }.command()).await.expect("raw failed");

            // RESP2 integer format
            assert!(result.starts_with(b":"), "RESP2 integer format");
            let output = WaitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.num_acknowledged(), 0);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_wait_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&WaitInput { num_replicas: 0, timeout: 0 }.command()).await.expect("raw failed");

            // RESP3 also uses : for integers
            assert!(result.starts_with(b":"), "RESP3 integer format");
            let output = WaitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.num_acknowledged(), 0);
            ctx.stop().await;
        }
    }
}
