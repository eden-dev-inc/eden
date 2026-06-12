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

const API_INFO: ApiInfo<RedisApi, WaitaofInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Waitaof,
    "Blocks until all of the preceding write commands sent by the connection are written to the append-only file of the master and/or replicas",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `WAITAOF`
/// https://redis.io/docs/latest/commands/waitaof/
///
/// Available since Redis 7.2.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct WaitaofInput {
    /// Number of local AOF fsync confirmations to wait for (0 or 1)
    pub(crate) num_local: i64,
    /// Number of replica AOF fsync confirmations to wait for
    pub(crate) num_replicas: i64,
    /// Timeout in milliseconds (0 means block indefinitely)
    pub(crate) timeout: i64,
}

impl Serialize for WaitaofInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("WaitaofInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("num_local", &self.num_local)?;
        state.serialize_field("num_replicas", &self.num_replicas)?;
        state.serialize_field("timeout", &self.timeout)?;
        state.end()
    }
}

impl_redis_operation!(
    WaitaofInput,
    API_INFO,
    { num_local, num_replicas, timeout }
);

impl RedisCommandInput for WaitaofInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(self.num_local).arg(self.num_replicas).arg(self.timeout);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!(
                "WAITAOF requires 3 arguments (num_local, num_replicas, timeout), given {}",
                args.len()
            )));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "WAITAOF expects 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let num_local = parse_non_negative_integer(&args[0], "num_local")?;
        let num_replicas = parse_non_negative_integer(&args[1], "num_replicas")?;
        let timeout = parse_non_negative_integer(&args[2], "timeout")?;

        if num_local > 1 {
            return Err(EpError::parse("num_local must be 0 or 1"));
        }

        Ok(Self { num_local, num_replicas, timeout })
    }
}

fn parse_non_negative_integer(value: &RedisJsonValue, field_name: &str) -> Result<i64, EpError> {
    let n = match value {
        RedisJsonValue::Integer(i) => *i,
        RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse(format!("{} must be a valid integer", field_name)))?,
        _ => return Err(EpError::parse(format!("{} must be an integer", field_name))),
    };

    if n < 0 {
        return Err(EpError::parse(format!("{} must be a non-negative integer", field_name)));
    }

    Ok(n)
}

/// Output for Redis WAITAOF command
///
/// Returns an array of two integers:
/// - Number of local AOF fsyncs acknowledged
/// - Number of replica AOF fsyncs acknowledged
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct WaitaofOutput {
    /// Number of local AOF fsync confirmations received
    num_local: i64,
    /// Number of replica AOF fsync confirmations received
    num_replicas: i64,
}

impl WaitaofOutput {
    pub fn new(num_local: i64, num_replicas: i64) -> Self {
        Self { num_local, num_replicas }
    }

    /// Get the number of local AOF fsync confirmations
    pub fn num_local(&self) -> i64 {
        self.num_local
    }

    /// Get the number of replica AOF fsync confirmations
    pub fn num_replicas(&self) -> i64 {
        self.num_replicas
    }

    /// Decode the Redis protocol response into a WaitaofOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(elements) => Self::parse_array_elements(&elements),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected WAITAOF response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::parse_resp3_array(&data),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected WAITAOF response: {:?}", other))),
            },
        }
    }

    fn parse_array_elements(elements: &[Resp2Frame]) -> Result<Self, EpError> {
        if elements.len() != 2 {
            return Err(EpError::parse(format!("WAITAOF expected 2 elements, got {}", elements.len())));
        }

        let num_local = match &elements[0] {
            Resp2Frame::Integer(i) => *i,
            other => {
                return Err(EpError::parse(format!("expected integer for num_local, got {:?}", other)));
            }
        };

        let num_replicas = match &elements[1] {
            Resp2Frame::Integer(i) => *i,
            other => {
                return Err(EpError::parse(format!("expected integer for num_replicas, got {:?}", other)));
            }
        };

        Ok(Self { num_local, num_replicas })
    }

    fn parse_resp3_array(elements: &[Resp3Frame]) -> Result<Self, EpError> {
        if elements.len() != 2 {
            return Err(EpError::parse(format!("WAITAOF expected 2 elements, got {}", elements.len())));
        }

        let num_local = match &elements[0] {
            Resp3Frame::Number { data, .. } => *data,
            other => {
                return Err(EpError::parse(format!("expected integer for num_local, got {:?}", other)));
            }
        };

        let num_replicas = match &elements[1] {
            Resp3Frame::Number { data, .. } => *data,
            other => {
                return Err(EpError::parse(format!("expected integer for num_replicas, got {:?}", other)));
            }
        };

        Ok(Self { num_local, num_replicas })
    }
}

impl Serialize for WaitaofOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("WaitaofOutput", 2)?;
        state.serialize_field("num_local", &self.num_local)?;
        state.serialize_field("num_replicas", &self.num_replicas)?;
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
            let input = WaitaofInput { num_local: 1, num_replicas: 0, timeout: 1000 };
            assert_eq!(input.command().to_vec(), b"*4\r\n$7\r\nWAITAOF\r\n$1\r\n1\r\n$1\r\n0\r\n$4\r\n1000\r\n");
        }

        #[test]
        fn test_encode_command_zero_timeout() {
            let input = WaitaofInput { num_local: 0, num_replicas: 2, timeout: 0 };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$7\r\nWAITAOF\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(5000),
            ];
            let input = WaitaofInput::decode(args).unwrap();
            assert_eq!(input.num_local, 1);
            assert_eq!(input.num_replicas, 0);
            assert_eq!(input.timeout, 5000);
        }

        #[test]
        fn test_decode_input_string_values() {
            let args = vec![
                RedisJsonValue::String("1".into()),
                RedisJsonValue::String("2".into()),
                RedisJsonValue::String("1000".into()),
            ];
            let input = WaitaofInput::decode(args).unwrap();
            assert_eq!(input.num_local, 1);
            assert_eq!(input.num_replicas, 2);
            assert_eq!(input.timeout, 1000);
        }

        #[test]
        fn test_decode_input_num_local_invalid() {
            let args = vec![
                RedisJsonValue::Integer(2), // Invalid: must be 0 or 1
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1000),
            ];
            let err = WaitaofInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("num_local must be 0 or 1"));
        }

        #[test]
        fn test_decode_input_negative_fails() {
            let args = vec![
                RedisJsonValue::Integer(-1),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1000),
            ];
            let err = WaitaofInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("non-negative"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(0)];
            let err = WaitaofInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_decode_output_resp2_array() {
            // RESP2 array: *2\r\n:1\r\n:0\r\n
            let output = WaitaofOutput::decode(b"*2\r\n:1\r\n:0\r\n").unwrap();
            assert_eq!(output.num_local(), 1);
            assert_eq!(output.num_replicas(), 0);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = WaitaofOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = WaitaofInput { num_local: 1, num_replicas: 0, timeout: 1000 };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialization() {
            let input = WaitaofInput { num_local: 1, num_replicas: 2, timeout: 5000 };
            let json = serde_json::to_value(&input).unwrap();
            assert_eq!(json["num_local"], 1);
            assert_eq!(json["num_replicas"], 2);
            assert_eq!(json["timeout"], 5000);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        // WAITAOF requires Redis 7.2+
        const MIN_VERSION: &str = "7.2";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_waitaof_basic() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // First write something
                    ctx.write(SetInput {
                        key: RedisKey::String("waitaof_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Then wait for AOF sync with timeout
                    let result =
                        ctx.raw(&WaitaofInput { num_local: 0, num_replicas: 0, timeout: 100 }.command()).await.expect("raw failed");

                    let output = WaitaofOutput::decode(&result).expect("decode failed");
                    // In standalone mode without AOF, both should be 0
                    assert!(output.num_local() >= 0);
                    assert!(output.num_replicas() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_waitaof_zero_timeout() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // With timeout=0 and num_local=0, num_replicas=0, should return immediately
                    let result = ctx.raw(&WaitaofInput { num_local: 0, num_replicas: 0, timeout: 0 }.command()).await.expect("raw failed");

                    let output = WaitaofOutput::decode(&result).expect("decode failed");
                    assert!(output.num_local() >= 0);
                    assert!(output.num_replicas() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_waitaof_request_local() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Enable AOF because WAITAOF with num_local > 0 needs it
                    ctx.raw(b"*4\r\n$6\r\nCONFIG\r\n$3\r\nSET\r\n$10\r\nappendonly\r\n$3\r\nyes\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&WaitaofInput { num_local: 1, num_replicas: 0, timeout: 100 }.command()).await.expect("raw failed");

                    // Should succeed (though local count depends on AOF config)
                    let output = WaitaofOutput::decode(&result).expect("decode failed");
                    assert!(output.num_local() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_waitaof_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier(MIN_VERSION, version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;
                let result = ctx.raw(&WaitaofInput { num_local: 0, num_replicas: 0, timeout: 100 }.command()).await.expect("raw failed");

                // Should be RESP2 array format: *2\r\n:N\r\n:N\r\n
                assert!(result.starts_with(b"*2\r\n"), "Expected RESP2 array");
                let output = WaitaofOutput::decode(&result).expect("decode failed");
                assert!(output.num_local() >= 0);
                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_waitaof_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier(MIN_VERSION, version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;
                let result = ctx.raw(&WaitaofInput { num_local: 0, num_replicas: 0, timeout: 100 }.command()).await.expect("raw failed");

                let output = WaitaofOutput::decode(&result).expect("decode failed");
                assert!(output.num_local() >= 0);
                ctx.stop().await;
            }
        }
    }
}
