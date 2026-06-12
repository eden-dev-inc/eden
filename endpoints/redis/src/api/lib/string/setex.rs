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

const API_INFO: ApiInfo<RedisApi, SetexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Setex,
    "Set key to hold the string value and set key to timeout after a given number of seconds. This command is equivalent to `SET key value EX seconds`. SETEX is atomic and can be used as a single command. An error is returned when seconds is invalid.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SETEX`
/// https://redis.io/docs/latest/commands/setex/
///
/// Note: As of Redis 2.6.12, this command is considered deprecated.
/// The recommended alternative is `SET` with the `EX` option.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SetexInput {
    /// The key to set
    pub(crate) key: RedisKey,
    /// Expiration time in seconds (must be positive)
    pub(crate) seconds: RedisJsonValue,
    /// The value to store
    pub(crate) value: RedisJsonValue,
}

impl Serialize for SetexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SetexInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("seconds", &self.seconds)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(SetexInput, API_INFO, { key, seconds, value });

impl RedisCommandInput for SetexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.seconds).arg(&self.value);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("SETEX requires 3 arguments (key, seconds, value), given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SETEX expects 3 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let seconds = match &args[1] {
            RedisJsonValue::Integer(i) => *i,
            RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("seconds must be a valid integer"))?,
            _ => return Err(EpError::parse("seconds must be an integer")),
        };

        if seconds <= 0 {
            return Err(EpError::parse("seconds must be a positive integer"));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            seconds: RedisJsonValue::Integer(seconds),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis SETEX command
///
/// Returns OK on success. An error is returned if seconds is invalid
/// or if the value stored at key is not a string.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SetexOutput {
    /// Always "OK" on success
    status: String,
}

impl SetexOutput {
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

    /// Decode the Redis protocol response into a SetexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SETEX response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SETEX response: {:?}", other))),
            },
        }
    }
}

impl Default for SetexOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for SetexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SetexOutput", 1)?;
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
        fn test_encode_command() {
            let input = SetexInput {
                key: RedisKey::String("mykey".into()),
                seconds: 10.into(),
                value: RedisJsonValue::String("myvalue".into()),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$5\r\nSETEX\r\n$5\r\nmykey\r\n$2\r\n10\r\n$7\r\nmyvalue\r\n");
        }

        #[test]
        fn test_encode_command_large_ttl() {
            let input = SetexInput {
                key: RedisKey::String("k".into()),
                seconds: 86400.into(), // 1 day
                value: RedisJsonValue::String("v".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nSETEX\r\n"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = SetexOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = SetexOutput::decode(b"-ERR invalid expire time\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid expire time"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(60),
                RedisJsonValue::String("value".into()),
            ];
            let input = SetexInput::decode(args).unwrap();
            assert_eq!(input.seconds, 60.into());
        }

        #[test]
        fn test_decode_input_string_seconds() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("30".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = SetexInput::decode(args).unwrap();
            assert_eq!(input.seconds, 30.into());
        }

        #[test]
        fn test_decode_input_zero_seconds_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("value".into()),
            ];
            let err = SetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("positive"));
        }

        #[test]
        fn test_decode_input_negative_seconds_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(-10),
                RedisJsonValue::String("value".into()),
            ];
            let err = SetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("positive"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(10)];
            let err = SetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SetexInput {
                key: RedisKey::String("testkey".into()),
                seconds: 10.into(),
                value: RedisJsonValue::String("val".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::TtlInput;
        use crate::api::lib::string::get::GetInput;
        use crate::api::lib::string::get::GetOutput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetexInput {
                                key: RedisKey::String("setex_key".into()),
                                seconds: 100.into(),
                                value: RedisJsonValue::String("setex_value".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify value was set
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("setex_key".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode get failed");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("setex_value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_overwrites_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("overwrite_key".into()),
                            seconds: 100.into(),
                            value: RedisJsonValue::String("first".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Overwrite
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("overwrite_key".into()),
                            seconds: 200.into(),
                            value: RedisJsonValue::String("second".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Verify new value
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("overwrite_key".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("second")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_empty_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetexInput {
                                key: RedisKey::String("empty_val".into()),
                                seconds: 60.into(),
                                value: RedisJsonValue::String("".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("empty_val".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_ttl_is_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("ttl_key".into()),
                            seconds: 300.into(),
                            value: RedisJsonValue::String("ttl_value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check TTL is approximately correct (within a second or two)
                    let ttl_result = ctx.raw(&TtlInput { key: RedisKey::String("ttl_key".into()) }.command()).await.expect("raw failed");

                    // TTL response is an integer, parse it
                    let ttl_str = String::from_utf8_lossy(&ttl_result);
                    // RESP integer format: :300\r\n
                    let ttl: i64 = ttl_str.trim_start_matches(':').trim_end_matches("\r\n").parse().expect("parse ttl");

                    assert!(ttl > 295 && ttl <= 300, "TTL should be ~300, got {}", ttl);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_expiry() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set with 1 second TTL
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("expire_key".into()),
                            seconds: 1.into(),
                            value: RedisJsonValue::String("expire_value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Verify it exists
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("expire_key".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());

                    // Wait for expiry
                    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

                    // Should be gone
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("expire_key".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(!get_output.exists(), "key should have expired");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetexInput {
                            key: RedisKey::String("pipe1".into()),
                            seconds: 60.into(),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetexInput {
                            key: RedisKey::String("pipe2".into()),
                            seconds: 60.into(),
                            value: RedisJsonValue::String("v2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe1".into()) }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 4);

                    // Both SETEX should return OK
                    let out1 = SetexOutput::decode(responses[0]).expect("decode setex1");
                    assert!(out1.is_ok());
                    let out2 = SetexOutput::decode(responses[1]).expect("decode setex2");
                    assert!(out2.is_ok());

                    // GET results
                    let get1 = GetOutput::decode(responses[2]).expect("decode get1");
                    assert_eq!(get1.value(), Some(&RedisJsonValue::from("v1")));
                    let get2 = GetOutput::decode(responses[3]).expect("decode get2");
                    assert_eq!(get2.value(), Some(&RedisJsonValue::from("v2")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_resp2_ok_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx
                .raw(
                    &SetexInput {
                        key: RedisKey::String("resp2key".into()),
                        seconds: 60.into(),
                        value: RedisJsonValue::String("resp2val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
            let output = SetexOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setex_resp3_ok_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx
                .raw(
                    &SetexInput {
                        key: RedisKey::String("resp3key".into()),
                        seconds: 60.into(),
                        value: RedisJsonValue::String("resp3val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string OK format");
            let output = SetexOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());
            ctx.stop().await;
        }
    }
}
