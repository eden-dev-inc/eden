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

const API_INFO: ApiInfo<RedisApi, PsetexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Psetex,
    "Sets both string value and expiration time in milliseconds. The key is created if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PSETEX`
/// https://redis.io/docs/latest/commands/psetex/
///
/// Note: As of Redis 2.6.12, this command is considered deprecated.
/// The recommended alternative is `SET` with the `PX` option.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PsetexInput {
    /// The key to set
    pub(crate) key: RedisKey,
    /// Expiration time in milliseconds (must be positive)
    pub(crate) milliseconds: RedisJsonValue,
    /// The value to store
    pub(crate) value: RedisJsonValue,
}

impl Serialize for PsetexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PsetexInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("milliseconds", &self.milliseconds)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    PsetexInput,
    API_INFO,
    {key, milliseconds, value}
);

impl RedisCommandInput for PsetexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        // PSETEX key milliseconds value
        command.arg(&self.key).arg(&self.milliseconds).arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!(
                "PSETEX requires 3 arguments (key, milliseconds, value), given {}",
                args.len()
            )));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "PSETEX expects 3 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let milliseconds = match &args[1] {
            RedisJsonValue::Integer(i) => *i,
            RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("milliseconds must be a valid integer"))?,
            _ => return Err(EpError::parse("milliseconds must be an integer")),
        };

        if milliseconds <= 0 {
            return Err(EpError::parse("milliseconds must be a positive integer"));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            milliseconds: RedisJsonValue::Integer(milliseconds),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis PSETEX command
///
/// Returns OK on success. An error is returned if milliseconds is invalid.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PsetexOutput {
    /// Always "OK" on success
    status: String,
}

impl PsetexOutput {
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

    /// Decode the Redis protocol response into a PsetexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected PSETEX response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected PSETEX response: {:?}", other))),
            },
        }
    }
}

impl Default for PsetexOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for PsetexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PsetexOutput", 1)?;
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
            let input = PsetexInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 1000.into(),
                value: RedisJsonValue::String("myvalue".into()),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nPSETEX\r\n$5\r\nmykey\r\n$4\r\n1000\r\n$7\r\nmyvalue\r\n");
        }

        #[test]
        fn test_encode_command_large_ttl() {
            let input = PsetexInput {
                key: RedisKey::String("k".into()),
                milliseconds: 86400000.into(), // 1 day in ms
                value: RedisJsonValue::String("v".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$6\r\nPSETEX\r\n"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = PsetexOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = PsetexOutput::decode(b"-ERR invalid expire time\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(5000),
                RedisJsonValue::String("value".into()),
            ];
            let input = PsetexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.milliseconds, RedisJsonValue::Integer(5000));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_string_milliseconds() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("3000".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = PsetexInput::decode(args).unwrap();
            assert_eq!(input.milliseconds, RedisJsonValue::Integer(3000));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1000)];
            let err = PsetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_decode_input_zero_milliseconds_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("value".into()),
            ];
            let err = PsetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("positive integer"));
        }

        #[test]
        fn test_decode_input_negative_milliseconds_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(-1000),
                RedisJsonValue::String("value".into()),
            ];
            let err = PsetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("positive integer"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PsetexInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 1000.into(),
                value: RedisJsonValue::String("val".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::GetInput;
        use crate::api::lib::generic::pttl::{PttlInput, PttlOutput};
        use crate::api::lib::string::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PsetexInput {
                                key: RedisKey::String("psetex_basic".into()),
                                milliseconds: 60000.into(), // 60 seconds
                                value: RedisJsonValue::String("myvalue".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PsetexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify the value was set
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("psetex_basic".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("myvalue")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_overwrites_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First set
                    ctx.raw(
                        &PsetexInput {
                            key: RedisKey::String("psetex_overwrite".into()),
                            milliseconds: 60000.into(),
                            value: RedisJsonValue::String("original".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Overwrite
                    let result = ctx
                        .raw(
                            &PsetexInput {
                                key: RedisKey::String("psetex_overwrite".into()),
                                milliseconds: 30000.into(),
                                value: RedisJsonValue::String("updated".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PsetexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify update
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("psetex_overwrite".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("updated")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_empty_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PsetexInput {
                                key: RedisKey::String("psetex_empty".into()),
                                milliseconds: 60000.into(),
                                value: RedisJsonValue::String("".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PsetexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("psetex_empty".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_ttl_is_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &PsetexInput {
                            key: RedisKey::String("psetex_ttl".into()),
                            milliseconds: 300000.into(), // 300 seconds = 5 minutes
                            value: RedisJsonValue::String("ttl_value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check PTTL is approximately correct
                    let pttl_result =
                        ctx.raw(&PttlInput { key: RedisKey::String("psetex_ttl".into()) }.command()).await.expect("raw failed");

                    let pttl_output = PttlOutput::decode(&pttl_result).expect("decode failed");
                    let ttl_ms = pttl_output.ttl_ms().unwrap();

                    // Should be close to 300000ms (allowing for some time to pass)
                    assert!(ttl_ms > 295000 && ttl_ms <= 300000, "PTTL should be ~300000ms, got {}", ttl_ms);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_expiry() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set with 100ms TTL
                    ctx.raw(
                        &PsetexInput {
                            key: RedisKey::String("psetex_expire".into()),
                            milliseconds: 100.into(),
                            value: RedisJsonValue::String("expire_value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Verify it exists
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("psetex_expire".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());

                    // Wait for expiry
                    tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

                    // Should be gone
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("psetex_expire".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(!get_output.exists(), "key should have expired");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &PsetexInput {
                            key: RedisKey::String("psetex_pipe1".into()),
                            milliseconds: 60000.into(),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &PsetexInput {
                            key: RedisKey::String("psetex_pipe2".into()),
                            milliseconds: 60000.into(),
                            value: RedisJsonValue::String("v2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("psetex_pipe1".into()) }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("psetex_pipe2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 4);

                    // Both PSETEX should return OK
                    let out1 = PsetexOutput::decode(responses[0]).expect("decode psetex1");
                    assert!(out1.is_ok());
                    let out2 = PsetexOutput::decode(responses[1]).expect("decode psetex2");
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
        async fn test_psetex_resp2_ok_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &PsetexInput {
                        key: RedisKey::String("resp2key".into()),
                        milliseconds: 60000.into(),
                        value: RedisJsonValue::String("resp2val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
            let output = PsetexOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_psetex_resp3_ok_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &PsetexInput {
                        key: RedisKey::String("resp3key".into()),
                        milliseconds: 60000.into(),
                        value: RedisJsonValue::String("resp3val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string OK format");
            let output = PsetexOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
