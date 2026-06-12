use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{ExpireOption, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, PexpireatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pexpireat,
    "Sets the expiration time of a key as a Unix millisecond timestamp. Returns 1 if the timeout was set, 0 if the key does not exist or the operation was skipped due to the provided options.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PEXPIREAT`
/// https://redis.io/docs/latest/commands/pexpireat/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PexpireatInput {
    /// The key to set expiration on
    pub(crate) key: RedisKey,
    /// Unix timestamp in milliseconds
    pub(crate) unix_time_milliseconds: RedisJsonValue,
    /// Optional condition for setting expiry (requires Redis 7.0+)
    #[builder(default)]
    pub(crate) option: Option<ExpireOption>,
}

impl Serialize for PexpireatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.option.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("PexpireatInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("unix_time_milliseconds", &self.unix_time_milliseconds)?;
        if let Some(option) = &self.option {
            state.serialize_field("option", option)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    PexpireatInput,
    API_INFO,
    { key, unix_time_milliseconds, option }
);

impl RedisCommandInput for PexpireatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.unix_time_milliseconds);

        if let Some(option) = &self.option {
            match option {
                ExpireOption::NX => command.arg("NX"),
                ExpireOption::XX => command.arg("XX"),
                ExpireOption::GT => command.arg("GT"),
                ExpireOption::LT => command.arg("LT"),
            };
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("PEXPIREAT requires at least 2 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "PEXPIREAT expects at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let option = if args.len() >= 3 {
            match &args[2] {
                RedisJsonValue::String(s) => Some(match s.to_uppercase().as_str() {
                    "NX" => ExpireOption::NX,
                    "XX" => ExpireOption::XX,
                    "GT" => ExpireOption::GT,
                    "LT" => ExpireOption::LT,
                    _ => return Err(EpError::request(format!("PEXPIREAT unknown option: {}", s))),
                }),
                _ => return Err(EpError::parse("PEXPIREAT option must be a string")),
            }
        } else {
            None
        };

        Ok(Self {
            key: args[0].clone().try_into()?,
            unix_time_milliseconds: args[1].clone(),
            option,
        })
    }
}

/// Output for Redis PEXPIREAT command
///
/// Returns 1 if the timeout was set, 0 if the key does not exist
/// or the operation was skipped due to the provided options.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PexpireatOutput {
    /// 1 if timeout was set, 0 otherwise
    result: i64,
}

impl PexpireatOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Get the raw result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Check if the timeout was successfully set
    pub fn was_set(&self) -> bool {
        self.result == 1
    }

    /// Decode the Redis protocol response into a PexpireatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIREAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIREAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for PexpireatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PexpireatOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = PexpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$9\r\nPEXPIREAT\r\n$5\r\nmykey\r\n$13\r\n1672531200000\r\n");
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = PexpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: Some(ExpireOption::NX),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$9\r\nPEXPIREAT\r\n"));
            assert!(cmd.ends_with(b"$2\r\nNX\r\n"));
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = PexpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: Some(ExpireOption::XX),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nXX\r\n"));
        }

        #[test]
        fn test_encode_command_with_gt() {
            let input = PexpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: Some(ExpireOption::GT),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nGT\r\n"));
        }

        #[test]
        fn test_encode_command_with_lt() {
            let input = PexpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: Some(ExpireOption::LT),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nLT\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = PexpireatOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_set());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_failure_key_missing() {
            let output = PexpireatOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_set());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = PexpireatOutput::decode(b"-ERR invalid expire time\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid expire time"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(1672531200000)];
            let input = PexpireatInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.option.is_none());
        }

        #[test]
        fn test_decode_input_with_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1672531200000),
                RedisJsonValue::String("NX".into()),
            ];
            let input = PexpireatInput::decode(args).unwrap();
            assert_eq!(input.option, Some(ExpireOption::NX));
        }

        #[test]
        fn test_decode_input_option_case_insensitive() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1672531200000),
                RedisJsonValue::String("nx".into()),
            ];
            let input = PexpireatInput::decode(args).unwrap();
            assert_eq!(input.option, Some(ExpireOption::NX));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = PexpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1672531200000),
                RedisJsonValue::String("INVALID".into()),
            ];
            let err = PexpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("unknown option"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PexpireatInput {
                key: RedisKey::String("testkey".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1672531200000),
                option: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::PttlInput;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn future_timestamp_ms(seconds_from_now: u64) -> i64 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            now + (seconds_from_now as i64 * 1000)
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create key first
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let expire_at = future_timestamp_ms(300);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_key".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "should set expiry on existing key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let expire_at = future_timestamp_ms(300);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("nonexistent_pexpireat".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "should return 0 for nonexistent key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_verifies_ttl() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_ttl".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let expire_at = future_timestamp_ms(59);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_ttl".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check PTTL is approximately correct
                    let pttl_result =
                        ctx.raw(&PttlInput { key: RedisKey::String("pexpireat_ttl".into()) }.command()).await.expect("raw failed");

                    let pttl_str = String::from_utf8_lossy(&pttl_result);
                    let pttl: i64 = pttl_str.trim_start_matches(':').trim_end_matches("\r\n").parse().expect("parse pttl");

                    assert!(pttl > 55000 && pttl <= 60000, "PTTL should be ~59ms, got {}", pttl);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_expiry() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_expire".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set to expire in 1 second
                    let expire_at = future_timestamp_ms(1);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_expire".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Verify key exists
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("pexpireat_expire".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());

                    // Wait for expiry
                    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

                    // Should be gone
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("pexpireat_expire".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(!get_output.exists(), "key should have expired");
                })
            })
            .await;
        }

        // Tests for options require Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_nx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // Create key without expiry
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_nx".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let expire_at = future_timestamp_ms(300);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_nx".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "NX should set when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_nx_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // Create key with expiry
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_nx_exists".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let expire_at = future_timestamp_ms(300);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_nx_exists".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try NX - should fail since expiry already exists
                    let new_expire_at = future_timestamp_ms(600);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_nx_exists".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(new_expire_at),
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "NX should not set when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_xx_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_xx".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    let expire_at = future_timestamp_ms(300);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_xx".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // XX should succeed
                    let new_expire_at = future_timestamp_ms(600);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_xx".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(new_expire_at),
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "XX should set when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_xx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_xx_no".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // XX should fail - no existing expiry
                    let expire_at = future_timestamp_ms(300);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_xx_no".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "XX should not set when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_gt() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_gt".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry at 300s
                    let expire_at = future_timestamp_ms(300);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_gt".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GT with greater value should succeed
                    let greater_expire_at = future_timestamp_ms(600);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_gt".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(greater_expire_at),
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "GT should set when new expiry is greater");

                    // GT with lesser value should fail
                    let lesser_expire_at = future_timestamp_ms(100);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_gt".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(lesser_expire_at),
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "GT should not set when new expiry is lesser");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_lt() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpireat_lt".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry at 300s
                    let expire_at = future_timestamp_ms(300);
                    ctx.raw(
                        &PexpireatInput {
                            key: RedisKey::String("pexpireat_lt".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // LT with lesser value should succeed
                    let lesser_expire_at = future_timestamp_ms(100);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_lt".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(lesser_expire_at),
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "LT should set when new expiry is lesser");

                    // LT with greater value should fail
                    let greater_expire_at = future_timestamp_ms(600);
                    let result = ctx
                        .raw(
                            &PexpireatInput {
                                key: RedisKey::String("pexpireat_lt".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(greater_expire_at),
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "LT should not set when new expiry is greater");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_pexpireat1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_pexpireat2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    let expire_at = future_timestamp_ms(300);
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &PexpireatInput {
                            key: RedisKey::String("pipe_pexpireat1".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &PexpireatInput {
                            key: RedisKey::String("nonexistent".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &PexpireatInput {
                            key: RedisKey::String("pipe_pexpireat2".into()),
                            unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                            option: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = PexpireatOutput::decode(responses[0]).expect("decode 1");
                    assert!(out1.was_set());

                    let out2 = PexpireatOutput::decode(responses[1]).expect("decode 2");
                    assert!(!out2.was_set()); // nonexistent key

                    let out3 = PexpireatOutput::decode(responses[2]).expect("decode 3");
                    assert!(out3.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp2_pexpireat".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let expire_at = future_timestamp_ms(300);
            let result = ctx
                .raw(
                    &PexpireatInput {
                        key: RedisKey::String("resp2_pexpireat".into()),
                        unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = PexpireatOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpireat_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp3_pexpireat".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let expire_at = future_timestamp_ms(300);
            let result = ctx
                .raw(
                    &PexpireatInput {
                        key: RedisKey::String("resp3_pexpireat".into()),
                        unix_time_milliseconds: RedisJsonValue::Integer(expire_at),
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = PexpireatOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());
            ctx.stop().await;
        }
    }
}
