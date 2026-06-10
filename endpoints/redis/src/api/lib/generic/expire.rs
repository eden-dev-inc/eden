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

const API_INFO: ApiInfo<RedisApi, ExpireInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Expire,
    "Sets the expiration time of a key in seconds. Returns 1 if the timeout was set, 0 if the key does not exist or the timeout could not be set.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `EXPIRE`
/// https://redis.io/docs/latest/commands/expire/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, Default)]
#[builder(default)]
pub struct ExpireInput {
    pub(crate) key: RedisKey,
    pub(crate) seconds: i64,
    pub(crate) option: Option<ExpireOption>,
}

impl Serialize for ExpireInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let field_count = if self.option.is_some() { 4 } else { 3 };

        let mut state = serializer.serialize_struct("ExpireInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("seconds", &self.seconds)?;
        if let Some(option) = &self.option {
            state.serialize_field("option", option)?;
        }
        state.end()
    }
}

impl_redis_operation!(ExpireInput, API_INFO, { key, seconds, option });

impl RedisCommandInput for ExpireInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(self.seconds);

        if let Some(option) = &self.option {
            command.arg(option.to_string());
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("EXPIRE requires at least 2 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "EXPIRE expects at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let seconds = match &args[1] {
            RedisJsonValue::Integer(i) => *i,
            RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("EXPIRE seconds must be a valid integer"))?,
            _ => return Err(EpError::parse("EXPIRE seconds must be a number or string")),
        };

        let option = if args.len() >= 3 {
            match &args[2] {
                RedisJsonValue::String(s) => Some(match s.to_uppercase().as_str() {
                    "NX" => ExpireOption::NX,
                    "XX" => ExpireOption::XX,
                    "GT" => ExpireOption::GT,
                    "LT" => ExpireOption::LT,
                    _ => {
                        return Err(EpError::parse(format!("EXPIRE invalid option '{}', expected NX, XX, GT, or LT", s)));
                    }
                }),
                _ => return Err(EpError::parse("EXPIRE option must be a string")),
            }
        } else {
            None
        };

        Ok(Self { key: args[0].clone().try_into()?, seconds, option })
    }
}

/// Output for Redis EXPIRE command
///
/// Returns whether the timeout was successfully set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ExpireOutput {
    /// 1 if the timeout was set, 0 if key doesn't exist or conditions not met
    result: i64,
}

impl ExpireOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Returns the raw result value (1 or 0)
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Returns true if the timeout was successfully set
    pub fn was_set(&self) -> bool {
        self.result == 1
    }

    /// Decode the Redis protocol response into an ExpireOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIRE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIRE response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ExpireOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ExpireOutput", 1)?;
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
            let input = ExpireInput {
                key: RedisKey::String("mykey".into()),
                seconds: 10,
                option: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n10\r\n");
        }

        #[test]
        fn test_encode_command_with_nx_option() {
            let input = ExpireInput {
                key: RedisKey::String("mykey".into()),
                seconds: 60,
                option: Some(ExpireOption::NX),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$2\r\n60\r\n$2\r\nNX\r\n");
        }

        #[test]
        fn test_encode_command_with_xx_option() {
            let input = ExpireInput {
                key: RedisKey::String("mykey".into()),
                seconds: 120,
                option: Some(ExpireOption::XX),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nEXPIRE\r\n$5\r\nmykey\r\n$3\r\n120\r\n$2\r\nXX\r\n");
        }

        #[test]
        fn test_encode_command_with_gt_option() {
            let input = ExpireInput {
                key: RedisKey::String("mykey".into()),
                seconds: 300,
                option: Some(ExpireOption::GT),
            };
            assert!(String::from_utf8_lossy(&input.command()).contains("GT"));
        }

        #[test]
        fn test_encode_command_with_lt_option() {
            let input = ExpireInput {
                key: RedisKey::String("mykey".into()),
                seconds: 5,
                option: Some(ExpireOption::LT),
            };
            assert!(String::from_utf8_lossy(&input.command()).contains("LT"));
        }

        #[test]
        fn test_decode_success() {
            let output = ExpireOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.result(), 1);
            assert!(output.was_set());
        }

        #[test]
        fn test_decode_failure() {
            let output = ExpireOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.result(), 0);
            assert!(!output.was_set());
        }

        #[test]
        fn test_decode_error_response() {
            let err = ExpireOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_option_display() {
            assert_eq!(ExpireOption::NX.to_string(), "NX");
            assert_eq!(ExpireOption::XX.to_string(), "XX");
            assert_eq!(ExpireOption::GT.to_string(), "GT");
            assert_eq!(ExpireOption::LT.to_string(), "LT");
        }

        #[test]
        fn test_default_input() {
            let input = ExpireInput::default();
            assert!(input.option.is_none());
            assert_eq!(input.seconds, 0);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("missing".into()),
                                seconds: 10,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.result(), 0);
                    assert!(!output.was_set(), "should fail on nonexistent key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("exp_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("exp_key".into()),
                                seconds: 100,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.result(), 1);
                    assert!(output.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_negative_seconds() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("neg_exp".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Negative TTL should delete the key
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("neg_exp".into()),
                                seconds: -1,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.result(), 1, "negative expire should succeed");
                })
            })
            .await;
        }

        // Options tests require Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_nx_option_no_existing_ttl() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("nx_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // NX should succeed since key has no TTL
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("nx_key".into()),
                                seconds: 100,
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "NX should succeed when no TTL exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_nx_option_existing_ttl() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("nx_key2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // First, set a TTL
                    ctx.raw(
                        &ExpireInput {
                            key: RedisKey::String("nx_key2".into()),
                            seconds: 1000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // NX should fail since key already has TTL
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("nx_key2".into()),
                                seconds: 100,
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "NX should fail when TTL already exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_xx_option() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("xx_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // XX should fail since key has no TTL
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("xx_key".into()),
                                seconds: 100,
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "XX should fail when no TTL exists");

                    // Set a TTL first
                    ctx.raw(
                        &ExpireInput {
                            key: RedisKey::String("xx_key".into()),
                            seconds: 1000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // XX should now succeed
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("xx_key".into()),
                                seconds: 500,
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "XX should succeed when TTL exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_gt_option() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("gt_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial TTL of 100
                    ctx.raw(
                        &ExpireInput {
                            key: RedisKey::String("gt_key".into()),
                            seconds: 100,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GT with smaller value should fail
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("gt_key".into()),
                                seconds: 50,
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "GT should fail when new TTL is smaller");

                    // GT with larger value should succeed
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("gt_key".into()),
                                seconds: 200,
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "GT should succeed when new TTL is larger");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_lt_option() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("lt_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial TTL of 100
                    ctx.raw(
                        &ExpireInput {
                            key: RedisKey::String("lt_key".into()),
                            seconds: 100,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // LT with larger value should fail
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("lt_key".into()),
                                seconds: 200,
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "LT should fail when new TTL is larger");

                    // LT with smaller value should succeed
                    let result = ctx
                        .raw(
                            &ExpireInput {
                                key: RedisKey::String("lt_key".into()),
                                seconds: 50,
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "LT should succeed when new TTL is smaller");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2exp".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &ExpireInput {
                        key: RedisKey::String("r2exp".into()),
                        seconds: 60,
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = ExpireOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expire_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3exp".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &ExpireInput {
                        key: RedisKey::String("r3exp".into()),
                        seconds: 60,
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = ExpireOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());
            ctx.stop().await;
        }
    }
}
