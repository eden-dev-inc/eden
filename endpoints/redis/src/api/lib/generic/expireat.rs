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

const API_INFO: ApiInfo<RedisApi, ExpireatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Expireat,
    "Set the expiration for a key as a UNIX timestamp. Returns 1 if the timeout was set, 0 if the key does not exist or the timeout could not be set due to the specified option.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `EXPIREAT`
/// https://redis.io/docs/latest/commands/expireat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ExpireatInput {
    pub(crate) key: RedisKey,
    pub(crate) unix_time_seconds: i64,
    #[builder(default)]
    pub(crate) option: Option<ExpireOption>,
}

impl Serialize for ExpireatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let field_count = if self.option.is_some() { 4 } else { 3 };
        let mut state = serializer.serialize_struct("ExpireatInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("unix_time_seconds", &self.unix_time_seconds)?;
        if let Some(opt) = &self.option {
            state.serialize_field("option", opt)?;
        }
        state.end()
    }
}

impl_redis_operation!(ExpireatInput, API_INFO, { key, unix_time_seconds, option });

impl RedisCommandInput for ExpireatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(self.unix_time_seconds);

        if let Some(opt) = &self.option {
            command.arg(match opt {
                ExpireOption::NX => "NX",
                ExpireOption::XX => "XX",
                ExpireOption::GT => "GT",
                ExpireOption::LT => "LT",
            });
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("EXPIREAT requires at least 2 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "EXPIREAT expects at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let unix_time_seconds = match &args[1] {
            RedisJsonValue::Integer(i) => *i,
            RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("unix_time_seconds must be a valid integer"))?,
            _ => {
                return Err(EpError::parse("unix_time_seconds must be a number or numeric string"));
            }
        };

        let option = if args.len() >= 3 {
            match &args[2] {
                RedisJsonValue::String(s) => Some(match s.to_uppercase().as_str() {
                    "NX" => ExpireOption::NX,
                    "XX" => ExpireOption::XX,
                    "GT" => ExpireOption::GT,
                    "LT" => ExpireOption::LT,
                    other => {
                        return Err(EpError::parse(format!("EXPIREAT invalid option '{}', expected NX, XX, GT, or LT", other)));
                    }
                }),
                _ => return Err(EpError::parse("EXPIREAT option must be a string")),
            }
        } else {
            None
        };

        Ok(Self { key: args[0].clone().try_into()?, unix_time_seconds, option })
    }
}

/// Output for Redis EXPIREAT command
///
/// Returns 1 if the timeout was set, 0 if key does not exist or timeout could not be set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ExpireatOutput {
    /// 1 if timeout was set, 0 otherwise
    result: i64,
}

impl ExpireatOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Returns true if the timeout was successfully set
    pub fn was_set(&self) -> bool {
        self.result == 1
    }

    /// Returns the raw result value (1 or 0)
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Decode the Redis protocol response into ExpireatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIREAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIREAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for ExpireatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ExpireatOutput", 1)?;
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
            let input = ExpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_seconds: 1609459200,
                option: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nEXPIREAT\r\n$5\r\nmykey\r\n$10\r\n1609459200\r\n");
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = ExpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_seconds: 1609459200,
                option: Some(ExpireOption::NX),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            assert!(cmd.windows(4).any(|w| w == b"$2\r\n"));
            assert!(cmd.windows(4).any(|w| w == b"NX\r\n"));
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = ExpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_seconds: 1609459200,
                option: Some(ExpireOption::XX),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"XX\r\n"));
        }

        #[test]
        fn test_encode_command_with_gt() {
            let input = ExpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_seconds: 1609459200,
                option: Some(ExpireOption::GT),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"GT\r\n"));
        }

        #[test]
        fn test_encode_command_with_lt() {
            let input = ExpireatInput {
                key: RedisKey::String("mykey".into()),
                unix_time_seconds: 1609459200,
                option: Some(ExpireOption::LT),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"LT\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = ExpireatOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_set());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_failure() {
            let output = ExpireatOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_set());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = ExpireatOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_input_decode_basic() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(1609459200.into())];
            let input = ExpireatInput::decode(args).unwrap();
            assert_eq!(input.unix_time_seconds, 1609459200);
            assert!(input.option.is_none());
        }

        #[test]
        fn test_input_decode_with_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1609459200.into()),
                RedisJsonValue::String("NX".into()),
            ];
            let input = ExpireatInput::decode(args).unwrap();
            assert_eq!(input.option, Some(ExpireOption::NX));
        }

        #[test]
        fn test_input_decode_timestamp_as_string() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("1609459200".into())];
            let input = ExpireatInput::decode(args).unwrap();
            assert_eq!(input.unix_time_seconds, 1609459200);
        }

        #[test]
        fn test_input_decode_insufficient_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = ExpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_input_decode_invalid_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1609459200.into()),
                RedisJsonValue::String("INVALID".into()),
            ];
            let err = ExpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("invalid option"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn future_timestamp(seconds_from_now: u64) -> i64 {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as i64 + seconds_from_now as i64
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("missing".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "nonexistent key should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("expkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("expkey".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "existing key should return 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_past_timestamp_deletes_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pastkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set expiration in the past
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("pastkey".into()),
                                unix_time_seconds: 1, // 1970
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());

                    // Key should be deleted
                    let get_result = ctx
                        .raw(&crate::api::lib::string::get::GetInput { key: RedisKey::String("pastkey".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = crate::api::lib::string::get::GetOutput::decode(&get_result).unwrap();
                    assert!(!get_output.exists(), "key should be deleted after past expiration");
                })
            })
            .await;
        }

        // Options tests require Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_nx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("nxkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("nxkey".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "NX should succeed when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_nx_with_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("nxkey2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &ExpireatInput {
                            key: RedisKey::String("nxkey2".into()),
                            unix_time_seconds: future_timestamp(3600),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try NX - should fail
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("nxkey2".into()),
                                unix_time_seconds: future_timestamp(7200),
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "NX should fail when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_xx_with_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("xxkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &ExpireatInput {
                            key: RedisKey::String("xxkey".into()),
                            unix_time_seconds: future_timestamp(3600),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // XX should succeed
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("xxkey".into()),
                                unix_time_seconds: future_timestamp(7200),
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "XX should succeed when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_xx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("xxkey2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("xxkey2".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "XX should fail when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_gt_greater_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("gtkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &ExpireatInput {
                            key: RedisKey::String("gtkey".into()),
                            unix_time_seconds: future_timestamp(3600),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GT with greater value should succeed
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("gtkey".into()),
                                unix_time_seconds: future_timestamp(7200),
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "GT should succeed with greater expiry");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_gt_lesser_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("gtkey2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry far in future
                    ctx.raw(
                        &ExpireatInput {
                            key: RedisKey::String("gtkey2".into()),
                            unix_time_seconds: future_timestamp(7200),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GT with lesser value should fail
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("gtkey2".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "GT should fail with lesser expiry");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_lt_lesser_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("ltkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &ExpireatInput {
                            key: RedisKey::String("ltkey".into()),
                            unix_time_seconds: future_timestamp(7200),
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // LT with lesser value should succeed
                    let result = ctx
                        .raw(
                            &ExpireatInput {
                                key: RedisKey::String("ltkey".into()),
                                unix_time_seconds: future_timestamp(3600),
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExpireatOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "LT should succeed with lesser expiry");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pkey1".into()),
                        value: RedisJsonValue::String("val1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("pkey2".into()),
                        value: RedisJsonValue::String("val2".into()),
                        ..Default::default()
                    })
                    .await;

                    let ts = future_timestamp(3600);
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &ExpireatInput {
                            key: RedisKey::String("pkey1".into()),
                            unix_time_seconds: ts,
                            option: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &ExpireatInput {
                            key: RedisKey::String("missing".into()),
                            unix_time_seconds: ts,
                            option: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &ExpireatInput {
                            key: RedisKey::String("pkey2".into()),
                            unix_time_seconds: ts,
                            option: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = ExpireatOutput::decode(responses[0]).expect("decode pkey1");
                    assert!(out1.was_set());

                    let out2 = ExpireatOutput::decode(responses[1]).expect("decode missing");
                    assert!(!out2.was_set());

                    let out3 = ExpireatOutput::decode(responses[2]).expect("decode pkey2");
                    assert!(out3.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2key".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &ExpireatInput {
                        key: RedisKey::String("r2key".into()),
                        unix_time_seconds: future_timestamp(3600),
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expireat_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3key".into()),
                value: RedisJsonValue::String("value".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &ExpireatInput {
                        key: RedisKey::String("r3key".into()),
                        unix_time_seconds: future_timestamp(3600),
                        option: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            ctx.stop().await;
        }
    }
}
