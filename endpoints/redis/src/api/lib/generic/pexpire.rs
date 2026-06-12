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

const API_INFO: ApiInfo<RedisApi, PexpireInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pexpire,
    "Sets the expiration time of a key in milliseconds",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PEXPIRE`
/// https://redis.io/docs/latest/commands/pexpire/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PexpireInput {
    pub(crate) key: RedisKey,
    pub(crate) milliseconds: i64,
    #[builder(default)]
    pub(crate) option: Option<ExpireOption>,
}

impl Serialize for PexpireInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let field_count = if self.option.is_some() { 4 } else { 3 };
        let mut state = serializer.serialize_struct("PexpireInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("milliseconds", &self.milliseconds)?;
        if let Some(ref option) = self.option {
            state.serialize_field("option", option)?;
        }
        state.end()
    }
}

impl_redis_operation!(PexpireInput, API_INFO, { key, milliseconds, option });

impl RedisCommandInput for PexpireInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(self.milliseconds);

        if let Some(ref option) = self.option {
            command.arg(match option {
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
            return Err(EpError::parse(format!("PEXPIRE requires at least 2 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "PEXPIRE expects at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let milliseconds = match &args[1] {
            RedisJsonValue::Integer(i) => *i,
            RedisJsonValue::String(s) => s.parse::<i64>().map_err(|_| EpError::parse("PEXPIRE milliseconds must be a valid integer"))?,
            _ => return Err(EpError::parse("PEXPIRE milliseconds must be a number")),
        };

        let option = if args.len() >= 3 {
            match &args[2] {
                RedisJsonValue::String(s) => {
                    let opt = match s.to_uppercase().as_str() {
                        "NX" => ExpireOption::NX,
                        "XX" => ExpireOption::XX,
                        "GT" => ExpireOption::GT,
                        "LT" => ExpireOption::LT,
                        _ => return Err(EpError::parse(format!("PEXPIRE invalid option: {}", s))),
                    };
                    Some(opt)
                }
                _ => return Err(EpError::parse("PEXPIRE option must be a string")),
            }
        } else {
            None
        };

        Ok(Self { key: args[0].clone().try_into()?, milliseconds, option })
    }
}

/// Output for Redis PEXPIRE command
///
/// Returns 1 if the timeout was set, 0 if the key does not exist
/// or the timeout could not be set due to option constraints.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PexpireOutput {
    /// 1 if timeout was set, 0 otherwise
    result: i64,
}

impl PexpireOutput {
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

    /// Decode the Redis protocol response into a PexpireOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIRE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIRE response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for PexpireOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PexpireOutput", 1)?;
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
            let input = PexpireInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 10000,
                option: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nPEXPIRE\r\n$5\r\nmykey\r\n$5\r\n10000\r\n");
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input = PexpireInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 5000,
                option: Some(ExpireOption::NX),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$7\r\nPEXPIRE\r\n$5\r\nmykey\r\n$4\r\n5000\r\n$2\r\nNX\r\n");
        }

        #[test]
        fn test_encode_command_with_xx() {
            let input = PexpireInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 5000,
                option: Some(ExpireOption::XX),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nXX\r\n"));
        }

        #[test]
        fn test_encode_command_with_gt() {
            let input = PexpireInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 5000,
                option: Some(ExpireOption::GT),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nGT\r\n"));
        }

        #[test]
        fn test_encode_command_with_lt() {
            let input = PexpireInput {
                key: RedisKey::String("mykey".into()),
                milliseconds: 5000,
                option: Some(ExpireOption::LT),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$2\r\nLT\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = PexpireOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_set());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_failure() {
            let output = PexpireOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_set());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = PexpireOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(10000)];
            let input = PexpireInput::decode(args).unwrap();
            assert_eq!(input.milliseconds, 10000);
            assert!(input.option.is_none());
        }

        #[test]
        fn test_decode_input_with_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("NX".into()),
            ];
            let input = PexpireInput::decode(args).unwrap();
            assert!(matches!(input.option, Some(ExpireOption::NX)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = PexpireInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_option() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("INVALID".into()),
            ];
            let err = PexpireInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("invalid option"));
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
        async fn test_pexpire_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("missing".into()),
                                milliseconds: 10000,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "nonexistent key should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_test".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_test".into()),
                                milliseconds: 10000,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "existing key should return 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_zero_milliseconds() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_zero".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Setting 0ms should effectively delete the key
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_zero".into()),
                                milliseconds: 0,
                                option: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                })
            })
            .await;
        }

        // Options NX, XX, GT, LT require Redis 7.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_nx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_nx".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_nx".into()),
                                milliseconds: 10000,
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "NX should set when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_nx_with_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_nx2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // First set an expiry
                    ctx.raw(
                        &PexpireInput {
                            key: RedisKey::String("pexpire_nx2".into()),
                            milliseconds: 50000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // NX should fail when expiry already exists
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_nx2".into()),
                                milliseconds: 10000,
                                option: Some(ExpireOption::NX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "NX should not set when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_xx_with_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_xx".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // First set an expiry
                    ctx.raw(
                        &PexpireInput {
                            key: RedisKey::String("pexpire_xx".into()),
                            milliseconds: 50000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // XX should succeed when expiry exists
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_xx".into()),
                                milliseconds: 10000,
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "XX should set when expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_xx_no_existing_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_xx2".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // XX should fail when no expiry exists
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_xx2".into()),
                                milliseconds: 10000,
                                option: Some(ExpireOption::XX),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "XX should not set when no expiry exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_gt() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_gt".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &PexpireInput {
                            key: RedisKey::String("pexpire_gt".into()),
                            milliseconds: 10000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GT with greater value should succeed
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_gt".into()),
                                milliseconds: 50000,
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "GT should set when new expiry is greater");

                    // GT with smaller value should fail
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_gt".into()),
                                milliseconds: 1000,
                                option: Some(ExpireOption::GT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "GT should not set when new expiry is smaller");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpire_lt() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pexpire_lt".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set initial expiry
                    ctx.raw(
                        &PexpireInput {
                            key: RedisKey::String("pexpire_lt".into()),
                            milliseconds: 50000,
                            option: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // LT with smaller value should succeed
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_lt".into()),
                                milliseconds: 10000,
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "LT should set when new expiry is smaller");

                    // LT with greater value should fail
                    let result = ctx
                        .raw(
                            &PexpireInput {
                                key: RedisKey::String("pexpire_lt".into()),
                                milliseconds: 100000,
                                option: Some(ExpireOption::LT),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PexpireOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set(), "LT should not set when new expiry is greater");
                })
            })
            .await;
        }
    }
}
