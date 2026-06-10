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

const API_INFO: ApiInfo<RedisApi, IncrbyfloatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Incrbyfloat,
    "Increment the string representing a floating point number stored at key by the specified increment. By using a negative increment value, the result is that the value stored at the key is decremented (by the obvious properties of addition). If the key does not exist, it is set to 0 before performing the operation. An error is returned if one of the following conditions occur: the key contains a value of the wrong type, the current value or increment is not parsable as a double precision floating point number",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `INCRBYFLOAT`
/// https://redis.io/docs/latest/commands/incrbyfloat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct IncrbyfloatInput {
    pub(crate) key: RedisKey,
    pub(crate) increment: RedisJsonValue,
}

impl Serialize for IncrbyfloatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("IncrbyfloatInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("increment", &self.increment)?;
        state.end()
    }
}

impl_redis_operation!(
    IncrbyfloatInput,
    API_INFO,
    {key, increment}
);

impl RedisCommandInput for IncrbyfloatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.increment);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("INCRBYFLOAT requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "INCRBYFLOAT expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, increment: args[1].clone() })
    }
}

/// Output for Redis INCRBYFLOAT command
///
/// Returns the value of key after the increment as a string (Redis returns bulk string).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct IncrbyfloatOutput {
    /// The value after incrementing (as string representation)
    value: String,
}

impl IncrbyfloatOutput {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    /// Get the incremented value as a string
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Get the incremented value as f64
    pub fn value_as_f64(&self) -> Option<f64> {
        self.value.parse().ok()
    }

    /// Decode the Redis protocol response into an IncrbyfloatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected INCRBYFLOAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected INCRBYFLOAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for IncrbyfloatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("IncrbyfloatOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = IncrbyfloatInput {
                key: RedisKey::String("mykey".into()),
                increment: RedisJsonValue::String("0.1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$11\r\nINCRBYFLOAT\r\n$5\r\nmykey\r\n$3\r\n0.1\r\n");
        }

        #[test]
        fn test_decode_positive_value() {
            let output = IncrbyfloatOutput::decode(b"$4\r\n10.5\r\n").unwrap();
            assert_eq!(output.value(), "10.5");
            assert_eq!(output.value_as_f64(), Some(10.5));
        }

        #[test]
        fn test_decode_zero_value() {
            let output = IncrbyfloatOutput::decode(b"$1\r\n0\r\n").unwrap();
            assert_eq!(output.value(), "0");
            assert_eq!(output.value_as_f64(), Some(0.0));
        }

        #[test]
        fn test_decode_negative_value() {
            let output = IncrbyfloatOutput::decode(b"$5\r\n-1.25\r\n").unwrap();
            assert_eq!(output.value(), "-1.25");
            assert_eq!(output.value_as_f64(), Some(-1.25));
        }

        #[test]
        fn test_decode_scientific_notation() {
            let output = IncrbyfloatOutput::decode(b"$12\r\n3.14159e+100\r\n").unwrap();
            assert_eq!(output.value(), "3.14159e+100");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = IncrbyfloatOutput::decode(b"-ERR value is not a valid float\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("0.5".into())];
            let input = IncrbyfloatInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.increment, RedisJsonValue::String("0.5".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = IncrbyfloatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = IncrbyfloatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = IncrbyfloatInput {
                key: RedisKey::String("mykey".into()),
                increment: RedisJsonValue::String("0.1".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
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
        async fn test_incrbyfloat_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // INCRBYFLOAT on non-existent key initializes to 0, then increments
                    let result = ctx
                        .raw(
                            &IncrbyfloatInput {
                                key: RedisKey::String("incrbyfloat_new".into()),
                                increment: RedisJsonValue::String("0.5".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyfloatOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value_as_f64(), Some(0.5));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrbyfloat_existing_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrbyfloat_exist".into()),
                            value: RedisJsonValue::String("10.5".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &IncrbyfloatInput {
                                key: RedisKey::String("incrbyfloat_exist".into()),
                                increment: RedisJsonValue::String("0.1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyfloatOutput::decode(&result).expect("decode failed");
                    let val = output.value_as_f64().unwrap();
                    assert!((val - 10.6).abs() < 0.0001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrbyfloat_negative_increment() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrbyfloat_neg".into()),
                            value: RedisJsonValue::String("5.0".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &IncrbyfloatInput {
                                key: RedisKey::String("incrbyfloat_neg".into()),
                                increment: RedisJsonValue::String("-2.5".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = IncrbyfloatOutput::decode(&result).expect("decode failed");
                    let val = output.value_as_f64().unwrap();
                    assert!((val - 2.5).abs() < 0.0001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrbyfloat_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("incrbyfloat_pipe".into()),
                            value: RedisJsonValue::String("0".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &IncrbyfloatInput {
                            key: RedisKey::String("incrbyfloat_pipe".into()),
                            increment: RedisJsonValue::String("1.1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &IncrbyfloatInput {
                            key: RedisKey::String("incrbyfloat_pipe".into()),
                            increment: RedisJsonValue::String("2.2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &IncrbyfloatInput {
                            key: RedisKey::String("incrbyfloat_pipe".into()),
                            increment: RedisJsonValue::String("3.3".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = IncrbyfloatOutput::decode(responses[0]).expect("decode first");
                    let val1 = out1.value_as_f64().unwrap();
                    assert!((val1 - 1.1).abs() < 0.0001);

                    let out2 = IncrbyfloatOutput::decode(responses[1]).expect("decode second");
                    let val2 = out2.value_as_f64().unwrap();
                    assert!((val2 - 3.3).abs() < 0.0001);

                    let out3 = IncrbyfloatOutput::decode(responses[2]).expect("decode third");
                    let val3 = out3.value_as_f64().unwrap();
                    assert!((val3 - 6.6).abs() < 0.0001);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrbyfloat_resp2_bulk_string_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("10.0".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &IncrbyfloatInput {
                        key: RedisKey::String("r2key".into()),
                        increment: RedisJsonValue::String("0.5".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            // RESP2 returns bulk string
            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = IncrbyfloatOutput::decode(&result).expect("decode failed");
            let val = output.value_as_f64().unwrap();
            assert!((val - 10.5).abs() < 0.0001);

            ctx.stop().await;
        }
    }
}
