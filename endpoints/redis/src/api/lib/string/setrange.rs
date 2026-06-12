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

const API_INFO: ApiInfo<RedisApi, SetrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Setrange,
    "Overwrites a part of a string value with another by an offset. Creates the key if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SETRANGE`
/// https://redis.io/docs/latest/commands/setrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SetrangeInput {
    pub(crate) key: RedisKey,
    pub(crate) offset: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Serialize for SetrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SetrangeInput", 4)?;

        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("offset", &self.offset)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    SetrangeInput,
    API_INFO,
    {key, offset, value}
);

impl RedisCommandInput for SetrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.offset).arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("SETRANGE requires 3 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SETRANGE expects 3 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            offset: args[1].clone(),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis SETRANGE command
///
/// Returns the length of the string after the modification.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SetrangeOutput {
    /// The length of the string after modification
    length: i64,
}

impl SetrangeOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the resulting string length
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Decode the Redis protocol response into a SetrangeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SETRANGE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SETRANGE response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for SetrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SetrangeOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = SetrangeInput {
                key: RedisKey::String("mykey".into()),
                offset: RedisJsonValue::Integer(6),
                value: RedisJsonValue::String("Redis".into()),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$8\r\nSETRANGE\r\n$5\r\nmykey\r\n$1\r\n6\r\n$5\r\nRedis\r\n");
        }

        #[test]
        fn test_encode_command_zero_offset() {
            let input = SetrangeInput {
                key: RedisKey::String("key".into()),
                offset: RedisJsonValue::Integer(0),
                value: RedisJsonValue::String("hello".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$8\r\nSETRANGE\r\n"));
        }

        #[test]
        fn test_decode_length() {
            let output = SetrangeOutput::decode(b":11\r\n").unwrap();
            assert_eq!(output.length(), 11);
        }

        #[test]
        fn test_decode_zero_length() {
            let output = SetrangeOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SetrangeOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("value".into()),
            ];
            let input = SetrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.offset, RedisJsonValue::Integer(5));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(5)];
            let err = SetrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SetrangeInput {
                key: RedisKey::String("mykey".into()),
                offset: RedisJsonValue::Integer(0),
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
        use crate::api::SetInput;
        use crate::api::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sr_key".into()),
                            value: RedisJsonValue::String("Hello World".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // SETRANGE to replace "World" with "Redis"
                    let result = ctx
                        .raw(
                            &SetrangeInput {
                                key: RedisKey::String("sr_key".into()),
                                offset: RedisJsonValue::Integer(6),
                                value: RedisJsonValue::String("Redis".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 11); // "Hello Redis"

                    // Verify the change
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("sr_key".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("Hello Redis")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SETRANGE on non-existent key pads with zero bytes
                    let result = ctx
                        .raw(
                            &SetrangeInput {
                                key: RedisKey::String("sr_new".into()),
                                offset: RedisJsonValue::Integer(5),
                                value: RedisJsonValue::String("Hello".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 10); // 5 null bytes + "Hello"
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_extend() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set short value
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sr_extend".into()),
                            value: RedisJsonValue::String("Hi".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Extend beyond current length
                    let result = ctx
                        .raw(
                            &SetrangeInput {
                                key: RedisKey::String("sr_extend".into()),
                                offset: RedisJsonValue::Integer(5),
                                value: RedisJsonValue::String("World".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 10);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_offset_zero() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sr_zero".into()),
                            value: RedisJsonValue::String("World".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SetrangeInput {
                                key: RedisKey::String("sr_zero".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::String("Hello".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 5);

                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("sr_zero".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("Hello")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sr_pipe".into()),
                            value: RedisJsonValue::String("AAAAAAAAAA".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetrangeInput {
                            key: RedisKey::String("sr_pipe".into()),
                            offset: RedisJsonValue::Integer(0),
                            value: RedisJsonValue::String("BB".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetrangeInput {
                            key: RedisKey::String("sr_pipe".into()),
                            offset: RedisJsonValue::Integer(4),
                            value: RedisJsonValue::String("CC".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("sr_pipe".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = SetrangeOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.length(), 10);

                    let out2 = SetrangeOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.length(), 10);

                    let get_out = GetOutput::decode(responses[2]).expect("decode get");
                    assert_eq!(get_out.value(), Some(&RedisJsonValue::from("BBAACCAAAA")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setrange_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("test".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &SetrangeInput {
                        key: RedisKey::String("r2key".into()),
                        offset: RedisJsonValue::Integer(0),
                        value: RedisJsonValue::String("TEST".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = SetrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 4);

            ctx.stop().await;
        }
    }
}
