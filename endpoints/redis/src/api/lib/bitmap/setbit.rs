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

const API_INFO: ApiInfo<RedisApi, SetbitInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Setbit,
    "Sets or clears the bit at offset in the string value stored at key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SETBIT`
/// https://redis.io/docs/latest/commands/setbit/
///
/// Official example: `SETBIT mykey 7 1`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SetbitInput {
    pub(crate) key: RedisKey,
    pub(crate) offset: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Serialize for SetbitInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SetbitInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("offset", &self.offset)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    SetbitInput,
    API_INFO,
    {key, offset, value}
);

impl RedisCommandInput for SetbitInput {
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
            return Err(EpError::parse(format!("SETBIT requires 3 arguments, given {}", args.len())));
        } else if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SETBIT takes 3 arguments, but given {}",
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

/// Output for Redis SETBIT command
///
/// Returns the original bit value stored at offset before it was set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SetbitOutput {
    /// The original bit value at the offset (0 or 1)
    original_bit: i64,
}

impl SetbitOutput {
    pub fn new(original_bit: i64) -> Self {
        Self { original_bit }
    }

    /// Get the original bit value before the set operation
    pub fn original_bit(&self) -> i64 {
        self.original_bit
    }

    /// Check if the original bit was set (1)
    pub fn was_set(&self) -> bool {
        self.original_bit == 1
    }

    /// Decode the Redis protocol response into a SetbitOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let original_bit = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SETBIT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SETBIT response: {:?}", other)));
                }
            },
        };

        Ok(Self { original_bit })
    }
}

impl Serialize for SetbitOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SetbitOutput", 1)?;
        state.serialize_field("original_bit", &self.original_bit)?;
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
            let input = SetbitInput {
                key: RedisKey::String("mykey".into()),
                offset: RedisJsonValue::Integer(7),
                value: RedisJsonValue::Integer(1),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nSETBIT\r\n$5\r\nmykey\r\n$1\r\n7\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_encode_command_zero_value() {
            let input = SetbitInput {
                key: RedisKey::String("k".into()),
                offset: RedisJsonValue::Integer(0),
                value: RedisJsonValue::Integer(0),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$6\r\nSETBIT\r\n"));
        }

        #[test]
        fn test_decode_original_zero() {
            let output = SetbitOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.original_bit(), 0);
            assert!(!output.was_set());
        }

        #[test]
        fn test_decode_original_one() {
            let output = SetbitOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.original_bit(), 1);
            assert!(output.was_set());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SetbitOutput::decode(b"-ERR bit is not an integer\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(7),
                RedisJsonValue::Integer(1),
            ];
            let input = SetbitInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(7)];
            let err = SetbitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SetbitInput {
                key: RedisKey::String("testkey".into()),
                offset: RedisJsonValue::Integer(0),
                value: RedisJsonValue::Integer(1),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bitmap::getbit::{GetbitInput, GetbitOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetbitInput {
                                key: RedisKey::String("newbitkey".into()),
                                offset: RedisJsonValue::Integer(7),
                                value: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.original_bit(), 0, "new key should have 0 at offset");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_toggle() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First set: 0 -> 1
                    let result1 = ctx
                        .raw(
                            &SetbitInput {
                                key: RedisKey::String("togglekey".into()),
                                offset: RedisJsonValue::Integer(7),
                                value: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output1 = SetbitOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.original_bit(), 0);

                    // Second set: 1 -> 0
                    let result2 = ctx
                        .raw(
                            &SetbitInput {
                                key: RedisKey::String("togglekey".into()),
                                offset: RedisJsonValue::Integer(7),
                                value: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output2 = SetbitOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.original_bit(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_same_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set bit to 1
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("samekey".into()),
                            offset: RedisJsonValue::Integer(5),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set same bit to 1 again
                    let result = ctx
                        .raw(
                            &SetbitInput {
                                key: RedisKey::String("samekey".into()),
                                offset: RedisJsonValue::Integer(5),
                                value: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.original_bit(), 1, "should return previous value 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_large_offset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetbitInput {
                                key: RedisKey::String("largeoffset".into()),
                                offset: RedisJsonValue::Integer(100000),
                                value: RedisJsonValue::Integer(1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.original_bit(), 0);

                    // Verify it was set
                    let get_result = ctx
                        .raw(
                            &GetbitInput {
                                key: RedisKey::String("largeoffset".into()),
                                offset: RedisJsonValue::Integer(100000),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let get_output = GetbitOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.bit(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetbitInput {
                            key: RedisKey::String("pipebitkey".into()),
                            offset: RedisJsonValue::Integer(0),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetbitInput {
                            key: RedisKey::String("pipebitkey".into()),
                            offset: RedisJsonValue::Integer(7),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetbitInput {
                            key: RedisKey::String("pipebitkey".into()),
                            offset: RedisJsonValue::Integer(0),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetbitInput {
                            key: RedisKey::String("pipebitkey".into()),
                            offset: RedisJsonValue::Integer(7),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 4);

                    // SETBIT returns original values (0)
                    let set1 = SetbitOutput::decode(responses[0]).expect("decode set1");
                    assert_eq!(set1.original_bit(), 0);
                    let set2 = SetbitOutput::decode(responses[1]).expect("decode set2");
                    assert_eq!(set2.original_bit(), 0);

                    // GETBIT returns current values (1)
                    let get1 = GetbitOutput::decode(responses[2]).expect("decode get1");
                    assert_eq!(get1.bit(), 1);
                    let get2 = GetbitOutput::decode(responses[3]).expect("decode get2");
                    assert_eq!(get2.bit(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx
                .raw(
                    &SetbitInput {
                        key: RedisKey::String("resp2bitkey".into()),
                        offset: RedisJsonValue::Integer(0),
                        value: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":0\r\n", "RESP2 integer format");
            let output = SetbitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.original_bit(), 0);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setbit_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx
                .raw(
                    &SetbitInput {
                        key: RedisKey::String("resp3bitkey".into()),
                        offset: RedisJsonValue::Integer(0),
                        value: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":0\r\n", "RESP3 integer format");
            let output = SetbitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.original_bit(), 0);
            ctx.stop().await;
        }
    }
}
