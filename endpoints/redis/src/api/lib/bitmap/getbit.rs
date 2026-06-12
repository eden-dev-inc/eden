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

const API_INFO: ApiInfo<RedisApi, GetbitInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Getbit,
    "Returns the bit value at offset in the string value stored at key",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GETBIT`
/// https://redis.io/docs/latest/commands/getbit/
///
/// Official example: `GETBIT mykey 7`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetbitInput {
    pub(crate) key: RedisKey,
    pub(crate) offset: RedisJsonValue,
}

impl Serialize for GetbitInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GetbitInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("offset", &self.offset)?;
        state.end()
    }
}

impl_redis_operation!(
    GetbitInput,
    API_INFO,
    {key, offset}
);

impl RedisCommandInput for GetbitInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.offset);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("GETBIT requires 2 arguments, given {}", args.len())));
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GETBIT takes 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, offset: args[1].clone() })
    }
}

/// Output for Redis GETBIT command
///
/// Returns the bit value (0 or 1) at the specified offset.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetbitOutput {
    /// The bit value at the offset (0 or 1)
    bit: i64,
}

impl GetbitOutput {
    pub fn new(bit: i64) -> Self {
        Self { bit }
    }

    /// Get the bit value
    pub fn bit(&self) -> i64 {
        self.bit
    }

    /// Check if the bit is set (1)
    pub fn is_set(&self) -> bool {
        self.bit == 1
    }

    /// Decode the Redis protocol response into a GetbitOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let bit = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GETBIT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected GETBIT response: {:?}", other)));
                }
            },
        };

        Ok(Self { bit })
    }
}

impl Serialize for GetbitOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GetbitOutput", 1)?;
        state.serialize_field("bit", &self.bit)?;
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
            let input = GetbitInput {
                key: RedisKey::String("mykey".into()),
                offset: RedisJsonValue::Integer(7),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nGETBIT\r\n$5\r\nmykey\r\n$1\r\n7\r\n");
        }

        #[test]
        fn test_encode_command_large_offset() {
            let input = GetbitInput {
                key: RedisKey::String("k".into()),
                offset: RedisJsonValue::Integer(10000),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nGETBIT\r\n"));
        }

        #[test]
        fn test_decode_bit_zero() {
            let output = GetbitOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.bit(), 0);
            assert!(!output.is_set());
        }

        #[test]
        fn test_decode_bit_one() {
            let output = GetbitOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.bit(), 1);
            assert!(output.is_set());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetbitOutput::decode(b"-ERR bit is not an integer\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(100)];
            let input = GetbitInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = GetbitInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GetbitInput {
                key: RedisKey::String("testkey".into()),
                offset: RedisJsonValue::Integer(0),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bitmap::setbit::SetbitInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GetbitInput {
                                key: RedisKey::String("missing_bit_key".into()),
                                offset: RedisJsonValue::Integer(7),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.bit(), 0, "nonexistent key should return 0");
                    assert!(!output.is_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_after_setbit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set bit at offset 7
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitkey".into()),
                            offset: RedisJsonValue::Integer(7),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Get the same bit
                    let result = ctx
                        .raw(
                            &GetbitInput {
                                key: RedisKey::String("bitkey".into()),
                                offset: RedisJsonValue::Integer(7),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.bit(), 1);
                    assert!(output.is_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_unset_offset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set bit at offset 7
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitkey2".into()),
                            offset: RedisJsonValue::Integer(7),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Get bit at different offset (should be 0)
                    let result = ctx
                        .raw(
                            &GetbitInput {
                                key: RedisKey::String("bitkey2".into()),
                                offset: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.bit(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_large_offset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Get bit at large offset on nonexistent key
                    let result = ctx
                        .raw(
                            &GetbitInput {
                                key: RedisKey::String("large_offset_key".into()),
                                offset: RedisJsonValue::Integer(100000),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetbitOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.bit(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set multiple bits
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("pipekey".into()),
                            offset: RedisJsonValue::Integer(0),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("pipekey".into()),
                            offset: RedisJsonValue::Integer(7),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Pipeline multiple GETBITs
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &GetbitInput {
                            key: RedisKey::String("pipekey".into()),
                            offset: RedisJsonValue::Integer(0),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetbitInput {
                            key: RedisKey::String("pipekey".into()),
                            offset: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetbitInput {
                            key: RedisKey::String("pipekey".into()),
                            offset: RedisJsonValue::Integer(7),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out0 = GetbitOutput::decode(responses[0]).expect("decode 0");
                    assert_eq!(out0.bit(), 1);

                    let out1 = GetbitOutput::decode(responses[1]).expect("decode 1");
                    assert_eq!(out1.bit(), 0);

                    let out7 = GetbitOutput::decode(responses[2]).expect("decode 7");
                    assert_eq!(out7.bit(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx
                .raw(
                    &GetbitInput {
                        key: RedisKey::String("resp2key".into()),
                        offset: RedisJsonValue::Integer(0),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":0\r\n", "RESP2 integer format");
            let output = GetbitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.bit(), 0);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getbit_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx
                .raw(
                    &GetbitInput {
                        key: RedisKey::String("resp3key".into()),
                        offset: RedisJsonValue::Integer(0),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":0\r\n", "RESP3 integer format");
            let output = GetbitOutput::decode(&result).expect("decode failed");
            assert_eq!(output.bit(), 0);
            ctx.stop().await;
        }
    }
}
