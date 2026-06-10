use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BitMode, BitcountRange, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, BitcountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bitcount,
    "Counts the number of set bits (population counting) in a string",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BITCOUNT`
/// https://redis.io/docs/latest/commands/bitcount/
///
/// Official example: `BITCOUNT mykey 5 30 BIT`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BitcountInput {
    pub(crate) key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) range: Option<BitcountRange>,
}

impl Serialize for BitcountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if let Some(range) = &self.range {
            fields += 2;
            if range.mode.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("BitcountInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(range) = &self.range {
            state.serialize_field("start", &range.start)?;
            state.serialize_field("end", &range.end)?;
            if let Some(mode) = &range.mode {
                state.serialize_field("mode", mode)?;
            }
        }
        state.end()
    }
}

impl_redis_operation!(
    BitcountInput,
    API_INFO,
    {key, range}
);

impl RedisCommandInput for BitcountInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(range) = &self.range {
            command.arg(&range.start).arg(&range.end);

            if let Some(mode) = &range.mode {
                match mode {
                    BitMode::BIT => command.arg("BIT"),
                    BitMode::BYTE => command.arg("BYTE"),
                };
            }
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("BITCOUNT requires at least 1 argument, given none"));
        }

        if args.len() > 4 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BITCOUNT takes at most 4 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let key = args[0].clone().try_into()?;

        let range = match args.len() {
            1 => None,
            2 => {
                let _ctx = ctx_with_trace!().with_feature("redis");
                log_warn!(_ctx, "BITCOUNT expects 1, 3, or 4 arguments, given 2", audience = LogAudience::Client);
                None
            }
            3 => Some(BitcountRange { start: args[1].clone(), end: args[2].clone(), mode: None }),
            _ => {
                let mode = match &args[3] {
                    RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                        "BYTE" => Some(BitMode::BYTE),
                        "BIT" => Some(BitMode::BIT),
                        _ => {
                            return Err(EpError::parse(format!("Expected BIT or BYTE, given {}", s)));
                        }
                    },
                    _ => return Err(EpError::parse("Mode must be a string")),
                };
                Some(BitcountRange { start: args[1].clone(), end: args[2].clone(), mode })
            }
        };

        Ok(Self { key, range })
    }
}

/// Output for Redis BITCOUNT command
///
/// Returns the count of set bits in the string.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BitcountOutput {
    /// The number of bits set to 1
    count: i64,
}

impl BitcountOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the bit count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Decode the Redis protocol response into a BitcountOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BITCOUNT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BITCOUNT response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for BitcountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BitcountOutput", 1)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_key_only() {
            let input = BitcountInput { key: RedisKey::String("mykey".into()), range: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nBITCOUNT\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_with_range() {
            let input = BitcountInput {
                key: RedisKey::String("mykey".into()),
                range: Some(BitcountRange {
                    start: RedisJsonValue::Integer(0),
                    end: RedisJsonValue::Integer(-1),
                    mode: None,
                }),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$8\r\nBITCOUNT\r\n"));
        }

        #[test]
        fn test_encode_command_with_bit_mode() {
            let input = BitcountInput {
                key: RedisKey::String("mykey".into()),
                range: Some(BitcountRange {
                    start: RedisJsonValue::Integer(5),
                    end: RedisJsonValue::Integer(30),
                    mode: Some(BitMode::BIT),
                }),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$3\r\nBIT\r\n"));
        }

        #[test]
        fn test_decode_count() {
            let output = BitcountOutput::decode(b":26\r\n").unwrap();
            assert_eq!(output.count(), 26);
        }

        #[test]
        fn test_decode_zero_count() {
            let output = BitcountOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BitcountOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = BitcountInput::decode(args).unwrap();
            assert!(input.range.is_none());
        }

        #[test]
        fn test_decode_input_with_range() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            ];
            let input = BitcountInput::decode(args).unwrap();
            assert!(input.range.is_some());
        }

        #[test]
        fn test_decode_input_with_mode() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
                RedisJsonValue::String("BIT".into()),
            ];
            let input = BitcountInput::decode(args).unwrap();
            let range = input.range.unwrap();
            assert!(matches!(range.mode, Some(BitMode::BIT)));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = BitcountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BitcountInput { key: RedisKey::String("testkey".into()), range: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bitmap::setbit::SetbitInput;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_empty_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&BitcountInput { key: RedisKey::String("missing_key".into()), range: None }.command())
                        .await
                        .expect("raw failed");

                    let output = BitcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_string_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set string "foobar" and count bits
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitcount_str".into()),
                            value: RedisJsonValue::String("foobar".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&BitcountInput { key: RedisKey::String("bitcount_str".into()), range: None }.command())
                        .await
                        .expect("raw failed");

                    let output = BitcountOutput::decode(&result).expect("decode failed");
                    // "foobar" has 26 bits set
                    assert_eq!(output.count(), 26);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_with_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitcount_range".into()),
                            value: RedisJsonValue::String("foobar".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Count bits in first byte only
                    let result = ctx
                        .raw(
                            &BitcountInput {
                                key: RedisKey::String("bitcount_range".into()),
                                range: Some(BitcountRange {
                                    start: RedisJsonValue::Integer(0),
                                    end: RedisJsonValue::Integer(0),
                                    mode: None,
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitcountOutput::decode(&result).expect("decode failed");
                    // 'f' = 0x66 = 01100110 = 4 bits
                    assert_eq!(output.count(), 4);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_bit_mode() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitcount_bitmode".into()),
                            value: RedisJsonValue::String("foobar".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Count bits in bit range
                    let result = ctx
                        .raw(
                            &BitcountInput {
                                key: RedisKey::String("bitcount_bitmode".into()),
                                range: Some(BitcountRange {
                                    start: RedisJsonValue::Integer(5),
                                    end: RedisJsonValue::Integer(30),
                                    mode: Some(BitMode::BIT),
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitcountOutput::decode(&result).expect("decode failed");
                    assert!(output.count() > 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_after_setbit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set specific bits
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitcount_setbit".into()),
                            offset: RedisJsonValue::Integer(0),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitcount_setbit".into()),
                            offset: RedisJsonValue::Integer(7),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitcount_setbit".into()),
                            offset: RedisJsonValue::Integer(15),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&BitcountInput { key: RedisKey::String("bitcount_setbit".into()), range: None }.command())
                        .await
                        .expect("raw failed");

                    let output = BitcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("pipe_bc1".into()),
                            value: RedisJsonValue::String("foo".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("pipe_bc2".into()),
                            value: RedisJsonValue::String("bar".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&BitcountInput { key: RedisKey::String("pipe_bc1".into()), range: None }.command());
                    pipeline.extend_from_slice(&BitcountInput { key: RedisKey::String("pipe_bc2".into()), range: None }.command());
                    pipeline.extend_from_slice(&BitcountInput { key: RedisKey::String("missing".into()), range: None }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = BitcountOutput::decode(responses[0]).expect("decode 1");
                    // "foo" = 16 bits
                    assert_eq!(out1.count(), 16);

                    let out2 = BitcountOutput::decode(responses[1]).expect("decode 2");
                    // "bar" = 10 bits
                    assert_eq!(out2.count(), 10);

                    let out3 = BitcountOutput::decode(responses[2]).expect("decode 3");
                    assert_eq!(out3.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp2bc".into()),
                    value: RedisJsonValue::String("a".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result =
                ctx.raw(&BitcountInput { key: RedisKey::String("resp2bc".into()), range: None }.command()).await.expect("raw failed");

            // 'a' = 0x61 = 01100001 = 3 bits
            assert_eq!(&result[..], b":3\r\n");
            let output = BitcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 3);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitcount_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp3bc".into()),
                    value: RedisJsonValue::String("a".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result =
                ctx.raw(&BitcountInput { key: RedisKey::String("resp3bc".into()), range: None }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":3\r\n");
            let output = BitcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 3);
            ctx.stop().await;
        }
    }
}
