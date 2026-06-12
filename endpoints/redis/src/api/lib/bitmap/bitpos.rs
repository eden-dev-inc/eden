use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BitMode, BitposRange, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, BitposInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bitpos,
    "Finds the first bit set to 1 or 0 in a string",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BITPOS`
/// https://redis.io/docs/latest/commands/bitpos/
///
/// Official example: `BITPOS mykey 1 2 -1 BYTE`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BitposInput {
    pub(crate) key: RedisKey,
    pub(crate) bit: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) range: Option<BitposRange>,
}

impl Serialize for BitposInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if let Some(range) = &self.range {
            fields += 1;
            if range.end.is_some() {
                fields += 1;
            }
            if range.mode.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("BitposInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("bit", &self.bit)?;
        if let Some(range) = &self.range {
            state.serialize_field("start", &range.start)?;
            if let Some(end) = &range.end {
                state.serialize_field("end", end)?;
            }
            if let Some(mode) = &range.mode {
                state.serialize_field("mode", mode)?;
            }
        }
        state.end()
    }
}

impl_redis_operation!(
    BitposInput,
    API_INFO,
    {key, bit, range}
);

impl RedisCommandInput for BitposInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.bit);

        if let Some(range) = &self.range {
            command.arg(&range.start);

            if let Some(end) = &range.end {
                command.arg(end);

                if let Some(mode) = &range.mode {
                    match mode {
                        BitMode::BYTE => command.arg("BYTE"),
                        BitMode::BIT => command.arg("BIT"),
                    };
                }
            }
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse("BITPOS requires at least key and bit"));
        }

        if args.len() > 5 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BITPOS takes at most 5 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let key = args[0].clone().try_into()?;
        let bit = args[1].clone();

        let range = match args.len() {
            2 => None,
            3 => Some(BitposRange { start: args[2].clone(), end: None, mode: None }),
            4 => Some(BitposRange {
                start: args[2].clone(),
                end: Some(args[3].clone()),
                mode: None,
            }),
            _ => {
                let mode = match &args[4] {
                    RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                        "BYTE" => Some(BitMode::BYTE),
                        "BIT" => Some(BitMode::BIT),
                        _ => {
                            return Err(EpError::parse(format!("Expected BYTE or BIT, got {}", s)));
                        }
                    },
                    _ => return Err(EpError::parse("Mode must be a string")),
                };
                Some(BitposRange { start: args[2].clone(), end: Some(args[3].clone()), mode })
            }
        };

        Ok(BitposInput { key, bit, range })
    }
}

/// Output for Redis BITPOS command
///
/// Returns the position of the first bit set to 0 or 1.
/// Returns -1 if the bit is not found and the string is not empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BitposOutput {
    /// The position of the first bit, or -1 if not found
    position: i64,
}

impl BitposOutput {
    pub fn new(position: i64) -> Self {
        Self { position }
    }

    /// Get the bit position
    pub fn position(&self) -> i64 {
        self.position
    }

    /// Check if the bit was found
    pub fn found(&self) -> bool {
        self.position >= 0
    }

    /// Decode the Redis protocol response into a BitposOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let position = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BITPOS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BITPOS response: {:?}", other)));
                }
            },
        };

        Ok(Self { position })
    }
}

impl Serialize for BitposOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BitposOutput", 1)?;
        state.serialize_field("position", &self.position)?;
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
            let input = BitposInput {
                key: RedisKey::String("mykey".into()),
                bit: RedisJsonValue::Integer(1),
                range: None,
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nBITPOS\r\n$5\r\nmykey\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_encode_command_with_start() {
            let input = BitposInput {
                key: RedisKey::String("mykey".into()),
                bit: RedisJsonValue::Integer(1),
                range: Some(BitposRange { start: RedisJsonValue::Integer(0), end: None, mode: None }),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$6\r\nBITPOS\r\n"));
        }

        #[test]
        fn test_encode_command_with_range() {
            let input = BitposInput {
                key: RedisKey::String("mykey".into()),
                bit: RedisJsonValue::Integer(0),
                range: Some(BitposRange {
                    start: RedisJsonValue::Integer(2),
                    end: Some(RedisJsonValue::Integer(-1)),
                    mode: None,
                }),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$6\r\nBITPOS\r\n"));
        }

        #[test]
        fn test_encode_command_with_bit_mode() {
            let input = BitposInput {
                key: RedisKey::String("mykey".into()),
                bit: RedisJsonValue::Integer(1),
                range: Some(BitposRange {
                    start: RedisJsonValue::Integer(7),
                    end: Some(RedisJsonValue::Integer(15)),
                    mode: Some(BitMode::BIT),
                }),
            };
            let cmd = input.command();
            assert!(cmd.ends_with(b"$3\r\nBIT\r\n"));
        }

        #[test]
        fn test_decode_position_found() {
            let output = BitposOutput::decode(b":7\r\n").unwrap();
            assert_eq!(output.position(), 7);
            assert!(output.found());
        }

        #[test]
        fn test_decode_position_not_found() {
            let output = BitposOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.position(), -1);
            assert!(!output.found());
        }

        #[test]
        fn test_decode_position_zero() {
            let output = BitposOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.position(), 0);
            assert!(output.found());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BitposOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1)];
            let input = BitposInput::decode(args).unwrap();
            assert!(input.range.is_none());
        }

        #[test]
        fn test_decode_input_with_start() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(0),
            ];
            let input = BitposInput::decode(args).unwrap();
            assert!(input.range.is_some());
            assert!(input.range.as_ref().unwrap().end.is_none());
        }

        #[test]
        fn test_decode_input_with_range() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            ];
            let input = BitposInput::decode(args).unwrap();
            assert!(input.range.as_ref().unwrap().end.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = BitposInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BitposInput {
                key: RedisKey::String("testkey".into()),
                bit: RedisJsonValue::Integer(1),
                range: None,
            };
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
        async fn test_bitpos_empty_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // BITPOS on non-existent key looking for 1 returns -1
                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("missing_pos_key".into()),
                                bit: RedisJsonValue::Integer(1),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.position(), -1);
                    assert!(!output.found());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_find_first_one() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a string and find first 1 bit
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitpos_str".into()),
                            value: RedisJsonValue::Bytes(vec![0xff, 0xf0, 0x00]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_str".into()),
                                bit: RedisJsonValue::Integer(1),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.position(), 0);
                    assert!(output.found());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_find_first_zero() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitpos_zero".into()),
                            value: RedisJsonValue::Bytes(vec![0xff, 0xf0, 0x00]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_zero".into()),
                                bit: RedisJsonValue::Integer(0),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    // First 0 bit is at position 12 (after \xff\xf = 12 ones)
                    assert_eq!(output.position(), 12);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_with_start() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitpos_start".into()),
                            value: RedisJsonValue::Bytes(vec![0x00, 0xff, 0xf0]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Start searching from byte 1
                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_start".into()),
                                bit: RedisJsonValue::Integer(1),
                                range: Some(BitposRange { start: RedisJsonValue::Integer(1), end: None, mode: None }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    // First 1 bit starting from byte 1 is at position 8
                    assert_eq!(output.position(), 8);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_with_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitpos_range".into()),
                            value: RedisJsonValue::Bytes(vec![0x00, 0x00, 0xff]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Search in range [0, 1] (bytes 0 and 1)
                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_range".into()),
                                bit: RedisJsonValue::Integer(1),
                                range: Some(BitposRange {
                                    start: RedisJsonValue::Integer(0),
                                    end: Some(RedisJsonValue::Integer(1)),
                                    mode: None,
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    // No 1 bits in bytes 0-1
                    assert_eq!(output.position(), -1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_bit_mode() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("bitpos_bitmode".into()),
                            value: RedisJsonValue::Bytes(vec![0xff, 0xf0, 0x00]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Search for 0 in bit range
                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_bitmode".into()),
                                bit: RedisJsonValue::Integer(0),
                                range: Some(BitposRange {
                                    start: RedisJsonValue::Integer(0),
                                    end: Some(RedisJsonValue::Integer(15)),
                                    mode: Some(BitMode::BIT),
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.position(), 12);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_after_setbit() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create key with specific bits
                    ctx.raw(
                        &SetbitInput {
                            key: RedisKey::String("bitpos_setbit".into()),
                            offset: RedisJsonValue::Integer(100),
                            value: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitposInput {
                                key: RedisKey::String("bitpos_setbit".into()),
                                bit: RedisJsonValue::Integer(1),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.position(), 100);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("pipe_bp".into()),
                            value: RedisJsonValue::Bytes(vec![0x00, 0xff]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    // Find first 1
                    pipeline.extend_from_slice(
                        &BitposInput {
                            key: RedisKey::String("pipe_bp".into()),
                            bit: RedisJsonValue::Integer(1),
                            range: None,
                        }
                        .command(),
                    );
                    // Find first 0
                    pipeline.extend_from_slice(
                        &BitposInput {
                            key: RedisKey::String("pipe_bp".into()),
                            bit: RedisJsonValue::Integer(0),
                            range: None,
                        }
                        .command(),
                    );
                    // Find 1 in non-existent key
                    pipeline.extend_from_slice(
                        &BitposInput {
                            key: RedisKey::String("missing".into()),
                            bit: RedisJsonValue::Integer(1),
                            range: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = BitposOutput::decode(responses[0]).expect("decode 1");
                    assert_eq!(out1.position(), 8); // First 1 at byte 1

                    let out2 = BitposOutput::decode(responses[1]).expect("decode 2");
                    assert_eq!(out2.position(), 0); // First 0 at byte 0

                    let out3 = BitposOutput::decode(responses[2]).expect("decode 3");
                    assert_eq!(out3.position(), -1); // Not found
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            ctx.raw(
                &SetbitInput {
                    key: RedisKey::String("resp2bp".into()),
                    offset: RedisJsonValue::Integer(7),
                    value: RedisJsonValue::Integer(1),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitposInput {
                        key: RedisKey::String("resp2bp".into()),
                        bit: RedisJsonValue::Integer(1),
                        range: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":7\r\n");
            let output = BitposOutput::decode(&result).expect("decode failed");
            assert_eq!(output.position(), 7);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitpos_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            ctx.raw(
                &SetbitInput {
                    key: RedisKey::String("resp3bp".into()),
                    offset: RedisJsonValue::Integer(7),
                    value: RedisJsonValue::Integer(1),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitposInput {
                        key: RedisKey::String("resp3bp".into()),
                        bit: RedisJsonValue::Integer(1),
                        range: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":7\r\n");
            let output = BitposOutput::decode(&result).expect("decode failed");
            assert_eq!(output.position(), 7);
            ctx.stop().await;
        }
    }
}
