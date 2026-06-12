use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BitfieldRoGet, key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, BitfieldRoInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BitfieldRo,
    "Performs arbitrary read-only bitfield integer operations on strings",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BITFIELD_RO`
/// https://redis.io/docs/latest/commands/bitfield_ro/
///
/// Official example: `BITFIELD_RO hello GET i8 16`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BitfieldRoInput {
    pub(crate) key: RedisKey,
    pub(crate) gets: Vec<BitfieldRoGet>,
}

impl Serialize for BitfieldRoInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BitfieldRoInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("gets", &self.gets)?;
        state.end()
    }
}

impl_redis_operation!(
    BitfieldRoInput,
    API_INFO,
    {key, gets}
);

impl RedisCommandInput for BitfieldRoInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        for get in &self.gets {
            command.arg("GET").arg(&get.encoding).arg(&get.offset);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("BITFIELD_RO requires at least 1 argument (key)"));
        }

        let key = args[0].clone().try_into()?;
        let mut gets = Vec::new();
        let mut i = 1;

        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) if s.to_uppercase() == "GET" => {
                    if i + 2 >= args.len() {
                        return Err(EpError::parse("GET requires encoding and offset"));
                    }
                    gets.push(BitfieldRoGet { encoding: args[i + 1].clone(), offset: args[i + 2].clone() });
                    i += 3;
                }
                RedisJsonValue::String(s) => {
                    return Err(EpError::parse(format!("BITFIELD_RO only supports GET operation, got: {}", s)));
                }
                _ => {
                    return Err(EpError::parse("Operations must be strings"));
                }
            }
        }

        if gets.is_empty() {
            return Err(EpError::parse("BITFIELD_RO requires at least one GET operation"));
        }

        Ok(BitfieldRoInput { key, gets })
    }
}

/// Output for Redis BITFIELD_RO command
///
/// Returns an array of results, one for each GET operation performed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BitfieldRoOutput {
    /// Array of values from each GET operation
    results: Vec<i64>,
}

impl BitfieldRoOutput {
    pub fn new(results: Vec<i64>) -> Self {
        Self { results }
    }

    /// Get all results
    pub fn results(&self) -> &[i64] {
        &self.results
    }

    /// Get a specific result by index
    pub fn get(&self, index: usize) -> Option<i64> {
        self.results.get(index).copied()
    }

    /// Decode the Redis protocol response into a BitfieldRoOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => {
                    let mut results = Vec::new();
                    for item in items {
                        match item {
                            Resp2Frame::Integer(i) => results.push(i),
                            other => {
                                return Err(EpError::parse(format!("unexpected BITFIELD_RO array item: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BITFIELD_RO response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut results = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::Number { data, .. } => results.push(data),
                            other => {
                                return Err(EpError::parse(format!("unexpected BITFIELD_RO array item: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BITFIELD_RO response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for BitfieldRoOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BitfieldRoOutput", 1)?;
        state.serialize_field("results", &self.results)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_get() {
            let input = BitfieldRoInput {
                key: RedisKey::String("mykey".into()),
                gets: vec![BitfieldRoGet {
                    encoding: RedisJsonValue::String("u8".into()),
                    offset: RedisJsonValue::Integer(0),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$11\r\nBITFIELD_RO\r\n"));
        }

        #[test]
        fn test_encode_command_multiple_gets() {
            let input = BitfieldRoInput {
                key: RedisKey::String("mykey".into()),
                gets: vec![
                    BitfieldRoGet {
                        encoding: RedisJsonValue::String("u8".into()),
                        offset: RedisJsonValue::Integer(0),
                    },
                    BitfieldRoGet {
                        encoding: RedisJsonValue::String("i8".into()),
                        offset: RedisJsonValue::Integer(16),
                    },
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*8\r\n$11\r\nBITFIELD_RO\r\n"));
        }

        #[test]
        fn test_decode_array_response() {
            let output = BitfieldRoOutput::decode(b"*2\r\n:10\r\n:20\r\n").unwrap();
            assert_eq!(output.results().len(), 2);
            assert_eq!(output.results()[0], 10);
            assert_eq!(output.results()[1], 20);
        }

        #[test]
        fn test_decode_single_response() {
            let output = BitfieldRoOutput::decode(b"*1\r\n:42\r\n").unwrap();
            assert_eq!(output.results().len(), 1);
            assert_eq!(output.get(0), Some(42));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = BitfieldRoOutput::decode(b"*0\r\n").unwrap();
            assert!(output.results().is_empty());
        }

        #[test]
        fn test_decode_negative_value() {
            let output = BitfieldRoOutput::decode(b"*1\r\n:-10\r\n").unwrap();
            assert_eq!(output.results()[0], -10);
        }

        #[test]
        fn test_decode_input_single_get() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("GET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(0),
            ];
            let input = BitfieldRoInput::decode(args).unwrap();
            assert_eq!(input.gets.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_gets() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("GET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("GET".into()),
                RedisJsonValue::String("i16".into()),
                RedisJsonValue::Integer(8),
            ];
            let input = BitfieldRoInput::decode(args).unwrap();
            assert_eq!(input.gets.len(), 2);
        }

        #[test]
        fn test_decode_input_no_gets_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = BitfieldRoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least one GET"));
        }

        #[test]
        fn test_decode_input_invalid_op_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("SET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(0),
            ];
            let err = BitfieldRoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("only supports GET"));
        }

        #[test]
        fn test_decode_input_no_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = BitfieldRoInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BitfieldRoInput { key: RedisKey::String("testkey".into()), gets: vec![] };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bitmap::{BitfieldInput, BitfieldOp};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_basic() {
            // BITFIELD_RO was added in Redis 6.0
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // First set some data using BITFIELD
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_ro_basic".into()),
                            operations: vec![BitfieldOp::Set {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::Integer(42),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Read with BITFIELD_RO
                    let result = ctx
                        .raw(
                            &BitfieldRoInput {
                                key: RedisKey::String("bf_ro_basic".into()),
                                gets: vec![BitfieldRoGet {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 1);
                    assert_eq!(output.results()[0], 42);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_multiple_gets() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Set multiple values
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_ro_multi".into()),
                            operations: vec![
                                BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                    value: RedisJsonValue::Integer(10),
                                },
                                BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(8),
                                    value: RedisJsonValue::Integer(20),
                                },
                                BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(16),
                                    value: RedisJsonValue::Integer(30),
                                },
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Read all values
                    let result = ctx
                        .raw(
                            &BitfieldRoInput {
                                key: RedisKey::String("bf_ro_multi".into()),
                                gets: vec![
                                    BitfieldRoGet {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                    },
                                    BitfieldRoGet {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(8),
                                    },
                                    BitfieldRoGet {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(16),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 3);
                    assert_eq!(output.results()[0], 10);
                    assert_eq!(output.results()[1], 20);
                    assert_eq!(output.results()[2], 30);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_signed() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Set signed value
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_ro_signed".into()),
                            operations: vec![BitfieldOp::Set {
                                encoding: RedisJsonValue::String("i8".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::Integer(-50),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Read signed value
                    let result = ctx
                        .raw(
                            &BitfieldRoInput {
                                key: RedisKey::String("bf_ro_signed".into()),
                                gets: vec![BitfieldRoGet {
                                    encoding: RedisJsonValue::String("i8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], -50);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BitfieldRoInput {
                                key: RedisKey::String("nonexistent_bf_ro".into()),
                                gets: vec![BitfieldRoGet {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], 0); // Non-existent key = 0
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_different_encodings() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Set a 16-bit value
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_ro_enc".into()),
                            operations: vec![BitfieldOp::Set {
                                encoding: RedisJsonValue::String("u16".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::Integer(1000),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Read as different sizes
                    let result = ctx
                        .raw(
                            &BitfieldRoInput {
                                key: RedisKey::String("bf_ro_enc".into()),
                                gets: vec![
                                    BitfieldRoGet {
                                        encoding: RedisJsonValue::String("u16".into()),
                                        offset: RedisJsonValue::Integer(0),
                                    },
                                    BitfieldRoGet {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldRoOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 2);
                    assert_eq!(output.results()[0], 1000); // Full u16
                    // u8 should read first 8 bits of 1000 (0x03E8) = 0x03 = 3
                    assert_eq!(output.results()[1], 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_pipeline() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Set values
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_ro_pipe".into()),
                            operations: vec![
                                BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                    value: RedisJsonValue::Integer(100),
                                },
                                BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(8),
                                    value: RedisJsonValue::Integer(200),
                                },
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &BitfieldRoInput {
                            key: RedisKey::String("bf_ro_pipe".into()),
                            gets: vec![BitfieldRoGet {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(0),
                            }],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &BitfieldRoInput {
                            key: RedisKey::String("bf_ro_pipe".into()),
                            gets: vec![BitfieldRoGet {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(8),
                            }],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = BitfieldRoOutput::decode(responses[0]).expect("decode 1");
                    assert_eq!(out1.results()[0], 100);

                    let out2 = BitfieldRoOutput::decode(responses[1]).expect("decode 2");
                    assert_eq!(out2.results()[0], 200);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("6")).await;
            ctx.raw(
                &BitfieldInput {
                    key: RedisKey::String("bf_ro_resp2".into()),
                    operations: vec![BitfieldOp::Set {
                        encoding: RedisJsonValue::String("u8".into()),
                        offset: RedisJsonValue::Integer(0),
                        value: RedisJsonValue::Integer(99),
                    }],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitfieldRoInput {
                        key: RedisKey::String("bf_ro_resp2".into()),
                        gets: vec![BitfieldRoGet {
                            encoding: RedisJsonValue::String("u8".into()),
                            offset: RedisJsonValue::Integer(0),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"*1\r\n:99\r\n");
            let output = BitfieldRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.results()[0], 99);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_ro_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("6")).await;
            ctx.raw(
                &BitfieldInput {
                    key: RedisKey::String("bf_ro_resp3".into()),
                    operations: vec![BitfieldOp::Set {
                        encoding: RedisJsonValue::String("u8".into()),
                        offset: RedisJsonValue::Integer(0),
                        value: RedisJsonValue::Integer(77),
                    }],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitfieldRoInput {
                        key: RedisKey::String("bf_ro_resp3".into()),
                        gets: vec![BitfieldRoGet {
                            encoding: RedisJsonValue::String("u8".into()),
                            offset: RedisJsonValue::Integer(0),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = BitfieldRoOutput::decode(&result).expect("decode failed");
            assert_eq!(output.results()[0], 77);
            ctx.stop().await;
        }
    }
}
