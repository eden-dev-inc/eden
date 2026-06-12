use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BitfieldOp, OverflowBehavior, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, BitfieldInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bitfield,
    "Performs arbitrary bitfield integer operations on strings",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `BITFIELD`
/// https://redis.io/docs/latest/commands/bitfield/
///
/// Official example: `BITFIELD mykey INCRBY i5 100 1 GET u4 0`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BitfieldInput {
    pub(crate) key: RedisKey,
    pub(crate) operations: Vec<BitfieldOp>,
}

impl Serialize for BitfieldInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BitfieldInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("operations", &self.operations)?;
        state.end()
    }
}

impl_redis_operation!(
    BitfieldInput,
    API_INFO,
    {key, operations}
);

impl RedisCommandInput for BitfieldInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        for op in &self.operations {
            match op {
                BitfieldOp::Get { encoding, offset } => {
                    command.arg("GET").arg(encoding).arg(offset);
                }
                BitfieldOp::Set { encoding, offset, value } => {
                    command.arg("SET").arg(encoding).arg(offset).arg(value);
                }
                BitfieldOp::Incrby { encoding, offset, increment } => {
                    command.arg("INCRBY").arg(encoding).arg(offset).arg(increment);
                }
                BitfieldOp::Overflow(behavior) => {
                    command.arg("OVERFLOW");
                    match behavior {
                        OverflowBehavior::WRAP => command.arg("WRAP"),
                        OverflowBehavior::SAT => command.arg("SAT"),
                        OverflowBehavior::FAIL => command.arg("FAIL"),
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
        if args.is_empty() {
            return Err(EpError::parse("BITFIELD requires at least 1 argument (key)"));
        }

        let key = args[0].clone().try_into()?;
        let mut operations = Vec::new();
        let mut i = 1;

        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "GET" => {
                        if i + 2 >= args.len() {
                            return Err(EpError::parse("GET requires encoding and offset"));
                        }
                        operations.push(BitfieldOp::Get { encoding: args[i + 1].clone(), offset: args[i + 2].clone() });
                        i += 3;
                    }
                    "SET" => {
                        if i + 3 >= args.len() {
                            return Err(EpError::parse("SET requires encoding, offset, and value"));
                        }
                        operations.push(BitfieldOp::Set {
                            encoding: args[i + 1].clone(),
                            offset: args[i + 2].clone(),
                            value: args[i + 3].clone(),
                        });
                        i += 4;
                    }
                    "INCRBY" => {
                        if i + 3 >= args.len() {
                            return Err(EpError::parse("INCRBY requires encoding, offset, and increment"));
                        }
                        operations.push(BitfieldOp::Incrby {
                            encoding: args[i + 1].clone(),
                            offset: args[i + 2].clone(),
                            increment: args[i + 3].clone(),
                        });
                        i += 4;
                    }
                    "OVERFLOW" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("OVERFLOW requires a behavior"));
                        }
                        let behavior = match &args[i + 1] {
                            RedisJsonValue::String(b) => match b.to_uppercase().as_str() {
                                "WRAP" => OverflowBehavior::WRAP,
                                "SAT" => OverflowBehavior::SAT,
                                "FAIL" => OverflowBehavior::FAIL,
                                _ => {
                                    return Err(EpError::parse(format!("Invalid overflow behavior: {}", b)));
                                }
                            },
                            _ => {
                                return Err(EpError::parse("Overflow behavior must be a string"));
                            }
                        };
                        operations.push(BitfieldOp::Overflow(behavior));
                        i += 2;
                    }
                    _ => return Err(EpError::parse(format!("Unknown BITFIELD operation: {}", s))),
                },
                _ => return Err(EpError::parse("BITFIELD operations must be strings")),
            }
        }

        Ok(BitfieldInput { key, operations })
    }
}

/// Output for Redis BITFIELD command
///
/// Returns an array of results, one for each operation performed.
/// GET returns the value at the specified offset.
/// SET returns the previous value before the set.
/// INCRBY returns the new value after incrementing.
/// OVERFLOW operations return None.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BitfieldOutput {
    /// Array of results from each operation (None for OVERFLOW operations or FAIL overflow)
    results: Vec<Option<i64>>,
}

impl BitfieldOutput {
    pub fn new(results: Vec<Option<i64>>) -> Self {
        Self { results }
    }

    /// Get all results
    pub fn results(&self) -> &[Option<i64>] {
        &self.results
    }

    /// Get a specific result by index
    pub fn get(&self, index: usize) -> Option<&Option<i64>> {
        self.results.get(index)
    }

    /// Decode the Redis protocol response into a BitfieldOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => {
                    let mut results = Vec::new();
                    for item in items {
                        match item {
                            Resp2Frame::Integer(i) => results.push(Some(i)),
                            Resp2Frame::Null => results.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected BITFIELD array item: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BITFIELD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut results = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::Number { data, .. } => results.push(Some(data)),
                            Resp3Frame::Null => results.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected BITFIELD array item: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected BITFIELD response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for BitfieldOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BitfieldOutput", 1)?;
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
        fn test_encode_command_get() {
            let input = BitfieldInput {
                key: RedisKey::String("mykey".into()),
                operations: vec![BitfieldOp::Get {
                    encoding: RedisJsonValue::String("u8".into()),
                    offset: RedisJsonValue::Integer(0),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$8\r\nBITFIELD\r\n"));
        }

        #[test]
        fn test_encode_command_set() {
            let input = BitfieldInput {
                key: RedisKey::String("mykey".into()),
                operations: vec![BitfieldOp::Set {
                    encoding: RedisJsonValue::String("u8".into()),
                    offset: RedisJsonValue::Integer(0),
                    value: RedisJsonValue::Integer(100),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n$8\r\nBITFIELD\r\n"));
        }

        #[test]
        fn test_encode_command_incrby() {
            let input = BitfieldInput {
                key: RedisKey::String("mykey".into()),
                operations: vec![BitfieldOp::Incrby {
                    encoding: RedisJsonValue::String("i5".into()),
                    offset: RedisJsonValue::Integer(100),
                    increment: RedisJsonValue::Integer(1),
                }],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n$8\r\nBITFIELD\r\n"));
        }

        #[test]
        fn test_encode_command_overflow() {
            let input = BitfieldInput {
                key: RedisKey::String("mykey".into()),
                operations: vec![
                    BitfieldOp::Overflow(OverflowBehavior::SAT),
                    BitfieldOp::Incrby {
                        encoding: RedisJsonValue::String("u8".into()),
                        offset: RedisJsonValue::Integer(0),
                        increment: RedisJsonValue::Integer(300),
                    },
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*"));
        }

        #[test]
        fn test_decode_array_response() {
            // *2\r\n:1\r\n:0\r\n
            let output = BitfieldOutput::decode(b"*2\r\n:1\r\n:0\r\n").unwrap();
            assert_eq!(output.results().len(), 2);
            assert_eq!(output.results()[0], Some(1));
            assert_eq!(output.results()[1], Some(0));
        }

        #[test]
        fn test_decode_with_nil() {
            // *2\r\n:5\r\n$-1\r\n (nil for overflow FAIL)
            let output = BitfieldOutput::decode(b"*2\r\n:5\r\n$-1\r\n").unwrap();
            assert_eq!(output.results().len(), 2);
            assert_eq!(output.results()[0], Some(5));
            assert_eq!(output.results()[1], None);
        }

        #[test]
        fn test_decode_empty_array() {
            let output = BitfieldOutput::decode(b"*0\r\n").unwrap();
            assert!(output.results().is_empty());
        }

        #[test]
        fn test_decode_input_get() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("GET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(0),
            ];
            let input = BitfieldInput::decode(args).unwrap();
            assert_eq!(input.operations.len(), 1);
            assert!(matches!(input.operations[0], BitfieldOp::Get { .. }));
        }

        #[test]
        fn test_decode_input_multiple_ops() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("GET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("SET".into()),
                RedisJsonValue::String("u8".into()),
                RedisJsonValue::Integer(8),
                RedisJsonValue::Integer(100),
            ];
            let input = BitfieldInput::decode(args).unwrap();
            assert_eq!(input.operations.len(), 2);
        }

        #[test]
        fn test_decode_input_no_key() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = BitfieldInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BitfieldInput { key: RedisKey::String("testkey".into()), operations: vec![] };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_get() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_get".into()),
                                operations: vec![BitfieldOp::Get {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 1);
                    assert_eq!(output.results()[0], Some(0)); // New key = 0
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_set".into()),
                                operations: vec![BitfieldOp::Set {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                    value: RedisJsonValue::Integer(200),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], Some(0)); // Previous value

                    // Verify it was set
                    let get_result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_set".into()),
                                operations: vec![BitfieldOp::Get {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let get_output = BitfieldOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.results()[0], Some(200));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_incrby() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &BitfieldInput {
                            key: RedisKey::String("bf_incr".into()),
                            operations: vec![BitfieldOp::Set {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::Integer(100),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Increment
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_incr".into()),
                                operations: vec![BitfieldOp::Incrby {
                                    encoding: RedisJsonValue::String("u8".into()),
                                    offset: RedisJsonValue::Integer(0),
                                    increment: RedisJsonValue::Integer(10),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], Some(110)); // New value after increment
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_signed() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set and get signed value
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_signed".into()),
                                operations: vec![
                                    BitfieldOp::Set {
                                        encoding: RedisJsonValue::String("i8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                        value: RedisJsonValue::Integer(-10),
                                    },
                                    BitfieldOp::Get {
                                        encoding: RedisJsonValue::String("i8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 2);
                    assert_eq!(output.results()[1], Some(-10));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_overflow_sat() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SAT overflow should clamp to max value
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_sat".into()),
                                operations: vec![
                                    BitfieldOp::Overflow(OverflowBehavior::SAT),
                                    BitfieldOp::Set {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                        value: RedisJsonValue::Integer(200),
                                    },
                                    BitfieldOp::Incrby {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                        increment: RedisJsonValue::Integer(100),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    // With SAT, 200 + 100 should clamp to 255 (u8 max)
                    assert_eq!(output.results()[1], Some(255));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_multiple_operations() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BitfieldInput {
                                key: RedisKey::String("bf_multi".into()),
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
                                    BitfieldOp::Get {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(0),
                                    },
                                    BitfieldOp::Get {
                                        encoding: RedisJsonValue::String("u8".into()),
                                        offset: RedisJsonValue::Integer(8),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitfieldOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 4);
                    assert_eq!(output.results()[2], Some(10));
                    assert_eq!(output.results()[3], Some(20));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &BitfieldInput {
                            key: RedisKey::String("bf_pipe1".into()),
                            operations: vec![BitfieldOp::Set {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(0),
                                value: RedisJsonValue::Integer(42),
                            }],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &BitfieldInput {
                            key: RedisKey::String("bf_pipe1".into()),
                            operations: vec![BitfieldOp::Get {
                                encoding: RedisJsonValue::String("u8".into()),
                                offset: RedisJsonValue::Integer(0),
                            }],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = BitfieldOutput::decode(responses[0]).expect("decode set");
                    assert_eq!(out1.results()[0], Some(0));

                    let out2 = BitfieldOutput::decode(responses[1]).expect("decode get");
                    assert_eq!(out2.results()[0], Some(42));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx
                .raw(
                    &BitfieldInput {
                        key: RedisKey::String("bf_resp2".into()),
                        operations: vec![BitfieldOp::Get {
                            encoding: RedisJsonValue::String("u8".into()),
                            offset: RedisJsonValue::Integer(0),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"*1\r\n:0\r\n");
            let output = BitfieldOutput::decode(&result).expect("decode failed");
            assert_eq!(output.results()[0], Some(0));
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitfield_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx
                .raw(
                    &BitfieldInput {
                        key: RedisKey::String("bf_resp3".into()),
                        operations: vec![BitfieldOp::Get {
                            encoding: RedisJsonValue::String("u8".into()),
                            offset: RedisJsonValue::Integer(0),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = BitfieldOutput::decode(&result).expect("decode failed");
            assert_eq!(output.results()[0], Some(0));
            ctx.stop().await;
        }
    }
}
