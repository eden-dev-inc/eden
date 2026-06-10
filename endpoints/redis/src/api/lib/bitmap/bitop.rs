use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{BitopOperation, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, BitopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Bitop,
    "Performs bitwise operations between strings and stores the result in the destination key",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `BITOP`
/// https://redis.io/docs/latest/commands/bitop/
///
/// Official example: `BITOP AND destkey srckey1 srckey2`
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BitopInput {
    pub(crate) operation: BitopOperation,
    pub(crate) destkey: RedisKey,
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for BitopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BitopInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("operation", &self.operation)?;
        state.serialize_field("destkey", &self.destkey)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(
    BitopInput,
    API_INFO,
    {operation, destkey, keys}
);

impl RedisCommandInput for BitopInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        let mut keys: Vec<RedisKey> = vec![self.destkey.clone()];
        keys.extend(self.keys.clone());
        keys
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        match self.operation {
            BitopOperation::AND => command.arg("AND"),
            BitopOperation::OR => command.arg("OR"),
            BitopOperation::XOR => command.arg("XOR"),
            BitopOperation::NOT => command.arg("NOT"),
        };

        command.arg(&self.destkey);

        for key in &self.keys {
            command.arg(key);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse("BITOP requires at least 3 arguments (operation, destkey, srckey)"));
        }

        let operation = match &args[0] {
            RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                "AND" => BitopOperation::AND,
                "OR" => BitopOperation::OR,
                "XOR" => BitopOperation::XOR,
                "NOT" => BitopOperation::NOT,
                _ => {
                    return Err(EpError::parse(format!("Invalid BITOP operation: {}. Valid operations: AND, OR, XOR, NOT", s)));
                }
            },
            _ => return Err(EpError::parse("BITOP operation must be a string")),
        };

        // NOT operation requires exactly one source key
        if matches!(operation, BitopOperation::NOT) && args.len() != 3 {
            return Err(EpError::parse("BITOP NOT requires exactly one source key"));
        }

        let destkey = args[1].clone().try_into()?;
        let mut keys = Vec::new();
        for key in args[2..].iter() {
            keys.push(key.clone().try_into()?);
        }

        Ok(Self { operation, destkey, keys })
    }
}

/// Output for Redis BITOP command
///
/// Returns the size of the string stored in the destination key,
/// which is equal to the size of the longest input string.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BitopOutput {
    /// The size of the resulting string in bytes
    size: i64,
}

impl BitopOutput {
    pub fn new(size: i64) -> Self {
        Self { size }
    }

    /// Get the size of the resulting string
    pub fn size(&self) -> i64 {
        self.size
    }

    /// Decode the Redis protocol response into a BitopOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let size = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BITOP response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BITOP response: {:?}", other)));
                }
            },
        };

        Ok(Self { size })
    }
}

impl Serialize for BitopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BitopOutput", 1)?;
        state.serialize_field("size", &self.size)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_and() {
            let input = BitopInput {
                operation: BitopOperation::AND,
                destkey: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("key1".into()), RedisKey::String("key2".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n"));
        }

        #[test]
        fn test_encode_command_or() {
            let input = BitopInput {
                operation: BitopOperation::OR,
                destkey: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("key1".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nBITOP\r\n$2\r\nOR\r\n"));
        }

        #[test]
        fn test_encode_command_xor() {
            let input = BitopInput {
                operation: BitopOperation::XOR,
                destkey: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("k1".into()), RedisKey::String("k2".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$5\r\nBITOP\r\n$3\r\nXOR\r\n"));
        }

        #[test]
        fn test_encode_command_not() {
            let input = BitopInput {
                operation: BitopOperation::NOT,
                destkey: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("src".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$5\r\nBITOP\r\n$3\r\nNOT\r\n"));
        }

        #[test]
        fn test_decode_size() {
            let output = BitopOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.size(), 3);
        }

        #[test]
        fn test_decode_zero_size() {
            let output = BitopOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.size(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BitopOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_and() {
            let args = vec![
                RedisJsonValue::String("AND".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let input = BitopInput::decode(args).unwrap();
            assert!(matches!(input.operation, BitopOperation::AND));
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_not() {
            let args = vec![
                RedisJsonValue::String("NOT".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("src".into()),
            ];
            let input = BitopInput::decode(args).unwrap();
            assert!(matches!(input.operation, BitopOperation::NOT));
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_not_multiple_keys_fails() {
            let args = vec![
                RedisJsonValue::String("NOT".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let err = BitopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly one source key"));
        }

        #[test]
        fn test_decode_input_invalid_operation() {
            let args = vec![
                RedisJsonValue::String("INVALID".into()),
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::String("src".into()),
            ];
            let err = BitopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Invalid BITOP operation"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("AND".into()), RedisJsonValue::String("dest".into())];
            let err = BitopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = BitopInput {
                operation: BitopOperation::AND,
                destkey: RedisKey::String("dest".into()),
                keys: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 3); // dest + 2 src
            assert_eq!(keys[0], RedisKey::String("dest".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_and() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set two keys with known values
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("and1".into()),
                            value: RedisJsonValue::String("foof".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("and2".into()),
                            value: RedisJsonValue::String("foof".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::AND,
                                destkey: RedisKey::String("and_dest".into()),
                                keys: vec![RedisKey::String("and1".into()), RedisKey::String("and2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 4);

                    // Verify result
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("and_dest".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode get");
                    assert!(get_output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_or() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("or1".into()),
                            value: RedisJsonValue::Bytes(vec![0x00, 0xff]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("or2".into()),
                            value: RedisJsonValue::Bytes(vec![0xff, 0x00]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::OR,
                                destkey: RedisKey::String("or_dest".into()),
                                keys: vec![RedisKey::String("or1".into()), RedisKey::String("or2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_xor() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("xor1".into()),
                            value: RedisJsonValue::String("abc".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("xor2".into()),
                            value: RedisJsonValue::String("abc".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::XOR,
                                destkey: RedisKey::String("xor_dest".into()),
                                keys: vec![RedisKey::String("xor1".into()), RedisKey::String("xor2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 3);

                    // XOR of identical strings should be all zeros
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("xor_dest".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode get");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("\x00\x00\x00")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_not() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("not_src".into()),
                            value: RedisJsonValue::Bytes(vec![0x00, 0xff]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::NOT,
                                destkey: RedisKey::String("not_dest".into()),
                                keys: vec![RedisKey::String("not_src".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 2);

                    // NOT should invert bits
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("not_dest".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode get");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::Bytes(vec![0xff, 0x00])));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_empty_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Operation with non-existent key
                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::AND,
                                destkey: RedisKey::String("empty_dest".into()),
                                keys: vec![RedisKey::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_different_lengths() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("short".into()),
                            value: RedisJsonValue::String("ab".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("long".into()),
                            value: RedisJsonValue::String("abcd".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BitopInput {
                                operation: BitopOperation::AND,
                                destkey: RedisKey::String("diff_len_dest".into()),
                                keys: vec![RedisKey::String("short".into()), RedisKey::String("long".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BitopOutput::decode(&result).expect("decode failed");
                    // Result should be size of longest input
                    assert_eq!(output.size(), 4);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("p1".into()),
                            value: RedisJsonValue::Bytes(vec![0xff]),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("p2".into()),
                            value: RedisJsonValue::String("\x0f".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &BitopInput {
                            operation: BitopOperation::AND,
                            destkey: RedisKey::String("pipe_and".into()),
                            keys: vec![RedisKey::String("p1".into()), RedisKey::String("p2".into())],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &BitopInput {
                            operation: BitopOperation::OR,
                            destkey: RedisKey::String("pipe_or".into()),
                            keys: vec![RedisKey::String("p1".into()), RedisKey::String("p2".into())],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = BitopOutput::decode(responses[0]).expect("decode and");
                    assert_eq!(out1.size(), 1);

                    let out2 = BitopOutput::decode(responses[1]).expect("decode or");
                    assert_eq!(out2.size(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp2_op".into()),
                    value: RedisJsonValue::String("abc".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitopInput {
                        operation: BitopOperation::NOT,
                        destkey: RedisKey::String("resp2_op_dest".into()),
                        keys: vec![RedisKey::String("resp2_op".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":3\r\n");
            let output = BitopOutput::decode(&result).expect("decode failed");
            assert_eq!(output.size(), 3);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bitop_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp3_op".into()),
                    value: RedisJsonValue::String("abc".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &BitopInput {
                        operation: BitopOperation::NOT,
                        destkey: RedisKey::String("resp3_op_dest".into()),
                        keys: vec![RedisKey::String("resp3_op".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":3\r\n");
            let output = BitopOutput::decode(&result).expect("decode failed");
            assert_eq!(output.size(), 3);
            ctx.stop().await;
        }
    }
}
