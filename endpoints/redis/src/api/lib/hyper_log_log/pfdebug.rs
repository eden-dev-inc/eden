use crate::api::lib::hyper_log_log::PfdebugValue;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, PfdebugInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pfdebug,
    "Internal command for debugging HyperLogLog values.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PFDEBUG`
/// https://redis.io/docs/latest/commands/pfdebug/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PfdebugInput {
    pub(crate) subcommand: RedisJsonValue,
    pub(crate) key: RedisKey,
}

impl Serialize for PfdebugInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PfdebugInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("subcommand", &self.subcommand)?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(PfdebugInput, API_INFO, { subcommand, key });

impl RedisCommandInput for PfdebugInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.subcommand).arg(&self.key);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::parse(format!("PFDEBUG requires 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            subcommand: args[0].clone(),
            key: args[1].clone().try_into()?,
        })
    }
}

/// Output for Redis PFDEBUG command
///
/// PFDEBUG is an internal debugging command. The output varies depending on the subcommand:
/// - DECODE: Returns a string representation of the HyperLogLog registers
/// - ENCODING: Returns the encoding type (dense/sparse)
/// - TODENSE: Converts sparse to dense encoding, returns 1 on conversion or 0 if already dense
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PfdebugOutput {
    /// The raw response value from PFDEBUG
    value: PfdebugValue,
}

impl PfdebugOutput {
    pub fn new_string(s: String) -> Self {
        Self { value: PfdebugValue::String(s) }
    }

    pub fn new_integer(n: i64) -> Self {
        Self { value: PfdebugValue::Integer(n) }
    }

    pub fn new_array(arr: Vec<i64>) -> Self {
        Self { value: PfdebugValue::Array(arr) }
    }

    /// Get the value
    pub fn value(&self) -> &PfdebugValue {
        &self.value
    }

    /// Try to get as string
    pub fn as_string(&self) -> Option<&str> {
        match &self.value {
            PfdebugValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Try to get as integer
    pub fn as_integer(&self) -> Option<i64> {
        match &self.value {
            PfdebugValue::Integer(n) => Some(*n),
            _ => None,
        }
    }

    /// Try to get as array
    pub fn as_array(&self) -> Option<&[i64]> {
        match &self.value {
            PfdebugValue::Array(arr) => Some(arr),
            _ => None,
        }
    }

    /// Decode the Redis protocol response into a PfdebugOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::SimpleString(s) => Ok(Self::new_string(String::from_utf8(s).map_err(EpError::parse)?)),
            Resp2Frame::BulkString(bytes) => Ok(Self::new_string(String::from_utf8(bytes).map_err(EpError::parse)?)),
            Resp2Frame::Integer(n) => Ok(Self::new_integer(n)),
            Resp2Frame::Array(items) => {
                let mut arr = Vec::with_capacity(items.len());
                for item in items {
                    match item {
                        Resp2Frame::Integer(n) => arr.push(n),
                        other => {
                            return Err(EpError::parse(format!("unexpected array element in PFDEBUG response: {:?}", other)));
                        }
                    }
                }
                Ok(Self::new_array(arr))
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected PFDEBUG response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } => Ok(Self::new_string(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::BlobString { data, .. } => Ok(Self::new_string(String::from_utf8(data).map_err(EpError::parse)?)),
            Resp3Frame::Number { data, .. } => Ok(Self::new_integer(data)),
            Resp3Frame::Array { data, .. } => {
                let mut arr = Vec::with_capacity(data.len());
                for item in data {
                    match item {
                        Resp3Frame::Number { data: n, .. } => arr.push(n),
                        other => {
                            return Err(EpError::parse(format!("unexpected array element in PFDEBUG response: {:?}", other)));
                        }
                    }
                }
                Ok(Self::new_array(arr))
            }
            Resp3Frame::Null => Ok(Self::new_string(String::new())),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
            other => Err(EpError::parse(format!("unexpected PFDEBUG response: {:?}", other))),
        }
    }
}

impl Serialize for PfdebugOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PfdebugOutput", 1)?;
        match &self.value {
            PfdebugValue::String(s) => state.serialize_field("value", s)?,
            PfdebugValue::Integer(n) => state.serialize_field("value", n)?,
            PfdebugValue::Array(arr) => state.serialize_field("value", arr)?,
        }
        state.end()
    }
}

impl Serialize for PfdebugValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            PfdebugValue::String(s) => serializer.serialize_str(s),
            PfdebugValue::Integer(n) => serializer.serialize_i64(*n),
            PfdebugValue::Array(arr) => arr.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = PfdebugInput {
                subcommand: RedisJsonValue::String("DECODE".into()),
                key: RedisKey::String("hll".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nPFDEBUG\r\n$6\r\nDECODE\r\n$3\r\nhll\r\n");
        }

        #[test]
        fn test_encode_command_encoding() {
            let input = PfdebugInput {
                subcommand: RedisJsonValue::String("ENCODING".into()),
                key: RedisKey::String("mykey".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nPFDEBUG\r\n$8\r\nENCODING\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_output_string() {
            let output = PfdebugOutput::decode(b"+sparse\r\n").unwrap();
            assert_eq!(output.as_string(), Some("sparse"));
        }

        #[test]
        fn test_decode_output_bulk_string() {
            let output = PfdebugOutput::decode(b"$5\r\ndense\r\n").unwrap();
            assert_eq!(output.as_string(), Some("dense"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = PfdebugOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.as_integer(), Some(1));
        }

        #[test]
        fn test_decode_output_integer_zero() {
            let output = PfdebugOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.as_integer(), Some(0));
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PfdebugOutput::decode(b"-ERR unknown subcommand\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("DECODE".into()), RedisJsonValue::String("mykey".into())];
            let input = PfdebugInput::decode(args).unwrap();
            assert_eq!(input.subcommand, RedisJsonValue::String("DECODE".into()));
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_wrong_arg_count() {
            let args = vec![RedisJsonValue::String("DECODE".into())];
            let err = PfdebugInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("DECODE".into()),
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = PfdebugInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PfdebugInput {
                subcommand: RedisJsonValue::String("ENCODING".into()),
                key: RedisKey::String("hll".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("hll".into()));
        }

        #[test]
        fn test_output_value_accessors() {
            let str_output = PfdebugOutput::new_string("test".into());
            assert!(str_output.as_string().is_some());
            assert!(str_output.as_integer().is_none());
            assert!(str_output.as_array().is_none());

            let int_output = PfdebugOutput::new_integer(42);
            assert!(int_output.as_string().is_none());
            assert!(int_output.as_integer().is_some());
            assert!(int_output.as_array().is_none());

            let arr_output = PfdebugOutput::new_array(vec![1, 2, 3]);
            assert!(arr_output.as_string().is_none());
            assert!(arr_output.as_integer().is_none());
            assert!(arr_output.as_array().is_some());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::PfaddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_encoding_sparse() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\npfdebug_enc_sparse\r\n").await.expect("raw failed");

                    // Create HLL with few elements (stays sparse)
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfdebug_enc_sparse".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("ENCODING".into()),
                                key: RedisKey::String("pfdebug_enc_sparse".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfdebugOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_string(), Some("sparse"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_todense() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\npfdebug_todense\r\n").await.expect("raw failed");

                    // Create sparse HLL
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfdebug_todense".into()),
                            elements: Some(vec![RedisJsonValue::String("x".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Convert to dense
                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("TODENSE".into()),
                                key: RedisKey::String("pfdebug_todense".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfdebugOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_integer(), Some(1), "should return 1 on conversion");

                    // Verify encoding is now dense
                    let enc_result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("ENCODING".into()),
                                key: RedisKey::String("pfdebug_todense".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let enc_output = PfdebugOutput::decode(&enc_result).expect("decode failed");
                    assert_eq!(enc_output.as_string(), Some("dense"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_todense_already_dense() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$21\r\npfdebug_already_dense\r\n").await.expect("raw failed");

                    // Create HLL and convert to dense
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfdebug_already_dense".into()),
                            elements: Some(vec![RedisJsonValue::String("y".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &PfdebugInput {
                            subcommand: RedisJsonValue::String("TODENSE".into()),
                            key: RedisKey::String("pfdebug_already_dense".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try to convert again
                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("TODENSE".into()),
                                key: RedisKey::String("pfdebug_already_dense".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfdebugOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_integer(), Some(0), "should return 0 when already dense");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("ENCODING".into()),
                                key: RedisKey::String("pfdebug_missing".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // PFDEBUG on nonexistent key returns error
                    let err = PfdebugOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("ERR"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_wrong_type_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\npfdebug_wrongtype\r\n$5\r\nhello\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("ENCODING".into()),
                                key: RedisKey::String("pfdebug_wrongtype".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = PfdebugOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"), "should fail with WRONGTYPE error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_decode_subcommand() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\npfdebug_decode\r\n").await.expect("raw failed");

                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfdebug_decode".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &PfdebugInput {
                                subcommand: RedisJsonValue::String("DECODE".into()),
                                key: RedisKey::String("pfdebug_decode".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfdebugOutput::decode(&result).expect("decode failed");
                    // DECODE returns a string representation of the registers
                    assert!(output.as_string().is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfdebug_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &PfaddInput {
                    key: RedisKey::String("pfdebug_r2".into()),
                    elements: Some(vec![RedisJsonValue::String("val".into())]),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &PfdebugInput {
                        subcommand: RedisJsonValue::String("ENCODING".into()),
                        key: RedisKey::String("pfdebug_r2".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = PfdebugOutput::decode(&result).expect("decode failed");
            assert!(output.as_string().is_some());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfdebug_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfdebug_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &PfaddInput {
                    key: RedisKey::String("pfdebug_r3".into()),
                    elements: Some(vec![RedisJsonValue::String("val".into())]),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &PfdebugInput {
                        subcommand: RedisJsonValue::String("ENCODING".into()),
                        key: RedisKey::String("pfdebug_r3".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = PfdebugOutput::decode(&result).expect("decode failed");
            assert!(output.as_string().is_some());

            ctx.stop().await;
        }
    }
}
