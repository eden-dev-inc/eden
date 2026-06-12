use crate::api::lib::string::get::{GetInput, GetOutput};
use crate::api::lib::{MultiCommand, RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::{RedisError, ResultEP};
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, MgetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Mget,
    "Returns the values of all specified keys. For every key that does not hold a string value or does not exist, the special value nil is returned. Because of this, the operation never fails",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `MGET`
/// https://redis.io/docs/latest/commands/mget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MgetInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for MgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MgetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(MgetInput, API_INFO, { keys });

impl RedisCommandInput for MgetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for key in &self.keys {
            command.arg(key);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("MGET requires at least one argument, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

impl MultiCommand for MgetInput {
    type Single = GetInput;
    type SingleOutput = GetOutput;
    type Output = MgetOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|key| GetInput { key }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let values = parts
            .into_iter()
            .map(|part| match part {
                Ok(part) => Ok(part.value().cloned()),
                Err(err) if is_wrongtype_error(&err) => Ok(None),
                Err(err) => Err(err),
            })
            .collect::<Result<Vec<_>, ::error::EpError>>()?;

        Ok(MgetOutput::new(values))
    }
}

fn is_wrongtype_error(err: &::error::EpError) -> bool {
    matches!(err, ::error::EpError::Redis(RedisError::WrongType)) || err.to_string().to_ascii_lowercase().contains("wrongtype")
}

/// Output for Redis MGET command
///
/// Returns an array of values for the specified keys.
/// Values are Option<RedisJsonValue> - None for missing or non-string keys.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MgetOutput {
    /// The values, in the same order as the requested keys
    values: Vec<Option<RedisJsonValue>>,
}

impl MgetOutput {
    pub fn new(values: Vec<Option<RedisJsonValue>>) -> Self {
        Self { values }
    }

    /// Get the values
    pub fn values(&self) -> &[Option<RedisJsonValue>] {
        &self.values
    }

    /// Get value at specific index
    pub fn get(&self, index: usize) -> Option<&RedisJsonValue> {
        self.values.get(index).and_then(|v| v.as_ref())
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Decode the Redis protocol response into an MgetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => Self::decode_array_resp2(items)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MGET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => Self::decode_array_resp3(data)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MGET response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }

    fn decode_array_resp2(items: Vec<Resp2Frame>) -> Result<Vec<Option<RedisJsonValue>>, EpError> {
        let mut values = Vec::with_capacity(items.len());

        for item in items {
            let value = match item {
                Resp2Frame::BulkString(bytes) => {
                    let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                    Some(RedisJsonValue::String(s))
                }
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    Some(RedisJsonValue::String(s))
                }
                Resp2Frame::Integer(n) => Some(RedisJsonValue::Integer(n)),
                Resp2Frame::Null => None,
                _ => None,
            };
            values.push(value);
        }

        Ok(values)
    }

    fn decode_array_resp3(items: Vec<Resp3Frame>) -> Result<Vec<Option<RedisJsonValue>>, EpError> {
        let mut values = Vec::with_capacity(items.len());

        for item in items {
            let value = match item {
                Resp3Frame::BlobString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Some(RedisJsonValue::String(s))
                }
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Some(RedisJsonValue::String(s))
                }
                Resp3Frame::Number { data, .. } => Some(RedisJsonValue::Integer(data)),
                Resp3Frame::Null => None,
                _ => None,
            };
            values.push(value);
        }

        Ok(values)
    }
}

impl Serialize for MgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MgetOutput", 1)?;
        state.serialize_field("values", &self.values)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_key() {
            let input = MgetInput { keys: vec![RedisKey::String("mykey".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nMGET\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = MgetInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$4\r\nMGET\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
        }

        #[test]
        fn test_decode_array_with_values() {
            let output = MgetOutput::decode(b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.get(0), Some(&RedisJsonValue::from("foo")));
            assert_eq!(output.get(1), Some(&RedisJsonValue::from("bar")));
        }

        #[test]
        fn test_decode_array_with_nulls() {
            let output = MgetOutput::decode(b"*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.get(0), Some(&RedisJsonValue::from("foo")));
            assert_eq!(output.get(1), None);
            assert_eq!(output.get(2), Some(&RedisJsonValue::from("bar")));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = MgetOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MgetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let input = MgetInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = MgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one argument"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = MgetInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
            };
            assert_eq!(input.keys().len(), 2);
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = MgetInput {
                keys: vec![
                    RedisKey::String("a".into()),
                    RedisKey::String("b".into()),
                    RedisKey::String("c".into()),
                ],
            };
            assert_eq!(input.deconstruct().len(), input.keys().len());
        }

        #[test]
        fn test_deconstruct_order_preserved() {
            let keys = vec![
                RedisKey::String("first".into()),
                RedisKey::String("second".into()),
                RedisKey::String("third".into()),
            ];
            let input = MgetInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = MgetInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
            assert_eq!(parts[0].command().to_vec(), b"*2\r\n$3\r\nGET\r\n$4\r\nonly\r\n");
        }

        #[test]
        fn test_reconstruct_roundtrip() {
            let parts = vec![
                Ok(GetOutput::new(Some(RedisJsonValue::from("v1")))),
                Ok(GetOutput::new(None)),
                Ok(GetOutput::new(Some(RedisJsonValue::from("v3")))),
            ];
            let output = MgetInput::reconstruct(parts).unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.get(0), Some(&RedisJsonValue::from("v1")));
            assert_eq!(output.get(1), None);
            assert_eq!(output.get(2), Some(&RedisJsonValue::from("v3")));
        }

        #[test]
        fn test_reconstruct_treats_wrongtype_as_nil() {
            let parts = vec![
                Ok(GetOutput::new(Some(RedisJsonValue::from("v1")))),
                Err(EpError::Redis(RedisError::WrongType)),
                Err(EpError::parse("WRONGTYPE Operation against a key holding the wrong kind of value")),
                Ok(GetOutput::new(None)),
            ];

            let output = MgetInput::reconstruct(parts).unwrap();
            assert_eq!(output.len(), 4);
            assert_eq!(output.get(0), Some(&RedisJsonValue::from("v1")));
            assert_eq!(output.get(1), None);
            assert_eq!(output.get(2), None);
            assert_eq!(output.get(3), None);
        }

        #[test]
        fn test_reconstruct_propagates_non_wrongtype_error() {
            let err = EpError::parse("NOAUTH Authentication required");
            let result = MgetInput::reconstruct(vec![Ok(GetOutput::new(None)), Err(err.clone())]);

            assert_eq!(result.unwrap_err(), err);
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
        async fn test_mget_all_exist() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("mget1".into()),
                            value: RedisJsonValue::String("val1".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("mget2".into()),
                            value: RedisJsonValue::String("val2".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &MgetInput {
                                keys: vec![RedisKey::String("mget1".into()), RedisKey::String("mget2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.get(0), Some(&RedisJsonValue::from("val1")));
                    assert_eq!(output.get(1), Some(&RedisJsonValue::from("val2")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_some_missing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("mget_exist".into()),
                            value: RedisJsonValue::String("exists".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &MgetInput {
                                keys: vec![RedisKey::String("mget_exist".into()), RedisKey::String("mget_missing".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.get(0), Some(&RedisJsonValue::from("exists")));
                    assert_eq!(output.get(1), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_all_missing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MgetInput {
                                keys: vec![RedisKey::String("missing1".into()), RedisKey::String("missing2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.get(0), None);
                    assert_eq!(output.get(1), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_single_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("mget_single".into()),
                            value: RedisJsonValue::String("single_val".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&MgetInput { keys: vec![RedisKey::String("mget_single".into())] }.command()).await.expect("raw failed");

                    let output = MgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.get(0), Some(&RedisJsonValue::from("single_val")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_resp2_array_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("r2val".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &MgetInput {
                        keys: vec![RedisKey::String("r2key".into()), RedisKey::String("missing".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = MgetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 2);
            assert_eq!(output.get(0), Some(&RedisJsonValue::from("r2val")));
            assert_eq!(output.get(1), None);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pipe1".into()),
                            value: RedisJsonValue::String("v1".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pipe2".into()),
                            value: RedisJsonValue::String("v2".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &MgetInput {
                            keys: vec![RedisKey::String("pipe1".into()), RedisKey::String("pipe2".into())],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let mget_output = MgetOutput::decode(responses[2]).expect("decode MGET");
                    assert_eq!(mget_output.len(), 2);
                    assert_eq!(mget_output.get(0), Some(&RedisJsonValue::from("v1")));
                    assert_eq!(mget_output.get(1), Some(&RedisJsonValue::from("v2")));
                })
            })
            .await;
        }
    }
}
