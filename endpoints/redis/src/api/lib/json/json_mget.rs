use crate::api::lib::json::json_get::{JsonGetInput, JsonGetOutput};
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
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, JsonMgetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonMget,
    "Returns the value at a path for one or more keys",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.MGET`
/// https://redis.io/docs/latest/commands/json.mget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonMgetInput {
    keys: Vec<RedisKey>,
    path: RedisJsonValue,
}

impl Serialize for JsonMgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMgetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("path", &self.path)?;
        state.end()
    }
}

impl_redis_operation!(
    JsonMgetInput,
    API_INFO,
    {keys, path}
);

impl RedisCommandInput for JsonMgetInput {
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
        command.arg(&self.path);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("JSON.MGET requires at least 2 arguments, given {}", args.len())));
        }

        let path = args[args.len() - 1].clone();
        let mut keys = vec![];
        for key in args[..args.len() - 1].iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { keys, path })
    }
}

/// `MultiCommand` impl for `JSON.MGET`.
///
/// `JSON.MGET key1 ... keyN path` is the JSON twin of `MGET`: it returns one
/// JSON-serialized value per key, using a single shared path. Decomposition
/// issues `JSON.GET <key> <path>` per key (with no formatting flags) and
/// reassembles the responses in input order. Per-key `WRONGTYPE` errors are
/// normalized to nil to match `JSON.MGET` behavior on keys holding non-JSON
/// values; other errors propagate unchanged.
impl MultiCommand for JsonMgetInput {
    type Single = JsonGetInput;
    type SingleOutput = JsonGetOutput;
    type Output = JsonMgetOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys
            .iter()
            .cloned()
            .map(|key| JsonGetInput {
                key,
                indent: None,
                newline: None,
                space: None,
                path: Some(vec![self.path.clone()]),
            })
            .collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let values = parts
            .into_iter()
            .map(|part| match part {
                Ok(part) => Ok(part.value().map(|s| s.to_string())),
                Err(err) if is_wrongtype_error(&err) => Ok(None),
                Err(err) => Err(err),
            })
            .collect::<Result<Vec<_>, ::error::EpError>>()?;

        Ok(JsonMgetOutput::new(values))
    }
}

fn is_wrongtype_error(err: &::error::EpError) -> bool {
    matches!(err, ::error::EpError::Redis(RedisError::WrongType)) || err.to_string().to_ascii_lowercase().contains("wrongtype")
}

/// Output for Redis JSON.MGET command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonMgetOutput {
    values: Vec<Option<String>>,
}

impl JsonMgetOutput {
    pub fn new(values: Vec<Option<String>>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Option<String>] {
        &self.values
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut values = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(data) => values.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp2Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.MGET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut values = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => values.push(Some(String::from_utf8_lossy(&data).to_string())),
                            Resp3Frame::Null => values.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    values
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.MGET response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for JsonMgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonMgetOutput", 1)?;
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
        fn test_encode_command() {
            let input = JsonMgetInput {
                keys: vec![RedisKey::String("key1".into()), RedisKey::String("key2".into())],
                path: RedisJsonValue::String("$".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$9\r\nJSON.MGET\r\n"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = JsonMgetOutput::decode(b"*2\r\n$3\r\n[1]\r\n$3\r\n[2]\r\n").unwrap();
            assert_eq!(output.values().len(), 2);
        }

        #[test]
        fn test_decode_output_with_null() {
            let output = JsonMgetOutput::decode(b"*2\r\n$3\r\n[1]\r\n$-1\r\n").unwrap();
            assert_eq!(output.values().len(), 2);
            assert!(output.values()[0].is_some());
            assert!(output.values()[1].is_none());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("$".into()),
            ];
            let input = JsonMgetInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key1".into())];
            let err = JsonMgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonMgetInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
                path: RedisJsonValue::String("$".into()),
            };
            assert_eq!(input.keys().len(), 2);
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = JsonMgetInput {
                keys: vec![
                    RedisKey::String("a".into()),
                    RedisKey::String("b".into()),
                    RedisKey::String("c".into()),
                ],
                path: RedisJsonValue::String("$".into()),
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
            let input = JsonMgetInput { keys: keys.clone(), path: RedisJsonValue::String("$".into()) };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = JsonMgetInput {
                keys: vec![RedisKey::String("only".into())],
                path: RedisJsonValue::String("$".into()),
            };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
            assert!(parts[0].command().starts_with(b"*3\r\n$8\r\nJSON.GET\r\n"));
        }

        #[test]
        fn test_deconstruct_propagates_path() {
            let input = JsonMgetInput {
                keys: vec![RedisKey::String("k1".into()), RedisKey::String("k2".into())],
                path: RedisJsonValue::String("$.foo".into()),
            };
            for part in input.deconstruct() {
                let cmd = part.command();
                assert!(cmd.windows(5).any(|w| w == b"$.foo"), "path must be forwarded to JSON.GET");
            }
        }

        #[test]
        fn test_reconstruct_roundtrip() {
            let parts = vec![
                Ok(JsonGetOutput::new(Some(r#"{"n":1}"#.into()))),
                Ok(JsonGetOutput::new(None)),
                Ok(JsonGetOutput::new(Some(r#"{"n":3}"#.into()))),
            ];
            let output = JsonMgetInput::reconstruct(parts).unwrap();
            assert_eq!(output.values().len(), 3);
            assert_eq!(output.values()[0].as_deref(), Some(r#"{"n":1}"#));
            assert!(output.values()[1].is_none());
            assert_eq!(output.values()[2].as_deref(), Some(r#"{"n":3}"#));
        }

        #[test]
        fn test_reconstruct_treats_wrongtype_as_nil() {
            let parts = vec![
                Ok(JsonGetOutput::new(Some(r#"{"n":1}"#.into()))),
                Err(EpError::Redis(RedisError::WrongType)),
                Err(EpError::parse("WRONGTYPE Operation against a key holding the wrong kind of value")),
                Ok(JsonGetOutput::new(None)),
            ];

            let output = JsonMgetInput::reconstruct(parts).unwrap();
            assert_eq!(output.values().len(), 4);
            assert_eq!(output.values()[0].as_deref(), Some(r#"{"n":1}"#));
            assert!(output.values()[1].is_none());
            assert!(output.values()[2].is_none());
            assert!(output.values()[3].is_none());
        }

        #[test]
        fn test_reconstruct_propagates_non_wrongtype_error() {
            let err = EpError::parse("NOAUTH Authentication required");
            let result = JsonMgetInput::reconstruct(vec![Ok(JsonGetOutput::new(None)), Err(err.clone())]);

            assert_eq!(result.unwrap_err(), err);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::json::json_set::JsonSetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mget_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    for i in 1..=3 {
                        ctx.raw(
                            &JsonSetInput {
                                key: RedisKey::String(format!("mgetkey{}", i)),
                                path: RedisJsonValue::String("$".into()),
                                value: RedisJsonValue::String(format!(r#"{{"n":{}}}"#, i)),
                                options: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("set failed");
                    }

                    let result = ctx
                        .raw(
                            &JsonMgetInput {
                                keys: vec![
                                    RedisKey::String("mgetkey1".into()),
                                    RedisKey::String("mgetkey2".into()),
                                    RedisKey::String("mgetkey3".into()),
                                ],
                                path: RedisJsonValue::String("$".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("mget failed");

                    let output = JsonMgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_mget_with_missing() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("existskey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"x":1}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonMgetInput {
                                keys: vec![RedisKey::String("existskey".into()), RedisKey::String("missingkey".into())],
                                path: RedisJsonValue::String("$".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("mget failed");

                    let output = JsonMgetOutput::decode(&result).expect("decode failed");
                    assert!(output.values()[0].is_some());
                    assert!(output.values()[1].is_none());
                })
            })
            .await;
        }
    }
}
