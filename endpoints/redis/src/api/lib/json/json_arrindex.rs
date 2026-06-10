use crate::api::lib::json::Range;
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

const API_INFO: ApiInfo<RedisApi, JsonArrindexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::JsonArrindex,
    "Returns the index of the first occurrence of a JSON scalar value in the array at path",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `JSON.ARRINDEX`
/// https://redis.io/docs/latest/commands/json.arrindex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct JsonArrindexInput {
    key: RedisKey,
    path: RedisJsonValue,
    value: RedisJsonValue,
    range: Option<Range>,
}

impl Serialize for JsonArrindexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.range.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("JsonArrindexInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("path", &self.path)?;
        state.serialize_field("value", &self.value)?;
        if let Some(range) = &self.range {
            state.serialize_field("range", range)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    JsonArrindexInput,
    API_INFO,
    {key, path, value, range}
);

impl RedisCommandInput for JsonArrindexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.path).arg(&self.value);

        if let Some(range) = self.range.as_ref() {
            range.cmd(&mut command);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("JSON.ARRINDEX requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let path = args[1].clone();
        let value = args[2].clone();

        let range = if args.len() > 3 {
            let start = args[3].clone();
            let stop = args.get(4).cloned();
            Some(Range { start, stop })
        } else {
            None
        };

        Ok(Self { key, path, value, range })
    }
}

/// Output for Redis JSON.ARRINDEX command
///
/// Returns an array of integer replies for each path:
/// - The index of the first occurrence of value
/// - -1 if value is not found
/// - null if the path doesn't contain an array
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct JsonArrindexOutput {
    indices: Vec<Option<i64>>,
}

impl JsonArrindexOutput {
    pub fn new(indices: Vec<Option<i64>>) -> Self {
        Self { indices }
    }

    pub fn indices(&self) -> &[Option<i64>] {
        &self.indices
    }

    pub fn first(&self) -> Option<i64> {
        self.indices.first().and_then(|i| *i)
    }

    /// Returns true if the value was found (index >= 0)
    pub fn found(&self) -> bool {
        self.first().map(|i| i >= 0).unwrap_or(false)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let indices = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut indices = Vec::new();
                    for item in arr {
                        match item {
                            Resp2Frame::Integer(n) => indices.push(Some(n)),
                            Resp2Frame::Null => indices.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    indices
                }
                Resp2Frame::Integer(n) => vec![Some(n)],
                Resp2Frame::Null => vec![None],
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.ARRINDEX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut indices = Vec::new();
                    for item in data {
                        match item {
                            Resp3Frame::Number { data, .. } => indices.push(Some(data)),
                            Resp3Frame::Null => indices.push(None),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element: {:?}", other)));
                            }
                        }
                    }
                    indices
                }
                Resp3Frame::Number { data, .. } => vec![Some(data)],
                Resp3Frame::Null => vec![None],
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected JSON.ARRINDEX response: {:?}", other)));
                }
            },
        };

        Ok(Self { indices })
    }
}

impl Serialize for JsonArrindexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("JsonArrindexOutput", 1)?;
        state.serialize_field("indices", &self.indices)?;
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
            let input = JsonArrindexInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.arr".into()),
                value: RedisJsonValue::String("5".into()),
                range: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$13\r\nJSON.ARRINDEX\r\n"));
        }

        #[test]
        fn test_encode_command_with_range() {
            let input = JsonArrindexInput {
                key: RedisKey::String("mykey".into()),
                path: RedisJsonValue::String("$.arr".into()),
                value: RedisJsonValue::String("5".into()),
                range: Some(Range {
                    start: RedisJsonValue::Integer(0),
                    stop: Some(RedisJsonValue::Integer(10)),
                }),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n$13\r\nJSON.ARRINDEX\r\n"));
        }

        #[test]
        fn test_decode_output_found() {
            let output = JsonArrindexOutput::decode(b"*1\r\n:2\r\n").unwrap();
            assert_eq!(output.first(), Some(2));
            assert!(output.found());
        }

        #[test]
        fn test_decode_output_not_found() {
            let output = JsonArrindexOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.first(), Some(-1));
            assert!(!output.found());
        }

        #[test]
        fn test_decode_output_null() {
            let output = JsonArrindexOutput::decode(b"*1\r\n$-1\r\n").unwrap();
            assert_eq!(output.first(), None);
            assert!(!output.found());
        }

        #[test]
        fn test_decode_output_error() {
            let err = JsonArrindexOutput::decode(b"-ERR not an array\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.arr".into()),
                RedisJsonValue::String("5".into()),
            ];
            let input = JsonArrindexInput::decode(args).unwrap();
            assert!(input.range.is_none());
        }

        #[test]
        fn test_decode_input_with_range() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("$.arr".into()),
                RedisJsonValue::String("5".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = JsonArrindexInput::decode(args).unwrap();
            assert!(input.range.is_some());
            let range = input.range.unwrap();
            assert!(range.stop.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("$.arr".into())];
            let err = JsonArrindexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = JsonArrindexInput {
                key: RedisKey::String("testkey".into()),
                path: RedisJsonValue::String("$".into()),
                value: RedisJsonValue::String("1".into()),
                range: None,
            };
            assert_eq!(input.keys().len(), 1);
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
        async fn test_json_arrindex_found() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("idxkey".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[1,2,3,4,5]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonArrindexInput {
                                key: RedisKey::String("idxkey".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                value: RedisJsonValue::String("3".into()),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("arrindex failed");

                    let output = JsonArrindexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(2)); // Index 2 (0-based)
                    assert!(output.found());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrindex_not_found() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("idxnf".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[1,2,3]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let result = ctx
                        .raw(
                            &JsonArrindexInput {
                                key: RedisKey::String("idxnf".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                value: RedisJsonValue::String("99".into()),
                                range: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("arrindex failed");

                    let output = JsonArrindexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(-1));
                    assert!(!output.found());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrindex_with_range() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &JsonSetInput {
                            key: RedisKey::String("idxrange".into()),
                            path: RedisJsonValue::String("$".into()),
                            value: RedisJsonValue::String(r#"{"arr":[1,2,3,2,1]}"#.into()),
                            options: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Search for 2 starting from index 2
                    let result = ctx
                        .raw(
                            &JsonArrindexInput {
                                key: RedisKey::String("idxrange".into()),
                                path: RedisJsonValue::String("$.arr".into()),
                                value: RedisJsonValue::String("2".into()),
                                range: Some(Range { start: RedisJsonValue::Integer(2), stop: None }),
                            }
                            .command(),
                        )
                        .await
                        .expect("arrindex failed");

                    let output = JsonArrindexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.first(), Some(3)); // Second occurrence at index 3
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_json_arrindex_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &JsonSetInput {
                    key: RedisKey::String("r2idx".into()),
                    path: RedisJsonValue::String("$".into()),
                    value: RedisJsonValue::String(r#"{"arr":[1,2,3]}"#.into()),
                    options: None,
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx
                .raw(
                    &JsonArrindexInput {
                        key: RedisKey::String("r2idx".into()),
                        path: RedisJsonValue::String("$.arr".into()),
                        value: RedisJsonValue::String("2".into()),
                        range: None,
                    }
                    .command(),
                )
                .await
                .expect("arrindex failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            ctx.stop().await;
        }
    }
}
