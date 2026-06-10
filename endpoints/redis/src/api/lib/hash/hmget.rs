use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldValue, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HmgetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hmget,
    "Returns the values of all specified fields in a hash",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HMGET`
/// https://redis.io/docs/latest/commands/hmget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HmgetInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HmgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HmgetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HmgetInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HmgetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.fields);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!(
                "HMGET requires at least 2 arguments (key + fields), given {}",
                args.len()
            )));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[1..].to_vec() })
    }
}

/// Output for Redis HMGET command
///
/// Returns the values of specified fields in the same order as requested.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HmgetOutput {
    /// Values for each field in the same order as requested
    values: Vec<FieldValue>,
}

impl HmgetOutput {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }

    /// Get the values
    pub fn values(&self) -> &[FieldValue] {
        &self.values
    }

    /// Get value at a specific index
    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Decode the Redis protocol response into a HmgetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::BulkString(data) => {
                            let s = String::from_utf8(data).map_err(|e| EpError::parse(e.to_string()))?;
                            Ok(FieldValue::Value(s.into()))
                        }
                        Resp2Frame::Null => Ok(FieldValue::NotFound),
                        other => Err(EpError::parse(format!("unexpected value in HMGET response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HMGET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::BlobString { data, .. } => {
                            let s = String::from_utf8(data).map_err(|e| EpError::parse(e.to_string()))?;
                            Ok(FieldValue::Value(s.into()))
                        }
                        Resp3Frame::SimpleString { data, .. } => {
                            Ok(FieldValue::Value(String::from_utf8(data).map_err(EpError::parse)?.into()))
                        }
                        Resp3Frame::Null => Ok(FieldValue::NotFound),
                        other => Err(EpError::parse(format!("unexpected value in HMGET response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HMGET response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for HmgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HmgetOutput", 1)?;
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
            let input = HmgetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into()), RedisJsonValue::String("field2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HMGET"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("field1"));
            assert!(cmd_str.contains("field2"));
        }

        #[test]
        fn test_encode_command_single_field() {
            let input = HmgetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HMGET"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_decode_output_all_values() {
            let output = HmgetOutput::decode(b"*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.values()[0], FieldValue::Value("value1".into()));
            assert_eq!(output.values()[1], FieldValue::Value("value2".into()));
        }

        #[test]
        fn test_decode_output_with_nil() {
            let output = HmgetOutput::decode(b"*3\r\n$6\r\nvalue1\r\n$-1\r\n$6\r\nvalue3\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.values()[0], FieldValue::Value("value1".into()));
            assert_eq!(output.values()[1], FieldValue::NotFound);
            assert_eq!(output.values()[2], FieldValue::Value("value3".into()));
        }

        #[test]
        fn test_decode_output_all_nil() {
            let output = HmgetOutput::decode(b"*2\r\n$-1\r\n$-1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.values()[0], FieldValue::NotFound);
            assert_eq!(output.values()[1], FieldValue::NotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HmgetOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let input = HmgetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = HmgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HmgetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_get_method() {
            let output = HmgetOutput::new(vec![FieldValue::Value("v1".into()), FieldValue::NotFound]);
            assert_eq!(output.get(0), Some(&FieldValue::Value("v1".into())));
            assert_eq!(output.get(1), Some(&FieldValue::NotFound));
            assert_eq!(output.get(2), None);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmget_existing_fields() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhmget_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hmget_test".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HmgetInput {
                                key: RedisKey::String("hmget_test".into()),
                                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HmgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.values()[0], FieldValue::Value("v1".into()));
                    assert_eq!(output.values()[1], FieldValue::Value("v2".into()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmget_mixed_existing_and_missing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhmget_mixed\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hmget_mixed".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("exists".into()),
                                RedisJsonValue::String("value".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HmgetInput {
                                key: RedisKey::String("hmget_mixed".into()),
                                fields: vec![RedisJsonValue::String("exists".into()), RedisJsonValue::String("missing".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HmgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.values()[0], FieldValue::Value("value".into()));
                    assert_eq!(output.values()[1], FieldValue::NotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmget_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nhmget_nonexistent\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HmgetInput {
                                key: RedisKey::String("hmget_nonexistent".into()),
                                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HmgetOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.values()[0], FieldValue::NotFound);
                    assert_eq!(output.values()[1], FieldValue::NotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmget_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhmget_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hmget_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HmgetInput {
                        key: RedisKey::String("hmget_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HmgetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            assert_eq!(output.values()[0], FieldValue::Value("v".into()));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmget_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhmget_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hmget_r3".into()),
                    fields: vec![Field::new(
                        RedisJsonValue::String("field".into()),
                        RedisJsonValue::String("value".into()),
                    )],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HmgetInput {
                        key: RedisKey::String("hmget_r3".into()),
                        fields: vec![RedisJsonValue::String("field".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HmgetOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);
            assert_eq!(output.values()[0], FieldValue::Value("value".into()));

            ctx.stop().await;
        }
    }
}
