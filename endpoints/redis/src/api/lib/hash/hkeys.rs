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

const API_INFO: ApiInfo<RedisApi, HkeysInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hkeys, "Returns all field names in a hash", ReqType::Read, true);

/// See official Redis documentation for `HKEYS`
/// https://redis.io/docs/latest/commands/hkeys/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HkeysInput {
    pub(crate) key: RedisKey,
}

impl Serialize for HkeysInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HkeysInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(HkeysInput, API_INFO, { key });

impl RedisCommandInput for HkeysInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("HKEYS requires exactly 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis HKEYS command
///
/// Returns all field names in the hash.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HkeysOutput {
    /// List of field names in the hash
    fields: Vec<String>,
}

impl HkeysOutput {
    pub fn new(fields: Vec<String>) -> Self {
        Self { fields }
    }

    /// Get the field names
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// Get the number of fields
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the hash is empty
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Check if a field exists
    pub fn contains(&self, field: &str) -> bool {
        self.fields.iter().any(|f| f == field)
    }

    /// Decode the Redis protocol response into a HkeysOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let fields = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(|e| EpError::parse(e.to_string())),
                        other => Err(EpError::parse(format!("unexpected value in HKEYS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HKEYS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(|e| EpError::parse(e.to_string())),
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse),
                        other => Err(EpError::parse(format!("unexpected value in HKEYS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HKEYS response: {:?}", other)));
                }
            },
        };

        Ok(Self { fields })
    }
}

impl Serialize for HkeysOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HkeysOutput", 1)?;
        state.serialize_field("fields", &self.fields)?;
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
            let input = HkeysInput { key: RedisKey::String("myhash".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HKEYS"));
            assert!(cmd_str.contains("myhash"));
        }

        #[test]
        fn test_decode_output_multiple_fields() {
            let output = HkeysOutput::decode(b"*3\r\n$6\r\nfield1\r\n$6\r\nfield2\r\n$6\r\nfield3\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.contains("field1"));
            assert!(output.contains("field2"));
            assert!(output.contains("field3"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = HkeysOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_single_field() {
            let output = HkeysOutput::decode(b"*1\r\n$4\r\nname\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.fields()[0], "name");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HkeysOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = HkeysInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = HkeysInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let err = HkeysInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HkeysInput { key: RedisKey::String("myhash".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_contains_method() {
            let output = HkeysOutput::new(vec!["field1".to_string(), "field2".to_string()]);
            assert!(output.contains("field1"));
            assert!(output.contains("field2"));
            assert!(!output.contains("field3"));
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
        async fn test_hkeys_existing_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhkeys_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hkeys_test".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("name".into()), RedisJsonValue::String("Alice".into())),
                                Field::new(RedisJsonValue::String("age".into()), RedisJsonValue::String("30".into())),
                                Field::new(RedisJsonValue::String("city".into()), RedisJsonValue::String("NYC".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HkeysInput { key: RedisKey::String("hkeys_test".into()) }.command()).await.expect("raw failed");

                    let output = HkeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert!(output.contains("name"));
                    assert!(output.contains("age"));
                    assert!(output.contains("city"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hkeys_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nhkeys_nonexistent\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&HkeysInput { key: RedisKey::String("hkeys_nonexistent".into()) }.command()).await.expect("raw failed");

                    let output = HkeysOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hkeys_single_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhkeys_single\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hkeys_single".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("only_field".into()),
                                RedisJsonValue::String("value".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HkeysInput { key: RedisKey::String("hkeys_single".into()) }.command()).await.expect("raw failed");

                    let output = HkeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.fields()[0], "only_field");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hkeys_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhkeys_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hkeys_r2".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                        Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HkeysInput { key: RedisKey::String("hkeys_r2".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HkeysOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 2);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hkeys_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhkeys_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hkeys_r3".into()),
                    fields: vec![Field::new(
                        RedisJsonValue::String("field".into()),
                        RedisJsonValue::String("value".into()),
                    )],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HkeysInput { key: RedisKey::String("hkeys_r3".into()) }.command()).await.expect("raw failed");

            let output = HkeysOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
