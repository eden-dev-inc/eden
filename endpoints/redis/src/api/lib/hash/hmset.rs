use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Field, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HmsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hmset,
    "Sets the values of multiple fields. Deprecated: Use HSET with multiple field-value pairs",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HMSET`
/// https://redis.io/docs/latest/commands/hmset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HmsetInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<Field>,
}

impl Serialize for HmsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HmsetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HmsetInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HmsetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        for field in &self.fields {
            command.arg(&field.field).arg(&field.value);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!(
                "HMSET requires at least 3 arguments (key + field/value pairs), given {}",
                args.len()
            )));
        }

        if !(args.len() - 1).is_multiple_of(2) {
            return Err(EpError::request("HMSET requires an even number of field/value arguments after key"));
        }

        let key = args[0].clone().try_into()?;
        let mut fields = Vec::new();

        for chunk in args[1..].chunks(2) {
            if chunk.len() == 2 {
                fields.push(Field { field: chunk[0].clone(), value: chunk[1].clone() });
            }
        }

        Ok(Self { key, fields })
    }
}

/// Output for Redis HMSET command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HmsetOutput {
    /// Always "OK" on success
    status: String,
}

impl HmsetOutput {
    pub fn new(status: String) -> Self {
        Self { status }
    }

    /// Get the status
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    /// Decode the Redis protocol response into a HmsetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let status = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HMSET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HMSET response: {:?}", other)));
                }
            },
        };

        Ok(Self { status })
    }
}

impl Serialize for HmsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HmsetOutput", 1)?;
        state.serialize_field("status", &self.status)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_pair() {
            let input = HmsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![Field::new(
                    RedisJsonValue::String("field1".into()),
                    RedisJsonValue::String("value1".into()),
                )],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HMSET"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("field1"));
            assert!(cmd_str.contains("value1"));
        }

        #[test]
        fn test_encode_command_multiple_pairs() {
            let input = HmsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![
                    Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                    Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HMSET"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("v1"));
            assert!(cmd_str.contains("f2"));
            assert!(cmd_str.contains("v2"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = HmsetOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.status(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HmsetOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("v1".into()),
            ];
            let input = HmsetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.fields.len(), 1);
            assert_eq!(input.fields[0].field, RedisJsonValue::String("f1".into()));
            assert_eq!(input.fields[0].value, RedisJsonValue::String("v1".into()));
        }

        #[test]
        fn test_decode_input_multiple_pairs() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("v1".into()),
                RedisJsonValue::String("f2".into()),
                RedisJsonValue::String("v2".into()),
            ];
            let input = HmsetInput::decode(args).unwrap();
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let err = HmsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_odd_args_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("v1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let err = HmsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("even number"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HmsetInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_field_accessors() {
            let field = Field::new(RedisJsonValue::String("myfield".into()), RedisJsonValue::String("myvalue".into()));
            assert_eq!(field.field(), &RedisJsonValue::String("myfield".into()));
            assert_eq!(field.value(), &RedisJsonValue::String("myvalue".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{FieldValue, HmgetInput, HmgetOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmset_new_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhmset_test\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HmsetInput {
                                key: RedisKey::String("hmset_test".into()),
                                fields: vec![
                                    Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                    Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HmsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify with HMGET
                    let verify = ctx
                        .raw(
                            &HmgetInput {
                                key: RedisKey::String("hmset_test".into()),
                                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HmgetOutput::decode(&verify).expect("decode failed");
                    assert_eq!(verify_output.values()[0], FieldValue::Value("v1".into()));
                    assert_eq!(verify_output.values()[1], FieldValue::Value("v2".into()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmset_update_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhmset_update\r\n").await.expect("raw failed");

                    // Initial set
                    ctx.raw(
                        &HmsetInput {
                            key: RedisKey::String("hmset_update".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("f1".into()),
                                RedisJsonValue::String("original".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Update
                    let result = ctx
                        .raw(
                            &HmsetInput {
                                key: RedisKey::String("hmset_update".into()),
                                fields: vec![Field::new(
                                    RedisJsonValue::String("f1".into()),
                                    RedisJsonValue::String("updated".into()),
                                )],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HmsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify
                    let verify = ctx
                        .raw(
                            &HmgetInput {
                                key: RedisKey::String("hmset_update".into()),
                                fields: vec![RedisJsonValue::String("f1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HmgetOutput::decode(&verify).expect("decode failed");
                    assert_eq!(verify_output.values()[0], FieldValue::Value("updated".into()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhmset_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HmsetInput {
                        key: RedisKey::String("hmset_r2".into()),
                        fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+"), "RESP2 should return simple string");
            let output = HmsetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hmset_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhmset_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HmsetInput {
                        key: RedisKey::String("hmset_r3".into()),
                        fields: vec![
                            Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                            Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                        ],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HmsetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
