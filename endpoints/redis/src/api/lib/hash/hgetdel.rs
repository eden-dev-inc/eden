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

const API_INFO: ApiInfo<RedisApi, HgetdelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hgetdel,
    "Returns the value of a field and deletes it from the hash",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HGETDEL`
/// https://redis.io/docs/latest/commands/hgetdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HgetdelInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HgetdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HgetdelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(HgetdelInput, API_INFO, { key, fields });

impl RedisCommandInput for HgetdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg("FIELDS").arg(self.fields.len()).arg(&self.fields);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        // HGETDEL key FIELDS numfields field [field ...]
        if args.len() < 4 {
            return Err(EpError::request(format!("HGETDEL requires at least 4 arguments, given {}", args.len())));
        }

        // Expect "FIELDS" at index 1
        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request("HGETDEL expects FIELDS keyword"));
            }
        } else {
            return Err(EpError::request("HGETDEL expects FIELDS keyword"));
        }

        // Parse field count at index 2
        let num_fields = match &args[2] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("Invalid field count"))?,
            _ => return Err(EpError::parse("Field count must be a number")),
        };

        // Verify field count matches
        if args.len() - 3 != num_fields {
            return Err(EpError::request(format!("HGETDEL expected {} fields, got {}", num_fields, args.len() - 3)));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[3..].to_vec() })
    }
}

/// Output for Redis HGETDEL command
///
/// Returns the values of fields before deletion for each requested field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HgetdelOutput {
    values: Vec<FieldValue>,
}

impl HgetdelOutput {
    pub fn new(values: Vec<FieldValue>) -> Self {
        Self { values }
    }

    /// Get the values
    pub fn values(&self) -> &[FieldValue] {
        &self.values
    }

    /// Get value for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldValue> {
        self.values.get(index)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    fn parse_value(frame: &Resp2Frame) -> Result<FieldValue, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(FieldValue::Value(RedisJsonValue::String(String::from_utf8_lossy(data).to_string()))),
            Resp2Frame::Null => Ok(FieldValue::NotFound),
            other => Err(EpError::parse(format!("unexpected HGETDEL value: {:?}", other))),
        }
    }

    fn parse_value_resp3(frame: Resp3Frame) -> Result<FieldValue, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => {
                Ok(FieldValue::Value(RedisJsonValue::String(String::from_utf8_lossy(&data).to_string())))
            }
            Resp3Frame::Null => Ok(FieldValue::NotFound),
            other => Err(EpError::parse(format!("unexpected HGETDEL value: {:?}", other))),
        }
    }

    /// Decode the Redis protocol response into a HgetdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.iter().map(Self::parse_value).collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HGETDEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.into_iter().map(Self::parse_value_resp3).collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HGETDEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for HgetdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HgetdelOutput", 1)?;
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
        fn test_encode_command_single_field() {
            let input = HgetdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HGETDEL"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HgetdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_with_value() {
            // *1\r\n$5\r\nvalue\r\n
            let output = HgetdelOutput::decode(b"*1\r\n$5\r\nvalue\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("value".into())));
        }

        #[test]
        fn test_decode_output_not_found() {
            // *1\r\n$-1\r\n (array with nil)
            let output = HgetdelOutput::decode(b"*1\r\n$-1\r\n").unwrap();
            assert_eq!(output.values()[0], FieldValue::NotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HgetdelOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HgetdelInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("FIELDS".into())];
            let err = HgetdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 4 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::hash::Field;
        use crate::api::{HexistsInput, HexistsOutput, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetdel_existing_field() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhgetdel_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetdel_test".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HgetdelInput {
                                key: RedisKey::String("hgetdel_test".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("value1".into())));

                    // Verify field is deleted
                    let verify = ctx
                        .raw(
                            &HexistsInput {
                                key: RedisKey::String("hgetdel_test".into()),
                                field: RedisJsonValue::String("field1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HexistsOutput::decode(&verify).expect("decode failed");
                    assert!(!verify_output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetdel_nonexistent_field() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhgetdel_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetdel_missing".into()),
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
                            &HgetdelInput {
                                key: RedisKey::String("hgetdel_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values()[0], FieldValue::NotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetdel_multiple_fields() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhgetdel_mult\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetdel_mult".into()),
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
                            &HgetdelInput {
                                key: RedisKey::String("hgetdel_mult".into()),
                                fields: vec![
                                    RedisJsonValue::String("f1".into()),
                                    RedisJsonValue::String("missing".into()),
                                    RedisJsonValue::String("f2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HgetdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert_eq!(output.values()[0], FieldValue::Value(RedisJsonValue::String("v1".into())));
                    assert_eq!(output.values()[1], FieldValue::NotFound);
                    assert_eq!(output.values()[2], FieldValue::Value(RedisJsonValue::String("v2".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetdel_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhgetdel_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hgetdel_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HgetdelInput {
                        key: RedisKey::String("hgetdel_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HgetdelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
