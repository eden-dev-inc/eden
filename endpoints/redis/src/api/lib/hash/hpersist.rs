use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldPersistResult, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HpersistInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hpersist,
    "Removes the expiration time for each specified field",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HPERSIST`
/// https://redis.io/docs/latest/commands/hpersist/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HpersistInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HpersistInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HpersistInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HpersistInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HpersistInput {
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
        if args.len() < 4 {
            return Err(EpError::request(format!("HPERSIST requires at least 4 arguments, given {}", args.len())));
        }

        // Parse FIELDS format: KEY FIELDS numfields field1 field2...
        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request("HPERSIST expects FIELDS keyword"));
            }
        } else {
            return Err(EpError::request("HPERSIST expects FIELDS keyword"));
        }

        let num_fields = match &args[2] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("Invalid field count"))?,
            _ => return Err(EpError::parse("Field count must be a number")),
        };

        if args.len() - 3 != num_fields {
            return Err(EpError::request(format!("HPERSIST expected {} fields, got {}", num_fields, args.len() - 3)));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[3..].to_vec() })
    }
}

/// Output for Redis HPERSIST command
///
/// Returns the result of the persist operation for each field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HpersistOutput {
    /// Results for each field in the same order as requested
    results: Vec<FieldPersistResult>,
}

impl HpersistOutput {
    pub fn new(results: Vec<FieldPersistResult>) -> Self {
        Self { results }
    }

    /// Get the results
    pub fn results(&self) -> &[FieldPersistResult] {
        &self.results
    }

    /// Get result for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldPersistResult> {
        self.results.get(index)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    fn parse_result(value: i64) -> FieldPersistResult {
        match value {
            -2 => FieldPersistResult::FieldNotFound,
            -1 => FieldPersistResult::NoExpire,
            1 => FieldPersistResult::Persisted,
            _ => FieldPersistResult::FieldNotFound, // Fallback for unexpected values
        }
    }

    /// Decode the Redis protocol response into a HpersistOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_result(i)),
                        other => Err(EpError::parse(format!("unexpected value in HPERSIST response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HPERSIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_result(data)),
                        other => Err(EpError::parse(format!("unexpected value in HPERSIST response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HPERSIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for HpersistOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HpersistOutput", 1)?;
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
        fn test_encode_command_single_field() {
            let input = HpersistInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPERSIST"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HpersistInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPERSIST"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_persisted() {
            let output = HpersistOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.results()[0], FieldPersistResult::Persisted);
        }

        #[test]
        fn test_decode_output_no_expire() {
            let output = HpersistOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.results()[0], FieldPersistResult::NoExpire);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HpersistOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.results()[0], FieldPersistResult::FieldNotFound);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = HpersistOutput::decode(b"*3\r\n:1\r\n:-1\r\n:-2\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.results()[0], FieldPersistResult::Persisted);
            assert_eq!(output.results()[1], FieldPersistResult::NoExpire);
            assert_eq!(output.results()[2], FieldPersistResult::FieldNotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HpersistOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("FIELDS".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let input = HpersistInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_missing_fields_keyword() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("WRONG".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("f1".into()),
            ];
            let err = HpersistInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("FIELDS"));
        }

        #[test]
        fn test_decode_input_wrong_field_count() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("FIELDS".into()),
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let err = HpersistInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("expected 3 fields"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HpersistInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
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
        async fn test_hpersist_with_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhpersist_exp\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpersist_exp".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set expiry
                    ctx.raw(b"*6\r\n$7\r\nHEXPIRE\r\n$12\r\nhpersist_exp\r\n$2\r\n60\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$6\r\nfield1\r\n")
                        .await
                        .expect("raw failed");

                    // Persist
                    let result = ctx
                        .raw(
                            &HpersistInput {
                                key: RedisKey::String("hpersist_exp".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpersistOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.results()[0], FieldPersistResult::Persisted);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpersist_no_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhpersist_noexp\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpersist_noexp".into()),
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
                            &HpersistInput {
                                key: RedisKey::String("hpersist_noexp".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpersistOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], FieldPersistResult::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpersist_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhpersist_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpersist_missing".into()),
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
                            &HpersistInput {
                                key: RedisKey::String("hpersist_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpersistOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], FieldPersistResult::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpersist_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhpersist_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpersist_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpersistInput {
                        key: RedisKey::String("hpersist_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HpersistOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
