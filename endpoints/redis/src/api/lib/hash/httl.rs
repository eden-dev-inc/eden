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

const API_INFO: ApiInfo<RedisApi, HttlInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Httl, "Returns the TTL in seconds of a hash field", ReqType::Read, true);

/// See official Redis documentation for `HTTL`
/// https://redis.io/docs/latest/commands/httl/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HttlInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HttlInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HttlInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HttlInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HttlInput {
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
        // HTTL key FIELDS numfields field [field ...]
        if args.len() < 4 {
            return Err(EpError::request(format!("HTTL requires at least 4 arguments, given {}", args.len())));
        }

        // Validate FIELDS keyword at position 1
        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request(format!("HTTL expects FIELDS keyword at position 1, got {}", s)));
            }
        } else {
            return Err(EpError::request("HTTL expects FIELDS keyword at position 1"));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[3..].to_vec() })
    }
}

/// TTL result for a single hash field
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub enum FieldTtl {
    /// Field does not exist (-2)
    FieldNotFound,
    /// Field exists but has no TTL (-1)
    NoExpire,
    /// TTL in seconds
    Seconds(i64),
}

/// Output for Redis HTTL command
///
/// Returns the TTL in seconds for each requested hash field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HttlOutput {
    /// TTL results for each field in the same order as requested
    ttls: Vec<FieldTtl>,
}

impl HttlOutput {
    pub fn new(ttls: Vec<FieldTtl>) -> Self {
        Self { ttls }
    }

    /// Get the TTL results
    pub fn ttls(&self) -> &[FieldTtl] {
        &self.ttls
    }

    /// Get TTL for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldTtl> {
        self.ttls.get(index)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.ttls.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.ttls.is_empty()
    }

    fn parse_ttl(value: i64) -> FieldTtl {
        match value {
            -2 => FieldTtl::FieldNotFound,
            -1 => FieldTtl::NoExpire,
            n => FieldTtl::Seconds(n),
        }
    }

    /// Decode the Redis protocol response into a HttlOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let ttls = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_ttl(i)),
                        other => Err(EpError::parse(format!("unexpected value in HTTL response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HTTL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_ttl(data)),
                        other => Err(EpError::parse(format!("unexpected value in HTTL response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HTTL response: {:?}", other)));
                }
            },
        };

        Ok(Self { ttls })
    }
}

impl Serialize for HttlOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HttlOutput", 1)?;
        state.serialize_field("ttls", &self.ttls)?;
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
            let input = HttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HTTL"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HTTL"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_with_ttls() {
            let output = HttlOutput::decode(b"*2\r\n:60\r\n:-1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.get(0), Some(&FieldTtl::Seconds(60)));
            assert_eq!(output.get(1), Some(&FieldTtl::NoExpire));
        }

        #[test]
        fn test_decode_field_not_found() {
            let output = HttlOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldTtl::FieldNotFound);
        }

        #[test]
        fn test_decode_no_expire() {
            let output = HttlOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldTtl::NoExpire);
        }

        #[test]
        fn test_decode_positive_ttl() {
            let output = HttlOutput::decode(b"*1\r\n:3600\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldTtl::Seconds(3600));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HttlOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
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
            let input = HttlInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.fields.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("FIELDS".into()),
                RedisJsonValue::Integer(1),
            ];
            let err = HttlInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_empty_results() {
            let output = HttlOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: HTTL requires Redis 7.4+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_field_with_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhttl_hash\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("httl_hash".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set field expiry: HEXPIRE key seconds FIELDS numfields field
                    ctx.raw(b"*6\r\n$7\r\nHEXPIRE\r\n$9\r\nhttl_hash\r\n$2\r\n60\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$6\r\nfield1\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HttlInput {
                                key: RedisKey::String("httl_hash".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);

                    match &output.ttls()[0] {
                        FieldTtl::Seconds(s) => assert!(*s > 0 && *s <= 60),
                        other => panic!("Expected Seconds, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_field_no_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhttl_no_expire\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("httl_no_expire".into()),
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
                            &HttlInput {
                                key: RedisKey::String("httl_no_expire".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.ttls()[0], FieldTtl::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhttl_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("httl_missing".into()),
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
                            &HttlInput {
                                key: RedisKey::String("httl_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.ttls()[0], FieldTtl::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_multiple_fields() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhttl_multi\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("httl_multi".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set TTL on f1 only
                    ctx.raw(b"*6\r\n$7\r\nHEXPIRE\r\n$10\r\nhttl_multi\r\n$3\r\n120\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$2\r\nf1\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HttlInput {
                                key: RedisKey::String("httl_multi".into()),
                                fields: vec![
                                    RedisJsonValue::String("f1".into()),
                                    RedisJsonValue::String("f2".into()),
                                    RedisJsonValue::String("missing".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);

                    match &output.ttls()[0] {
                        FieldTtl::Seconds(s) => assert!(*s > 0 && *s <= 120),
                        other => panic!("Expected Seconds for f1, got {:?}", other),
                    }

                    assert_eq!(output.ttls()[1], FieldTtl::NoExpire);
                    assert_eq!(output.ttls()[2], FieldTtl::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhttl_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("httl_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HttlInput {
                        key: RedisKey::String("httl_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HttlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_httl_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhttl_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("httl_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HttlInput {
                        key: RedisKey::String("httl_r3".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HttlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
