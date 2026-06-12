use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldExpireTime, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HpttlInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hpttl,
    "Returns the TTL in milliseconds of a hash field",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HPTTL`
/// https://redis.io/docs/latest/commands/hpttl/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HpttlInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HpttlInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HpttlInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HpttlInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HpttlInput {
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
        // HPTTL key FIELDS numfields field [field ...]
        if args.len() < 4 {
            return Err(EpError::request(format!("HPTTL requires at least 4 arguments, given {}", args.len())));
        }

        // Validate FIELDS keyword at position 1
        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request(format!("HPTTL expects FIELDS keyword at position 1, got {}", s)));
            }
        } else {
            return Err(EpError::request("HPTTL expects FIELDS keyword at position 1"));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[3..].to_vec() })
    }
}

/// Output for Redis HPTTL command
///
/// Returns the TTL in milliseconds for each requested hash field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HpttlOutput {
    /// TTL results for each field in the same order as requested
    ttls: Vec<FieldExpireTime>,
}

impl HpttlOutput {
    pub fn new(ttls: Vec<FieldExpireTime>) -> Self {
        Self { ttls }
    }

    /// Get the TTL results
    pub fn ttls(&self) -> &[FieldExpireTime] {
        &self.ttls
    }

    /// Get TTL for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldExpireTime> {
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

    fn parse_ttl(value: i64) -> FieldExpireTime {
        match value {
            -2 => FieldExpireTime::FieldNotFound,
            -1 => FieldExpireTime::NoExpire,
            n => FieldExpireTime::UnixTimeMillis(n),
        }
    }

    /// Decode the Redis protocol response into a HpttlOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let ttls = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_ttl(i)),
                        other => Err(EpError::parse(format!("unexpected value in HPTTL response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HPTTL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_ttl(data)),
                        other => Err(EpError::parse(format!("unexpected value in HPTTL response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HPTTL response: {:?}", other)));
                }
            },
        };

        Ok(Self { ttls })
    }
}

impl Serialize for HpttlOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HpttlOutput", 1)?;
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
            let input = HpttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPTTL"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HpttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPTTL"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_with_ttls() {
            let output = HpttlOutput::decode(b"*2\r\n:60000\r\n:-1\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.get(0), Some(&FieldExpireTime::UnixTimeMillis(60000)));
            assert_eq!(output.get(1), Some(&FieldExpireTime::NoExpire));
        }

        #[test]
        fn test_decode_field_not_found() {
            let output = HpttlOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldExpireTime::FieldNotFound);
        }

        #[test]
        fn test_decode_no_expire() {
            let output = HpttlOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldExpireTime::NoExpire);
        }

        #[test]
        fn test_decode_positive_ttl() {
            let output = HpttlOutput::decode(b"*1\r\n:3600000\r\n").unwrap();
            assert_eq!(output.ttls()[0], FieldExpireTime::UnixTimeMillis(3600000));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HpttlOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
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
            let input = HpttlInput::decode(args).unwrap();
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
            let err = HpttlInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HpttlInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_empty_results() {
            let output = HpttlOutput::decode(b"*0\r\n").unwrap();
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

        // Note: HPTTL requires Redis 7.4+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_field_with_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhpttl_hash\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpttl_hash".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set field expiry using HPEXPIRE (milliseconds)
                    ctx.raw(b"*6\r\n$8\r\nHPEXPIRE\r\n$10\r\nhpttl_hash\r\n$5\r\n60000\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$6\r\nfield1\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HpttlInput {
                                key: RedisKey::String("hpttl_hash".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);

                    match &output.ttls()[0] {
                        FieldExpireTime::UnixTimeMillis(ms) => assert!(*ms > 0 && *ms <= 60000),
                        other => panic!("Expected UnixTimeMillis, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_field_no_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhpttl_no_expire\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpttl_no_expire".into()),
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
                            &HpttlInput {
                                key: RedisKey::String("hpttl_no_expire".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.ttls()[0], FieldExpireTime::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhpttl_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpttl_missing".into()),
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
                            &HpttlInput {
                                key: RedisKey::String("hpttl_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.ttls()[0], FieldExpireTime::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_multiple_fields() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhpttl_multi\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpttl_multi".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set TTL on f1 only (120 seconds = 120000ms)
                    ctx.raw(b"*6\r\n$8\r\nHPEXPIRE\r\n$11\r\nhpttl_multi\r\n$6\r\n120000\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$2\r\nf1\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HpttlInput {
                                key: RedisKey::String("hpttl_multi".into()),
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

                    let output = HpttlOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);

                    match &output.ttls()[0] {
                        FieldExpireTime::UnixTimeMillis(ms) => assert!(*ms > 0 && *ms <= 120000),
                        other => panic!("Expected UnixTimeMillis for f1, got {:?}", other),
                    }

                    assert_eq!(output.ttls()[1], FieldExpireTime::NoExpire);
                    assert_eq!(output.ttls()[2], FieldExpireTime::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_precision() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhpttl_precision\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpttl_precision".into()),
                            fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set 5 second TTL
                    ctx.raw(b"*6\r\n$8\r\nHPEXPIRE\r\n$15\r\nhpttl_precision\r\n$4\r\n5000\r\n$6\r\nFIELDS\r\n$1\r\n1\r\n$1\r\nf\r\n")
                        .await
                        .expect("raw failed");

                    // Wait 100ms
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    let result = ctx
                        .raw(
                            &HpttlInput {
                                key: RedisKey::String("hpttl_precision".into()),
                                fields: vec![RedisJsonValue::String("f".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpttlOutput::decode(&result).expect("decode failed");

                    match &output.ttls()[0] {
                        FieldExpireTime::UnixTimeMillis(ms) => {
                            assert!(*ms < 5000 && *ms > 4800, "HPTTL should reflect elapsed time, got {}", ms);
                        }
                        other => panic!("Expected UnixTimeMillis, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhpttl_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpttl_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpttlInput {
                        key: RedisKey::String("hpttl_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HpttlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpttl_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nhpttl_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpttl_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpttlInput {
                        key: RedisKey::String("hpttl_r3".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HpttlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
