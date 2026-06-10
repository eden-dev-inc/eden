use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldExpiretime, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HexpiretimeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hexpiretime,
    "Returns the expiration time of a hash field as a Unix timestamp, in seconds",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HEXPIRETIME`
/// https://redis.io/docs/latest/commands/hexpiretime/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HexpiretimeInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HexpiretimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HexpiretimeInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(HexpiretimeInput, API_INFO, { key, fields });

impl RedisCommandInput for HexpiretimeInput {
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
            return Err(EpError::request(format!("HEXPIRETIME requires at least 4 arguments, given {}", args.len())));
        }

        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request(format!("HEXPIRETIME expects FIELDS keyword at position 1, got {}", s)));
            }
        } else {
            return Err(EpError::request("HEXPIRETIME expects FIELDS keyword at position 1"));
        }

        Ok(Self { key: args[0].clone().try_into()?, fields: args[3..].to_vec() })
    }
}

/// Output for Redis HEXPIRETIME command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HexpiretimeOutput {
    times: Vec<FieldExpiretime>,
}

impl HexpiretimeOutput {
    pub fn new(times: Vec<FieldExpiretime>) -> Self {
        Self { times }
    }

    pub fn times(&self) -> &[FieldExpiretime] {
        &self.times
    }

    pub fn get(&self, index: usize) -> Option<&FieldExpiretime> {
        self.times.get(index)
    }

    pub fn len(&self) -> usize {
        self.times.len()
    }

    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    fn parse_time(value: i64) -> FieldExpiretime {
        match value {
            -2 => FieldExpiretime::FieldNotFound,
            -1 => FieldExpiretime::NoExpire,
            n => FieldExpiretime::Timestamp(n),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let times = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_time(i)),
                        other => Err(EpError::parse(format!("unexpected HEXPIRETIME value: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIRETIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_time(data)),
                        other => Err(EpError::parse(format!("unexpected HEXPIRETIME value: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIRETIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { times })
    }
}

impl Serialize for HexpiretimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HexpiretimeOutput", 1)?;
        state.serialize_field("times", &self.times)?;
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
            let input = HexpiretimeInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HEXPIRETIME"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HexpiretimeInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_timestamp() {
            let output = HexpiretimeOutput::decode(b"*1\r\n:1893456000\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.times()[0], FieldExpiretime::Timestamp(1893456000));
        }

        #[test]
        fn test_decode_output_no_expire() {
            let output = HexpiretimeOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.times()[0], FieldExpiretime::NoExpire);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HexpiretimeOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.times()[0], FieldExpiretime::FieldNotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HexpiretimeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HexpiretimeInput {
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
            let err = HexpiretimeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 4 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::hash::Field;
        use crate::api::{HexpireInput, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpiretime_with_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhexpiretime_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpiretime_test".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &HexpireInput {
                            key: RedisKey::String("hexpiretime_test".into()),
                            seconds: RedisJsonValue::Integer(3600),
                            options: None,
                            fields: vec![RedisJsonValue::String("field1".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HexpiretimeInput {
                                key: RedisKey::String("hexpiretime_test".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    match &output.times()[0] {
                        FieldExpiretime::Timestamp(ts) => assert!(*ts > 0),
                        other => panic!("Expected Timestamp, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpiretime_no_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nhexpiretime_noexp\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpiretime_noexp".into()),
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
                            &HexpiretimeInput {
                                key: RedisKey::String("hexpiretime_noexp".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.times()[0], FieldExpiretime::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpiretime_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhexpiretime_miss\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpiretime_miss".into()),
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
                            &HexpiretimeInput {
                                key: RedisKey::String("hexpiretime_miss".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.times()[0], FieldExpiretime::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpiretime_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhexpiretime_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hexpiretime_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HexpiretimeInput {
                        key: RedisKey::String("hexpiretime_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HexpiretimeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
