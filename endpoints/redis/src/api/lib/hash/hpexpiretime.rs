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

const API_INFO: ApiInfo<RedisApi, HpexpiretimeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hpexpiretime,
    "Returns the expiration time of a hash field as a Unix timestamp, in milliseconds",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `HPEXPIRETIME`
/// https://redis.io/docs/latest/commands/hpexpiretime/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HpexpiretimeInput {
    pub(crate) key: RedisKey,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HpexpiretimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HpexpiretimeInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HpexpiretimeInput,
    API_INFO,
    {key, fields}
);

impl RedisCommandInput for HpexpiretimeInput {
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
            return Err(EpError::request(format!("HPEXPIRETIME requires at least 4 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;

        // Check for "FIELDS" keyword
        if let RedisJsonValue::String(s) = &args[1] {
            if s.to_uppercase() != "FIELDS" {
                return Err(EpError::request("Expected 'FIELDS' keyword"));
            }
        } else {
            return Err(EpError::request("Expected 'FIELDS' keyword"));
        }

        // Parse numfields
        let numfields = match &args[2] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::request("numfields must be an integer"))?,
            _ => return Err(EpError::request("numfields must be an integer")),
        };

        // Parse fields
        let remaining_args = args.len() - 3;
        if remaining_args != numfields {
            return Err(EpError::request(format!("Expected {} fields, found {}", numfields, remaining_args)));
        }

        let fields = args[3..].to_vec();

        Ok(HpexpiretimeInput { key, fields })
    }
}

/// Output for Redis HPEXPIRETIME command
///
/// Returns the expiration time for each requested hash field as Unix timestamps in milliseconds.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HpexpiretimeOutput {
    /// Expiration times for each field in the same order as requested
    times: Vec<FieldExpireTime>,
}

impl HpexpiretimeOutput {
    pub fn new(times: Vec<FieldExpireTime>) -> Self {
        Self { times }
    }

    /// Get the expiration times
    pub fn times(&self) -> &[FieldExpireTime] {
        &self.times
    }

    /// Get expiration time for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldExpireTime> {
        self.times.get(index)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.times.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.times.is_empty()
    }

    fn parse_time(value: i64) -> FieldExpireTime {
        match value {
            -2 => FieldExpireTime::FieldNotFound,
            -1 => FieldExpireTime::NoExpire,
            ts => FieldExpireTime::UnixTimeMillis(ts),
        }
    }

    /// Decode the Redis protocol response into a HpexpiretimeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let times = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_time(i)),
                        other => Err(EpError::parse(format!("unexpected value in HPEXPIRETIME response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HPEXPIRETIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_time(data)),
                        other => Err(EpError::parse(format!("unexpected value in HPEXPIRETIME response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HPEXPIRETIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { times })
    }
}

impl Serialize for HpexpiretimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HpexpiretimeOutput", 1)?;
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
            let input = HpexpiretimeInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPEXPIRETIME"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HpexpiretimeInput {
                key: RedisKey::String("myhash".into()),
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPEXPIRETIME"));
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_with_timestamp() {
            let output = HpexpiretimeOutput::decode(b"*1\r\n:1893456000000\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.times()[0], FieldExpireTime::UnixTimeMillis(1893456000000));
        }

        #[test]
        fn test_decode_output_no_expire() {
            let output = HpexpiretimeOutput::decode(b"*1\r\n:-1\r\n").unwrap();
            assert_eq!(output.times()[0], FieldExpireTime::NoExpire);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HpexpiretimeOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.times()[0], FieldExpireTime::FieldNotFound);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = HpexpiretimeOutput::decode(b"*3\r\n:1893456000000\r\n:-1\r\n:-2\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.times()[0], FieldExpireTime::UnixTimeMillis(1893456000000));
            assert_eq!(output.times()[1], FieldExpireTime::NoExpire);
            assert_eq!(output.times()[2], FieldExpireTime::FieldNotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HpexpiretimeOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
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
            let input = HpexpiretimeInput::decode(args).unwrap();
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
            let err = HpexpiretimeInput::decode(args).unwrap_err();
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
            let err = HpexpiretimeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Expected 3 fields"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HpexpiretimeInput {
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
        use crate::api::lib::hash::Field;
        use crate::api::{HpexpireInput, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn current_timestamp_ms() -> i64 {
            SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_with_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhpexpiretime_exp\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpiretime_exp".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set expiry using HPEXPIRE (60000 ms = 60 seconds)
                    ctx.raw(
                        &HpexpireInput {
                            key: RedisKey::String("hpexpiretime_exp".into()),
                            milliseconds: RedisJsonValue::Integer(60000),
                            options: None,
                            fields: vec![RedisJsonValue::String("field1".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let before_ts = current_timestamp_ms();

                    let result = ctx
                        .raw(
                            &HpexpiretimeInput {
                                key: RedisKey::String("hpexpiretime_exp".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);

                    match &output.times()[0] {
                        FieldExpireTime::UnixTimeMillis(ts) => {
                            // Should be approximately 60 seconds from now
                            let expected_min = before_ts + 59000;
                            let expected_max = before_ts + 61000;
                            assert!(*ts >= expected_min && *ts <= expected_max);
                        }
                        other => panic!("Expected UnixTimeMillis, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_no_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nhpexpiretime_noexp\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpiretime_noexp".into()),
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
                            &HpexpiretimeInput {
                                key: RedisKey::String("hpexpiretime_noexp".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.times()[0], FieldExpireTime::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nhpexpiretime_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpiretime_missing".into()),
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
                            &HpexpiretimeInput {
                                key: RedisKey::String("hpexpiretime_missing".into()),
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.times()[0], FieldExpireTime::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_multiple_fields() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhpexpiretime_mul\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpiretime_mul".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Set expiry on f1 only
                    ctx.raw(
                        &HpexpireInput {
                            key: RedisKey::String("hpexpiretime_mul".into()),
                            milliseconds: RedisJsonValue::Integer(120000),
                            options: None,
                            fields: vec![RedisJsonValue::String("f1".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HpexpiretimeInput {
                                key: RedisKey::String("hpexpiretime_mul".into()),
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

                    let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);

                    match &output.times()[0] {
                        FieldExpireTime::UnixTimeMillis(_) => {}
                        other => panic!("Expected UnixTimeMillis for f1, got {:?}", other),
                    }
                    assert_eq!(output.times()[1], FieldExpireTime::NoExpire);
                    assert_eq!(output.times()[2], FieldExpireTime::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhpexpiretime_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpexpiretime_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpexpiretimeInput {
                        key: RedisKey::String("hpexpiretime_r2".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpiretime_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhpexpiretime_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpexpiretime_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpexpiretimeInput {
                        key: RedisKey::String("hpexpiretime_r3".into()),
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HpexpiretimeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
