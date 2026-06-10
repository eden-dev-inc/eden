use crate::api::lib::hash::Options;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldExpireAtResult, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HpexpireatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hpexpireat,
    "Set expiry for hash field using an absolute Unix timestamp (milliseconds)",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HPEXPIREAT`
/// https://redis.io/docs/latest/commands/hpexpireat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HpexpireatInput {
    pub(crate) key: RedisKey,
    pub(crate) unix_time_milliseconds: RedisJsonValue,
    pub(crate) options: Option<Options>,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HpexpireatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 4; // type, key, unix_time_milliseconds, fields
        if self.options.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("HpexpireatInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("unix_time_milliseconds", &self.unix_time_milliseconds)?;
        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        state.serialize_field("fields", &self.fields)?;

        state.end()
    }
}

impl_redis_operation!(
    HpexpireatInput,
    API_INFO,
    {key, unix_time_milliseconds, options, fields}
);

impl RedisCommandInput for HpexpireatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.unix_time_milliseconds);

        if let Some(options) = &self.options {
            match options {
                Options::NX => command.arg("NX"),
                Options::XX => command.arg("XX"),
                Options::GT => command.arg("GT"),
                Options::LT => command.arg("LT"),
            };
        }
        command.arg("FIELDS").arg(self.fields.len()).arg(&self.fields);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request(format!("HPEXPIREAT requires at least 5 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let unix_time_milliseconds = args[1].clone();
        let mut options = None;
        let mut i = 2;

        // Parse optional condition (NX | XX | GT | LT)
        if let RedisJsonValue::String(s) = &args[i] {
            match s.to_uppercase().as_str() {
                "NX" => {
                    options = Some(Options::NX);
                    i += 1;
                }
                "XX" => {
                    options = Some(Options::XX);
                    i += 1;
                }
                "GT" => {
                    options = Some(Options::GT);
                    i += 1;
                }
                "LT" => {
                    options = Some(Options::LT);
                    i += 1;
                }
                "FIELDS" => {
                    // No condition option, continue to FIELDS parsing
                }
                _ => {
                    return Err(EpError::request(format!("Unknown option: {}", s)));
                }
            }
        }

        // Check for "FIELDS" keyword
        if i >= args.len() || !matches!(&args[i], RedisJsonValue::String(s) if s.to_uppercase() == "FIELDS") {
            return Err(EpError::request("Expected 'FIELDS' keyword"));
        }
        i += 1;

        // Parse numfields
        if i >= args.len() {
            return Err(EpError::request("Missing field count"));
        }

        let numfields = match &args[i] {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::request("Field count must be an integer"))?,
            _ => return Err(EpError::request("Field count must be an integer")),
        };
        i += 1;

        // Parse fields
        let remaining_args = args.len() - i;
        if remaining_args != numfields {
            return Err(EpError::request(format!("Expected {} fields, found {}", numfields, remaining_args)));
        }

        let fields = args[i..].to_vec();

        Ok(HpexpireatInput { key, unix_time_milliseconds, options, fields })
    }
}

/// Output for Redis HPEXPIREAT command
///
/// Returns the result of the expire operation for each field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HpexpireatOutput {
    /// Results for each field in the same order as requested
    results: Vec<FieldExpireAtResult>,
}

impl HpexpireatOutput {
    pub fn new(results: Vec<FieldExpireAtResult>) -> Self {
        Self { results }
    }

    /// Get the results
    pub fn results(&self) -> &[FieldExpireAtResult] {
        &self.results
    }

    /// Get result for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldExpireAtResult> {
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

    fn parse_result(value: i64) -> FieldExpireAtResult {
        match value {
            -2 => FieldExpireAtResult::FieldNotFound,
            0 => FieldExpireAtResult::ConditionNotMet,
            1 => FieldExpireAtResult::ExpirationSet,
            2 => FieldExpireAtResult::ExpirationDeleted,
            _ => FieldExpireAtResult::FieldNotFound,
        }
    }

    /// Decode the Redis protocol response into a HpexpireatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_result(i)),
                        other => Err(EpError::parse(format!("unexpected value in HPEXPIREAT response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HPEXPIREAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_result(data)),
                        other => Err(EpError::parse(format!("unexpected value in HPEXPIREAT response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HPEXPIREAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for HpexpireatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HpexpireatOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = HpexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1893456000000),
                options: None,
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPEXPIREAT"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("1893456000000"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_with_gt_option() {
            let input = HpexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1893456000000),
                options: Some(Options::GT),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HPEXPIREAT"));
            assert!(cmd_str.contains("GT"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HpexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1893456000000),
                options: None,
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_expiration_set() {
            let output = HpexpireatOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.results()[0], FieldExpireAtResult::ExpirationSet);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HpexpireatOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.results()[0], FieldExpireAtResult::FieldNotFound);
        }

        #[test]
        fn test_decode_output_condition_not_met() {
            let output = HpexpireatOutput::decode(b"*1\r\n:0\r\n").unwrap();
            assert_eq!(output.results()[0], FieldExpireAtResult::ConditionNotMet);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = HpexpireatOutput::decode(b"*3\r\n:1\r\n:0\r\n:-2\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.results()[0], FieldExpireAtResult::ExpirationSet);
            assert_eq!(output.results()[1], FieldExpireAtResult::ConditionNotMet);
            assert_eq!(output.results()[2], FieldExpireAtResult::FieldNotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HpexpireatOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid_basic() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1893456000000),
                RedisJsonValue::String("FIELDS".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("f1".into()),
            ];
            let input = HpexpireatInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.unix_time_milliseconds, RedisJsonValue::Integer(1893456000000));
            assert!(input.options.is_none());
            assert_eq!(input.fields.len(), 1);
        }

        #[test]
        fn test_decode_input_with_option() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1893456000000),
                RedisJsonValue::String("XX".into()),
                RedisJsonValue::String("FIELDS".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("f1".into()),
            ];
            let input = HpexpireatInput::decode(args).unwrap();
            assert_eq!(input.options, Some(Options::XX));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1893456000000)];
            let err = HpexpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("HPEXPIREAT requires at least 5 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HpexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_milliseconds: RedisJsonValue::Integer(1893456000000),
                options: None,
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
        use crate::api::lib::hash::{Field, FieldExpireTime, HpexpiretimeInput, HpexpiretimeOutput, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn future_timestamp_ms(seconds_from_now: u64) -> i64 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
            now + (seconds_from_now as i64 * 1000)
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpireat_set_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nhpexpireat_set\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpireat_set".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let future_ts = future_timestamp_ms(120);

                    let result = ctx
                        .raw(
                            &HpexpireatInput {
                                key: RedisKey::String("hpexpireat_set".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(future_ts),
                                options: None,
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpexpireatOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.results()[0], FieldExpireAtResult::ExpirationSet);

                    // Verify with HPEXPIRETIME
                    let verify = ctx
                        .raw(
                            &HpexpiretimeInput {
                                key: RedisKey::String("hpexpireat_set".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HpexpiretimeOutput::decode(&verify).expect("decode failed");
                    match &verify_output.times()[0] {
                        FieldExpireTime::UnixTimeMillis(ts) => {
                            assert!((*ts - future_ts).abs() < 1000);
                        }
                        other => panic!("Expected UnixTimeMillis, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpireat_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nhpexpireat_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hpexpireat_missing".into()),
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
                            &HpexpireatInput {
                                key: RedisKey::String("hpexpireat_missing".into()),
                                unix_time_milliseconds: RedisJsonValue::Integer(future_timestamp_ms(60)),
                                options: None,
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HpexpireatOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], FieldExpireAtResult::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hpexpireat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhpexpireat_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hpexpireat_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HpexpireatInput {
                        key: RedisKey::String("hpexpireat_r2".into()),
                        unix_time_milliseconds: RedisJsonValue::Integer(future_timestamp_ms(300)),
                        options: None,
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HpexpireatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
