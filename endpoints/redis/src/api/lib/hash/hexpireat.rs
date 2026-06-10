use crate::api::lib::hash::Options;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{FieldExpireResult, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, HexpireatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hexpireat,
    "Set expiry for hash field using an absolute Unix timestamp (seconds)",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HEXPIREAT`
/// https://redis.io/docs/latest/commands/hexpireat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HexpireatInput {
    pub(crate) key: RedisKey,
    pub(crate) unix_time_seconds: RedisJsonValue,
    pub(crate) options: Option<Options>,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HexpireatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 4; // type, key, unix_time_seconds, fields
        if self.options.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("HexpireatInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("unix_time_seconds", &self.unix_time_seconds)?;
        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HexpireatInput,
    API_INFO,
    {key, unix_time_seconds, options, fields}
);

impl RedisCommandInput for HexpireatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.unix_time_seconds);

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
        // HEXPIREAT key unix-time-seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
        if args.len() < 5 {
            return Err(EpError::request(format!("HEXPIREAT requires at least 5 arguments, found {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let unix_time_seconds = args[1].clone();
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

        Ok(Self { key, unix_time_seconds, options, fields })
    }
}

/// Output for Redis HEXPIREAT command
///
/// Returns the result of the expire operation for each field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HexpireatOutput {
    /// Results for each field in the same order as requested
    results: Vec<FieldExpireResult>,
}

impl HexpireatOutput {
    pub fn new(results: Vec<FieldExpireResult>) -> Self {
        Self { results }
    }

    /// Get the results
    pub fn results(&self) -> &[FieldExpireResult] {
        &self.results
    }

    /// Get result for a specific field by index
    pub fn get(&self, index: usize) -> Option<&FieldExpireResult> {
        self.results.get(index)
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    fn parse_result(value: i64) -> FieldExpireResult {
        match value {
            -2 => FieldExpireResult::FieldNotFound,
            0 => FieldExpireResult::ConditionNotMet,
            1 => FieldExpireResult::ExpirationSet,
            2 => FieldExpireResult::ExpirationDeleted,
            _ => FieldExpireResult::ConditionNotMet,
        }
    }

    /// Decode the Redis protocol response into a HexpireatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_result(i)),
                        other => Err(EpError::parse(format!("unexpected value in HEXPIREAT response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIREAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_result(data)),
                        other => Err(EpError::parse(format!("unexpected value in HEXPIREAT response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIREAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for HexpireatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HexpireatOutput", 1)?;
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
            let input = HexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_seconds: RedisJsonValue::Integer(1893456000), // 2030-01-01
                options: None,
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HEXPIREAT"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("1893456000"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_with_gt_option() {
            let input = HexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_seconds: RedisJsonValue::Integer(1893456000),
                options: Some(Options::GT),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GT"));
        }

        #[test]
        fn test_decode_output_success() {
            let output = HexpireatOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.results()[0], FieldExpireResult::ExpirationSet);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HexpireatOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.results()[0], FieldExpireResult::FieldNotFound);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HexpireatOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HexpireatInput {
                key: RedisKey::String("myhash".into()),
                unix_time_seconds: RedisJsonValue::Integer(1893456000),
                options: None,
                fields: vec![RedisJsonValue::String("f".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1893456000),
                RedisJsonValue::String("FIELDS".into()),
            ];
            let err = HexpireatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 5 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::hash::{Field, FieldExpireResult, FieldExpiretime, HexpiretimeInput, HexpiretimeOutput, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;
        use std::time::{SystemTime, UNIX_EPOCH};

        fn future_timestamp(seconds_from_now: u64) -> i64 {
            let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
            (now + seconds_from_now) as i64
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpireat_set_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhexpireat_set\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpireat_set".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let expire_at = future_timestamp(60);
                    let result = ctx
                        .raw(
                            &HexpireatInput {
                                key: RedisKey::String("hexpireat_set".into()),
                                unix_time_seconds: RedisJsonValue::Integer(expire_at),
                                options: None,
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpireatOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.results()[0], FieldExpireResult::ExpirationSet);

                    // Verify with HEXPIRETIME
                    let verify = ctx
                        .raw(
                            &HexpiretimeInput {
                                key: RedisKey::String("hexpireat_set".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HexpiretimeOutput::decode(&verify).expect("decode failed");
                    match verify_output.times().first() {
                        Some(FieldExpiretime::Timestamp(ts)) => {
                            assert!(ts > &0_i64);
                            assert!((ts - expire_at).abs() <= 1);
                        }
                        other => panic!("Expected Timestamp, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpireat_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nhexpireat_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpireat_missing".into()),
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
                            &HexpireatInput {
                                key: RedisKey::String("hexpireat_missing".into()),
                                unix_time_seconds: RedisJsonValue::Integer(future_timestamp(60)),
                                options: None,
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpireatOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], FieldExpireResult::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpireat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nhexpireat_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hexpireat_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HexpireatInput {
                        key: RedisKey::String("hexpireat_r2".into()),
                        unix_time_seconds: RedisJsonValue::Integer(future_timestamp(60)),
                        options: None,
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HexpireatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
