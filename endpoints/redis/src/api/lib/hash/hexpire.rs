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

const API_INFO: ApiInfo<RedisApi, HexpireInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hexpire,
    "Set expiry for hash field using relative time to expire (seconds)",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HEXPIRE`
/// https://redis.io/docs/latest/commands/hexpire/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HexpireInput {
    pub(crate) key: RedisKey,
    pub(crate) seconds: RedisJsonValue,
    pub(crate) options: Option<Options>,
    pub(crate) fields: Vec<RedisJsonValue>,
}

impl Serialize for HexpireInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 4; // type, key, seconds, fields
        if self.options.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("HexpireInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("seconds", &self.seconds)?;
        if let Some(options) = &self.options {
            state.serialize_field("options", options)?;
        }
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl_redis_operation!(
    HexpireInput,
    API_INFO,
    {key, seconds, options, fields}
);

impl RedisCommandInput for HexpireInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.seconds);

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
        // HEXPIRE key seconds [NX | XX | GT | LT] FIELDS numfields field [field ...]
        if args.len() < 5 {
            return Err(EpError::request(format!("HEXPIRE requires at least 5 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let seconds = args[1].clone();
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

        Ok(Self { key, seconds, options, fields })
    }
}

/// Output for Redis HEXPIRE command
///
/// Returns the result of the expire operation for each field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HexpireOutput {
    /// Results for each field in the same order as requested
    results: Vec<FieldExpireResult>,
}

impl HexpireOutput {
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
            _ => FieldExpireResult::ConditionNotMet, // Default for unknown values
        }
    }

    /// Decode the Redis protocol response into a HexpireOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::Integer(i) => Ok(Self::parse_result(i)),
                        other => Err(EpError::parse(format!("unexpected value in HEXPIRE response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIRE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::Number { data, .. } => Ok(Self::parse_result(data)),
                        other => Err(EpError::parse(format!("unexpected value in HEXPIRE response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HEXPIRE response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for HexpireOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HexpireOutput", 1)?;
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
            let input = HexpireInput {
                key: RedisKey::String("myhash".into()),
                seconds: RedisJsonValue::Integer(60),
                options: None,
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HEXPIRE"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("60"));
            assert!(cmd_str.contains("FIELDS"));
            assert!(cmd_str.contains("field1"));
        }

        #[test]
        fn test_encode_command_with_nx_option() {
            let input = HexpireInput {
                key: RedisKey::String("myhash".into()),
                seconds: RedisJsonValue::Integer(60),
                options: Some(Options::NX),
                fields: vec![RedisJsonValue::String("field1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NX"));
        }

        #[test]
        fn test_encode_command_multiple_fields() {
            let input = HexpireInput {
                key: RedisKey::String("myhash".into()),
                seconds: RedisJsonValue::Integer(120),
                options: None,
                fields: vec![RedisJsonValue::String("f1".into()), RedisJsonValue::String("f2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("f1"));
            assert!(cmd_str.contains("f2"));
        }

        #[test]
        fn test_decode_output_success() {
            // *1\r\n:1\r\n (array with one integer = 1)
            let output = HexpireOutput::decode(b"*1\r\n:1\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.results()[0], FieldExpireResult::ExpirationSet);
        }

        #[test]
        fn test_decode_output_field_not_found() {
            let output = HexpireOutput::decode(b"*1\r\n:-2\r\n").unwrap();
            assert_eq!(output.results()[0], FieldExpireResult::FieldNotFound);
        }

        #[test]
        fn test_decode_output_condition_not_met() {
            let output = HexpireOutput::decode(b"*1\r\n:0\r\n").unwrap();
            assert_eq!(output.results()[0], FieldExpireResult::ConditionNotMet);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HexpireOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HexpireInput {
                key: RedisKey::String("myhash".into()),
                seconds: RedisJsonValue::Integer(60),
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
                RedisJsonValue::Integer(60),
                RedisJsonValue::String("FIELDS".into()),
            ];
            let err = HexpireInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 5 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::hash::Field;
        use crate::api::{FieldTtl, HsetInput, HttlInput, HttlOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpire_set_expiry() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhexpire_set\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpire_set".into()),
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
                            &HexpireInput {
                                key: RedisKey::String("hexpire_set".into()),
                                seconds: RedisJsonValue::Integer(60),
                                options: None,
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpireOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.results()[0], FieldExpireResult::ExpirationSet);

                    // Verify with HTTL
                    let verify = ctx
                        .raw(
                            &HttlInput {
                                key: RedisKey::String("hexpire_set".into()),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let verify_output = HttlOutput::decode(&verify).expect("decode failed");
                    match &verify_output.ttls()[0] {
                        FieldTtl::Seconds(s) => assert!(*s > 0 && *s <= 60),
                        other => panic!("Expected Seconds, got {:?}", other),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpire_nx_option() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhexpire_nx\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpire_nx".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // First call with NX should succeed
                    let result1 = ctx
                        .raw(
                            &HexpireInput {
                                key: RedisKey::String("hexpire_nx".into()),
                                seconds: RedisJsonValue::Integer(60),
                                options: Some(Options::NX),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output1 = HexpireOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.results()[0], FieldExpireResult::ExpirationSet);

                    // Second call with NX should fail (condition not met)
                    let result2 = ctx
                        .raw(
                            &HexpireInput {
                                key: RedisKey::String("hexpire_nx".into()),
                                seconds: RedisJsonValue::Integer(120),
                                options: Some(Options::NX),
                                fields: vec![RedisJsonValue::String("field1".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output2 = HexpireOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.results()[0], FieldExpireResult::ConditionNotMet);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpire_field_not_found() {
            test_all_protocols_min_version("7.4", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhexpire_missing\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexpire_missing".into()),
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
                            &HexpireInput {
                                key: RedisKey::String("hexpire_missing".into()),
                                seconds: RedisJsonValue::Integer(60),
                                options: None,
                                fields: vec![RedisJsonValue::String("nonexistent".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexpireOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results()[0], FieldExpireResult::FieldNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexpire_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhexpire_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hexpire_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HexpireInput {
                        key: RedisKey::String("hexpire_r2".into()),
                        seconds: RedisJsonValue::Integer(60),
                        options: None,
                        fields: vec![RedisJsonValue::String("f".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HexpireOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
