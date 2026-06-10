use crate::api::lib::time_series::common::{TsIgnore, TsLabel, append_labels_to_cmd, parse_labels_from_args};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsAlterInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsAlter,
    "Update the retention, chunk size, duplicate policy, and labels of an existing time series",
    ReqType::Write,
    true,
);

/// Input for Redis `TS.ALTER` command.
///
/// Update settings of an existing time series.
/// Note: ENCODING cannot be altered after creation.
///
/// See official Redis documentation for `TS.ALTER`:
/// https://redis.io/docs/latest/commands/ts.alter/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsAlterInput {
    /// Key name of the time series to alter
    key: RedisKey,
    /// New retention period in milliseconds
    #[builder(default)]
    retention: Option<RedisJsonValue>,
    /// New chunk size in bytes
    #[builder(default)]
    chunk_size: Option<RedisJsonValue>,
    /// New duplicate policy
    #[builder(default)]
    duplicate_policy: Option<RedisJsonValue>,
    /// New ignore parameters
    #[builder(default)]
    ignore: Option<TsIgnore>,
    /// New labels (replaces existing labels)
    #[builder(default)]
    labels: Option<Vec<TsLabel>>,
}

impl Serialize for TsAlterInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, key
        if self.retention.is_some() {
            fields += 1;
        }
        if self.chunk_size.is_some() {
            fields += 1;
        }
        if self.duplicate_policy.is_some() {
            fields += 1;
        }
        if self.ignore.is_some() {
            fields += 1;
        }
        if self.labels.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsAlterInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(v) = &self.retention {
            state.serialize_field("retention", v)?;
        }
        if let Some(v) = &self.chunk_size {
            state.serialize_field("chunk_size", v)?;
        }
        if let Some(v) = &self.duplicate_policy {
            state.serialize_field("duplicate_policy", v)?;
        }
        if let Some(v) = &self.ignore {
            state.serialize_field("ignore", v)?;
        }
        if let Some(v) = &self.labels {
            state.serialize_field("labels", v)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsAlterInput,
    API_INFO,
    {key, retention, chunk_size, duplicate_policy, ignore, labels}
);

impl RedisCommandInput for TsAlterInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        if let Some(v) = &self.retention {
            command.arg("RETENTION").arg(v);
        }
        if let Some(v) = &self.chunk_size {
            command.arg("CHUNK_SIZE").arg(v);
        }
        if let Some(v) = &self.duplicate_policy {
            command.arg("DUPLICATE_POLICY").arg(v);
        }
        if let Some(v) = &self.ignore {
            v.cmd(&mut command);
        }
        if let Some(v) = &self.labels {
            append_labels_to_cmd(v, &mut command);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.ALTER requires at least 1 argument (key)"));
        }

        let key = args[0].clone().try_into()?;
        let mut retention = None;
        let mut chunk_size = None;
        let mut duplicate_policy = None;
        let mut ignore = None;
        let mut labels = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "RETENTION" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("RETENTION requires a value"));
                        }
                        retention = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "CHUNK_SIZE" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("CHUNK_SIZE requires a value"));
                        }
                        chunk_size = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "DUPLICATE_POLICY" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("DUPLICATE_POLICY requires a value"));
                        }
                        duplicate_policy = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "IGNORE" => {
                        if i + 2 >= args.len() {
                            return Err(EpError::request("IGNORE requires two values (maxTimeDiff, maxValDiff)"));
                        }
                        ignore = Some(TsIgnore {
                            max_time_diff: args[i + 1].clone(),
                            max_val_diff: args[i + 2].clone(),
                        });
                        i += 3;
                    }
                    "LABELS" => {
                        i += 1;
                        let (parsed, new_i) = parse_labels_from_args(&args, i);
                        if !parsed.is_empty() {
                            labels = Some(parsed);
                        }
                        i = new_i;
                    }
                    _ => return Err(EpError::request(format!("Unknown TS.ALTER option: {}", s))),
                }
            } else {
                return Err(EpError::request("TS.ALTER options must be strings"));
            }
        }

        Ok(TsAlterInput { key, retention, chunk_size, duplicate_policy, ignore, labels })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::time_series::common::TsOkOutput;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd_str = serde_json::to_string(&input).expect("json serialization failed");
            assert!(cmd_str.contains("TS.ALTER"));
            assert!(cmd_str.contains("ts:key"));
        }

        #[test]
        fn test_encode_command_with_retention() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: Some(RedisJsonValue::Integer(86400000)),
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("86400000"));
        }

        #[test]
        fn test_encode_command_with_chunk_size() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                chunk_size: Some(RedisJsonValue::Integer(8192)),
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("8192"));
        }

        #[test]
        fn test_encode_command_with_duplicate_policy() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: Some(RedisJsonValue::String("SUM".into())),
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("SUM"));
        }

        #[test]
        fn test_encode_command_with_ignore() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: Some(TsIgnore::new(1000, 0.5)),
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("IGNORE"));
        }

        #[test]
        fn test_encode_command_with_labels() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:key".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: Some(vec![TsLabel::new("sensor", "temperature"), TsLabel::new("location", "warehouse")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("temperature"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = TsAlterInput {
                key: RedisKey::String("ts:full".into()),
                retention: Some(RedisJsonValue::Integer(3600000)),
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: Some(RedisJsonValue::String("LAST".into())),
                ignore: Some(TsIgnore::new(500, 0.1)),
                labels: Some(vec![TsLabel::new("type", "test")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("IGNORE"));
            assert!(cmd_str.contains("LABELS"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TsAlterInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.retention.is_none());
        }

        #[test]
        fn test_decode_input_with_retention() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(86400000),
            ];
            let input = TsAlterInput::decode(args).unwrap();
            assert_eq!(input.retention, Some(RedisJsonValue::Integer(86400000)));
        }

        #[test]
        fn test_decode_input_with_labels() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("LABELS".into()),
                RedisJsonValue::String("sensor".into()),
                RedisJsonValue::String("temp".into()),
                RedisJsonValue::String("unit".into()),
                RedisJsonValue::String("celsius".into()),
            ];
            let input = TsAlterInput::decode(args).unwrap();
            assert!(input.labels.is_some());
            assert_eq!(input.labels.unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_with_ignore() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(0.1),
            ];
            let input = TsAlterInput::decode(args).unwrap();
            assert!(input.ignore.is_some());
        }

        #[test]
        fn test_decode_input_empty_args() {
            let err = TsAlterInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_decode_input_retention_missing_value() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("RETENTION".into())];
            let err = TsAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("RETENTION requires a value"));
        }

        #[test]
        fn test_decode_input_chunk_size_missing_value() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("CHUNK_SIZE".into())];
            let err = TsAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("CHUNK_SIZE requires a value"));
        }

        #[test]
        fn test_decode_input_ignore_missing_values() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(100),
            ];
            let err = TsAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("IGNORE requires two values"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("ENCODING".into()), // Not valid for ALTER
                RedisJsonValue::String("COMPRESSED".into()),
            ];
            let err = TsAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown TS.ALTER option"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TsOkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsOkOutput::decode(b"-ERR key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsAlterInput {
                key: RedisKey::String("mykey".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind() {
            let input = TsAlterInput {
                key: RedisKey::String("k".into()),
                retention: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsAlter);
        }

        #[test]
        fn test_serialization() {
            let input = TsAlterInput {
                key: RedisKey::String("mykey".into()),
                retention: Some(RedisJsonValue::Integer(3600000)),
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("TS.ALTER"));
            assert!(json.contains("mykey"));
            assert!(json.contains("retention"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::time_series::ts_create::TsCreateInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_alter_retention() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // First create a time series
                    let create_result = ctx
                        .raw(
                            &TsCreateInput {
                                key: RedisKey::String("ts:alter:test".into()),
                                retention: Some(RedisJsonValue::Integer(3600000)),
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match create_result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") && String::from_utf8_lossy(&bytes).contains("unknown command") {
                                println!("TimeSeries module not available");
                                return;
                            }
                        }
                        Err(e) => {
                            println!("Skipped: {}", e);
                            return;
                        }
                    }

                    // Now alter the retention
                    let alter_result = ctx
                        .raw(
                            &TsAlterInput {
                                key: RedisKey::String("ts:alter:test".into()),
                                retention: Some(RedisJsonValue::Integer(7200000)),
                                chunk_size: None,
                                duplicate_policy: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match alter_result {
                        Ok(bytes) => {
                            let output = TsOkOutput::decode(&bytes).expect("decode failed");
                            assert!(output.success);
                        }
                        Err(e) => println!("Alter failed: {}", e),
                    }
                })
            })
            .await;
        }
    }
}
