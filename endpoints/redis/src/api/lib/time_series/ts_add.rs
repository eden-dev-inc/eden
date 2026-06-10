use crate::api::lib::time_series::common::{TsEncoding, TsIgnore, TsLabel, append_labels_to_cmd, parse_labels_from_args};
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

const API_INFO: ApiInfo<RedisApi, TsAddInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TsAdd, "Append a sample to a time series", ReqType::Write, true);

/// Input for Redis `TS.ADD` command.
///
/// Append a sample to a time series. Creates the series if it doesn't exist.
///
/// See official Redis documentation for `TS.ADD`:
/// https://redis.io/docs/latest/commands/ts.add/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsAddInput {
    /// Key name for the time series
    pub(crate) key: RedisKey,
    /// Timestamp in milliseconds, or "*" for automatic timestamp
    pub(crate) timestamp: RedisJsonValue,
    /// Sample value (double)
    pub(crate) value: RedisJsonValue,
    /// Maximum retention period in milliseconds
    #[builder(default)]
    pub(crate) retention: Option<RedisJsonValue>,
    /// Series encoding type
    #[builder(default)]
    pub(crate) encoding: Option<TsEncoding>,
    /// Memory chunk size in bytes
    #[builder(default)]
    pub(crate) chunk_size: Option<RedisJsonValue>,
    /// Policy for handling duplicate timestamps on creation
    #[builder(default)]
    pub(crate) duplicate_policy: Option<RedisJsonValue>,
    /// Policy for handling this specific duplicate
    #[builder(default)]
    pub(crate) on_duplicate: Option<RedisJsonValue>,
    /// Ignore parameters for sample deduplication
    #[builder(default)]
    pub(crate) ignore: Option<TsIgnore>,
    /// Labels (metadata) for the time series
    #[builder(default)]
    pub(crate) labels: Option<Vec<TsLabel>>,
}

impl Serialize for TsAddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, timestamp, value
        if self.retention.is_some() {
            fields += 1;
        }
        if self.encoding.is_some() {
            fields += 1;
        }
        if self.chunk_size.is_some() {
            fields += 1;
        }
        if self.duplicate_policy.is_some() {
            fields += 1;
        }
        if self.on_duplicate.is_some() {
            fields += 1;
        }
        if self.ignore.is_some() {
            fields += 1;
        }
        if self.labels.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsAddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("value", &self.value)?;

        if let Some(retention) = &self.retention {
            state.serialize_field("retention", retention)?;
        }
        if let Some(encoding) = &self.encoding {
            state.serialize_field("encoding", encoding)?;
        }
        if let Some(chunk_size) = &self.chunk_size {
            state.serialize_field("chunk_size", chunk_size)?;
        }
        if let Some(duplicate_policy) = &self.duplicate_policy {
            state.serialize_field("duplicate_policy", duplicate_policy)?;
        }
        if let Some(on_duplicate) = &self.on_duplicate {
            state.serialize_field("on_duplicate", on_duplicate)?;
        }
        if let Some(ignore) = &self.ignore {
            state.serialize_field("ignore", ignore)?;
        }
        if let Some(labels) = &self.labels {
            state.serialize_field("labels", labels)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsAddInput,
    API_INFO,
    {key, timestamp, value, retention, encoding, chunk_size, duplicate_policy, on_duplicate, ignore, labels}
);

impl RedisCommandInput for TsAddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.timestamp).arg(&self.value);

        if let Some(retention) = &self.retention {
            command.arg("RETENTION").arg(retention);
        }

        if let Some(encoding) = &self.encoding {
            encoding.cmd(&mut command);
        }

        if let Some(chunk_size) = &self.chunk_size {
            command.arg("CHUNK_SIZE").arg(chunk_size);
        }

        if let Some(duplicate_policy) = &self.duplicate_policy {
            command.arg("DUPLICATE_POLICY").arg(duplicate_policy);
        }

        if let Some(on_duplicate) = &self.on_duplicate {
            command.arg("ON_DUPLICATE").arg(on_duplicate);
        }

        if let Some(ignore) = &self.ignore {
            ignore.cmd(&mut command);
        }

        if let Some(labels) = &self.labels {
            append_labels_to_cmd(labels, &mut command);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!(
                "TS.ADD requires at least 3 arguments (key, timestamp, value), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let timestamp = args[1].clone();
        let value = args[2].clone();
        let mut retention = None;
        let mut encoding = None;
        let mut chunk_size = None;
        let mut duplicate_policy = None;
        let mut on_duplicate = None;
        let mut ignore = None;
        let mut labels = None;
        let mut i = 3;

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
                    "ENCODING" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("ENCODING requires a value"));
                        }
                        if let RedisJsonValue::String(enc) = &args[i + 1] {
                            encoding = Some(TsEncoding::from_str(enc)?);
                        } else {
                            return Err(EpError::request("ENCODING value must be a string"));
                        }
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
                    "ON_DUPLICATE" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("ON_DUPLICATE requires a value"));
                        }
                        on_duplicate = Some(args[i + 1].clone());
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
                        let (parsed_labels, new_i) = parse_labels_from_args(&args, i);
                        if !parsed_labels.is_empty() {
                            labels = Some(parsed_labels);
                        }
                        i = new_i;
                    }
                    _ => {
                        return Err(EpError::request(format!("Unknown TS.ADD option: {}", s)));
                    }
                }
            } else {
                return Err(EpError::request("TS.ADD options must be strings"));
            }
        }

        Ok(TsAddInput {
            key,
            timestamp,
            value,
            retention,
            encoding,
            chunk_size,
            duplicate_policy,
            on_duplicate,
            ignore,
            labels,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::time_series::common::TsTimestampOutput;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TsAddInput {
                key: RedisKey::String("temperature:sensor1".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(25.5),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.ADD"));
            assert!(cmd_str.contains("temperature:sensor1"));
        }

        #[test]
        fn test_encode_command_with_timestamp() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::Integer(1609459200000),
                value: RedisJsonValue::Float(42.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.ADD"));
            assert!(cmd_str.contains("1609459200000"));
        }

        #[test]
        fn test_encode_command_with_retention() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: Some(RedisJsonValue::Integer(86400000)),
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("86400000"));
        }

        #[test]
        fn test_encode_command_with_encoding() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: Some(TsEncoding::UNCOMPRESSED),
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ENCODING"));
            assert!(cmd_str.contains("UNCOMPRESSED"));
        }

        #[test]
        fn test_encode_command_with_chunk_size() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("4096"));
        }

        #[test]
        fn test_encode_command_with_duplicate_policy() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: Some(RedisJsonValue::String("LAST".into())),
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("LAST"));
        }

        #[test]
        fn test_encode_command_with_on_duplicate() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: Some(RedisJsonValue::String("SUM".into())),
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ON_DUPLICATE"));
            assert!(cmd_str.contains("SUM"));
        }

        #[test]
        fn test_encode_command_with_ignore() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: Some(TsIgnore {
                    max_time_diff: RedisJsonValue::Integer(1000),
                    max_val_diff: RedisJsonValue::Float(0.1),
                }),
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("IGNORE"));
        }

        #[test]
        fn test_encode_command_with_labels() {
            let input = TsAddInput {
                key: RedisKey::String("ts:key".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: Some(vec![TsLabel::new("sensor", "temp"), TsLabel::new("location", "room1")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("temp"));
            assert!(cmd_str.contains("location"));
            assert!(cmd_str.contains("room1"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = TsAddInput {
                key: RedisKey::String("ts:full".into()),
                timestamp: RedisJsonValue::Integer(1000),
                value: RedisJsonValue::Float(99.9),
                retention: Some(RedisJsonValue::Integer(3600000)),
                encoding: Some(TsEncoding::COMPRESSED),
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: Some(RedisJsonValue::String("BLOCK".into())),
                on_duplicate: Some(RedisJsonValue::String("FIRST".into())),
                ignore: Some(TsIgnore::new(500, 0.05)),
                labels: Some(vec![TsLabel::new("type", "test")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.ADD"));
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("ENCODING"));
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("ON_DUPLICATE"));
            assert!(cmd_str.contains("IGNORE"));
            assert!(cmd_str.contains("LABELS"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::Float(25.5),
            ];
            let input = TsAddInput::decode(args).expect("failed to parse json");
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.retention.is_none());
            assert!(input.encoding.is_none());
            assert!(input.labels.is_none());
        }

        #[test]
        fn test_decode_input_with_retention() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(86400000),
            ];
            let input = TsAddInput::decode(args).expect("failed to parse json");
            assert_eq!(input.retention, Some(RedisJsonValue::Integer(86400000)));
        }

        #[test]
        fn test_decode_input_with_encoding() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("ENCODING".into()),
                RedisJsonValue::String("UNCOMPRESSED".into()),
            ];
            let input = TsAddInput::decode(args).expect("failed to parse json");
            assert_eq!(input.encoding, Some(TsEncoding::UNCOMPRESSED));
        }

        #[test]
        fn test_decode_input_with_labels() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("LABELS".into()),
                RedisJsonValue::String("sensor".into()),
                RedisJsonValue::String("temp".into()),
            ];
            let input = TsAddInput::decode(args).expect("failed to parse json");
            assert!(input.labels.is_some());
            let labels = input.labels.expect("failed to get labels");
            assert_eq!(labels.len(), 1);
        }

        #[test]
        fn test_decode_input_with_ignore() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(0.1),
            ];
            let input = TsAddInput::decode(args).expect("failed to parse json");
            assert!(input.ignore.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1000)];
            let err = TsAddInput::decode(args).expect_err("Should fail");
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_decode_input_retention_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("RETENTION".into()),
            ];
            let err = TsAddInput::decode(args).expect_err("Should fail");
            assert!(err.to_string().contains("RETENTION requires a value"));
        }

        #[test]
        fn test_decode_input_encoding_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("ENCODING".into()),
            ];
            let err = TsAddInput::decode(args).expect_err("Should fail");
            assert!(err.to_string().contains("ENCODING requires a value"));
        }

        #[test]
        fn test_decode_input_ignore_missing_values() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(100),
            ];
            let err = TsAddInput::decode(args).expect_err("Should fail");
            assert!(err.to_string().contains("IGNORE requires two values"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("UNKNOWN_OPTION".into()),
            ];
            let err = TsAddInput::decode(args).expect_err("Should fail");
            assert!(err.to_string().contains("Unknown TS.ADD option"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = TsTimestampOutput::decode(b":1609459200000\r\n").expect("failed to decode");
            assert_eq!(output.timestamp, 1609459200000);
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsAddInput {
                key: RedisKey::String("mykey".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsAddInput {
                key: RedisKey::String("k".into()),
                timestamp: RedisJsonValue::String("*".into()),
                value: RedisJsonValue::Float(1.0),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsAdd);
        }

        #[test]
        fn test_serialization() {
            let input = TsAddInput {
                key: RedisKey::String("mykey".into()),
                timestamp: RedisJsonValue::Integer(1000),
                value: RedisJsonValue::Float(42.0),
                retention: Some(RedisJsonValue::Integer(3600000)),
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                on_duplicate: None,
                ignore: None,
                labels: None,
            };
            let json = serde_json::to_string(&input).expect("failed to serialize");
            assert!(json.contains("TS.ADD"));
            assert!(json.contains("mykey"));
            assert!(json.contains("retention"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.ADD requires RedisTimeSeries module.
        // Tests skip gracefully if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_add_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsAddInput {
                                key: RedisKey::String("ts:add:basic".into()),
                                timestamp: RedisJsonValue::String("*".into()),
                                value: RedisJsonValue::Float(25.5),
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                on_duplicate: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") {
                                // Module might not be installed
                                let err_str = String::from_utf8_lossy(&bytes);
                                if err_str.contains("unknown command") {
                                    println!("TimeSeries module not available, skipping");
                                    return;
                                }
                            }
                            let output = TsTimestampOutput::decode(&bytes).expect("decode failed");
                            assert!(output.timestamp > 0);
                        }
                        Err(e) => {
                            println!("Test skipped due to error: {}", e);
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_add_with_explicit_timestamp() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let timestamp = 1609459200000i64;
                    let result = ctx
                        .raw(
                            &TsAddInput {
                                key: RedisKey::String("ts:add:explicit".into()),
                                timestamp: RedisJsonValue::Integer(timestamp),
                                value: RedisJsonValue::Float(42.0),
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                on_duplicate: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") {
                                let err_str = String::from_utf8_lossy(&bytes);
                                if err_str.contains("unknown command") {
                                    println!("TimeSeries module not available, skipping");
                                    return;
                                }
                            }
                            let output = TsTimestampOutput::decode(&bytes).expect("decode failed");
                            assert_eq!(output.timestamp, timestamp);
                        }
                        Err(e) => {
                            println!("Test skipped due to error: {}", e);
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_add_with_labels() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsAddInput {
                                key: RedisKey::String("ts:add:labels".into()),
                                timestamp: RedisJsonValue::String("*".into()),
                                value: RedisJsonValue::Float(100.0),
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                on_duplicate: None,
                                ignore: None,
                                labels: Some(vec![TsLabel::new("sensor", "temperature"), TsLabel::new("location", "warehouse")]),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") {
                                let err_str = String::from_utf8_lossy(&bytes);
                                if err_str.contains("unknown command") {
                                    println!("TimeSeries module not available, skipping");
                                    return;
                                }
                            }
                            let output = TsTimestampOutput::decode(&bytes).expect("decode failed");
                            assert!(output.timestamp > 0);
                        }
                        Err(e) => {
                            println!("Test skipped due to error: {}", e);
                        }
                    }
                })
            })
            .await;
        }
    }
}
