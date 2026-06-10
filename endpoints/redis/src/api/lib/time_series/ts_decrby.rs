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

const API_INFO: ApiInfo<RedisApi, TsDecrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsDecrby,
    "Decrease the value of the sample with the maximum existing timestamp, or create a new sample",
    ReqType::Write,
    true,
);

/// Input for Redis `TS.DECRBY` command.
///
/// Decrease the value of the sample with the maximum existing timestamp,
/// or create a new sample with a value equal to the value of the sample
/// with the maximum existing timestamp minus the given decrement.
///
/// See official Redis documentation for `TS.DECRBY`:
/// https://redis.io/docs/latest/commands/ts.decrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsDecrbyInput {
    /// Key name for the time series
    key: RedisKey,
    /// Value to subtract
    subtrahend: RedisJsonValue,
    /// Optional timestamp (default: current time)
    #[builder(default)]
    timestamp: Option<RedisJsonValue>,
    /// Maximum retention period in milliseconds
    #[builder(default)]
    retention: Option<RedisJsonValue>,
    /// Series encoding type
    #[builder(default)]
    encoding: Option<TsEncoding>,
    /// Memory chunk size in bytes
    #[builder(default)]
    chunk_size: Option<RedisJsonValue>,
    /// Policy for handling duplicate timestamps
    #[builder(default)]
    duplicate_policy: Option<RedisJsonValue>,
    /// Ignore parameters for sample deduplication
    #[builder(default)]
    ignore: Option<TsIgnore>,
    /// Labels (metadata) for the time series
    #[builder(default)]
    labels: Option<Vec<TsLabel>>,
}

impl Serialize for TsDecrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, subtrahend
        if self.timestamp.is_some() {
            fields += 1;
        }
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
        if self.ignore.is_some() {
            fields += 1;
        }
        if self.labels.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsDecrbyInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("subtrahend", &self.subtrahend)?;
        if let Some(v) = &self.timestamp {
            state.serialize_field("timestamp", v)?;
        }
        if let Some(v) = &self.retention {
            state.serialize_field("retention", v)?;
        }
        if let Some(v) = &self.encoding {
            state.serialize_field("encoding", v)?;
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
    TsDecrbyInput,
    API_INFO,
    {key, subtrahend, timestamp, retention, encoding, chunk_size, duplicate_policy, ignore, labels}
);

impl RedisCommandInput for TsDecrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.subtrahend);

        if let Some(v) = &self.timestamp {
            command.arg("TIMESTAMP").arg(v);
        }
        if let Some(v) = &self.retention {
            command.arg("RETENTION").arg(v);
        }
        if let Some(v) = &self.encoding {
            v.cmd(&mut command);
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
        if args.len() < 2 {
            return Err(EpError::request(format!(
                "TS.DECRBY requires at least 2 arguments (key, subtrahend), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let subtrahend = args[1].clone();
        let mut timestamp = None;
        let mut retention = None;
        let mut encoding = None;
        let mut chunk_size = None;
        let mut duplicate_policy = None;
        let mut ignore = None;
        let mut labels = None;
        let mut i = 2;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "TIMESTAMP" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("TIMESTAMP requires a value"));
                        }
                        timestamp = Some(args[i + 1].clone());
                        i += 2;
                    }
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
                    _ => return Err(EpError::request(format!("Unknown TS.DECRBY option: {}", s))),
                }
            } else {
                return Err(EpError::request("TS.DECRBY options must be strings"));
            }
        }

        Ok(TsDecrbyInput {
            key,
            subtrahend,
            timestamp,
            retention,
            encoding,
            chunk_size,
            duplicate_policy,
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
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:counter".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.DECRBY"));
            assert!(cmd_str.contains("ts:counter"));
        }

        #[test]
        fn test_encode_command_with_timestamp() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Integer(5),
                timestamp: Some(RedisJsonValue::Integer(1609459200000)),
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TIMESTAMP"));
            assert!(cmd_str.contains("1609459200000"));
        }

        #[test]
        fn test_encode_command_with_retention() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(10.0),
                timestamp: None,
                retention: Some(RedisJsonValue::Integer(86400000)),
                encoding: None,
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
        fn test_encode_command_with_encoding() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: Some(TsEncoding::UNCOMPRESSED),
                chunk_size: None,
                duplicate_policy: None,
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
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: None,
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
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: Some(RedisJsonValue::String("LAST".into())),
                ignore: None,
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("LAST"));
        }

        #[test]
        fn test_encode_command_with_ignore() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: Some(TsIgnore::new(1000, 0.1)),
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("IGNORE"));
        }

        #[test]
        fn test_encode_command_with_labels() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:key".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: Some(vec![TsLabel::new("sensor", "counter")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("counter"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = TsDecrbyInput {
                key: RedisKey::String("ts:full".into()),
                subtrahend: RedisJsonValue::Float(5.5),
                timestamp: Some(RedisJsonValue::String("*".into())),
                retention: Some(RedisJsonValue::Integer(3600000)),
                encoding: Some(TsEncoding::COMPRESSED),
                chunk_size: Some(RedisJsonValue::Integer(4096)),
                duplicate_policy: Some(RedisJsonValue::String("SUM".into())),
                ignore: Some(TsIgnore::new(500, 0.05)),
                labels: Some(vec![TsLabel::new("type", "test")]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TIMESTAMP"));
            assert!(cmd_str.contains("RETENTION"));
            assert!(cmd_str.contains("ENCODING"));
            assert!(cmd_str.contains("CHUNK_SIZE"));
            assert!(cmd_str.contains("DUPLICATE_POLICY"));
            assert!(cmd_str.contains("IGNORE"));
            assert!(cmd_str.contains("LABELS"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Float(10.0)];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.subtrahend, RedisJsonValue::Float(10.0));
            assert!(input.timestamp.is_none());
        }

        #[test]
        fn test_decode_input_with_timestamp() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("TIMESTAMP".into()),
                RedisJsonValue::Integer(1609459200000),
            ];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert_eq!(input.timestamp, Some(RedisJsonValue::Integer(1609459200000)));
        }

        #[test]
        fn test_decode_input_with_retention() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(86400000),
            ];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert_eq!(input.retention, Some(RedisJsonValue::Integer(86400000)));
        }

        #[test]
        fn test_decode_input_with_encoding() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("ENCODING".into()),
                RedisJsonValue::String("COMPRESSED".into()),
            ];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert_eq!(input.encoding, Some(TsEncoding::COMPRESSED));
        }

        #[test]
        fn test_decode_input_with_labels() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("LABELS".into()),
                RedisJsonValue::String("sensor".into()),
                RedisJsonValue::String("temp".into()),
            ];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert!(input.labels.is_some());
            assert_eq!(input.labels.expect("failed to get labesl").len(), 1);
        }

        #[test]
        fn test_decode_input_with_ignore() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(0.1),
            ];
            let input = TsDecrbyInput::decode(args).expect("failed to decode");
            assert!(input.ignore.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TsDecrbyInput::decode(args).expect_err("should error");
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_decode_input_timestamp_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("TIMESTAMP".into()),
            ];
            let err = TsDecrbyInput::decode(args).expect_err("should error");
            assert!(err.to_string().contains("TIMESTAMP requires a value"));
        }

        #[test]
        fn test_decode_input_retention_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("RETENTION".into()),
            ];
            let err = TsDecrbyInput::decode(args).expect_err("should error");
            assert!(err.to_string().contains("RETENTION requires a value"));
        }

        #[test]
        fn test_decode_input_encoding_missing_value() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("ENCODING".into()),
            ];
            let err = TsDecrbyInput::decode(args).expect_err("should error");
            assert!(err.to_string().contains("ENCODING requires a value"));
        }

        #[test]
        fn test_decode_input_ignore_missing_values() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("IGNORE".into()),
                RedisJsonValue::Integer(100),
            ];
            let err = TsDecrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("IGNORE requires two values"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Float(1.0),
                RedisJsonValue::String("UNKNOWN".into()),
            ];
            let err = TsDecrbyInput::decode(args).expect_err("should error");
            assert!(err.to_string().contains("Unknown TS.DECRBY option"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = TsTimestampOutput::decode(b":1609459200000\r\n").expect("failed to decode");
            assert_eq!(output.timestamp, 1609459200000);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsTimestampOutput::decode(b"-ERR something wrong\r\n").expect_err("Shoudl error");
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsDecrbyInput {
                key: RedisKey::String("mykey".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
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
            let input = TsDecrbyInput {
                key: RedisKey::String("k".into()),
                subtrahend: RedisJsonValue::Float(1.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsDecrby);
        }

        #[test]
        fn test_serialization() {
            let input = TsDecrbyInput {
                key: RedisKey::String("mykey".into()),
                subtrahend: RedisJsonValue::Float(5.0),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            let json = serde_json::to_string(&input).expect("failed to serialize");
            assert!(json.contains("TS.DECRBY"));
            assert!(json.contains("mykey"));
            assert!(json.contains("subtrahend"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::time_series::ts_add::TsAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_creates_series() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsDecrbyInput {
                                key: RedisKey::String("ts:decrby:new".into()),
                                subtrahend: RedisJsonValue::Float(5.0),
                                timestamp: None,
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") && String::from_utf8_lossy(&bytes).contains("unknown command") {
                                println!("TimeSeries module not available");
                                return;
                            }
                            // Should succeed and create the series
                            let output = TsTimestampOutput::decode(&bytes).expect("decode failed");
                            assert!(output.timestamp > 0);
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decrby_existing_series() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // First add a sample
                    let add_result = ctx
                        .raw(
                            &TsAddInput {
                                key: RedisKey::String("ts:decrby:existing".into()),
                                timestamp: RedisJsonValue::String("*".into()),
                                value: RedisJsonValue::Float(100.0),
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

                    if let Ok(bytes) = &add_result
                        && bytes.starts_with(b"-")
                        && String::from_utf8_lossy(bytes).contains("unknown command")
                    {
                        println!("TimeSeries module not available");
                        return;
                    }

                    // Now decrement
                    let decrby_result = ctx
                        .raw(
                            &TsDecrbyInput {
                                key: RedisKey::String("ts:decrby:existing".into()),
                                subtrahend: RedisJsonValue::Float(10.0),
                                timestamp: None,
                                retention: None,
                                encoding: None,
                                chunk_size: None,
                                duplicate_policy: None,
                                ignore: None,
                                labels: None,
                            }
                            .command(),
                        )
                        .await;

                    match decrby_result {
                        Ok(bytes) => {
                            let output = TsTimestampOutput::decode(&bytes).expect("decode failed");
                            assert!(output.timestamp > 0);
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }
    }
}
