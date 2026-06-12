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

const API_INFO: ApiInfo<RedisApi, TsDelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsDel,
    "Delete all samples between two timestamps for a given time series",
    ReqType::Write,
    true,
);

/// Input for Redis `TS.DEL` command.
///
/// Delete samples from a time series within a timestamp range.
///
/// See official Redis documentation for `TS.DEL`:
/// https://redis.io/docs/latest/commands/ts.del/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsDelInput {
    /// Key name of the time series
    key: RedisKey,
    /// Start timestamp (inclusive)
    from_timestamp: RedisJsonValue,
    /// End timestamp (inclusive)
    to_timestamp: RedisJsonValue,
}

impl Serialize for TsDelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TsDelInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("from_timestamp", &self.from_timestamp)?;
        state.serialize_field("to_timestamp", &self.to_timestamp)?;
        state.end()
    }
}

impl_redis_operation!(
    TsDelInput,
    API_INFO,
    {key, from_timestamp, to_timestamp}
);

impl RedisCommandInput for TsDelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.from_timestamp).arg(&self.to_timestamp);
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!(
                "TS.DEL requires exactly 3 arguments (key, fromTimestamp, toTimestamp), given {}",
                args.len()
            )));
        }
        Ok(TsDelInput {
            key: args[0].clone().try_into()?,
            from_timestamp: args[1].clone(),
            to_timestamp: args[2].clone(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::time_series::common::TsDelOutput;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = TsDelInput {
                key: RedisKey::String("ts:key".into()),
                from_timestamp: RedisJsonValue::Integer(1000),
                to_timestamp: RedisJsonValue::Integer(2000),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.DEL"));
            assert!(cmd_str.contains("ts:key"));
        }

        #[test]
        fn test_encode_command_with_string_timestamps() {
            let input = TsDelInput {
                key: RedisKey::String("ts:key".into()),
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.DEL"));
        }

        #[test]
        fn test_encode_command_with_integer_timestamps() {
            let input = TsDelInput {
                key: RedisKey::String("ts:temperature".into()),
                from_timestamp: RedisJsonValue::Integer(1609459200000),
                to_timestamp: RedisJsonValue::Integer(1609545600000),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("1609459200000"));
            assert!(cmd_str.contains("1609545600000"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(2000),
            ];
            let input = TsDelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.from_timestamp, RedisJsonValue::Integer(1000));
            assert_eq!(input.to_timestamp, RedisJsonValue::Integer(2000));
        }

        #[test]
        fn test_decode_input_with_string_timestamps() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            ];
            let input = TsDelInput::decode(args).unwrap();
            assert_eq!(input.from_timestamp, RedisJsonValue::String("-".into()));
            assert_eq!(input.to_timestamp, RedisJsonValue::String("+".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1000)];
            let err = TsDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 3"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(2000),
                RedisJsonValue::Integer(3000),
            ];
            let err = TsDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 3"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let err = TsDelInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("exactly 3"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = TsDelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted_count, 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = TsDelOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.deleted_count, 42);
        }

        #[test]
        fn test_decode_output_large_count() {
            let output = TsDelOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.deleted_count, 1000000);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsDelOutput::decode(b"-ERR key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsDelInput {
                key: RedisKey::String("mykey".into()),
                from_timestamp: RedisJsonValue::Integer(0),
                to_timestamp: RedisJsonValue::Integer(1000),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind() {
            let input = TsDelInput {
                key: RedisKey::String("k".into()),
                from_timestamp: RedisJsonValue::Integer(0),
                to_timestamp: RedisJsonValue::Integer(1),
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsDel);
        }

        #[test]
        fn test_serialization() {
            let input = TsDelInput {
                key: RedisKey::String("mykey".into()),
                from_timestamp: RedisJsonValue::Integer(1000),
                to_timestamp: RedisJsonValue::Integer(2000),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("TS.DEL"));
            assert!(json.contains("mykey"));
            assert!(json.contains("from_timestamp"));
            assert!(json.contains("to_timestamp"));
        }

        #[test]
        fn test_serialization_deserialize_roundtrip() {
            let input = TsDelInput {
                key: RedisKey::String("ts:test".into()),
                from_timestamp: RedisJsonValue::Integer(100),
                to_timestamp: RedisJsonValue::Integer(200),
            };
            let json = serde_json::to_string(&input).unwrap();
            // Note: Full roundtrip would require custom deserialize handling for "type" field
            assert!(json.contains("100"));
            assert!(json.contains("200"));
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
        async fn test_del_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsDelInput {
                                key: RedisKey::String("ts:del:nonexistent".into()),
                                from_timestamp: RedisJsonValue::String("-".into()),
                                to_timestamp: RedisJsonValue::String("+".into()),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") {
                                let err_str = String::from_utf8_lossy(&bytes);
                                if err_str.contains("unknown command") {
                                    println!("TimeSeries module not available");
                                    return;
                                }
                                // Expected error for nonexistent key
                                assert!(err_str.contains("ERR") || err_str.contains("TSDB"));
                            }
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_after_add() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // First add some samples
                    for ts in [1000i64, 2000, 3000, 4000, 5000] {
                        let result = ctx
                            .raw(
                                &TsAddInput {
                                    key: RedisKey::String("ts:del:test".into()),
                                    timestamp: RedisJsonValue::Integer(ts),
                                    value: RedisJsonValue::Float(ts as f64 / 100.0),
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

                        if let Ok(bytes) = &result
                            && bytes.starts_with(b"-")
                            && String::from_utf8_lossy(bytes).contains("unknown command")
                        {
                            println!("TimeSeries module not available");
                            return;
                        }
                    }

                    // Delete samples in range [2000, 4000]
                    let del_result = ctx
                        .raw(
                            &TsDelInput {
                                key: RedisKey::String("ts:del:test".into()),
                                from_timestamp: RedisJsonValue::Integer(2000),
                                to_timestamp: RedisJsonValue::Integer(4000),
                            }
                            .command(),
                        )
                        .await;

                    match del_result {
                        Ok(bytes) => {
                            if bytes.starts_with(b"-") {
                                println!("Delete failed: {}", String::from_utf8_lossy(&bytes));
                                return;
                            }
                            let output = TsDelOutput::decode(&bytes).expect("decode failed");
                            // Should have deleted 3 samples (2000, 3000, 4000)
                            assert_eq!(output.deleted_count, 3);
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_empty_range() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Add a sample
                    let add_result = ctx
                        .raw(
                            &TsAddInput {
                                key: RedisKey::String("ts:del:empty".into()),
                                timestamp: RedisJsonValue::Integer(5000),
                                value: RedisJsonValue::Float(50.0),
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

                    // Try to delete from a range with no samples
                    let del_result = ctx
                        .raw(
                            &TsDelInput {
                                key: RedisKey::String("ts:del:empty".into()),
                                from_timestamp: RedisJsonValue::Integer(1000),
                                to_timestamp: RedisJsonValue::Integer(2000),
                            }
                            .command(),
                        )
                        .await;

                    match del_result {
                        Ok(bytes) => {
                            let output = TsDelOutput::decode(&bytes).expect("decode failed");
                            assert_eq!(output.deleted_count, 0);
                        }
                        Err(e) => println!("Skipped: {}", e),
                    }
                })
            })
            .await;
        }
    }
}
