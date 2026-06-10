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

const API_INFO: ApiInfo<RedisApi, TsCreateruleInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TsCreaterule, "Create a compaction rule", ReqType::Write, true);

/// Input for Redis `TS.CREATERULE` command.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsCreateruleInput {
    source_key: RedisKey,
    dest_key: RedisKey,
    aggregator: RedisJsonValue,
    bucket_duration: RedisJsonValue,
    #[builder(default)]
    align_timestamp: Option<RedisJsonValue>,
}

impl Serialize for TsCreateruleInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let fields = if self.align_timestamp.is_some() { 6 } else { 5 };
        let mut state = serializer.serialize_struct("TsCreateruleInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source_key", &self.source_key)?;
        state.serialize_field("dest_key", &self.dest_key)?;
        state.serialize_field("aggregator", &self.aggregator)?;
        state.serialize_field("bucket_duration", &self.bucket_duration)?;
        if let Some(v) = &self.align_timestamp {
            state.serialize_field("align_timestamp", v)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsCreateruleInput,
    API_INFO,
    {source_key, dest_key, aggregator, bucket_duration, align_timestamp}
);

impl RedisCommandInput for TsCreateruleInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source_key.clone(), self.dest_key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.source_key).arg(&self.dest_key).arg("AGGREGATION").arg(&self.aggregator).arg(&self.bucket_duration);
        if let Some(v) = &self.align_timestamp {
            command.arg(v);
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request(format!("TS.CREATERULE requires at least 5 arguments, given {}", args.len())));
        }
        let source_key = args[0].clone().try_into()?;
        let dest_key = args[1].clone().try_into()?;
        if let RedisJsonValue::String(s) = &args[2] {
            if s.to_uppercase() != "AGGREGATION" {
                return Err(EpError::request("Expected 'AGGREGATION' keyword"));
            }
        } else {
            return Err(EpError::request("Expected 'AGGREGATION' keyword"));
        }
        let aggregator = args[3].clone();
        let bucket_duration = args[4].clone();
        let align_timestamp = args.get(5).cloned();

        Ok(TsCreateruleInput {
            source_key,
            dest_key,
            aggregator,
            bucket_duration,
            align_timestamp,
        })
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
            let input = TsCreateruleInput {
                source_key: RedisKey::String("ts:raw".into()),
                dest_key: RedisKey::String("ts:hourly".into()),
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(3600000),
                align_timestamp: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.CREATERULE"));
            assert!(cmd_str.contains("AGGREGATION"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("ts:source".into()),
                RedisJsonValue::String("ts:dest".into()),
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("avg".into()),
                RedisJsonValue::Integer(3600000),
            ];
            let input = TsCreateruleInput::decode(args).unwrap();
            assert_eq!(input.source_key, RedisKey::String("ts:source".into()));
        }

        #[test]
        fn test_decode_input_with_align() {
            let args = vec![
                RedisJsonValue::String("ts:source".into()),
                RedisJsonValue::String("ts:dest".into()),
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("sum".into()),
                RedisJsonValue::Integer(60000),
                RedisJsonValue::Integer(0),
            ];
            let input = TsCreateruleInput::decode(args).unwrap();
            assert!(input.align_timestamp.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let err = TsCreateruleInput::decode(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())]).unwrap_err();
            assert!(err.to_string().contains("requires at least 5"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TsOkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success);
        }

        #[test]
        fn test_keys_returns_both() {
            let input = TsCreateruleInput {
                source_key: RedisKey::String("s".into()),
                dest_key: RedisKey::String("d".into()),
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(1000),
                align_timestamp: None,
            };
            assert_eq!(input.keys().len(), 2);
        }

        #[test]
        fn test_kind() {
            let input = TsCreateruleInput {
                source_key: RedisKey::String("s".into()),
                dest_key: RedisKey::String("d".into()),
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(1000),
                align_timestamp: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsCreaterule);
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
        async fn test_createrule_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Create source and dest series first
                    for key in ["ts:rule:src", "ts:rule:dst"] {
                        let r = ctx
                            .raw(
                                &TsCreateInput {
                                    key: RedisKey::String(key.into()),
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
                        if let Ok(b) = &r
                            && b.starts_with(b"-")
                            && String::from_utf8_lossy(b).contains("unknown")
                        {
                            println!("TimeSeries module not available");
                            return;
                        }
                    }

                    let result = ctx
                        .raw(
                            &TsCreateruleInput {
                                source_key: RedisKey::String("ts:rule:src".into()),
                                dest_key: RedisKey::String("ts:rule:dst".into()),
                                aggregator: RedisJsonValue::String("avg".into()),
                                bucket_duration: RedisJsonValue::Integer(60000),
                                align_timestamp: None,
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(bytes) = result
                        && !bytes.starts_with(b"-")
                    {
                        let output = TsOkOutput::decode(&bytes).expect("decode");
                        assert!(output.success);
                    }
                })
            })
            .await;
        }
    }
}
