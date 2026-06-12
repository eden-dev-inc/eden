use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsMaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsMadd,
    "Append new samples to one or more time series",
    ReqType::Write,
    true,
);

/// Input for Redis `TS.MADD` command.
///
/// Appends new samples to one or more time series.
///
/// See official Redis documentation for `TS.MADD`:
/// https://redis.io/docs/latest/commands/ts.madd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsMaddInput {
    /// List of key/timestamp/value triplets to add
    samples: Vec<Sample>,
}

impl Serialize for TsMaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TsMaddInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("samples", &self.samples)?;
        state.end()
    }
}

/// A sample to add to a time series.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Sample {
    /// The key name of the time series
    pub key: RedisKey,
    /// Timestamp in milliseconds (use "*" for auto-timestamp)
    pub timestamp: RedisJsonValue,
    /// Value to add at the timestamp
    pub value: RedisJsonValue,
}

impl_redis_operation!(TsMaddInput, API_INFO, { samples });

impl RedisCommandInput for TsMaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.samples.iter().map(|s| s.key.clone()).collect()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for sample in &self.samples {
            command.arg(&sample.key).arg(&sample.timestamp).arg(&sample.value);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.MADD requires at least one key/timestamp/value triplet"));
        }

        if !args.len().is_multiple_of(3) {
            return Err(EpError::request(format!(
                "TS.MADD requires key/timestamp/value triplets, given {} arguments",
                args.len()
            )));
        }

        let mut samples = Vec::new();
        for chunk in args.chunks(3) {
            samples.push(Sample {
                key: chunk[0].clone().try_into()?,
                timestamp: chunk[1].clone(),
                value: chunk[2].clone(),
            });
        }

        Ok(TsMaddInput { samples })
    }
}

/// Result for a single sample in TS.MADD response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub enum SampleResult {
    /// Successfully added sample, contains the timestamp
    Success(i64),
    /// Failed to add sample, contains the error message
    Error(String),
}

/// Output for Redis `TS.MADD` command.
///
/// Returns an array of timestamps or errors for each sample.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsMaddOutput {
    /// Results for each sample in the same order as input
    results: Vec<SampleResult>,
}

impl TsMaddOutput {
    pub fn new(results: Vec<SampleResult>) -> Self {
        Self { results }
    }

    /// Get all results
    pub fn results(&self) -> &[SampleResult] {
        &self.results
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if results are empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Get count of successful operations
    pub fn success_count(&self) -> usize {
        self.results.iter().filter(|r| matches!(r, SampleResult::Success(_))).count()
    }

    /// Get count of failed operations
    pub fn error_count(&self) -> usize {
        self.results.iter().filter(|r| matches!(r, SampleResult::Error(_))).count()
    }

    /// Check if all operations succeeded
    pub fn all_success(&self) -> bool {
        self.results.iter().all(|r| matches!(r, SampleResult::Success(_)))
    }

    /// Get timestamps of successful operations
    pub fn timestamps(&self) -> Vec<i64> {
        self.results
            .iter()
            .filter_map(|r| match r {
                SampleResult::Success(ts) => Some(*ts),
                _ => None,
            })
            .collect()
    }

    /// Decode the Redis protocol response into a TsMaddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = Self::parse_frame(frame)?;
        Ok(Self { results })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<SampleResult>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<SampleResult>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::with_capacity(arr.len());
                for item in arr {
                    match item {
                        Resp2Frame::Integer(n) => {
                            results.push(SampleResult::Success(n));
                        }
                        Resp2Frame::BulkString(data) => {
                            let s = String::from_utf8(data).map_err(EpError::parse)?;
                            if let Ok(n) = s.parse::<i64>() {
                                results.push(SampleResult::Success(n));
                            } else {
                                results.push(SampleResult::Error(s));
                            }
                        }
                        Resp2Frame::Error(e) => {
                            results.push(SampleResult::Error(e));
                        }
                        other => {
                            results.push(SampleResult::Error(format!("unexpected element: {:?}", other)));
                        }
                    }
                }
                Ok(results)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.MADD response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<SampleResult>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut results = Vec::with_capacity(data.len());
                for item in data {
                    match item {
                        Resp3Frame::Number { data: n, .. } => {
                            results.push(SampleResult::Success(n));
                        }
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            let s = String::from_utf8(data).map_err(EpError::parse)?;
                            if let Ok(n) = s.parse::<i64>() {
                                results.push(SampleResult::Success(n));
                            } else {
                                results.push(SampleResult::Error(s));
                            }
                        }
                        Resp3Frame::SimpleError { data, .. } => {
                            results.push(SampleResult::Error(data));
                        }
                        Resp3Frame::BlobError { data, .. } => {
                            results.push(SampleResult::Error(String::from_utf8_lossy(&data).to_string()));
                        }
                        other => {
                            results.push(SampleResult::Error(format!("unexpected element: {:?}", other)));
                        }
                    }
                }
                Ok(results)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.MADD response: {:?}", other))),
        }
    }
}

impl Serialize for TsMaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsMaddOutput", 1)?;
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
        fn test_encode_command_single_sample() {
            let input = TsMaddInput {
                samples: vec![Sample {
                    key: RedisKey::String("ts:key".into()),
                    timestamp: RedisJsonValue::Integer(1609459200000),
                    value: RedisJsonValue::Float(25.5),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.MADD"));
            assert!(cmd_str.contains("ts:key"));
        }

        #[test]
        fn test_encode_command_multiple_samples() {
            let input = TsMaddInput {
                samples: vec![
                    Sample {
                        key: RedisKey::String("ts:temp".into()),
                        timestamp: RedisJsonValue::Integer(1000),
                        value: RedisJsonValue::Float(20.0),
                    },
                    Sample {
                        key: RedisKey::String("ts:humidity".into()),
                        timestamp: RedisJsonValue::Integer(1000),
                        value: RedisJsonValue::Float(60.0),
                    },
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ts:temp"));
            assert!(cmd_str.contains("ts:humidity"));
        }

        #[test]
        fn test_decode_input_single_triplet() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1609459200000),
                RedisJsonValue::Float(25.5),
            ];
            let input = TsMaddInput::decode(args).unwrap();
            assert_eq!(input.samples.len(), 1);
            assert_eq!(input.samples[0].key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_multiple_triplets() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Float(10.0),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::Integer(2000),
                RedisJsonValue::Float(20.0),
            ];
            let input = TsMaddInput::decode(args).unwrap();
            assert_eq!(input.samples.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TsMaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least one"));
        }

        #[test]
        fn test_decode_input_incomplete_triplet_fails() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::Integer(1000)];
            let err = TsMaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("triplets"));
        }

        #[test]
        fn test_decode_output_all_success() {
            // RESP2 array of integers
            let output = TsMaddOutput::decode(b"*2\r\n:1609459200000\r\n:1609459200001\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!(output.all_success());
            assert_eq!(output.success_count(), 2);
            assert_eq!(output.error_count(), 0);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = TsMaddOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsMaddOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = TsMaddInput {
                samples: vec![
                    Sample {
                        key: RedisKey::String("key1".into()),
                        timestamp: RedisJsonValue::Integer(1000),
                        value: RedisJsonValue::Float(10.0),
                    },
                    Sample {
                        key: RedisKey::String("key2".into()),
                        timestamp: RedisJsonValue::Integer(2000),
                        value: RedisJsonValue::Float(20.0),
                    },
                ],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("key1".into()));
            assert_eq!(keys[1], RedisKey::String("key2".into()));
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsMaddInput {
                samples: vec![Sample {
                    key: RedisKey::String("key".into()),
                    timestamp: RedisJsonValue::Integer(1000),
                    value: RedisJsonValue::Float(10.0),
                }],
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsMadd);
        }

        #[test]
        fn test_output_timestamps() {
            let output = TsMaddOutput::new(vec![
                SampleResult::Success(1000),
                SampleResult::Error("error".into()),
                SampleResult::Success(2000),
            ]);
            let timestamps = output.timestamps();
            assert_eq!(timestamps, vec![1000, 2000]);
        }

        #[test]
        fn test_output_mixed_results() {
            let output = TsMaddOutput::new(vec![SampleResult::Success(1000), SampleResult::Error("TSDB: key does not exist".into())]);
            assert!(!output.all_success());
            assert_eq!(output.success_count(), 1);
            assert_eq!(output.error_count(), 1);
        }

        #[test]
        fn test_sample_struct() {
            let sample = Sample {
                key: RedisKey::String("test".into()),
                timestamp: RedisJsonValue::Integer(1000),
                value: RedisJsonValue::Float(42.5),
            };
            assert_eq!(sample.key, RedisKey::String("test".into()));
            assert_eq!(sample.timestamp, RedisJsonValue::Integer(1000));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.MADD requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_madd_single_sample() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMaddInput {
                                samples: vec![Sample {
                                    key: RedisKey::String("test:madd:single".into()),
                                    timestamp: RedisJsonValue::Integer(1609459200000),
                                    value: RedisJsonValue::Float(25.5),
                                }],
                            }
                            .command(),
                        )
                        .await;

                    // Result depends on whether TimeSeries module is installed
                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsMaddOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 1);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_madd_multiple_samples() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMaddInput {
                                samples: vec![
                                    Sample {
                                        key: RedisKey::String("test:madd:multi1".into()),
                                        timestamp: RedisJsonValue::Integer(1000),
                                        value: RedisJsonValue::Float(10.0),
                                    },
                                    Sample {
                                        key: RedisKey::String("test:madd:multi2".into()),
                                        timestamp: RedisJsonValue::Integer(1000),
                                        value: RedisJsonValue::Float(20.0),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsMaddOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 2);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_madd_auto_timestamp() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMaddInput {
                                samples: vec![Sample {
                                    key: RedisKey::String("test:madd:auto".into()),
                                    timestamp: RedisJsonValue::String("*".into()),
                                    value: RedisJsonValue::Float(42.0),
                                }],
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsMaddOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 1);
                        if output.all_success() {
                            assert!(output.timestamps()[0] > 0);
                        }
                    }
                })
            })
            .await;
        }
    }
}
