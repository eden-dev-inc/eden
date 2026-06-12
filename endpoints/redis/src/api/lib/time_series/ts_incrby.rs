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

const API_INFO: ApiInfo<RedisApi, TsIncrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsIncrby,
    "Increases the value of the sample with the maximum existing timestamp, or creates a new sample with a value equal to the value of the sample with the maximum existing timestamp with a given increment",
    ReqType::Write,
    true,
);

/// Input for Redis `TS.INCRBY` command.
///
/// Increases the value of the sample with the maximum existing timestamp,
/// or creates a new sample with a value equal to the increment if the key does not exist.
///
/// See official Redis documentation for `TS.INCRBY`:
/// https://redis.io/docs/latest/commands/ts.incrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsIncrbyInput {
    /// The key name of the time series
    key: RedisKey,
    /// The value to add
    addend: RedisJsonValue,
    /// Timestamp for the new sample (use "*" for auto-timestamp)
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<RedisJsonValue>,
    /// Maximum retention period in milliseconds
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    retention: Option<RedisJsonValue>,
    /// Encoding type for the time series
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    encoding: Option<Encoding>,
    /// Memory size in bytes for each data chunk
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    chunk_size: Option<RedisJsonValue>,
    /// Policy for handling duplicate timestamps
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    duplicate_policy: Option<RedisJsonValue>,
    /// Ignore settings for deduplication
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    ignore: Option<Ignore>,
    /// Labels to assign to the time series
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    labels: Option<Vec<Label>>,
}

impl Serialize for TsIncrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, addend
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

        let mut state = serializer.serialize_struct("TsIncrbyInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("addend", &self.addend)?;

        if let Some(timestamp) = &self.timestamp {
            state.serialize_field("timestamp", timestamp)?;
        }
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
        if let Some(ignore) = &self.ignore {
            state.serialize_field("ignore", ignore)?;
        }
        if let Some(labels) = &self.labels {
            state.serialize_field("labels", labels)?;
        }
        state.end()
    }
}

/// Ignore settings for sample deduplication.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Ignore {
    /// Maximum time difference in milliseconds for ignoring samples
    pub ignore_max_time_diff: RedisJsonValue,
    /// Maximum value difference for ignoring samples
    pub ignore_max_val_diff: RedisJsonValue,
}

/// A label key-value pair for the time series.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Label {
    /// Label key
    pub label: RedisJsonValue,
    /// Label value
    pub value: RedisJsonValue,
}

/// Encoding type for time series data.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Encoding {
    /// Compressed encoding (default, uses double-delta compression)
    #[default]
    COMPRESSED,
    /// Uncompressed encoding
    UNCOMPRESSED,
}

impl_redis_operation!(
    TsIncrbyInput,
    API_INFO,
    {key, addend, timestamp, retention, encoding, chunk_size, duplicate_policy, ignore, labels}
);

impl RedisCommandInput for TsIncrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.addend);

        if let Some(timestamp) = &self.timestamp {
            command.arg("TIMESTAMP").arg(timestamp);
        }

        if let Some(retention) = &self.retention {
            command.arg("RETENTION").arg(retention);
        }

        if let Some(encoding) = &self.encoding {
            command.arg("ENCODING");
            match encoding {
                Encoding::COMPRESSED => command.arg("COMPRESSED"),
                Encoding::UNCOMPRESSED => command.arg("UNCOMPRESSED"),
            };
        }

        if let Some(chunk_size) = &self.chunk_size {
            command.arg("CHUNK_SIZE").arg(chunk_size);
        }

        if let Some(duplicate_policy) = &self.duplicate_policy {
            command.arg("DUPLICATE_POLICY").arg(duplicate_policy);
        }

        if let Some(ignore) = &self.ignore {
            command.arg("IGNORE").arg(&ignore.ignore_max_time_diff).arg(&ignore.ignore_max_val_diff);
        }

        if let Some(labels) = &self.labels {
            command.arg("LABELS");
            for label in labels {
                command.arg(&label.label).arg(&label.value);
            }
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!(
                "TS.INCRBY requires at least 2 arguments (key, addend), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let addend = args[1].clone();
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
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "TIMESTAMP" => {
                        if i + 1 < args.len() {
                            timestamp = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("TIMESTAMP requires a value"));
                        }
                    }
                    "RETENTION" => {
                        if i + 1 < args.len() {
                            retention = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("RETENTION requires a value"));
                        }
                    }
                    "ENCODING" => {
                        if i + 1 < args.len() {
                            if let RedisJsonValue::String(enc_str) = &args[i + 1] {
                                encoding = match enc_str.to_uppercase().as_str() {
                                    "COMPRESSED" => Some(Encoding::COMPRESSED),
                                    "UNCOMPRESSED" => Some(Encoding::UNCOMPRESSED),
                                    _ => None,
                                };
                            }
                            i += 2;
                        } else {
                            return Err(EpError::request("ENCODING requires a value"));
                        }
                    }
                    "CHUNK_SIZE" => {
                        if i + 1 < args.len() {
                            chunk_size = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("CHUNK_SIZE requires a value"));
                        }
                    }
                    "DUPLICATE_POLICY" => {
                        if i + 1 < args.len() {
                            duplicate_policy = Some(args[i + 1].clone());
                            i += 2;
                        } else {
                            return Err(EpError::request("DUPLICATE_POLICY requires a value"));
                        }
                    }
                    "IGNORE" => {
                        if i + 2 < args.len() {
                            ignore = Some(Ignore {
                                ignore_max_time_diff: args[i + 1].clone(),
                                ignore_max_val_diff: args[i + 2].clone(),
                            });
                            i += 3;
                        } else {
                            return Err(EpError::request("IGNORE requires maxTimeDiff and maxValDiff values"));
                        }
                    }
                    "LABELS" => {
                        i += 1;
                        let mut label_vec = Vec::new();
                        while i + 1 < args.len() {
                            // Check if next arg is another keyword
                            if let RedisJsonValue::String(s) = &args[i] {
                                let upper = s.to_uppercase();
                                if matches!(
                                    upper.as_str(),
                                    "TIMESTAMP" | "RETENTION" | "ENCODING" | "CHUNK_SIZE" | "DUPLICATE_POLICY" | "IGNORE"
                                ) {
                                    break;
                                }
                            }

                            label_vec.push(Label { label: args[i].clone(), value: args[i + 1].clone() });
                            i += 2;
                        }
                        if !label_vec.is_empty() {
                            labels = Some(label_vec);
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        Ok(Self {
            key,
            addend,
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

/// Output for Redis `TS.INCRBY` command.
///
/// Returns the timestamp of the upserted sample.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsIncrbyOutput {
    /// Timestamp of the upserted sample in milliseconds
    timestamp: i64,
}

impl TsIncrbyOutput {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// Get the timestamp of the upserted sample
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Decode the Redis protocol response into a TsIncrbyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let timestamp = Self::parse_frame(frame)?;
        Ok(Self { timestamp })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<i64, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(n),
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?.parse().map_err(EpError::parse),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TS.INCRBY response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(data),
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?.parse().map_err(EpError::parse),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TS.INCRBY response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TsIncrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsIncrbyOutput", 1)?;
        state.serialize_field("timestamp", &self.timestamp)?;
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
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
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
            assert!(cmd_str.contains("TS.INCRBY"));
            assert!(cmd_str.contains("ts:key"));
        }

        #[test]
        fn test_encode_command_with_timestamp() {
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
                timestamp: Some(RedisJsonValue::String("*".into())),
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
        }

        #[test]
        fn test_encode_command_with_retention() {
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
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
        }

        #[test]
        fn test_encode_command_with_encoding() {
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
                timestamp: None,
                retention: None,
                encoding: Some(Encoding::UNCOMPRESSED),
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
        fn test_encode_command_with_labels() {
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: Some(vec![Label {
                    label: RedisJsonValue::String("sensor".into()),
                    value: RedisJsonValue::String("temp".into()),
                }]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("temp"));
        }

        #[test]
        fn test_encode_command_with_ignore() {
            let input = TsIncrbyInput {
                key: RedisKey::String("ts:key".into()),
                addend: RedisJsonValue::Integer(10),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: Some(Ignore {
                    ignore_max_time_diff: RedisJsonValue::Integer(1000),
                    ignore_max_val_diff: RedisJsonValue::Float(0.1),
                }),
                labels: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("IGNORE"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(5)];
            let input = TsIncrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.addend, RedisJsonValue::Integer(5));
            assert!(input.timestamp.is_none());
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("TIMESTAMP".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(86400000),
            ];
            let input = TsIncrbyInput::decode(args).unwrap();
            assert!(input.timestamp.is_some());
            assert!(input.retention.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = TsIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 2"));
        }

        #[test]
        fn test_decode_output_integer() {
            let output = TsIncrbyOutput::decode(b":1609459200000\r\n").unwrap();
            assert_eq!(output.timestamp(), 1609459200000);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsIncrbyOutput::decode(b"-ERR TSDB: the key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("TSDB"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsIncrbyInput {
                key: RedisKey::String("mykey".into()),
                addend: RedisJsonValue::Integer(5),
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
        fn test_kind_returns_correct_api() {
            let input = TsIncrbyInput {
                key: RedisKey::String("mykey".into()),
                addend: RedisJsonValue::Integer(5),
                timestamp: None,
                retention: None,
                encoding: None,
                chunk_size: None,
                duplicate_policy: None,
                ignore: None,
                labels: None,
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsIncrby);
        }

        #[test]
        fn test_encoding_default() {
            let encoding = Encoding::default();
            assert!(matches!(encoding, Encoding::COMPRESSED));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.INCRBY requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_creates_new_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsIncrbyInput {
                                key: RedisKey::String("test:incrby:new".into()),
                                addend: RedisJsonValue::Integer(10),
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

                    // Result depends on whether TimeSeries module is installed
                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsIncrbyOutput::decode(&result).expect("decode failed");
                        assert!(output.timestamp() > 0);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_incrby_with_timestamp() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsIncrbyInput {
                                key: RedisKey::String("test:incrby:ts".into()),
                                addend: RedisJsonValue::Integer(5),
                                timestamp: Some(RedisJsonValue::Integer(1609459200000)),
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

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsIncrbyOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.timestamp(), 1609459200000);
                    }
                })
            })
            .await;
        }
    }
}
