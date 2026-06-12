#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::value::RedisJsonValue;
use crate::command::Cmd;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use error::EpError;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// Encoding type for time series data compression.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, PartialEq, Eq, ToSchema, JsonSchema)]
pub enum TsEncoding {
    /// Compressed encoding (default, more space-efficient)
    #[default]
    COMPRESSED,
    /// Uncompressed encoding (faster writes)
    UNCOMPRESSED,
}

impl TsEncoding {
    /// Append encoding argument to a Redis command.
    pub fn cmd(&self, command: &mut Cmd) {
        command.arg("ENCODING");
        match self {
            TsEncoding::COMPRESSED => command.arg("COMPRESSED"),
            TsEncoding::UNCOMPRESSED => command.arg("UNCOMPRESSED"),
        };
    }

    /// Parse encoding from string value.
    pub fn from_str(s: &str) -> Result<Self, EpError> {
        match s.to_uppercase().as_str() {
            "COMPRESSED" => Ok(TsEncoding::COMPRESSED),
            "UNCOMPRESSED" => Ok(TsEncoding::UNCOMPRESSED),
            _ => Err(EpError::request(format!("Invalid encoding type: {}", s))),
        }
    }
}

/// Ignore parameters for duplicate sample handling in time series.
///
/// Controls when to ignore new samples based on time and value differences.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, PartialEq, Builder, ToSchema, JsonSchema)]
pub struct TsIgnore {
    /// Maximum time difference (in milliseconds) to ignore
    pub max_time_diff: RedisJsonValue,
    /// Maximum value difference to ignore
    pub max_val_diff: RedisJsonValue,
}

impl TsIgnore {
    /// Create a new TsIgnore with the given parameters.
    pub fn new(max_time_diff: impl Into<RedisJsonValue>, max_val_diff: impl Into<RedisJsonValue>) -> Self {
        Self {
            max_time_diff: max_time_diff.into(),
            max_val_diff: max_val_diff.into(),
        }
    }

    /// Append ignore arguments to a Redis command.
    pub fn cmd(&self, command: &mut Cmd) {
        command.arg("IGNORE").arg(&self.max_time_diff).arg(&self.max_val_diff);
    }
}

/// A label key-value pair for time series metadata.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct TsLabel {
    /// Label name/key
    pub label: RedisJsonValue,
    /// Label value
    pub value: RedisJsonValue,
}

impl TsLabel {
    /// Create a new TsLabel with the given key and value.
    pub fn new(label: impl Into<RedisJsonValue>, value: impl Into<RedisJsonValue>) -> Self {
        Self { label: label.into(), value: value.into() }
    }
}

/// Helper to append labels to a Redis command.
pub fn append_labels_to_cmd(labels: &[TsLabel], command: &mut Cmd) {
    command.arg("LABELS");
    for label in labels {
        command.arg(&label.label).arg(&label.value);
    }
}

/// Helper to parse labels from command arguments.
///
/// Returns (Vec<TsLabel>, new_index) where new_index is the position after all labels.
pub fn parse_labels_from_args(args: &[RedisJsonValue], start_index: usize) -> (Vec<TsLabel>, usize) {
    let mut labels = Vec::new();
    let mut i = start_index;

    while i + 1 < args.len() {
        // Check if we've hit another keyword
        if let RedisJsonValue::String(s) = &args[i] {
            let upper = s.to_uppercase();
            if is_ts_keyword(&upper) {
                break;
            }
        }
        labels.push(TsLabel { label: args[i].clone(), value: args[i + 1].clone() });
        i += 2;
    }

    (labels, i)
}

/// Check if a string is a known TimeSeries command keyword.
fn is_ts_keyword(s: &str) -> bool {
    matches!(
        s,
        "RETENTION"
            | "ENCODING"
            | "CHUNK_SIZE"
            | "DUPLICATE_POLICY"
            | "ON_DUPLICATE"
            | "IGNORE"
            | "LABELS"
            | "TIMESTAMP"
            | "AGGREGATION"
            | "ALIGN"
            | "FILTER"
            | "FILTER_BY_TS"
            | "FILTER_BY_VALUE"
            | "COUNT"
            | "LATEST"
            | "WITHLABELS"
            | "SELECTED_LABELS"
            | "GROUPBY"
    )
}

/// Aggregation and alignment options for time series range queries.
///
/// Used to bucket and aggregate data points within a time range.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Align {
    /// Alignment timestamp for bucketing (e.g., "start", "end", or timestamp)
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub align: Option<RedisJsonValue>,
    /// Aggregation function (e.g., "avg", "sum", "min", "max", "count", etc.)
    pub aggregator: RedisJsonValue,
    /// Duration of each bucket in milliseconds
    pub bucket_duration: RedisJsonValue,
    /// Controls which timestamp is reported for each bucket ("start", "end", "mid")
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_timestamp: Option<RedisJsonValue>,
    /// If true, emit empty buckets
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub empty: Option<bool>,
}

impl Align {
    /// Append aggregation arguments to a Redis command.
    pub fn cmd(&self, command: &mut Cmd) {
        if let Some(align) = &self.align {
            command.arg("ALIGN").arg(align);
        }

        command.arg("AGGREGATION").arg(&self.aggregator).arg(&self.bucket_duration);

        if let Some(bucket_timestamp) = &self.bucket_timestamp {
            command.arg("BUCKETTIMESTAMP").arg(bucket_timestamp);
        }

        if let Some(true) = &self.empty {
            command.arg("EMPTY");
        }
    }
}

/// Filter by value range for time series queries.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct FilterByValue {
    /// Minimum value (inclusive)
    pub min: RedisJsonValue,
    /// Maximum value (inclusive)
    pub max: RedisJsonValue,
}

impl FilterByValue {
    /// Append filter arguments to a Redis command.
    pub fn cmd(&self, command: &mut Cmd) {
        command.arg("FILTER_BY_VALUE").arg(&self.min).arg(&self.max);
    }
}

/// Label output options for multi-key time series queries.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Label {
    /// Include all labels in the response
    WITHLABELS,
    /// Include only the specified labels in the response
    SELECTEDLABELS(Vec<RedisJsonValue>),
}

/// Grouping options for multi-key time series queries.
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Group {
    /// Label to group by
    pub group_by: RedisJsonValue,
    /// Reducer function (e.g., "sum", "min", "max", "avg", etc.)
    pub reduce: RedisJsonValue,
}

impl Group {
    /// Append grouping arguments to a Redis command.
    pub fn cmd(&self, command: &mut Cmd) {
        command.arg("GROUPBY").arg(&self.group_by).arg("REDUCE").arg(&self.reduce);
    }
}

/// Helper to parse Align from command arguments.
///
/// Returns (Align, new_index) on success.
pub fn parse_align_from_args(args: &[RedisJsonValue], start_index: usize, has_align_value: bool) -> Option<(Align, usize)> {
    let mut i = start_index;
    let align_val = if has_align_value && i < args.len() {
        let val = Some(args[i].clone());
        i += 1;
        val
    } else {
        None
    };

    // Must have AGGREGATION keyword followed by aggregator and bucket_duration
    if i >= args.len() {
        return None;
    }

    let (aggregator, bucket_duration, mut i) = if let RedisJsonValue::String(s) = &args[i] {
        if s.to_uppercase() == "AGGREGATION" && i + 2 < args.len() {
            (args[i + 1].clone(), args[i + 2].clone(), i + 3)
        } else if has_align_value {
            // ALIGN was specified but no AGGREGATION follows - use defaults
            (RedisJsonValue::String("avg".to_string()), RedisJsonValue::String("60".to_string()), i)
        } else {
            return None;
        }
    } else if has_align_value {
        (RedisJsonValue::String("avg".to_string()), RedisJsonValue::String("60".to_string()), i)
    } else {
        return None;
    };

    let mut bucket_timestamp = None;
    let mut empty = None;

    // Parse optional BUCKETTIMESTAMP and EMPTY
    while i < args.len() {
        if let RedisJsonValue::String(s) = &args[i] {
            let upper = s.to_uppercase();
            match upper.as_str() {
                "BUCKETTIMESTAMP" if i + 1 < args.len() => {
                    bucket_timestamp = Some(args[i + 1].clone());
                    i += 2;
                }
                "EMPTY" => {
                    empty = Some(true);
                    i += 1;
                }
                _ => break,
            }
        } else {
            break;
        }
    }

    Some((
        Align {
            align: align_val,
            aggregator,
            bucket_duration,
            bucket_timestamp,
            empty,
        },
        i,
    ))
}

/// Helper to parse SELECTED_LABELS from arguments.
///
/// Returns (Vec<labels>, new_index).
pub fn parse_selected_labels(args: &[RedisJsonValue], start_index: usize, stop_keywords: &[&str]) -> (Vec<RedisJsonValue>, usize) {
    let mut labels = Vec::new();
    let mut i = start_index;

    while i < args.len() {
        if let RedisJsonValue::String(s) = &args[i] {
            let upper = s.to_uppercase();
            if stop_keywords.contains(&upper.as_str()) {
                break;
            }
        }
        labels.push(args[i].clone());
        i += 1;
    }

    (labels, i)
}

/// Output for commands that return a timestamp (TS.ADD, TS.INCRBY, TS.DECRBY).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TsTimestampOutput {
    /// The timestamp of the added/modified sample
    pub timestamp: i64,
}

impl TsTimestampOutput {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// Decode the Redis protocol response.
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        use crate::protocol::RedisProtocol;
        use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
        use endpoint_types::protocol::EpProtocol;

        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let timestamp = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) => {
                String::from_utf8(data).map_err(EpError::parse)?.parse().map_err(EpError::parse)?
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                String::from_utf8(data).map_err(EpError::parse)?.parse().map_err(EpError::parse)?
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected response type: {:?}", other)));
            }
        };

        Ok(Self { timestamp })
    }
}

/// Output for TS.CREATE, TS.ALTER (simple OK response).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TsOkOutput {
    /// Whether the operation succeeded
    pub success: bool,
}

impl TsOkOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Decode the Redis protocol response.
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        use crate::protocol::RedisProtocol;
        use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
        use endpoint_types::protocol::EpProtocol;

        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(data)) => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Ok(Self { success: s == "OK" })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                Ok(Self { success: s == "OK" })
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected response type: {:?}", other))),
        }
    }
}

/// Output for TS.DEL (returns count of deleted samples).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TsDelOutput {
    /// Number of samples deleted
    pub deleted_count: i64,
}

impl TsDelOutput {
    pub fn new(deleted_count: i64) -> Self {
        Self { deleted_count }
    }

    /// Decode the Redis protocol response.
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        use crate::protocol::RedisProtocol;
        use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
        use endpoint_types::protocol::EpProtocol;

        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted_count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected response type: {:?}", other)));
            }
        };

        Ok(Self { deleted_count })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        // TsEncoding tests
        #[test]
        fn test_encoding_default() {
            let encoding = TsEncoding::default();
            assert_eq!(encoding, TsEncoding::COMPRESSED);
        }

        #[test]
        fn test_encoding_from_str_compressed() {
            let encoding = TsEncoding::from_str("COMPRESSED").unwrap();
            assert_eq!(encoding, TsEncoding::COMPRESSED);
        }

        #[test]
        fn test_encoding_from_str_uncompressed() {
            let encoding = TsEncoding::from_str("uncompressed").unwrap();
            assert_eq!(encoding, TsEncoding::UNCOMPRESSED);
        }

        #[test]
        fn test_encoding_from_str_invalid() {
            let err = TsEncoding::from_str("invalid").unwrap_err();
            assert!(err.to_string().contains("Invalid encoding"));
        }

        #[test]
        fn test_encoding_cmd_compressed() {
            let encoding = TsEncoding::COMPRESSED;
            let mut cmd = crate::command::cmd("TS.ADD");
            encoding.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("ENCODING"));
            assert!(cmd_str.contains("COMPRESSED"));
        }

        #[test]
        fn test_encoding_cmd_uncompressed() {
            let encoding = TsEncoding::UNCOMPRESSED;
            let mut cmd = crate::command::cmd("TS.ADD");
            encoding.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("UNCOMPRESSED"));
        }

        // TsIgnore tests
        #[test]
        fn test_ignore_new() {
            let ignore = TsIgnore::new(1000, 0.1);
            assert_eq!(ignore.max_time_diff, RedisJsonValue::Integer(1000));
        }

        #[test]
        fn test_ignore_cmd() {
            let ignore = TsIgnore {
                max_time_diff: RedisJsonValue::Integer(1000),
                max_val_diff: RedisJsonValue::Float(0.5),
            };
            let mut cmd = crate::command::cmd("TS.ADD");
            ignore.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("IGNORE"));
        }

        // TsLabel tests
        #[test]
        fn test_label_new() {
            let label = TsLabel::new("sensor", "temperature");
            assert_eq!(label.label, RedisJsonValue::String("sensor".into()));
            assert_eq!(label.value, RedisJsonValue::String("temperature".into()));
        }

        #[test]
        fn test_append_labels_to_cmd() {
            let labels = vec![TsLabel::new("sensor", "temp"), TsLabel::new("location", "room1")];
            let mut cmd = crate::command::cmd("TS.ADD");
            append_labels_to_cmd(&labels, &mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("temp"));
        }

        #[test]
        fn test_parse_labels_from_args() {
            let args = vec![
                RedisJsonValue::String("sensor".into()),
                RedisJsonValue::String("temp".into()),
                RedisJsonValue::String("location".into()),
                RedisJsonValue::String("room1".into()),
            ];
            let (labels, idx) = parse_labels_from_args(&args, 0);
            assert_eq!(labels.len(), 2);
            assert_eq!(idx, 4);
        }

        #[test]
        fn test_parse_labels_stops_at_keyword() {
            let args = vec![
                RedisJsonValue::String("sensor".into()),
                RedisJsonValue::String("temp".into()),
                RedisJsonValue::String("RETENTION".into()),
                RedisJsonValue::Integer(1000),
            ];
            let (labels, idx) = parse_labels_from_args(&args, 0);
            assert_eq!(labels.len(), 1);
            assert_eq!(idx, 2);
        }

        // Align tests
        #[test]
        fn test_align_cmd_basic() {
            let align = Align {
                align: None,
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(60000),
                bucket_timestamp: None,
                empty: None,
            };
            let mut cmd = crate::command::cmd("TS.RANGE");
            align.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("AGGREGATION"));
            assert!(cmd_str.contains("avg"));
            assert!(!cmd_str.contains("ALIGN"));
        }

        #[test]
        fn test_align_cmd_all_options() {
            let align = Align {
                align: Some(RedisJsonValue::String("start".into())),
                aggregator: RedisJsonValue::String("sum".into()),
                bucket_duration: RedisJsonValue::Integer(3600000),
                bucket_timestamp: Some(RedisJsonValue::String("end".into())),
                empty: Some(true),
            };
            let mut cmd = crate::command::cmd("TS.RANGE");
            align.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("ALIGN"));
            assert!(cmd_str.contains("AGGREGATION"));
            assert!(cmd_str.contains("BUCKETTIMESTAMP"));
            assert!(cmd_str.contains("EMPTY"));
        }

        #[test]
        fn test_align_cmd_empty_false() {
            let align = Align {
                align: None,
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(60000),
                bucket_timestamp: None,
                empty: Some(false),
            };
            let mut cmd = crate::command::cmd("TS.RANGE");
            align.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(!cmd_str.contains("EMPTY"));
        }

        // FilterByValue tests
        #[test]
        fn test_filter_by_value_cmd() {
            let filter = FilterByValue {
                min: RedisJsonValue::Integer(0),
                max: RedisJsonValue::Integer(100),
            };
            let mut cmd = crate::command::cmd("TS.RANGE");
            filter.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("FILTER_BY_VALUE"));
        }

        #[test]
        fn test_filter_by_value_with_floats() {
            let filter = FilterByValue {
                min: RedisJsonValue::Float(-10.5),
                max: RedisJsonValue::Float(100.5),
            };
            let mut cmd = crate::command::cmd("TS.RANGE");
            filter.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("FILTER_BY_VALUE"));
        }

        // Group tests
        #[test]
        fn test_group_cmd() {
            let group = Group {
                group_by: RedisJsonValue::String("sensor".into()),
                reduce: RedisJsonValue::String("avg".into()),
            };
            let mut cmd = crate::command::cmd("TS.MRANGE");
            group.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("GROUPBY"));
            assert!(cmd_str.contains("REDUCE"));
        }

        #[test]
        fn test_group_cmd_with_sum() {
            let group = Group {
                group_by: RedisJsonValue::String("location".into()),
                reduce: RedisJsonValue::String("sum".into()),
            };
            let mut cmd = crate::command::cmd("TS.MRANGE");
            group.cmd(&mut cmd);
            let packed = cmd.get_packed_command();
            let cmd_str = String::from_utf8_lossy(&packed);
            assert!(cmd_str.contains("location"));
            assert!(cmd_str.contains("sum"));
        }

        // Label enum tests
        #[test]
        fn test_label_enum_withlabels() {
            let label = Label::WITHLABELS;
            assert!(matches!(label, Label::WITHLABELS));
        }

        #[test]
        fn test_label_enum_selectedlabels() {
            let label = Label::SELECTEDLABELS(vec![RedisJsonValue::String("sensor".into()), RedisJsonValue::String("location".into())]);
            if let Label::SELECTEDLABELS(labels) = label {
                assert_eq!(labels.len(), 2);
            } else {
                panic!("Expected SELECTEDLABELS variant");
            }
        }

        // parse_align_from_args tests
        #[test]
        fn test_parse_align_aggregation_only() {
            let args = vec![
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("avg".into()),
                RedisJsonValue::Integer(60000),
            ];
            let result = parse_align_from_args(&args, 0, false);
            assert!(result.is_some());
            let (align, idx) = result.unwrap();
            assert!(align.align.is_none());
            assert_eq!(idx, 3);
        }

        #[test]
        fn test_parse_align_with_bucket_timestamp() {
            let args = vec![
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("sum".into()),
                RedisJsonValue::Integer(3600000),
                RedisJsonValue::String("BUCKETTIMESTAMP".into()),
                RedisJsonValue::String("end".into()),
            ];
            let result = parse_align_from_args(&args, 0, false);
            assert!(result.is_some());
            let (align, idx) = result.unwrap();
            assert!(align.bucket_timestamp.is_some());
            assert_eq!(idx, 5);
        }

        #[test]
        fn test_parse_align_with_empty() {
            let args = vec![
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("count".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("EMPTY".into()),
            ];
            let result = parse_align_from_args(&args, 0, false);
            assert!(result.is_some());
            let (align, idx) = result.unwrap();
            assert_eq!(align.empty, Some(true));
            assert_eq!(idx, 4);
        }

        #[test]
        fn test_parse_align_with_align_value() {
            let args = vec![
                RedisJsonValue::String("start".into()),
                RedisJsonValue::String("AGGREGATION".into()),
                RedisJsonValue::String("avg".into()),
                RedisJsonValue::Integer(60000),
            ];
            let result = parse_align_from_args(&args, 0, true);
            assert!(result.is_some());
            let (align, idx) = result.unwrap();
            assert!(align.align.is_some());
            assert_eq!(idx, 4);
        }

        #[test]
        fn test_parse_align_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let result = parse_align_from_args(&args, 0, false);
            assert!(result.is_none());
        }

        #[test]
        fn test_parse_align_no_aggregation() {
            let args = vec![RedisJsonValue::String("OTHER".into())];
            let result = parse_align_from_args(&args, 0, false);
            assert!(result.is_none());
        }

        // parse_selected_labels tests
        #[test]
        fn test_parse_selected_labels() {
            let args = vec![
                RedisJsonValue::String("label1".into()),
                RedisJsonValue::String("label2".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let (labels, idx) = parse_selected_labels(&args, 0, &["COUNT", "ALIGN", "AGGREGATION", "FILTER"]);
            assert_eq!(labels.len(), 2);
            assert_eq!(idx, 2);
        }

        #[test]
        fn test_parse_selected_labels_empty() {
            let args = vec![RedisJsonValue::String("COUNT".into())];
            let (labels, idx) = parse_selected_labels(&args, 0, &["COUNT"]);
            assert!(labels.is_empty());
            assert_eq!(idx, 0);
        }

        #[test]
        fn test_parse_selected_labels_all() {
            let args = vec![
                RedisJsonValue::String("label1".into()),
                RedisJsonValue::String("label2".into()),
                RedisJsonValue::String("label3".into()),
            ];
            let (labels, idx) = parse_selected_labels(&args, 0, &["NONE"]);
            assert_eq!(labels.len(), 3);
            assert_eq!(idx, 3);
        }

        #[test]
        fn test_parse_selected_labels_with_offset() {
            let args = vec![
                RedisJsonValue::String("ignored".into()),
                RedisJsonValue::String("label1".into()),
                RedisJsonValue::String("FILTER".into()),
            ];
            let (labels, idx) = parse_selected_labels(&args, 1, &["FILTER"]);
            assert_eq!(labels.len(), 1);
            assert_eq!(idx, 2);
        }

        // Output type tests
        #[test]
        fn test_ts_timestamp_output_new() {
            let output = TsTimestampOutput::new(1609459200000);
            assert_eq!(output.timestamp, 1609459200000);
        }

        #[test]
        fn test_ts_timestamp_output_decode_integer() {
            // RESP2 integer: :1609459200000\r\n
            let output = TsTimestampOutput::decode(b":1609459200000\r\n").unwrap();
            assert_eq!(output.timestamp, 1609459200000);
        }

        #[test]
        fn test_ts_timestamp_output_decode_error() {
            let err = TsTimestampOutput::decode(b"-ERR key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_ts_ok_output_new() {
            let output = TsOkOutput::new(true);
            assert!(output.success);
        }

        #[test]
        fn test_ts_ok_output_decode_ok() {
            let output = TsOkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success);
        }

        #[test]
        fn test_ts_ok_output_decode_error() {
            let err = TsOkOutput::decode(b"-ERR something\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_ts_del_output_new() {
            let output = TsDelOutput::new(5);
            assert_eq!(output.deleted_count, 5);
        }

        #[test]
        fn test_ts_del_output_decode_integer() {
            let output = TsDelOutput::decode(b":10\r\n").unwrap();
            assert_eq!(output.deleted_count, 10);
        }

        #[test]
        fn test_ts_del_output_decode_zero() {
            let output = TsDelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted_count, 0);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        // Note: Common types are tested through their usage in TS.RANGE, TS.MRANGE, etc.
        // Integration tests for these commands validate the common types work correctly
        // with real Redis TimeSeries module.

        #[tokio::test]
        async fn test_common_types_compile() {
            // This test ensures the common types can be used in async contexts
            // Actual functionality is tested in ts_range, ts_mrange, etc.
            use super::*;

            let _align = Align {
                align: None,
                aggregator: RedisJsonValue::String("avg".into()),
                bucket_duration: RedisJsonValue::Integer(60000),
                bucket_timestamp: None,
                empty: None,
            };

            let _filter = FilterByValue {
                min: RedisJsonValue::Integer(0),
                max: RedisJsonValue::Integer(100),
            };

            let _group = Group {
                group_by: RedisJsonValue::String("sensor".into()),
                reduce: RedisJsonValue::String("avg".into()),
            };

            let _label = Label::WITHLABELS;

            let _encoding = TsEncoding::COMPRESSED;

            let _ignore = TsIgnore::new(1000, 0.1);

            let _ts_label = TsLabel::new("sensor", "temp");
        }
    }
}
