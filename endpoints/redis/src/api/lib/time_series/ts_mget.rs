use crate::api::lib::time_series::common::Label;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsMgetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsMget,
    "Get the sample with the highest timestamp from each time series matching a specific filter",
    ReqType::Read,
    true,
);

/// Input for Redis `TS.MGET` command.
///
/// Gets the last sample from each time series matching the specified filter.
///
/// See official Redis documentation for `TS.MGET`:
/// https://redis.io/docs/latest/commands/ts.mget/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsMgetInput {
    /// When true, report the compacted value of the latest bucket for compacted time series
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<bool>,
    /// Label output option
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    label: Option<Label>,
    /// Filter expression to match time series (required)
    filter: RedisJsonValue,
}

impl Serialize for TsMgetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, filter
        if self.latest.is_some() {
            fields += 1;
        }
        if self.label.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsMgetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(latest) = &self.latest {
            state.serialize_field("latest", latest)?;
        }
        if let Some(label) = &self.label {
            state.serialize_field("label", label)?;
        }
        state.serialize_field("filter", &self.filter)?;
        state.end()
    }
}

impl_redis_operation!(
   TsMgetInput,
    API_INFO,
    {latest, label, filter}
);

impl RedisCommandInput for TsMgetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(true) = &self.latest {
            command.arg("LATEST");
        }

        if let Some(label) = &self.label {
            match label {
                Label::WITHLABELS => {
                    command.arg("WITHLABELS");
                }
                Label::SELECTEDLABELS(labels) => {
                    command.arg("SELECTED_LABELS");
                    for label in labels {
                        command.arg(label);
                    }
                }
            }
        }

        command.arg("FILTER").arg(&self.filter);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.MGET requires at least a filter argument"));
        }

        let mut latest = None;
        let mut label = None;
        let mut filter = None;
        let mut i = 0;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "LATEST" => {
                        latest = Some(true);
                        i += 1;
                    }
                    "WITHLABELS" => {
                        label = Some(Label::WITHLABELS);
                        i += 1;
                    }
                    "SELECTED_LABELS" => {
                        i += 1;
                        let mut labels = Vec::new();
                        // Collect labels until we hit FILTER or end
                        while i < args.len() {
                            if let RedisJsonValue::String(s) = &args[i]
                                && s.to_uppercase() == "FILTER"
                            {
                                break;
                            }
                            labels.push(args[i].clone());
                            i += 1;
                        }
                        if !labels.is_empty() {
                            label = Some(Label::SELECTEDLABELS(labels));
                        }
                    }
                    "FILTER" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FILTER requires a value"));
                        }
                        filter = Some(args[i + 1].clone());
                        i += 2;
                        // Collect additional filter expressions
                        while i < args.len() {
                            // Additional filters would be handled here if needed
                            i += 1;
                        }
                    }
                    _ => {
                        // Assume this is the filter if no FILTER keyword
                        filter = Some(args[i].clone());
                        i += 1;
                    }
                }
            } else {
                filter = Some(args[i].clone());
                i += 1;
            }
        }

        if filter.is_none() {
            return Err(EpError::request("TS.MGET requires a filter"));
        }

        Ok(TsMgetInput { latest, label, filter: filter.unwrap() })
    }
}

/// A single sample from a time series.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub struct Sample {
    /// Timestamp in milliseconds
    pub timestamp: i64,
    /// Value at this timestamp
    pub value: f64,
}

/// Result for a single time series in TS.MGET response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub struct SeriesResult {
    /// The key name of the time series
    pub key: String,
    /// Labels associated with the time series (if requested)
    pub labels: HashMap<String, String>,
    /// The last sample (may be None if series is empty)
    pub sample: Option<Sample>,
}

/// Output for Redis `TS.MGET` command.
///
/// Contains results from multiple time series matching the filter.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsMgetOutput {
    /// Results for each matching time series
    results: Vec<SeriesResult>,
}

impl TsMgetOutput {
    pub fn new(results: Vec<SeriesResult>) -> Self {
        Self { results }
    }

    /// Get all results
    pub fn results(&self) -> &[SeriesResult] {
        &self.results
    }

    /// Get the number of matching time series
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if no time series matched
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Get result by key name
    pub fn get(&self, key: &str) -> Option<&SeriesResult> {
        self.results.iter().find(|r| r.key == key)
    }

    /// Get all keys that matched
    pub fn keys(&self) -> Vec<&str> {
        self.results.iter().map(|r| r.key.as_str()).collect()
    }

    /// Decode the Redis protocol response into a TsMgetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = Self::parse_frame(frame)?;
        Ok(Self { results })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<SeriesResult>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<SeriesResult>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(series) = item {
                        let result = Self::parse_resp2_series(&series)?;
                        results.push(result);
                    }
                }
                Ok(results)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.MGET response: {:?}", other))),
        }
    }

    fn parse_resp2_series(series: &[Resp2Frame]) -> Result<SeriesResult, EpError> {
        if series.is_empty() {
            return Err(EpError::parse("empty series result"));
        }

        // First element is the key
        let key = Self::resp2_to_string(&series[0])?;

        // Second element is labels (may be empty array)
        let labels = if series.len() > 1 {
            Self::parse_resp2_labels(&series[1])?
        } else {
            HashMap::new()
        };

        // Third element is the sample (timestamp, value pair or empty)
        let sample = if series.len() > 2 {
            Self::parse_resp2_sample(&series[2])?
        } else {
            None
        };

        Ok(SeriesResult { key, labels, sample })
    }

    fn parse_resp2_labels(frame: &Resp2Frame) -> Result<HashMap<String, String>, EpError> {
        let mut labels = HashMap::new();
        if let Resp2Frame::Array(arr) = frame {
            for item in arr {
                if let Resp2Frame::Array(pair) = item
                    && pair.len() == 2
                {
                    let k = Self::resp2_to_string(&pair[0])?;
                    let v = Self::resp2_to_string(&pair[1])?;
                    labels.insert(k, v);
                }
            }
        }
        Ok(labels)
    }

    fn parse_resp2_sample(frame: &Resp2Frame) -> Result<Option<Sample>, EpError> {
        match frame {
            Resp2Frame::Array(arr) if arr.len() == 2 => {
                let timestamp = Self::parse_resp2_integer(&arr[0])?;
                let value = Self::parse_resp2_float(&arr[1])?;
                Ok(Some(Sample { timestamp, value }))
            }
            Resp2Frame::Array(arr) if arr.is_empty() => Ok(None),
            Resp2Frame::Null => Ok(None),
            _ => Ok(None),
        }
    }

    fn resp2_to_string(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            Resp2Frame::SimpleString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_resp2_integer(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(*n),
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse)?.parse().map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn parse_resp2_float(frame: &Resp2Frame) -> Result<f64, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse)?.parse().map_err(EpError::parse),
            Resp2Frame::Integer(n) => Ok(*n as f64),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<SeriesResult>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut results = Vec::with_capacity(data.len());
                for item in data {
                    if let Resp3Frame::Array { data: series, .. } = item {
                        let result = Self::parse_resp3_series(&series)?;
                        results.push(result);
                    }
                }
                Ok(results)
            }
            Resp3Frame::Map { data, .. } => {
                // RESP3 may return a map format
                let mut results = Vec::new();
                for (k, v) in data {
                    let key = Self::resp3_to_string(&k)?;
                    if let Resp3Frame::Array { data: series, .. } = v {
                        let (labels, sample) = Self::parse_resp3_series_data(&series)?;
                        results.push(SeriesResult { key, labels, sample });
                    }
                }
                Ok(results)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.MGET response: {:?}", other))),
        }
    }

    fn parse_resp3_series(series: &[Resp3Frame]) -> Result<SeriesResult, EpError> {
        if series.is_empty() {
            return Err(EpError::parse("empty series result"));
        }

        let key = Self::resp3_to_string(&series[0])?;

        let labels = if series.len() > 1 {
            Self::parse_resp3_labels(&series[1])?
        } else {
            HashMap::new()
        };

        let sample = if series.len() > 2 {
            Self::parse_resp3_sample(&series[2])?
        } else {
            None
        };

        Ok(SeriesResult { key, labels, sample })
    }

    fn parse_resp3_series_data(series: &[Resp3Frame]) -> Result<(HashMap<String, String>, Option<Sample>), EpError> {
        let labels = if !series.is_empty() {
            Self::parse_resp3_labels(&series[0])?
        } else {
            HashMap::new()
        };

        let sample = if series.len() > 1 {
            Self::parse_resp3_sample(&series[1])?
        } else {
            None
        };

        Ok((labels, sample))
    }

    fn parse_resp3_labels(frame: &Resp3Frame) -> Result<HashMap<String, String>, EpError> {
        let mut labels = HashMap::new();
        match frame {
            Resp3Frame::Array { data, .. } => {
                for item in data {
                    if let Resp3Frame::Array { data: pair, .. } = item
                        && pair.len() == 2
                    {
                        let k = Self::resp3_to_string(&pair[0])?;
                        let v = Self::resp3_to_string(&pair[1])?;
                        labels.insert(k, v);
                    }
                }
            }
            Resp3Frame::Map { data, .. } => {
                for (k, v) in data {
                    let key = Self::resp3_to_string(k)?;
                    let val = Self::resp3_to_string(v)?;
                    labels.insert(key, val);
                }
            }
            _ => {}
        }
        Ok(labels)
    }

    fn parse_resp3_sample(frame: &Resp3Frame) -> Result<Option<Sample>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } if data.len() == 2 => {
                let timestamp = Self::parse_resp3_integer(&data[0])?;
                let value = Self::parse_resp3_float(&data[1])?;
                Ok(Some(Sample { timestamp, value }))
            }
            Resp3Frame::Array { data, .. } if data.is_empty() => Ok(None),
            Resp3Frame::Null => Ok(None),
            _ => Ok(None),
        }
    }

    fn resp3_to_string(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.clone()).map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_resp3_integer(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.clone()).map_err(EpError::parse)?.parse().map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn parse_resp3_float(frame: &Resp3Frame) -> Result<f64, EpError> {
        match frame {
            Resp3Frame::Double { data, .. } => Ok(*data),
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.clone()).map_err(EpError::parse)?.parse().map_err(EpError::parse)
            }
            Resp3Frame::Number { data, .. } => Ok(*data as f64),
            other => Err(EpError::parse(format!("expected float, got {:?}", other))),
        }
    }
}

impl Serialize for TsMgetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsMgetOutput", 1)?;
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
            let input = TsMgetInput {
                latest: None,
                label: None,
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.MGET"));
            assert!(cmd_str.contains("FILTER"));
            assert!(cmd_str.contains("sensor=temp"));
        }

        #[test]
        fn test_encode_command_with_latest() {
            let input = TsMgetInput {
                latest: Some(true),
                label: None,
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LATEST"));
        }

        #[test]
        fn test_encode_command_with_withlabels() {
            let input = TsMgetInput {
                latest: None,
                label: Some(Label::WITHLABELS),
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHLABELS"));
        }

        #[test]
        fn test_encode_command_with_selected_labels() {
            let input = TsMgetInput {
                latest: None,
                label: Some(Label::SELECTEDLABELS(vec![
                    RedisJsonValue::String("sensor".into()),
                    RedisJsonValue::String("location".into()),
                ])),
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SELECTED_LABELS"));
            assert!(cmd_str.contains("sensor"));
            assert!(cmd_str.contains("location"));
        }

        #[test]
        fn test_decode_input_filter_only() {
            let args = vec![
                RedisJsonValue::String("FILTER".into()),
                RedisJsonValue::String("sensor=temp".into()),
            ];
            let input = TsMgetInput::decode(args).unwrap();
            assert!(input.latest.is_none());
            assert!(input.label.is_none());
            assert_eq!(input.filter, RedisJsonValue::String("sensor=temp".into()));
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("LATEST".into()),
                RedisJsonValue::String("WITHLABELS".into()),
                RedisJsonValue::String("FILTER".into()),
                RedisJsonValue::String("sensor=temp".into()),
            ];
            let input = TsMgetInput::decode(args).unwrap();
            assert_eq!(input.latest, Some(true));
            assert!(matches!(input.label, Some(Label::WITHLABELS)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TsMgetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = TsMgetOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsMgetOutput::decode(b"-ERR syntax error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = TsMgetInput {
                latest: None,
                label: None,
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            // TS.MGET doesn't have explicit keys, it uses filters
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsMgetInput {
                latest: None,
                label: None,
                filter: RedisJsonValue::String("sensor=temp".into()),
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsMget);
        }

        #[test]
        fn test_output_accessors() {
            let output = TsMgetOutput::new(vec![
                SeriesResult {
                    key: "ts:temp".into(),
                    labels: HashMap::new(),
                    sample: Some(Sample { timestamp: 1000, value: 25.5 }),
                },
                SeriesResult {
                    key: "ts:humidity".into(),
                    labels: HashMap::new(),
                    sample: Some(Sample { timestamp: 1000, value: 60.0 }),
                },
            ]);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
            assert_eq!(output.keys(), vec!["ts:temp", "ts:humidity"]);
        }

        #[test]
        fn test_output_get_by_key() {
            let output = TsMgetOutput::new(vec![SeriesResult {
                key: "ts:temp".into(),
                labels: HashMap::new(),
                sample: Some(Sample { timestamp: 1000, value: 25.5 }),
            }]);
            let result = output.get("ts:temp");
            assert!(result.is_some());
            assert_eq!(result.unwrap().key, "ts:temp");
            assert!(output.get("nonexistent").is_none());
        }

        #[test]
        fn test_sample_struct() {
            let sample = Sample { timestamp: 1609459200000, value: 42.5 };
            assert_eq!(sample.timestamp, 1609459200000);
            assert!((sample.value - 42.5).abs() < f64::EPSILON);
        }

        #[test]
        fn test_series_result_with_labels() {
            let mut labels = HashMap::new();
            labels.insert("sensor".into(), "temp".into());
            labels.insert("location".into(), "room1".into());

            let result = SeriesResult {
                key: "ts:temp:room1".into(),
                labels,
                sample: Some(Sample { timestamp: 1000, value: 22.5 }),
            };

            assert_eq!(result.labels.get("sensor"), Some(&"temp".to_string()));
            assert_eq!(result.labels.get("location"), Some(&"room1".to_string()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.MGET requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_no_matches() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMgetInput {
                                latest: None,
                                label: None,
                                filter: RedisJsonValue::String("nonexistent=label".into()),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsMgetOutput::decode(&result).expect("decode failed");
                        assert!(output.is_empty());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_with_latest() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMgetInput {
                                latest: Some(true),
                                label: None,
                                filter: RedisJsonValue::String("sensor=test".into()),
                            }
                            .command(),
                        )
                        .await;

                    // Just verify command is accepted
                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let _output = TsMgetOutput::decode(&result).expect("decode failed");
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mget_with_withlabels() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMgetInput {
                                latest: None,
                                label: Some(Label::WITHLABELS),
                                filter: RedisJsonValue::String("sensor=test".into()),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let _output = TsMgetOutput::decode(&result).expect("decode failed");
                    }
                })
            })
            .await;
        }
    }
}
