use crate::api::lib::time_series::common::{Align, FilterByValue, Group, Label, parse_align_from_args, parse_selected_labels};
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
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsMrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsMrange,
    "Query a range across multiple time series by filters in forward direction",
    ReqType::Read,
    true,
);

/// Input for Redis `TS.MRANGE` command.
///
/// Query multiple time series for data points within a timestamp range.
///
/// See official Redis documentation for `TS.MRANGE`:
/// https://redis.io/docs/latest/commands/ts.mrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsMrangeInput {
    /// Start timestamp (use "-" for minimum)
    from_timestamp: RedisJsonValue,
    /// End timestamp (use "+" for maximum)
    to_timestamp: RedisJsonValue,
    /// When true, report the compacted value of the latest bucket
    #[builder(default)]
    latest: Option<bool>,
    /// Filter by specific timestamps
    #[builder(default)]
    filter_by_ts: Option<RedisJsonValue>,
    /// Filter by value range
    #[builder(default)]
    filter_by_value: Option<FilterByValue>,
    /// Label output options
    #[builder(default)]
    label: Option<Label>,
    /// Maximum number of results per time series
    #[builder(default)]
    count: Option<RedisJsonValue>,
    /// Aggregation settings
    #[builder(default)]
    align: Option<Align>,
    /// Filter expression(s) to select time series
    filter: RedisJsonValue,
    /// Grouping options for aggregating across time series
    #[builder(default)]
    group: Option<Group>,
}

impl Serialize for TsMrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, from_timestamp, to_timestamp, filter
        if self.latest.is_some() {
            fields += 1;
        }
        if self.filter_by_ts.is_some() {
            fields += 1;
        }
        if self.filter_by_value.is_some() {
            fields += 2;
        }
        if self.label.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if let Some(ref align) = self.align {
            fields += 2;
            if align.align.is_some() {
                fields += 1;
            }
            if align.bucket_timestamp.is_some() {
                fields += 1;
            }
            if align.empty.is_some() {
                fields += 1;
            }
        }
        if self.group.is_some() {
            fields += 2;
        }

        let mut state = serializer.serialize_struct("TsMrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("from_timestamp", &self.from_timestamp)?;
        state.serialize_field("to_timestamp", &self.to_timestamp)?;

        if let Some(ref latest) = self.latest {
            state.serialize_field("latest", latest)?;
        }
        if let Some(ref filter_by_ts) = self.filter_by_ts {
            state.serialize_field("filter_by_ts", filter_by_ts)?;
        }
        if let Some(ref filter_by_value) = self.filter_by_value {
            state.serialize_field("filter_by_value_min", &filter_by_value.min)?;
            state.serialize_field("filter_by_value_max", &filter_by_value.max)?;
        }
        if let Some(ref label) = self.label {
            state.serialize_field("label", label)?;
        }
        if let Some(ref count) = self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(ref align) = self.align {
            if let Some(ref align_val) = align.align {
                state.serialize_field("align", align_val)?;
            }
            state.serialize_field("aggregator", &align.aggregator)?;
            state.serialize_field("bucket_duration", &align.bucket_duration)?;
            if let Some(ref bucket_timestamp) = align.bucket_timestamp {
                state.serialize_field("bucket_timestamp", bucket_timestamp)?;
            }
            if let Some(ref empty) = align.empty {
                state.serialize_field("empty", empty)?;
            }
        }
        state.serialize_field("filter", &self.filter)?;
        if let Some(ref group) = self.group {
            state.serialize_field("group_by", &group.group_by)?;
            state.serialize_field("reduce", &group.reduce)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsMrangeInput,
    API_INFO,
    { from_timestamp, to_timestamp, latest, filter_by_ts, filter_by_value, label, count, align, filter, group }
);

impl RedisCommandInput for TsMrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.from_timestamp).arg(&self.to_timestamp);

        if let Some(true) = self.latest {
            command.arg("LATEST");
        }

        if let Some(ref filter_by_ts) = self.filter_by_ts {
            command.arg("FILTER_BY_TS").arg(filter_by_ts);
        }

        if let Some(ref filter_by_value) = self.filter_by_value {
            filter_by_value.cmd(&mut command);
        }

        if let Some(ref label) = self.label {
            match label {
                Label::WITHLABELS => {
                    command.arg("WITHLABELS");
                }
                Label::SELECTEDLABELS(labels) => {
                    command.arg("SELECTED_LABELS");
                    for l in labels {
                        command.arg(l);
                    }
                }
            }
        }

        if let Some(ref count) = self.count {
            command.arg("COUNT").arg(count);
        }

        if let Some(ref align) = self.align {
            align.cmd(&mut command);
        }

        command.arg("FILTER").arg(&self.filter);

        if let Some(ref group) = self.group {
            group.cmd(&mut command);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("TS.MRANGE requires at least 4 arguments, given {}", args.len())));
        }

        let from_timestamp = args[0].clone();
        let to_timestamp = args[1].clone();

        let mut latest = None;
        let mut filter_by_ts = None;
        let mut filter_by_value = None;
        let mut label = None;
        let mut count = None;
        let mut align = None;
        let mut filter = None;
        let mut group = None;
        let mut i = 2;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "LATEST" => {
                        latest = Some(true);
                        i += 1;
                    }
                    "FILTER_BY_TS" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FILTER_BY_TS requires a timestamp argument"));
                        }
                        filter_by_ts = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FILTER_BY_VALUE" => {
                        if i + 2 >= args.len() {
                            return Err(EpError::request("FILTER_BY_VALUE requires min and max arguments"));
                        }
                        filter_by_value = Some(FilterByValue { min: args[i + 1].clone(), max: args[i + 2].clone() });
                        i += 3;
                    }
                    "WITHLABELS" => {
                        label = Some(Label::WITHLABELS);
                        i += 1;
                    }
                    "SELECTED_LABELS" => {
                        i += 1;
                        let stop_keywords = ["COUNT", "ALIGN", "AGGREGATION", "FILTER", "GROUPBY"];
                        let (labels, new_i) = parse_selected_labels(&args, i, &stop_keywords);
                        if !labels.is_empty() {
                            label = Some(Label::SELECTEDLABELS(labels));
                        }
                        i = new_i;
                    }
                    "COUNT" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("COUNT requires a numeric argument"));
                        }
                        count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "ALIGN" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("ALIGN requires a value argument"));
                        }
                        if let Some((parsed_align, new_i)) = parse_align_from_args(&args, i + 1, true) {
                            align = Some(parsed_align);
                            i = new_i;
                        } else {
                            i += 2;
                        }
                    }
                    "AGGREGATION" => {
                        if let Some((parsed_align, new_i)) = parse_align_from_args(&args, i, false) {
                            align = Some(parsed_align);
                            i = new_i;
                        } else {
                            return Err(EpError::request("AGGREGATION requires aggregator and bucketDuration arguments"));
                        }
                    }
                    "FILTER" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("FILTER requires at least one filter expression"));
                        }
                        filter = Some(args[i + 1].clone());
                        i += 2;
                        break;
                    }
                    _ => {
                        i += 1;
                    }
                }
            } else {
                i += 1;
            }
        }

        // Parse GROUPBY if present
        if i < args.len()
            && let RedisJsonValue::String(s) = &args[i]
            && s.to_uppercase() == "GROUPBY"
        {
            if i + 3 >= args.len() {
                return Err(EpError::request("GROUPBY requires label, REDUCE, and reducer arguments"));
            }
            if let RedisJsonValue::String(reduce_str) = &args[i + 2] {
                if reduce_str.to_uppercase() == "REDUCE" {
                    group = Some(Group { group_by: args[i + 1].clone(), reduce: args[i + 3].clone() });
                } else {
                    return Err(EpError::request("GROUPBY expects REDUCE keyword after label"));
                }
            }
        }

        let filter = filter.ok_or_else(|| EpError::request("TS.MRANGE requires FILTER parameter"))?;

        Ok(Self {
            from_timestamp,
            to_timestamp,
            latest,
            filter_by_ts,
            filter_by_value,
            label,
            count,
            align,
            filter,
            group,
        })
    }
}

/// A single data point in a time series.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub struct DataPoint {
    pub timestamp: i64,
    pub value: f64,
}

/// Result for a single time series in TS.MRANGE response.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TimeSeriesResult {
    /// The key name of this time series
    pub key: String,
    /// Labels associated with this time series
    pub labels: Vec<(String, String)>,
    /// Data points in the range
    pub data_points: Vec<DataPoint>,
}

/// Output for Redis `TS.MRANGE` command.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsMrangeOutput {
    /// Results for each matching time series
    results: Vec<TimeSeriesResult>,
}

impl TsMrangeOutput {
    pub fn new(results: Vec<TimeSeriesResult>) -> Self {
        Self { results }
    }

    pub fn results(&self) -> &[TimeSeriesResult] {
        &self.results
    }

    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    pub fn len(&self) -> usize {
        self.results.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = Self::parse_frame(frame)?;
        Ok(Self { results })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<TimeSeriesResult>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<TimeSeriesResult>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(ts_data) = item
                        && ts_data.len() >= 3
                    {
                        let key = Self::parse_resp2_string(&ts_data[0])?;
                        let labels = Self::parse_resp2_labels(&ts_data[1])?;
                        let data_points = Self::parse_resp2_data_points(&ts_data[2])?;
                        results.push(TimeSeriesResult { key, labels, data_points });
                    }
                }
                Ok(results)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.MRANGE response: {:?}", other))),
        }
    }

    fn parse_resp2_string(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            Resp2Frame::SimpleString(data) => String::from_utf8(data.clone()).map_err(EpError::parse),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_resp2_labels(frame: &Resp2Frame) -> Result<Vec<(String, String)>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut labels = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(pair) = item
                        && pair.len() == 2
                    {
                        let key = Self::parse_resp2_string(&pair[0])?;
                        let value = Self::parse_resp2_string(&pair[1])?;
                        labels.push((key, value));
                    }
                }
                Ok(labels)
            }
            _ => Ok(vec![]),
        }
    }

    fn parse_resp2_data_points(frame: &Resp2Frame) -> Result<Vec<DataPoint>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut points = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(pair) = item
                        && pair.len() == 2
                    {
                        let timestamp = Self::parse_resp2_integer(&pair[0])?;
                        let value = Self::parse_resp2_float(&pair[1])?;
                        points.push(DataPoint { timestamp, value });
                    }
                }
                Ok(points)
            }
            _ => Ok(vec![]),
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

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<TimeSeriesResult>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut results = Vec::new();
                // TS.MRANGE returns array of [key, labels, datapoints] tuples
                for item in data {
                    if let Resp3Frame::Array { data: ts_data, .. } = item
                        && ts_data.len() >= 3
                    {
                        let key = Self::parse_resp3_string(&ts_data[0])?;
                        let labels = Self::parse_resp3_labels(&ts_data[1])?;
                        let data_points = Self::parse_resp3_data_points(&ts_data[2])?;
                        results.push(TimeSeriesResult { key, labels, data_points });
                    }
                }
                Ok(results)
            }
            Resp3Frame::Map { data, .. } => {
                let mut results = Vec::new();
                for (key_frame, value_frame) in data {
                    let key = Self::parse_resp3_string(&key_frame)?;
                    // Value is typically [labels, datapoints]
                    if let Resp3Frame::Array { data: ts_data, .. } = value_frame
                        && ts_data.len() >= 2
                    {
                        let labels = Self::parse_resp3_labels(&ts_data[0])?;
                        let data_points = Self::parse_resp3_data_points(&ts_data[1])?;
                        results.push(TimeSeriesResult { key, labels, data_points });
                    }
                }
                Ok(results)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.MRANGE response: {:?}", other))),
        }
    }

    fn parse_resp3_string(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                String::from_utf8(data.clone()).map_err(EpError::parse)
            }
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_resp3_labels(frame: &Resp3Frame) -> Result<Vec<(String, String)>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut labels = Vec::new();
                for item in data {
                    if let Resp3Frame::Array { data: pair, .. } = item
                        && pair.len() == 2
                    {
                        let key = Self::parse_resp3_string(&pair[0])?;
                        let value = Self::parse_resp3_string(&pair[1])?;
                        labels.push((key, value));
                    }
                }
                Ok(labels)
            }
            Resp3Frame::Map { data, .. } => {
                let mut labels = Vec::with_capacity(data.len());
                for (k, v) in data {
                    let key = Self::parse_resp3_string(k)?;
                    let value = Self::parse_resp3_string(v)?;
                    labels.push((key, value));
                }
                Ok(labels)
            }
            _ => Ok(vec![]),
        }
    }

    fn parse_resp3_data_points(frame: &Resp3Frame) -> Result<Vec<DataPoint>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut points = Vec::new();
                for item in data {
                    if let Resp3Frame::Array { data: pair, .. } = item
                        && pair.len() == 2
                    {
                        let timestamp = Self::parse_resp3_integer(&pair[0])?;
                        let value = Self::parse_resp3_float(&pair[1])?;
                        points.push(DataPoint { timestamp, value });
                    }
                }
                Ok(points)
            }
            _ => Ok(vec![]),
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

impl Serialize for TsMrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsMrangeOutput", 1)?;
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
            let input = TsMrangeInput {
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                label: None,
                count: None,
                align: None,
                filter: RedisJsonValue::String("sensor_id=1".into()),
                group: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.MRANGE"));
            assert!(cmd_str.contains("FILTER"));
            assert!(cmd_str.contains("sensor_id=1"));
        }

        #[test]
        fn test_encode_command_with_withlabels() {
            let input = TsMrangeInput {
                from_timestamp: RedisJsonValue::Integer(1000),
                to_timestamp: RedisJsonValue::Integer(2000),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                label: Some(Label::WITHLABELS),
                count: None,
                align: None,
                filter: RedisJsonValue::String("sensor=*".into()),
                group: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHLABELS"));
        }

        #[test]
        fn test_encode_command_with_groupby() {
            let input = TsMrangeInput {
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                label: None,
                count: None,
                align: None,
                filter: RedisJsonValue::String("area=*".into()),
                group: Some(Group {
                    group_by: RedisJsonValue::String("area".into()),
                    reduce: RedisJsonValue::String("avg".into()),
                }),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("GROUPBY"));
            assert!(cmd_str.contains("REDUCE"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("FILTER".into()),
                RedisJsonValue::String("sensor=1".into()),
            ];
            let input = TsMrangeInput::decode(args).unwrap();
            assert_eq!(input.filter, RedisJsonValue::String("sensor=1".into()));
        }

        #[test]
        fn test_decode_input_missing_filter() {
            let args = vec![
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("LATEST".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let err = TsMrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("FILTER"));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(10000)];
            let err = TsMrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 4"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = TsMrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsMrangeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = TsMrangeInput {
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                label: None,
                count: None,
                align: None,
                filter: RedisJsonValue::String("test=1".into()),
                group: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let output = TsMrangeOutput::new(vec![TimeSeriesResult {
                key: "ts:1".into(),
                labels: vec![("sensor".into(), "1".into())],
                data_points: vec![DataPoint { timestamp: 1000, value: 1.0 }],
            }]);
            assert!(!output.is_empty());
            assert_eq!(output.len(), 1);
            assert_eq!(output.results()[0].key, "ts:1");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mrange_no_matches() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsMrangeInput {
                                from_timestamp: RedisJsonValue::String("-".into()),
                                to_timestamp: RedisJsonValue::String("+".into()),
                                latest: None,
                                filter_by_ts: None,
                                filter_by_value: None,
                                label: None,
                                count: None,
                                align: None,
                                filter: RedisJsonValue::String("nonexistent=value".into()),
                                group: None,
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsMrangeOutput::decode(&result).expect("decode failed");
                        assert!(output.is_empty());
                    }
                })
            })
            .await;
        }
    }
}
