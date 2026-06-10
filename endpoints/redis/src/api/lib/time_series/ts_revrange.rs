use crate::api::lib::time_series::common::{Align, FilterByValue, parse_align_from_args};
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

const API_INFO: ApiInfo<RedisApi, TsRevrangeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TsRevrange, "Query a range in reverse direction", ReqType::Read, true);

/// Input for Redis `TS.REVRANGE` command.
///
/// Query a time series for data points within a timestamp range in reverse order.
///
/// See official Redis documentation for `TS.REVRANGE`:
/// https://redis.io/docs/latest/commands/ts.revrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsRevrangeInput {
    /// The key name of the time series
    key: RedisKey,
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
    /// Maximum number of results to return
    #[builder(default)]
    count: Option<RedisJsonValue>,
    /// Aggregation settings
    #[builder(default)]
    align: Option<Align>,
}

impl Serialize for TsRevrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, from_timestamp, to_timestamp
        if self.latest.is_some() {
            fields += 1;
        }
        if self.filter_by_ts.is_some() {
            fields += 1;
        }
        if self.filter_by_value.is_some() {
            fields += 2; // min, max
        }
        if self.count.is_some() {
            fields += 1;
        }
        if let Some(ref align) = self.align {
            fields += 2; // aggregator, bucket_duration
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

        let mut state = serializer.serialize_struct("TsRevrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
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
        state.end()
    }
}

impl_redis_operation!(
    TsRevrangeInput,
    API_INFO,
    { key, from_timestamp, to_timestamp, latest, filter_by_ts, filter_by_value, count, align }
);

impl RedisCommandInput for TsRevrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.from_timestamp).arg(&self.to_timestamp);

        if let Some(true) = self.latest {
            command.arg("LATEST");
        }

        if let Some(ref filter_by_ts) = self.filter_by_ts {
            command.arg("FILTER_BY_TS").arg(filter_by_ts);
        }

        if let Some(ref filter_by_value) = self.filter_by_value {
            filter_by_value.cmd(&mut command);
        }

        if let Some(ref count) = self.count {
            command.arg("COUNT").arg(count);
        }

        if let Some(ref align) = self.align {
            align.cmd(&mut command);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!(
                "TS.REVRANGE requires at least 3 arguments (key, fromTimestamp, toTimestamp), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let from_timestamp = args[1].clone();
        let to_timestamp = args[2].clone();

        let mut latest = None;
        let mut filter_by_ts = None;
        let mut filter_by_value = None;
        let mut count = None;
        let mut align = None;
        let mut i = 3;

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
                            i += 2; // Skip ALIGN and its value
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
            from_timestamp,
            to_timestamp,
            latest,
            filter_by_ts,
            filter_by_value,
            count,
            align,
        })
    }
}

/// A single data point in a time series.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, ToSchema, JsonSchema)]
pub struct DataPoint {
    /// Timestamp in milliseconds
    pub timestamp: i64,
    /// Value at this timestamp
    pub value: f64,
}

/// Output for Redis `TS.REVRANGE` command.
///
/// Contains the list of timestamp-value pairs within the queried range in reverse order.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsRevrangeOutput {
    /// Data points in the range (reverse chronological order)
    data_points: Vec<DataPoint>,
}

impl TsRevrangeOutput {
    pub fn new(data_points: Vec<DataPoint>) -> Self {
        Self { data_points }
    }

    /// Get the data points
    pub fn data_points(&self) -> &[DataPoint] {
        &self.data_points
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.data_points.is_empty()
    }

    /// Get the number of data points
    pub fn len(&self) -> usize {
        self.data_points.len()
    }

    /// Decode the Redis protocol response into a TsRevrangeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let data_points = Self::parse_frame(frame)?;
        Ok(Self { data_points })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Vec<DataPoint>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<DataPoint>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut data_points = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(pair) = item {
                        if pair.len() == 2 {
                            let timestamp = Self::parse_resp2_integer(&pair[0])?;
                            let value = Self::parse_resp2_float(&pair[1])?;
                            data_points.push(DataPoint { timestamp, value });
                        } else {
                            return Err(EpError::parse(format!("expected 2-element array for data point, got {}", pair.len())));
                        }
                    } else {
                        return Err(EpError::parse(format!("expected array for data point: {:?}", item)));
                    }
                }
                Ok(data_points)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.REVRANGE response: {:?}", other))),
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

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<DataPoint>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut data_points = Vec::with_capacity(data.len());
                for item in data {
                    if let Resp3Frame::Array { data: pair, .. } = item {
                        if pair.len() == 2 {
                            let timestamp = Self::parse_resp3_integer(&pair[0])?;
                            let value = Self::parse_resp3_float(&pair[1])?;
                            data_points.push(DataPoint { timestamp, value });
                        } else {
                            return Err(EpError::parse(format!("expected 2-element array for data point, got {}", pair.len())));
                        }
                    } else {
                        return Err(EpError::parse(format!("expected array for data point: {:?}", item)));
                    }
                }
                Ok(data_points)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.REVRANGE response: {:?}", other))),
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

impl Serialize for TsRevrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsRevrangeOutput", 1)?;
        state.serialize_field("data_points", &self.data_points)?;
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
            let input = TsRevrangeInput {
                key: RedisKey::String("temperature:sensor1".into()),
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                count: None,
                align: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.REVRANGE"));
            assert!(cmd_str.contains("temperature:sensor1"));
        }

        #[test]
        fn test_encode_command_with_latest() {
            let input = TsRevrangeInput {
                key: RedisKey::String("ts:key".into()),
                from_timestamp: RedisJsonValue::Integer(1000),
                to_timestamp: RedisJsonValue::Integer(2000),
                latest: Some(true),
                filter_by_ts: None,
                filter_by_value: None,
                count: None,
                align: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LATEST"));
        }

        #[test]
        fn test_encode_command_with_filter_by_value() {
            let input = TsRevrangeInput {
                key: RedisKey::String("ts:key".into()),
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: Some(FilterByValue {
                    min: RedisJsonValue::Integer(0),
                    max: RedisJsonValue::Integer(100),
                }),
                count: None,
                align: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FILTER_BY_VALUE"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            ];
            let input = TsRevrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.latest.is_none());
        }

        #[test]
        fn test_decode_input_with_all_options() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(2000),
                RedisJsonValue::String("LATEST".into()),
                RedisJsonValue::String("FILTER_BY_VALUE".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(100),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = TsRevrangeInput::decode(args).unwrap();
            assert_eq!(input.latest, Some(true));
            assert!(input.filter_by_value.is_some());
            assert_eq!(input.count, Some(RedisJsonValue::Integer(10)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(1000)];
            let err = TsRevrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_decode_input_count_missing_arg() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(2000),
                RedisJsonValue::String("COUNT".into()),
            ];
            let err = TsRevrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("COUNT"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = TsRevrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsRevrangeOutput::decode(b"-ERR key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsRevrangeInput {
                key: RedisKey::String("mykey".into()),
                from_timestamp: RedisJsonValue::String("-".into()),
                to_timestamp: RedisJsonValue::String("+".into()),
                latest: None,
                filter_by_ts: None,
                filter_by_value: None,
                count: None,
                align: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_accessors() {
            let output = TsRevrangeOutput::new(vec![DataPoint { timestamp: 2000, value: 2.0 }, DataPoint { timestamp: 1000, value: 1.0 }]);
            assert!(!output.is_empty());
            assert_eq!(output.len(), 2);
            // REVRANGE returns newest first
            assert_eq!(output.data_points()[0].timestamp, 2000);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_revrange_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsRevrangeInput {
                                key: RedisKey::String("nonexistent:ts".into()),
                                from_timestamp: RedisJsonValue::String("-".into()),
                                to_timestamp: RedisJsonValue::String("+".into()),
                                latest: None,
                                filter_by_ts: None,
                                filter_by_value: None,
                                count: None,
                                align: None,
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(result) = result {
                        if result.starts_with(b"-") {
                        } else {
                            let output = TsRevrangeOutput::decode(&result).expect("decode failed");
                            assert!(output.is_empty());
                        }
                    }
                })
            })
            .await;
        }
    }
}
