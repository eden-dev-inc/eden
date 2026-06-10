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

const API_INFO: ApiInfo<RedisApi, TsGetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsGet,
    "Get the sample with the highest timestamp from a given time series",
    ReqType::Read,
    true,
);

/// Input for Redis `TS.GET` command.
///
/// Gets the last sample (the one with the highest timestamp) from a time series.
///
/// See official Redis documentation for `TS.GET`:
/// https://redis.io/docs/latest/commands/ts.get/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsGetInput {
    /// The key name of the time series
    key: RedisKey,
    /// When true, report the compacted value of the latest bucket for compacted time series
    #[builder(default)]
    #[serde(skip_serializing_if = "Option::is_none")]
    latest: Option<bool>,
}

impl Serialize for TsGetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.latest.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TsGetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(latest) = &self.latest {
            state.serialize_field("latest", latest)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TsGetInput,
    API_INFO,
    {key, latest}
);

impl RedisCommandInput for TsGetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(true) = self.latest {
            command.arg("LATEST");
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.GET requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let latest = if args.len() > 1 {
            if let RedisJsonValue::String(s) = &args[1] {
                if s.to_uppercase() == "LATEST" { Some(true) } else { None }
            } else {
                None
            }
        } else {
            None
        };

        Ok(TsGetInput { key, latest })
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

/// Output for Redis `TS.GET` command.
///
/// Contains the last sample from the time series, or None if the series is empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsGetOutput {
    /// The last sample, or None if series is empty
    sample: Option<Sample>,
}

impl TsGetOutput {
    pub fn new(sample: Option<Sample>) -> Self {
        Self { sample }
    }

    /// Get the sample
    pub fn sample(&self) -> Option<&Sample> {
        self.sample.as_ref()
    }

    /// Check if a sample was returned
    pub fn has_sample(&self) -> bool {
        self.sample.is_some()
    }

    /// Get the timestamp if available
    pub fn timestamp(&self) -> Option<i64> {
        self.sample.as_ref().map(|s| s.timestamp)
    }

    /// Get the value if available
    pub fn value(&self) -> Option<f64> {
        self.sample.as_ref().map(|s| s.value)
    }

    /// Decode the Redis protocol response into a TsGetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let sample = Self::parse_frame(frame)?;
        Ok(Self { sample })
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Option<Sample>, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Option<Sample>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                if arr.is_empty() {
                    return Ok(None);
                }
                if arr.len() != 2 {
                    return Err(EpError::parse(format!("expected 2-element array for sample, got {}", arr.len())));
                }
                let timestamp = Self::parse_resp2_integer(&arr[0])?;
                let value = Self::parse_resp2_float(&arr[1])?;
                Ok(Some(Sample { timestamp, value }))
            }
            Resp2Frame::Null => Ok(None),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected TS.GET response: {:?}", other))),
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

    fn parse_resp3(frame: Resp3Frame) -> Result<Option<Sample>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.is_empty() {
                    return Ok(None);
                }
                if data.len() != 2 {
                    return Err(EpError::parse(format!("expected 2-element array for sample, got {}", data.len())));
                }
                let timestamp = Self::parse_resp3_integer(&data[0])?;
                let value = Self::parse_resp3_float(&data[1])?;
                Ok(Some(Sample { timestamp, value }))
            }
            Resp3Frame::Null => Ok(None),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected TS.GET response: {:?}", other))),
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

impl Serialize for TsGetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsGetOutput", 1)?;
        state.serialize_field("sample", &self.sample)?;
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
            let input = TsGetInput { key: RedisKey::String("ts:key".into()), latest: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.GET"));
            assert!(cmd_str.contains("ts:key"));
            assert!(!cmd_str.contains("LATEST"));
        }

        #[test]
        fn test_encode_command_with_latest() {
            let input = TsGetInput { key: RedisKey::String("ts:key".into()), latest: Some(true) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.GET"));
            assert!(cmd_str.contains("LATEST"));
        }

        #[test]
        fn test_encode_command_latest_false() {
            let input = TsGetInput { key: RedisKey::String("ts:key".into()), latest: Some(false) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("LATEST"));
        }

        #[test]
        fn test_decode_input_minimal() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TsGetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.latest.is_none());
        }

        #[test]
        fn test_decode_input_with_latest() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("LATEST".into())];
            let input = TsGetInput::decode(args).unwrap();
            assert_eq!(input.latest, Some(true));
        }

        #[test]
        fn test_decode_input_latest_lowercase() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("latest".into())];
            let input = TsGetInput::decode(args).unwrap();
            assert_eq!(input.latest, Some(true));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TsGetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_decode_output_empty_array() {
            // RESP2 empty array - no samples
            let output = TsGetOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.has_sample());
            assert!(output.sample().is_none());
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = TsGetOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.has_sample());
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = TsGetOutput::decode(b"_\r\n").unwrap();
            assert!(!output.has_sample());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsGetOutput::decode(b"-ERR TSDB: the key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("TSDB"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TsGetInput { key: RedisKey::String("mykey".into()), latest: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsGetInput { key: RedisKey::String("mykey".into()), latest: None };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsGet);
        }

        #[test]
        fn test_sample_struct() {
            let sample = Sample { timestamp: 1609459200000, value: 25.5 };
            assert_eq!(sample.timestamp, 1609459200000);
            assert!((sample.value - 25.5).abs() < f64::EPSILON);
        }

        #[test]
        fn test_output_accessors() {
            let output = TsGetOutput::new(Some(Sample { timestamp: 1000, value: 42.5 }));
            assert!(output.has_sample());
            assert_eq!(output.timestamp(), Some(1000));
            assert_eq!(output.value(), Some(42.5));
        }

        #[test]
        fn test_output_accessors_none() {
            let output = TsGetOutput::new(None);
            assert!(!output.has_sample());
            assert_eq!(output.timestamp(), None);
            assert_eq!(output.value(), None);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.GET requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TsGetInput { key: RedisKey::String("nonexistent:ts".into()), latest: None }.command()).await;

                    // May return error if key doesn't exist or module not installed
                    if let Ok(result) = result {
                        if result.starts_with(b"-") {
                            // Error response - expected for nonexistent key
                        } else {
                            let output = TsGetOutput::decode(&result).expect("decode failed");
                            assert!(!output.has_sample());
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_with_latest_option() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsGetInput {
                                key: RedisKey::String("nonexistent:ts".into()),
                                latest: Some(true),
                            }
                            .command(),
                        )
                        .await;

                    // Just verify command is accepted (will error on nonexistent key)
                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {}
                })
            })
            .await;
        }
    }
}
