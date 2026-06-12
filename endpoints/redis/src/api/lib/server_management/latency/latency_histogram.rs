use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use redis_protocol::resp3::types::FrameMap;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, LatencyHistogramInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::LatencyHistogram,
    "Returns the cumulative distribution of latencies of a subset or all commands",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LATENCY HISTOGRAM`
/// https://redis.io/docs/latest/commands/latency-histogram/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyHistogramInput {
    /// Optional list of command names to get histograms for.
    /// If not specified, returns histograms for all commands.
    pub commands: Option<Vec<RedisJsonValue>>,
}

impl Serialize for LatencyHistogramInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.commands.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("LatencyHistogramInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(commands) = &self.commands {
            state.serialize_field("commands", commands)?;
        }
        state.end()
    }
}

impl_redis_operation!(LatencyHistogramInput, API_INFO, { commands });

impl RedisCommandInput for LatencyHistogramInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(commands) = &self.commands {
            for cmd in commands {
                command.arg(cmd);
            }
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let commands = if args.is_empty() { None } else { Some(args) };
        Ok(Self { commands })
    }
}

/// Histogram data for a single command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub struct CommandHistogram {
    /// Number of calls for this command
    pub calls: i64,
    /// Histogram buckets mapping latency (microseconds) to count
    pub histogram: Vec<(i64, i64)>,
}

impl Serialize for CommandHistogram {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CommandHistogram", 2)?;
        state.serialize_field("calls", &self.calls)?;
        state.serialize_field("histogram", &self.histogram)?;
        state.end()
    }
}

/// Output for Redis LATENCY HISTOGRAM command
///
/// Returns histograms showing latency distributions for commands.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyHistogramOutput {
    /// Map of command name to histogram data
    histograms: HashMap<String, CommandHistogram>,
}

impl LatencyHistogramOutput {
    pub fn new(histograms: HashMap<String, CommandHistogram>) -> Self {
        Self { histograms }
    }

    /// Get all histograms
    pub fn histograms(&self) -> &HashMap<String, CommandHistogram> {
        &self.histograms
    }

    /// Get histogram for a specific command
    pub fn get(&self, command: &str) -> Option<&CommandHistogram> {
        self.histograms.get(command)
    }

    /// Check if there are any histograms
    pub fn is_empty(&self) -> bool {
        self.histograms.is_empty()
    }

    /// Get the number of commands with histograms
    pub fn len(&self) -> usize {
        self.histograms.len()
    }

    /// Decode the Redis protocol response into a LatencyHistogramOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let histograms = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame)?,
        };

        Ok(Self { histograms })
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<HashMap<String, CommandHistogram>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut histograms = HashMap::new();
                // Response is array of [cmd_name, histogram_data, cmd_name, histogram_data, ...]
                let mut iter = items.into_iter();
                while let Some(name_frame) = iter.next() {
                    let name = match name_frame {
                        Resp2Frame::BulkString(b) => String::from_utf8(b).map_err(EpError::parse)?,
                        Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    if let Some(Resp2Frame::Array(data)) = iter.next() {
                        let hist = Self::parse_histogram_resp2(data)?;
                        histograms.insert(name, hist);
                    }
                }
                Ok(histograms)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected LATENCY HISTOGRAM response: {:?}", other))),
        }
    }

    fn parse_histogram_resp2(data: Vec<Resp2Frame>) -> Result<CommandHistogram, EpError> {
        let mut calls = 0i64;
        let mut histogram = Vec::new();

        // Parse key-value pairs: ["calls", N, "histogram_usec", [...]]
        let mut iter = data.into_iter();
        while let Some(key_frame) = iter.next() {
            let key = match key_frame {
                Resp2Frame::BulkString(b) => String::from_utf8(b).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                _ => continue,
            };
            if let Some(value_frame) = iter.next() {
                match key.as_str() {
                    "calls" => {
                        if let Resp2Frame::Integer(n) = value_frame {
                            calls = n;
                        }
                    }
                    "histogram_usec" => {
                        if let Resp2Frame::Array(buckets) = value_frame {
                            let mut bucket_iter = buckets.into_iter();
                            while let Some(usec_frame) = bucket_iter.next() {
                                if let Resp2Frame::Integer(usec) = usec_frame
                                    && let Some(Resp2Frame::Integer(count)) = bucket_iter.next()
                                {
                                    histogram.push((usec, count));
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        Ok(CommandHistogram { calls, histogram })
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<HashMap<String, CommandHistogram>, EpError> {
        match frame {
            Resp3Frame::Map { data, .. } => {
                let mut histograms = HashMap::new();
                for (key_frame, value_frame) in data {
                    let name = match key_frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    if let Resp3Frame::Map { data, .. } = value_frame {
                        let hist = Self::parse_histogram_resp3(data)?;
                        histograms.insert(name, hist);
                    }
                }
                Ok(histograms)
            }
            Resp3Frame::Array { data, .. } => {
                // Fallback to array parsing similar to RESP2
                let mut histograms = HashMap::new();
                let mut iter = data.into_iter();
                while let Some(name_frame) = iter.next() {
                    let name = match name_frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => continue,
                    };
                    if let Some(Resp3Frame::Map { data, .. }) = iter.next() {
                        let hist = Self::parse_histogram_resp3(data)?;
                        histograms.insert(name, hist);
                    }
                }
                Ok(histograms)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected LATENCY HISTOGRAM response: {:?}", other))),
        }
    }

    fn parse_histogram_resp3(data: FrameMap<Resp3Frame, Resp3Frame>) -> Result<CommandHistogram, EpError> {
        let mut calls = 0i64;
        let mut histogram = Vec::new();

        for (key_frame, value_frame) in data {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                _ => continue,
            };
            match key.as_str() {
                "calls" => {
                    if let Resp3Frame::Number { data, .. } = value_frame {
                        calls = data;
                    }
                }
                "histogram_usec" => {
                    if let Resp3Frame::Array { data: buckets, .. } = value_frame {
                        let mut bucket_iter = buckets.into_iter();
                        while let Some(usec_frame) = bucket_iter.next() {
                            if let Resp3Frame::Number { data: usec, .. } = usec_frame
                                && let Some(Resp3Frame::Number { data: count, .. }) = bucket_iter.next()
                            {
                                histogram.push((usec, count));
                            }
                        }
                    }
                }
                _ => {}
            }
        }

        Ok(CommandHistogram { calls, histogram })
    }
}

impl Serialize for LatencyHistogramOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyHistogramOutput", 1)?;
        state.serialize_field("histograms", &self.histograms)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_args() {
            let input = LatencyHistogramInput { commands: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nLATENCY\r\n$9\r\nHISTOGRAM\r\n");
        }

        #[test]
        fn test_encode_command_with_commands() {
            let input = LatencyHistogramInput {
                commands: Some(vec![RedisJsonValue::String("GET".into()), RedisJsonValue::String("SET".into())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(3).any(|w| w == b"GET"));
            assert!(cmd.windows(3).any(|w| w == b"SET"));
        }

        #[test]
        fn test_decode_empty_response() {
            // Empty array response
            let output = LatencyHistogramOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyHistogramOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = LatencyHistogramInput::decode(vec![]).unwrap();
            assert!(input.commands.is_none());
        }

        #[test]
        fn test_decode_input_with_commands() {
            let args = vec![RedisJsonValue::String("GET".into()), RedisJsonValue::String("SET".into())];
            let input = LatencyHistogramInput::decode(args).unwrap();
            assert!(input.commands.is_some());
            assert_eq!(input.commands.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyHistogramInput { commands: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyHistogramInput { commands: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyHistogram);
        }

        #[test]
        fn test_command_histogram_accessors() {
            let hist = CommandHistogram { calls: 100, histogram: vec![(1, 50), (10, 30), (100, 20)] };
            assert_eq!(hist.calls, 100);
            assert_eq!(hist.histogram.len(), 3);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_histogram_basic() {
            // LATENCY HISTOGRAM requires Redis 7.0+
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&LatencyHistogramInput { commands: None }.command()).await.expect("raw failed");

                    let output = LatencyHistogramOutput::decode(&result).expect("decode failed");
                    // Just verify it decodes - may be empty if no commands have run
                    let _ = output.len();
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_histogram_specific_commands() {
            // LATENCY HISTOGRAM requires Redis 7.0+
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // Run some GET commands first
                    ctx.raw(b"*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&LatencyHistogramInput { commands: Some(vec![RedisJsonValue::String("GET".into())]) }.command())
                        .await
                        .expect("raw failed");

                    let output = LatencyHistogramOutput::decode(&result).expect("decode failed");
                    // Should have GET histogram if tracking is enabled
                    let _ = output.get("get");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_histogram_resp_formats() {
            // Test both RESP2 and RESP3 explicitly
            for resp in [RespVersion::Resp2, RespVersion::Resp3] {
                let mut ctx = setup(resp, Some("7.4")).await;

                let result = ctx.raw(&LatencyHistogramInput { commands: None }.command()).await.expect("raw failed");

                let output = LatencyHistogramOutput::decode(&result).expect("decode failed");
                let _ = output.len();

                ctx.stop().await;
            }
        }
    }
}
