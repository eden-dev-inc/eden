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

const API_INFO: ApiInfo<RedisApi, LatencyHistoryInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::LatencyHistory,
    "Returns timestamp-latency samples for an event",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LATENCY HISTORY`
/// https://redis.io/docs/latest/commands/latency-history/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyHistoryInput {
    /// The event name to get history for (e.g., "command", "fork")
    pub event: RedisJsonValue,
}

impl Serialize for LatencyHistoryInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyHistoryInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("event", &self.event)?;
        state.end()
    }
}

impl_redis_operation!(LatencyHistoryInput, API_INFO, { event });

impl RedisCommandInput for LatencyHistoryInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.event);
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("LATENCY HISTORY requires 1 argument, given {}", args.len())));
        }

        Ok(Self { event: args[0].clone() })
    }
}

/// A single latency sample with timestamp and duration
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, PartialEq)]
pub struct LatencySample {
    /// Unix timestamp when the latency spike occurred
    pub timestamp: i64,
    /// Latency in milliseconds
    pub latency_ms: i64,
}

impl Serialize for LatencySample {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencySample", 2)?;
        state.serialize_field("timestamp", &self.timestamp)?;
        state.serialize_field("latency_ms", &self.latency_ms)?;
        state.end()
    }
}

/// Output for Redis LATENCY HISTORY command
///
/// Returns timestamp-latency samples for a specific event.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyHistoryOutput {
    /// List of latency samples
    samples: Vec<LatencySample>,
}

impl LatencyHistoryOutput {
    pub fn new(samples: Vec<LatencySample>) -> Self {
        Self { samples }
    }

    /// Get the latency samples
    pub fn samples(&self) -> &[LatencySample] {
        &self.samples
    }

    /// Check if there are any samples
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// Get the number of samples
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Get the maximum latency from all samples
    pub fn max_latency(&self) -> Option<i64> {
        self.samples.iter().map(|s| s.latency_ms).max()
    }

    /// Get the average latency from all samples
    pub fn avg_latency(&self) -> Option<f64> {
        if self.samples.is_empty() {
            None
        } else {
            let sum: i64 = self.samples.iter().map(|s| s.latency_ms).sum();
            Some(sum as f64 / self.samples.len() as f64)
        }
    }

    /// Decode the Redis protocol response into a LatencyHistoryOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let samples = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame)?,
        };

        Ok(Self { samples })
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Vec<LatencySample>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut samples = Vec::with_capacity(items.len());
                for item in items {
                    if let Resp2Frame::Array(sample_data) = item
                        && sample_data.len() >= 2
                    {
                        let timestamp = match &sample_data[0] {
                            Resp2Frame::Integer(i) => *i,
                            _ => continue,
                        };
                        let latency_ms = match &sample_data[1] {
                            Resp2Frame::Integer(i) => *i,
                            _ => continue,
                        };
                        samples.push(LatencySample { timestamp, latency_ms });
                    }
                }
                Ok(samples)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected LATENCY HISTORY response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Vec<LatencySample>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut samples = Vec::with_capacity(data.len());
                for item in data {
                    if let Resp3Frame::Array { data: sample_data, .. } = item
                        && sample_data.len() >= 2
                    {
                        let timestamp = match &sample_data[0] {
                            Resp3Frame::Number { data, .. } => *data,
                            _ => continue,
                        };
                        let latency_ms = match &sample_data[1] {
                            Resp3Frame::Number { data, .. } => *data,
                            _ => continue,
                        };
                        samples.push(LatencySample { timestamp, latency_ms });
                    }
                }
                Ok(samples)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected LATENCY HISTORY response: {:?}", other))),
        }
    }
}

impl Serialize for LatencyHistoryOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyHistoryOutput", 1)?;
        state.serialize_field("samples", &self.samples)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = LatencyHistoryInput { event: RedisJsonValue::String("command".into()) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nLATENCY\r\n$7\r\nHISTORY\r\n$7\r\ncommand\r\n");
        }

        #[test]
        fn test_decode_empty_array() {
            let output = LatencyHistoryOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
            assert_eq!(output.max_latency(), None);
            assert_eq!(output.avg_latency(), None);
        }

        #[test]
        fn test_decode_single_sample() {
            // *1\r\n*2\r\n:1234567890\r\n:5\r\n
            let resp = b"*1\r\n*2\r\n:1234567890\r\n:5\r\n";
            let output = LatencyHistoryOutput::decode(resp).unwrap();
            assert_eq!(output.len(), 1);
            let sample = &output.samples()[0];
            assert_eq!(sample.timestamp, 1234567890);
            assert_eq!(sample.latency_ms, 5);
        }

        #[test]
        fn test_decode_multiple_samples() {
            // *2\r\n*2\r\n:100\r\n:5\r\n*2\r\n:200\r\n:10\r\n
            let resp = b"*2\r\n*2\r\n:100\r\n:5\r\n*2\r\n:200\r\n:10\r\n";
            let output = LatencyHistoryOutput::decode(resp).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.max_latency(), Some(10));
            assert_eq!(output.avg_latency(), Some(7.5));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyHistoryOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("command".into())];
            let input = LatencyHistoryInput::decode(args).unwrap();
            assert_eq!(input.event, RedisJsonValue::String("command".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = LatencyHistoryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("command".into()), RedisJsonValue::String("extra".into())];
            let err = LatencyHistoryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyHistoryInput { event: RedisJsonValue::String("command".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyHistoryInput { event: RedisJsonValue::String("command".into()) };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyHistory);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_history_empty() {
            // LATENCY HISTORY requires Redis 2.8.13+
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset latency data first
                    ctx.raw(b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&LatencyHistoryInput { event: RedisJsonValue::String("command".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = LatencyHistoryOutput::decode(&result).expect("decode failed");
                    // After reset, should be empty for this event
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_history_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result =
                ctx.raw(&LatencyHistoryInput { event: RedisJsonValue::String("command".into()) }.command()).await.expect("raw failed");

            // RESP2 should return array
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            LatencyHistoryOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_history_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result =
                ctx.raw(&LatencyHistoryInput { event: RedisJsonValue::String("command".into()) }.command()).await.expect("raw failed");

            LatencyHistoryOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }
    }
}
