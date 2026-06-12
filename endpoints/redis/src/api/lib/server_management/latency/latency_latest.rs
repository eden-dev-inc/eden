use crate::api::lib::server_management::latency::LatencyEvent;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, LatencyLatestInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::LatencyLatest,
    "Returns the latest latency samples for all events",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LATENCY LATEST`
/// https://redis.io/docs/latest/commands/latency-latest/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyLatestInput {}

impl Serialize for LatencyLatestInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyLatestInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(LatencyLatestInput, API_INFO);

impl RedisCommandInput for LatencyLatestInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "LATENCY LATEST expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis LATENCY LATEST command
///
/// Returns the latest latency samples for all events.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyLatestOutput {
    /// List of latency events
    events: Vec<LatencyEvent>,
}

impl LatencyLatestOutput {
    pub fn new(events: Vec<LatencyEvent>) -> Self {
        Self { events }
    }

    /// Get the latency events
    pub fn events(&self) -> &[LatencyEvent] {
        &self.events
    }

    /// Check if there are any latency events
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Get the number of latency events
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Find an event by name
    pub fn find_event(&self, name: &str) -> Option<&LatencyEvent> {
        self.events.iter().find(|e| e.name == name)
    }

    /// Decode the Redis protocol response into a LatencyLatestOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let events = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame)?,
        };

        Ok(Self { events })
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Vec<LatencyEvent>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut events = Vec::with_capacity(items.len());
                for item in items {
                    if let Resp2Frame::Array(event_data) = item
                        && event_data.len() >= 4
                    {
                        let name = match &event_data[0] {
                            Resp2Frame::BulkString(b) => String::from_utf8(b.clone()).map_err(EpError::parse)?,
                            Resp2Frame::SimpleString(s) => String::from_utf8(s.clone()).map_err(EpError::parse)?,
                            _ => continue,
                        };
                        let timestamp = match &event_data[1] {
                            Resp2Frame::Integer(i) => *i,
                            _ => continue,
                        };
                        let latest_latency_ms = match &event_data[2] {
                            Resp2Frame::Integer(i) => *i,
                            _ => continue,
                        };
                        let max_latency_ms = match &event_data[3] {
                            Resp2Frame::Integer(i) => *i,
                            _ => continue,
                        };
                        events.push(LatencyEvent { name, timestamp, latest_latency_ms, max_latency_ms });
                    }
                }
                Ok(events)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected LATENCY LATEST response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Vec<LatencyEvent>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut events = Vec::with_capacity(data.len());
                for item in data {
                    if let Resp3Frame::Array { data: event_data, .. } = item
                        && event_data.len() >= 4
                    {
                        let name = match &event_data[0] {
                            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse)?,
                            _ => continue,
                        };
                        let timestamp = match &event_data[1] {
                            Resp3Frame::Number { data, .. } => *data,
                            _ => continue,
                        };
                        let latest_latency_ms = match &event_data[2] {
                            Resp3Frame::Number { data, .. } => *data,
                            _ => continue,
                        };
                        let max_latency_ms = match &event_data[3] {
                            Resp3Frame::Number { data, .. } => *data,
                            _ => continue,
                        };
                        events.push(LatencyEvent { name, timestamp, latest_latency_ms, max_latency_ms });
                    }
                }
                Ok(events)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected LATENCY LATEST response: {:?}", other))),
        }
    }
}

impl Serialize for LatencyLatestOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyLatestOutput", 1)?;
        state.serialize_field("events", &self.events)?;
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
            let input = LatencyLatestInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nLATENCY\r\n$6\r\nLATEST\r\n");
        }

        #[test]
        fn test_decode_empty_array() {
            let output = LatencyLatestOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_single_event() {
            // *1\r\n*4\r\n$7\r\ncommand\r\n:1234567890\r\n:5\r\n:10\r\n
            let resp = b"*1\r\n*4\r\n$7\r\ncommand\r\n:1234567890\r\n:5\r\n:10\r\n";
            let output = LatencyLatestOutput::decode(resp).unwrap();
            assert_eq!(output.len(), 1);
            let event = &output.events()[0];
            assert_eq!(event.name, "command");
            assert_eq!(event.timestamp, 1234567890);
            assert_eq!(event.latest_latency_ms, 5);
            assert_eq!(event.max_latency_ms, 10);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyLatestOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = LatencyLatestInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyLatestInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyLatestInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyLatest);
        }

        #[test]
        fn test_find_event() {
            let events = vec![
                LatencyEvent {
                    name: "command".into(),
                    timestamp: 123,
                    latest_latency_ms: 5,
                    max_latency_ms: 10,
                },
                LatencyEvent {
                    name: "fork".into(),
                    timestamp: 456,
                    latest_latency_ms: 100,
                    max_latency_ms: 200,
                },
            ];
            let output = LatencyLatestOutput::new(events);
            assert!(output.find_event("command").is_some());
            assert!(output.find_event("fork").is_some());
            assert!(output.find_event("missing").is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_latest_empty() {
            // LATENCY LATEST requires Redis 2.8.13+
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset latency data first
                    ctx.raw(b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n").await.expect("raw failed");

                    let result = ctx.raw(&LatencyLatestInput {}.command()).await.expect("raw failed");

                    LatencyLatestOutput::decode(&result).expect("decode failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_latest_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&LatencyLatestInput {}.command()).await.expect("raw failed");

            // RESP2 should return array
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            LatencyLatestOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_latest_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&LatencyLatestInput {}.command()).await.expect("raw failed");

            LatencyLatestOutput::decode(&result).expect("decode failed");

            ctx.stop().await;
        }
    }
}
