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

const API_INFO: ApiInfo<RedisApi, LatencyGraphInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::LatencyGraph, "Returns a latency graph for an event", ReqType::Read, true);

/// See official Redis documentation for `LATENCY GRAPH`
/// https://redis.io/docs/latest/commands/latency-graph/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyGraphInput {
    /// The event name to get the graph for (e.g., "command", "fork")
    pub event: RedisJsonValue,
}

impl Serialize for LatencyGraphInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyGraphInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("event", &self.event)?;
        state.end()
    }
}

impl_redis_operation!(LatencyGraphInput, API_INFO, { event });

impl RedisCommandInput for LatencyGraphInput {
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
            return Err(EpError::request(format!("LATENCY GRAPH requires 1 argument, given {}", args.len())));
        }

        Ok(Self { event: args[0].clone() })
    }
}

/// Output for Redis LATENCY GRAPH command
///
/// Returns an ASCII art graph representing latency samples for an event.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyGraphOutput {
    /// The ASCII art graph representation
    graph: String,
}

impl LatencyGraphOutput {
    pub fn new(graph: String) -> Self {
        Self { graph }
    }

    /// Get the ASCII art graph
    pub fn graph(&self) -> &str {
        &self.graph
    }

    /// Check if the graph is empty (no data for the event)
    pub fn is_empty(&self) -> bool {
        self.graph.is_empty()
    }

    /// Decode the Redis protocol response into a LatencyGraphOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let graph = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected LATENCY GRAPH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::VerbatimString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected LATENCY GRAPH response: {:?}", other)));
                }
            },
        };

        Ok(Self { graph })
    }
}

impl Serialize for LatencyGraphOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyGraphOutput", 1)?;
        state.serialize_field("graph", &self.graph)?;
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
            let input = LatencyGraphInput { event: RedisJsonValue::String("command".into()) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nLATENCY\r\n$5\r\nGRAPH\r\n$7\r\ncommand\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let graph = "graph data here";
            let resp = format!("${}\r\n{}\r\n", graph.len(), graph);
            let output = LatencyGraphOutput::decode(resp.as_bytes()).unwrap();
            assert_eq!(output.graph(), graph);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_empty_string() {
            let output = LatencyGraphOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyGraphOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("command".into())];
            let input = LatencyGraphInput::decode(args).unwrap();
            assert_eq!(input.event, RedisJsonValue::String("command".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = LatencyGraphInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("command".into()), RedisJsonValue::String("extra".into())];
            let err = LatencyGraphInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyGraphInput { event: RedisJsonValue::String("command".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyGraphInput { event: RedisJsonValue::String("command".into()) };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyGraph);
        }

        #[test]
        fn test_serialization_uses_event_not_to_string() {
            let input = LatencyGraphInput { event: RedisJsonValue::String("fork".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"event\":\"fork\""));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_graph_no_data() {
            // LATENCY GRAPH requires Redis 2.8.13+
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset latency data first
                    ctx.raw(b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&LatencyGraphInput { event: RedisJsonValue::String("command".into()) }.command())
                        .await
                        .expect("raw failed");

                    // Redis 7+ returns an error when there's no latency data
                    // Older versions return an empty bulk string
                    if result.starts_with(b"-") {
                        // Error response is expected when no samples available
                        assert!(result.windows(10).any(|w| w == b"No samples"));
                    } else {
                        let output = LatencyGraphOutput::decode(&result).expect("decode failed");
                        // After reset, graph should be empty for this event
                        assert!(output.is_empty());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_graph_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result =
                ctx.raw(&LatencyGraphInput { event: RedisJsonValue::String("command".into()) }.command()).await.expect("raw failed");

            // RESP2 should return bulk string or error (if no samples)
            assert!(result.starts_with(b"$") || result.starts_with(b"-"), "RESP2 should return bulk string or error");
            // If error (no samples), that's expected in fresh Redis
            if !result.starts_with(b"-") {
                let output = LatencyGraphOutput::decode(&result).expect("decode failed");
                let _ = output.graph();
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_graph_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result =
                ctx.raw(&LatencyGraphInput { event: RedisJsonValue::String("command".into()) }.command()).await.expect("raw failed");

            // May return error if no latency samples available
            if !result.starts_with(b"-") {
                let output = LatencyGraphOutput::decode(&result).expect("decode failed");
                let _ = output.graph();
            }

            ctx.stop().await;
        }
    }
}
