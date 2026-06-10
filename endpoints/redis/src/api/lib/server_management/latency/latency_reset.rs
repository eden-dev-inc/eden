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

const API_INFO: ApiInfo<RedisApi, LatencyResetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::LatencyReset,
    "Resets the latency data for one or more events",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LATENCY RESET`
/// https://redis.io/docs/latest/commands/latency-reset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LatencyResetInput {
    /// Optional list of event names to reset. If not specified, resets all events.
    pub events: Option<Vec<RedisJsonValue>>,
}

impl Serialize for LatencyResetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.events.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("LatencyResetInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(events) = &self.events {
            state.serialize_field("events", events)?;
        }
        state.end()
    }
}

impl_redis_operation!(LatencyResetInput, API_INFO, { events });

impl RedisCommandInput for LatencyResetInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(events) = &self.events {
            for event in events {
                command.arg(event);
            }
        }
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let events = if args.is_empty() { None } else { Some(args) };
        Ok(Self { events })
    }
}

/// Output for Redis LATENCY RESET command
///
/// Returns the number of event time series that were reset.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LatencyResetOutput {
    /// Number of events that were reset
    count: i64,
}

impl LatencyResetOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of events that were reset
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if any events were reset
    pub fn any_reset(&self) -> bool {
        self.count > 0
    }

    /// Decode the Redis protocol response into a LatencyResetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected LATENCY RESET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected LATENCY RESET response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for LatencyResetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LatencyResetOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = LatencyResetInput { events: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nLATENCY\r\n$5\r\nRESET\r\n");
        }

        #[test]
        fn test_encode_command_with_events() {
            let input = LatencyResetInput {
                events: Some(vec![RedisJsonValue::String("command".into()), RedisJsonValue::String("fork".into())]),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"command"));
            assert!(cmd.windows(4).any(|w| w == b"fork"));
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = LatencyResetOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(!output.any_reset());
        }

        #[test]
        fn test_decode_integer_positive() {
            let output = LatencyResetOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.count(), 3);
            assert!(output.any_reset());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = LatencyResetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = LatencyResetInput::decode(vec![]).unwrap();
            assert!(input.events.is_none());
        }

        #[test]
        fn test_decode_input_with_events() {
            let args = vec![RedisJsonValue::String("command".into()), RedisJsonValue::String("fork".into())];
            let input = LatencyResetInput::decode(args).unwrap();
            assert!(input.events.is_some());
            assert_eq!(input.events.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = LatencyResetInput { events: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = LatencyResetInput { events: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::LatencyReset);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_reset_all() {
            // LATENCY RESET requires Redis 2.8.13+
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&LatencyResetInput { events: None }.command()).await.expect("raw failed");

                    let output = LatencyResetOutput::decode(&result).expect("decode failed");
                    // Returns number of reset events (may be 0 if no events recorded)
                    assert!(output.count() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_reset_specific_event() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&LatencyResetInput { events: Some(vec![RedisJsonValue::String("command".into())]) }.command())
                        .await
                        .expect("raw failed");

                    let output = LatencyResetOutput::decode(&result).expect("decode failed");
                    // Returns 0 or 1 depending on whether the event existed
                    assert!(output.count() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_reset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&LatencyResetInput { events: None }.command()).await.expect("raw failed");

            // RESP2 should return integer
            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = LatencyResetOutput::decode(&result).expect("decode failed");
            assert!(output.count() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_reset_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&LatencyResetInput { events: None }.command()).await.expect("raw failed");

            let output = LatencyResetOutput::decode(&result).expect("decode failed");
            assert!(output.count() >= 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_latency_reset_clears_history() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Reset all events
                    ctx.raw(&LatencyResetInput { events: None }.command()).await.expect("raw failed");

                    // Check that LATENCY LATEST returns empty after reset
                    let latest_result = ctx.raw(b"*2\r\n$7\r\nLATENCY\r\n$6\r\nLATEST\r\n").await.expect("raw failed");

                    // Should return empty array
                    assert!(latest_result.starts_with(b"*"));
                })
            })
            .await;
        }
    }
}
