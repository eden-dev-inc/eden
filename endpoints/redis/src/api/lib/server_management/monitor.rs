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
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, MonitorInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Monitor,
    "Listens for all requests received by the server in real-time",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `MONITOR`
/// https://redis.io/docs/latest/commands/monitor/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MonitorInput {}

impl Serialize for MonitorInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MonitorInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MonitorInput, API_INFO);

impl RedisCommandInput for MonitorInput {
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
                "MONITOR expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis MONITOR command
///
/// The initial response is "OK". Subsequent responses are streaming monitor events.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MonitorOutput {
    /// The response message (typically "OK" on success)
    message: String,
}

impl MonitorOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    /// Get the response message
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Check if the monitor was started successfully
    pub fn is_ok(&self) -> bool {
        self.message == "OK"
    }

    /// Decode the Redis protocol response into a MonitorOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MONITOR response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MONITOR response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for MonitorOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MonitorOutput", 1)?;
        state.serialize_field("message", &self.message)?;
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
            let input = MonitorInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$7\r\nMONITOR\r\n");
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = MonitorInput::decode(args).unwrap();
            assert_eq!(format!("{:?}", input), "MonitorInput");
        }

        #[test]
        fn test_decode_input_with_extra_args_succeeds() {
            // MONITOR ignores extra args with a warning
            let args = vec![RedisJsonValue::String("extra".into())];
            let result = MonitorInput::decode(args);
            assert!(result.is_ok());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MonitorInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_ok_response() {
            let output = MonitorOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_response() {
            let err = MonitorOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = MonitorOutput::new("OK".to_string());
            assert!(output.is_ok());
        }

        #[test]
        fn test_kind() {
            let input = MonitorInput {};
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Monitor);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: MONITOR is a streaming command that keeps the connection open.
        // Testing is limited because it doesn't return until connection closes.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_monitor_returns_ok() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MonitorInput {}.command()).await.expect("raw failed");

                    let output = MonitorOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }
    }
}
