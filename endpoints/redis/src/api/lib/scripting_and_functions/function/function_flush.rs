use crate::api::lib::scripting_and_functions::function::FlushMode;
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

const API_INFO: ApiInfo<RedisApi, FunctionFlushInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FunctionFlush, "Deletes all libraries and functions", ReqType::Write, true);

/// See official Redis documentation for `FUNCTION FLUSH`
/// https://redis.io/docs/latest/commands/function-flush/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionFlushInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<FlushMode>,
}

impl FunctionFlushInput {
    /// Create a new FUNCTION FLUSH input with default (SYNC) mode
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a FUNCTION FLUSH input with ASYNC mode
    pub fn async_mode() -> Self {
        Self { mode: Some(FlushMode::ASYNC) }
    }

    /// Create a FUNCTION FLUSH input with SYNC mode
    pub fn sync_mode() -> Self {
        Self { mode: Some(FlushMode::SYNC) }
    }
}

impl Serialize for FunctionFlushInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.mode.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FunctionFlushInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(mode) = &self.mode {
            state.serialize_field("mode", mode)?;
        }
        state.end()
    }
}

impl_redis_operation!(FunctionFlushInput, API_INFO, { mode });

impl RedisCommandInput for FunctionFlushInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(mode) = &self.mode {
            match mode {
                FlushMode::SYNC => command.arg("SYNC"),
                FlushMode::ASYNC => command.arg("ASYNC"),
            };
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut mode = None;

        if !args.is_empty()
            && let RedisJsonValue::String(s) = &args[0]
        {
            mode = match s.to_uppercase().as_str() {
                "SYNC" => Some(FlushMode::SYNC),
                "ASYNC" => Some(FlushMode::ASYNC),
                other => {
                    return Err(EpError::request(format!("FUNCTION FLUSH invalid mode '{}', expected SYNC or ASYNC", other)));
                }
            };
        }

        Ok(Self { mode })
    }
}

/// Output for Redis FUNCTION FLUSH command
///
/// Returns OK when all libraries and functions have been deleted.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FunctionFlushOutput {
    /// Whether the flush operation succeeded
    success: bool,
}

impl FunctionFlushOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the flush was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FunctionFlushOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => s == b"OK",
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION FLUSH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => data == b"OK",
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FUNCTION FLUSH response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

impl Serialize for FunctionFlushOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("FunctionFlushOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_mode() {
            let input = FunctionFlushInput::new();
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n");
        }

        #[test]
        fn test_encode_command_sync_mode() {
            let input = FunctionFlushInput::sync_mode();
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n$4\r\nSYNC\r\n");
        }

        #[test]
        fn test_encode_command_async_mode() {
            let input = FunctionFlushInput::async_mode();
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n$5\r\nASYNC\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = FunctionFlushOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FunctionFlushOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = FunctionFlushInput::decode(vec![]).unwrap();
            assert!(input.mode.is_none());
        }

        #[test]
        fn test_decode_input_sync() {
            let args = vec![RedisJsonValue::String("SYNC".into())];
            let input = FunctionFlushInput::decode(args).unwrap();
            assert_eq!(input.mode, Some(FlushMode::SYNC));
        }

        #[test]
        fn test_decode_input_async() {
            let args = vec![RedisJsonValue::String("ASYNC".into())];
            let input = FunctionFlushInput::decode(args).unwrap();
            assert_eq!(input.mode, Some(FlushMode::ASYNC));
        }

        #[test]
        fn test_decode_input_case_insensitive() {
            let args = vec![RedisJsonValue::String("async".into())];
            let input = FunctionFlushInput::decode(args).unwrap();
            assert_eq!(input.mode, Some(FlushMode::ASYNC));
        }

        #[test]
        fn test_decode_input_invalid_mode() {
            let args = vec![RedisJsonValue::String("INVALID".into())];
            let err = FunctionFlushInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("invalid mode"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FunctionFlushInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FunctionFlushInput::sync_mode();
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\""));
            assert!(json.contains("\"mode\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = FunctionFlushOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_kind() {
            let input = FunctionFlushInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::FunctionFlush);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // FUNCTION commands require Redis 7.0+
        const MIN_VERSION: &str = "7";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_empty() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Flush on empty function set should succeed
                    let result = ctx.raw(&FunctionFlushInput::new().command()).await.expect("raw failed");

                    let output = FunctionFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "FUNCTION FLUSH should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_sync_mode() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FunctionFlushInput::sync_mode().command()).await.expect("raw failed");

                    let output = FunctionFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "FUNCTION FLUSH SYNC should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_async_mode() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FunctionFlushInput::async_mode().command()).await.expect("raw failed");

                    let output = FunctionFlushOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "FUNCTION FLUSH ASYNC should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&FunctionFlushInput::new().command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = FunctionFlushOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&FunctionFlushInput::new().command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = FunctionFlushOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_flush_pipeline() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Pipeline: FUNCTION FLUSH + FUNCTION FLUSH
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&FunctionFlushInput::new().command());
                    pipeline.extend_from_slice(&FunctionFlushInput::sync_mode().command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let output1 = FunctionFlushOutput::decode(responses[0]).expect("decode first");
                    assert!(output1.success());

                    let output2 = FunctionFlushOutput::decode(responses[1]).expect("decode second");
                    assert!(output2.success());
                })
            })
            .await;
        }
    }
}
