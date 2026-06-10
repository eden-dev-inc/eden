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

const API_INFO: ApiInfo<RedisApi, MultiInput> = ApiInfo::new(EpKind::Redis, RedisApi::Multi, "Starts a transaction", ReqType::Write, false);

/// See official Redis documentation for `MULTI`
/// https://redis.io/docs/latest/commands/multi/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MultiInput {}

impl Serialize for MultiInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MultiInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(MultiInput, API_INFO);

impl RedisCommandInput for MultiInput {
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
            log_warn!(_ctx, "MULTI takes no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self::default())
    }
}

/// Output for Redis MULTI command
///
/// MULTI always returns OK when successful, indicating that the transaction
/// has been started and subsequent commands will be queued.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MultiOutput {
    /// Whether the transaction was successfully started
    success: bool,
}

impl MultiOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if MULTI was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a MultiOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let response = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected MULTI response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let response = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected MULTI response: {:?}", other))),
            },
        }
    }
}

impl Serialize for MultiOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MultiOutput", 1)?;
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
        fn test_encode_command() {
            let input = MultiInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$5\r\nMULTI\r\n");
        }

        #[test]
        fn test_decode_ok() {
            let output = MultiOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MultiOutput::decode(b"-ERR MULTI calls can not be nested\r\n").unwrap_err();
            assert!(err.to_string().contains("MULTI"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = MultiInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_warns() {
            // Should succeed but log a warning
            let input = MultiInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = MultiInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = MultiInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Multi);
        }

        #[test]
        fn test_serialize_input() {
            let input = MultiInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"MULTI\"") || json.contains("\"type\":\"Multi\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = MultiOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::{DiscardInput, ExecInput, ExecOutput, SetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_multi_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");

                    let output = MultiOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "MULTI should return OK");

                    // Clean up - discard the transaction
                    ctx.raw(&DiscardInput {}.command()).await.expect("discard failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_multi_nested_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First MULTI should succeed
                    let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");
                    let output = MultiOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Second MULTI should fail (nested)
                    let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");
                    let err = MultiOutput::decode(&result);
                    assert!(err.is_err(), "Nested MULTI should fail");

                    // Clean up
                    ctx.raw(&DiscardInput {}.command()).await.expect("discard failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_multi_exec_transaction() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Start transaction
                    let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");
                    let output = MultiOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Queue a SET command
                    let set_result = ctx
                        .raw(
                            &SetInput {
                                key: RedisKey::String("tx_key".into()),
                                value: RedisJsonValue::String("tx_value".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    // Should return QUEUED
                    assert!(set_result.starts_with(b"+QUEUED"), "Command should be queued");

                    // Execute transaction
                    let exec_result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");
                    let exec_output = ExecOutput::decode(&exec_result).expect("decode failed");
                    assert!(exec_output.was_executed(), "Transaction should be executed");

                    // Verify the SET worked
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("tx_key".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::String("tx_value".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_multi_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = MultiOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            // Clean up
            ctx.raw(&DiscardInput {}.command()).await.expect("discard failed");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_multi_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&MultiInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = MultiOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            // Clean up
            ctx.raw(&DiscardInput {}.command()).await.expect("discard failed");
            ctx.stop().await;
        }
    }
}
