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

const API_INFO: ApiInfo<RedisApi, DiscardInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Discard, "Discards a transaction", ReqType::Write, false);

/// See official Redis documentation for `DISCARD`
/// https://redis.io/docs/latest/commands/discard/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DiscardInput {}

impl Serialize for DiscardInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DiscardInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(DiscardInput, API_INFO);

impl RedisCommandInput for DiscardInput {
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
                "DISCARD takes no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis DISCARD command
///
/// DISCARD always returns OK when successful, indicating that the transaction
/// has been discarded and all queued commands have been dropped.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DiscardOutput {
    /// Whether the transaction was successfully discarded
    success: bool,
}

impl DiscardOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if DISCARD was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a DiscardOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let response = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected DISCARD response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let response = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected DISCARD response: {:?}", other))),
            },
        }
    }
}

impl Serialize for DiscardOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("DiscardOutput", 1)?;
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
            let input = DiscardInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$7\r\nDISCARD\r\n");
        }

        #[test]
        fn test_decode_ok() {
            let output = DiscardOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = DiscardOutput::decode(b"-ERR DISCARD without MULTI\r\n").unwrap_err();
            assert!(err.to_string().contains("DISCARD"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = DiscardInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_warns() {
            let input = DiscardInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = DiscardInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = DiscardInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Discard);
        }

        #[test]
        fn test_serialize_input() {
            let input = DiscardInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"DISCARD\"") || json.contains("\"type\":\"Discard\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = DiscardOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::{MultiInput, SetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_without_multi_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

                    let err = DiscardOutput::decode(&result);
                    assert!(err.is_err(), "DISCARD without MULTI should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Discard it
                    let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

                    let output = DiscardOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "DISCARD should return OK");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_drops_queued_commands() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Ensure key doesn't exist
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\ndiscard_test\r\n").await.expect("del failed");

                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue a SET command
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("discard_test".into()),
                            value: RedisJsonValue::String("should_not_exist".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Discard the transaction
                    let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

                    let output = DiscardOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Verify the key was NOT set
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("discard_test".into()) }.command()).await.expect("get failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode get");
                    assert!(!get_output.exists(), "Key should not exist after DISCARD");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_allows_new_transaction() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First transaction - discard
                    ctx.raw(&MultiInput {}.command()).await.expect("multi1 failed");
                    ctx.raw(&DiscardInput {}.command()).await.expect("discard failed");

                    // Second transaction - should work
                    let result = ctx.raw(&MultiInput {}.command()).await.expect("multi2 failed");

                    // Should succeed (not nested error)
                    assert!(result.starts_with(b"+OK"), "Should be able to start new transaction after DISCARD");

                    // Clean up
                    ctx.raw(&DiscardInput {}.command()).await.expect("discard2 failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

            let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = DiscardOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

            let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = DiscardOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_discard_multiple_queued_commands() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue multiple commands
                    for i in 0..5 {
                        ctx.raw(
                            &SetInput {
                                key: RedisKey::String(format!("discard_multi_{}", i)),
                                value: RedisJsonValue::String("value".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("set failed");
                    }

                    // Discard all
                    let result = ctx.raw(&DiscardInput {}.command()).await.expect("raw failed");

                    let output = DiscardOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Verify none of the keys exist
                    for i in 0..5 {
                        let get_result = ctx
                            .raw(&GetInput { key: RedisKey::String(format!("discard_multi_{}", i)) }.command())
                            .await
                            .expect("get failed");

                        let get_output = GetOutput::decode(&get_result).expect("decode get");
                        assert!(!get_output.exists(), "Key {} should not exist", i);
                    }
                })
            })
            .await;
        }
    }
}
