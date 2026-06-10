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

const API_INFO: ApiInfo<RedisApi, UnwatchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Unwatch,
    "Forgets about watched keys of a transaction",
    ReqType::Write,
    false,
);

/// See official Redis documentation for `UNWATCH`
/// https://redis.io/docs/latest/commands/unwatch/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct UnwatchInput {}

impl Serialize for UnwatchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UnwatchInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(UnwatchInput, API_INFO);

impl RedisCommandInput for UnwatchInput {
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
                "UNWATCH takes no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self::default())
    }
}

/// Output for Redis UNWATCH command
///
/// UNWATCH always returns OK, indicating that all watched keys have been
/// unwatched. This command never fails.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct UnwatchOutput {
    /// Whether the unwatch was successful (always true for valid responses)
    success: bool,
}

impl UnwatchOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if UNWATCH was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an UnwatchOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let response = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected UNWATCH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let response = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected UNWATCH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for UnwatchOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("UnwatchOutput", 1)?;
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
            let input = UnwatchInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$7\r\nUNWATCH\r\n");
        }

        #[test]
        fn test_decode_ok() {
            let output = UnwatchOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = UnwatchOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = UnwatchInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_warns() {
            // Should succeed but log a warning
            let input = UnwatchInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = UnwatchInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = UnwatchInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Unwatch);
        }

        #[test]
        fn test_serialize_input() {
            let input = UnwatchInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"UNWATCH\"") || json.contains("\"type\":\"Unwatch\""));
        }

        #[test]
        fn test_serialize_output() {
            let output = UnwatchOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{ExecInput, ExecOutput, MultiInput, SetInput, WatchInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_without_watch() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // UNWATCH without prior WATCH should still return OK
                    let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

                    let output = UnwatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "UNWATCH without WATCH should return OK");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_after_watch() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Watch a key
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("unwatch_test".into())] }.command()).await.expect("watch failed");

                    // Unwatch
                    let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

                    let output = UnwatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_allows_transaction_after_modification() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.write(SetInput {
                        key: RedisKey::String("unwatch_mod_key".into()),
                        value: RedisJsonValue::String("initial".into()),
                        ..Default::default()
                    })
                    .await;

                    // Watch the key
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("unwatch_mod_key".into())] }.command()).await.expect("watch failed");

                    // Modify the key
                    ctx.write(SetInput {
                        key: RedisKey::String("unwatch_mod_key".into()),
                        value: RedisJsonValue::String("modified".into()),
                        ..Default::default()
                    })
                    .await;

                    // UNWATCH - this should clear the watch
                    ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");

                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue SET
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("unwatch_mod_key".into()),
                            value: RedisJsonValue::String("tx_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Execute - should succeed because we unwatched
                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result).expect("decode failed");
                    assert!(output.was_executed(), "Transaction should succeed after UNWATCH despite modification");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_multiple_calls() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Watch something
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("multi_unwatch".into())] }.command()).await.expect("watch failed");

                    // Multiple UNWATCH calls should all succeed
                    for _ in 0..3 {
                        let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

                        let output = UnwatchOutput::decode(&result).expect("decode failed");
                        assert!(output.success());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_clears_all_watches() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial values
                    for i in 1..=3 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("unwatch_clear_{}", i)),
                            value: RedisJsonValue::String("initial".into()),
                            ..Default::default()
                        })
                        .await;
                    }

                    // Watch multiple keys
                    ctx.raw(
                        &WatchInput {
                            keys: vec![
                                RedisKey::String("unwatch_clear_1".into()),
                                RedisKey::String("unwatch_clear_2".into()),
                                RedisKey::String("unwatch_clear_3".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("watch failed");

                    // Modify all watched keys
                    for i in 1..=3 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("unwatch_clear_{}", i)),
                            value: RedisJsonValue::String("modified".into()),
                            ..Default::default()
                        })
                        .await;
                    }

                    // UNWATCH all
                    let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");
                    let output = UnwatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Transaction should succeed
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("unwatch_clear_1".into()),
                            value: RedisJsonValue::String("tx_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    let exec_result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let exec_output = ExecOutput::decode(&exec_result).expect("decode failed");
                    assert!(exec_output.was_executed(), "Transaction should succeed - all watches were cleared");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = UnwatchOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = UnwatchOutput::decode(&result).expect("decode failed");
            assert!(output.success());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unwatch_inside_multi() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Watch a key first
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("unwatch_in_multi".into())] }.command()).await.expect("watch failed");

                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // UNWATCH inside MULTI - gets queued
                    let result = ctx.raw(&UnwatchInput {}.command()).await.expect("raw failed");

                    // Should return QUEUED
                    assert!(result.starts_with(b"+QUEUED"), "UNWATCH inside MULTI should be queued");

                    // Execute
                    let exec_result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let exec_output = ExecOutput::decode(&exec_result).expect("decode failed");
                    assert!(exec_output.was_executed());
                })
            })
            .await;
        }
    }
}
