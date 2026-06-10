use crate::api::lib::{MultiCommand, RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, WatchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Watch,
    "Monitors changes to keys to determine the execution of a transaction",
    ReqType::Read,
    false,
);

/// See official Redis documentation for `WATCH`
/// https://redis.io/docs/latest/commands/watch/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct WatchInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for WatchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("WatchInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(WatchInput, API_INFO, { keys });

impl RedisCommandInput for WatchInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.keys);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("WATCH requires at least 1 argument, given None"));
        }

        let mut keys = Vec::with_capacity(args.len());
        for key in args.into_iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { keys })
    }
}

/// `MultiCommand` impl for `WATCH`.
///
/// `WATCH k1 ... kN` is observably equivalent to `WATCH k1; ...; WATCH kN`
/// on the same connection: the connection's watch list is a set, key order
/// is irrelevant, and the operation is purely client-side state mutation
/// per key. Watching the same key multiple times is a no-op
/// (`test_watch_can_watch_same_key_twice`). Reconstruction returns success
/// only when every per-key call succeeded; the first error short-circuits
/// and propagates unchanged (e.g. `WATCH` issued inside `MULTI`, exercised
/// by `test_watch_inside_multi_fails`).
impl MultiCommand for WatchInput {
    type Single = WatchInput;
    type SingleOutput = WatchOutput;
    type Output = WatchOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|k| WatchInput { keys: vec![k] }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        for part in parts {
            let _ok = part?;
        }
        Ok(WatchOutput::new(true))
    }
}

/// Output for Redis WATCH command
///
/// WATCH always returns OK when successful, indicating that the keys
/// are now being monitored for changes.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct WatchOutput {
    /// Whether the keys are now being watched
    success: bool,
}

impl WatchOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if WATCH was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a WatchOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let response = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected WATCH response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let response = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: response == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?)),
                other => Err(EpError::parse(format!("unexpected WATCH response: {:?}", other))),
            },
        }
    }
}

impl Serialize for WatchOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("WatchOutput", 1)?;
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
        fn test_encode_command_single_key() {
            let input = WatchInput { keys: vec![RedisKey::String("mykey".into())] };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WATCH"));
            assert!(cmd_str.contains("mykey"));
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = WatchInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WATCH"));
            assert!(cmd_str.contains("key1"));
            assert!(cmd_str.contains("key2"));
            assert!(cmd_str.contains("key3"));
        }

        #[test]
        fn test_decode_ok() {
            let output = WatchOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = WatchOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = WatchInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let input = WatchInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let err = WatchInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_watched_keys() {
            let input = WatchInput {
                keys: vec![RedisKey::String("key1".into()), RedisKey::String("key2".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("key1".into()));
            assert_eq!(keys[1], RedisKey::String("key2".into()));
        }

        #[test]
        fn test_kind() {
            let input = WatchInput { keys: vec![RedisKey::String("key".into())] };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Watch);
        }

        #[test]
        fn test_serialize_input() {
            let input = WatchInput { keys: vec![RedisKey::String("testkey".into())] };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"WATCH\"") || json.contains("\"type\":\"Watch\""));
            assert!(json.contains("testkey"));
        }

        #[test]
        fn test_serialize_output() {
            let output = WatchOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = WatchInput {
                keys: vec![
                    RedisKey::String("a".into()),
                    RedisKey::String("b".into()),
                    RedisKey::String("c".into()),
                ],
            };
            assert_eq!(input.deconstruct().len(), input.keys().len());
        }

        #[test]
        fn test_deconstruct_order_preserved() {
            let keys = vec![
                RedisKey::String("first".into()),
                RedisKey::String("second".into()),
                RedisKey::String("third".into()),
            ];
            let input = WatchInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = WatchInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
        }

        #[test]
        fn test_reconstruct_returns_success_on_all_ok() {
            let parts = vec![Ok(WatchOutput::new(true)), Ok(WatchOutput::new(true)), Ok(WatchOutput::new(true))];
            let output = WatchInput::reconstruct(parts).unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_reconstruct_propagates_error() {
            let err = EpError::parse("ERR synthetic failure");
            let result = WatchInput::reconstruct(vec![Ok(WatchOutput::new(true)), Err(err.clone())]);
            assert_eq!(result.unwrap_err(), err);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{DiscardInput, ExecInput, ExecOutput, MultiInput, SetInput, UnwatchInput};
        use crate::test_utils::*;
        use redis_core::RedisClient;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_single_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&WatchInput { keys: vec![RedisKey::String("watch_single".into())] }.command()).await.expect("raw failed");

                    let output = WatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "WATCH should return OK");

                    // Clean up - unwatch
                    ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &WatchInput {
                                keys: vec![
                                    RedisKey::String("watch_multi_1".into()),
                                    RedisKey::String("watch_multi_2".into()),
                                    RedisKey::String("watch_multi_3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = WatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success());

                    // Clean up
                    ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // WATCH works on non-existent keys too
                    let result = ctx
                        .raw(&WatchInput { keys: vec![RedisKey::String("nonexistent_watch_key".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = WatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "WATCH on nonexistent key should succeed");

                    ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_transaction_success() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.write(SetInput {
                        key: RedisKey::String("watch_tx_key".into()),
                        value: RedisJsonValue::String("initial".into()),
                        ..Default::default()
                    })
                    .await;

                    // Watch the key
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("watch_tx_key".into())] }.command()).await.expect("watch failed");

                    // Start transaction (key not modified)
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue SET
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("watch_tx_key".into()),
                            value: RedisJsonValue::String("updated".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Execute - should succeed (key wasn't modified)
                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result).expect("decode failed");
                    assert!(output.was_executed(), "Transaction should succeed when watched key not modified");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_transaction_abort() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Use a single connection for watch+transaction
                    let mut conn = ctx.pinned_connection().await.expect("failed to get conn");

                    // Set initial value
                    conn.send_command_raw(
                        &SetInput {
                            key: RedisKey::String("watch_abort_key".into()),
                            value: RedisJsonValue::String("initial".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Watch the key
                    conn.send_command_raw(&WatchInput { keys: vec![RedisKey::String("watch_abort_key".into())] }.command())
                        .await
                        .expect("watch failed");

                    // Modify the key (simulating concurrent modification)
                    {
                        let mut client = RedisClient::connect(&ctx.connection_config()).await.expect("Failed to connect");

                        client
                            .send_command_raw(
                                &SetInput {
                                    key: RedisKey::String("watch_abort_key".into()),
                                    value: RedisJsonValue::String("concurrent_update".into()),
                                    ..Default::default()
                                }
                                .command(),
                            )
                            .await
                            .expect("set command failed");
                    }

                    // Start transaction
                    conn.send_command_raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue SET
                    conn.send_command_raw(
                        &SetInput {
                            key: RedisKey::String("watch_abort_key".into()),
                            value: RedisJsonValue::String("tx_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Execute - should be aborted
                    let (result, _latency) = conn.send_command_raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result.to_bytes()).expect("decode failed");
                    assert!(output.was_aborted(), "Transaction should abort when watched key was modified");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_inside_multi_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Use a single connection for watch+transaction
                    let mut conn = ctx.pinned_connection().await.expect("failed to get conn");

                    // Start transaction first
                    conn.send_command_raw(&MultiInput {}.command()).await.expect("multi failed");

                    // WATCH inside MULTI should fail
                    let (result, _latency) = conn
                        .send_command_raw(&WatchInput { keys: vec![RedisKey::String("key".into())] }.command())
                        .await
                        .expect("raw failed");

                    let err = WatchOutput::decode(&result.to_bytes());
                    assert!(err.is_err(), "WATCH inside MULTI should fail");

                    // Clean up
                    conn.send_command_raw(&DiscardInput {}.command()).await.expect("discard failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&WatchInput { keys: vec![RedisKey::String("resp2_watch".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string format");
            let output = WatchOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&WatchInput { keys: vec![RedisKey::String("resp3_watch".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string format");
            let output = WatchOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_watch_can_watch_same_key_twice() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Watch the same key twice - should succeed
                    ctx.raw(&WatchInput { keys: vec![RedisKey::String("double_watch".into())] }.command()).await.expect("watch1 failed");

                    let result =
                        ctx.raw(&WatchInput { keys: vec![RedisKey::String("double_watch".into())] }.command()).await.expect("raw failed");

                    let output = WatchOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "Should be able to WATCH same key twice");

                    ctx.raw(&UnwatchInput {}.command()).await.expect("unwatch failed");
                })
            })
            .await;
        }
    }
}
