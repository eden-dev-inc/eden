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

const API_INFO: ApiInfo<RedisApi, ExecInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Exec, "Executes all commands in a transaction", ReqType::Write, false);

/// See official Redis documentation for `EXEC`
/// https://redis.io/docs/latest/commands/exec/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ExecInput {}

impl Serialize for ExecInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ExecInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ExecInput, API_INFO);

impl RedisCommandInput for ExecInput {
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
            log_warn!(_ctx, "EXEC takes no arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self::default())
    }
}

/// Output for Redis EXEC command
///
/// EXEC returns an array of results from the queued commands, or null if
/// the transaction was aborted (e.g., due to a WATCH condition failing).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ExecOutput {
    /// The results of the queued commands, or None if transaction was aborted
    results: Option<Vec<RedisJsonValue>>,
}

impl ExecOutput {
    pub fn new(results: Option<Vec<RedisJsonValue>>) -> Self {
        Self { results }
    }

    /// Get the results of the executed commands
    pub fn results(&self) -> Option<&Vec<RedisJsonValue>> {
        self.results.as_ref()
    }

    /// Check if the transaction was executed (not aborted)
    pub fn was_executed(&self) -> bool {
        self.results.is_some()
    }

    /// Check if the transaction was aborted (e.g., due to WATCH)
    pub fn was_aborted(&self) -> bool {
        self.results.is_none()
    }

    /// Get the number of commands that were executed
    pub fn len(&self) -> usize {
        self.results.as_ref().map(|r| r.len()).unwrap_or(0)
    }

    /// Check if no commands were queued
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Decode the Redis protocol response into an ExecOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut results = Vec::with_capacity(arr.len());
                    for item in arr {
                        results.push(item.try_into()?);
                    }
                    Some(results)
                }
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected EXEC response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut results = Vec::with_capacity(data.len());
                    for item in data {
                        results.push(item.try_into()?);
                    }
                    Some(results)
                }
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected EXEC response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for ExecOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ExecOutput", 1)?;
        state.serialize_field("results", &self.results)?;
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
            let input = ExecInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$4\r\nEXEC\r\n");
        }

        #[test]
        fn test_decode_array_resp2() {
            // Array with two OK responses
            let output = ExecOutput::decode(b"*2\r\n+OK\r\n+OK\r\n").unwrap();
            assert!(output.was_executed());
            assert!(!output.was_aborted());
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_empty_array() {
            let output = ExecOutput::decode(b"*0\r\n").unwrap();
            assert!(output.was_executed());
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_null_resp2() {
            // Null (transaction aborted due to WATCH)
            let output = ExecOutput::decode(b"*-1\r\n").unwrap();
            assert!(!output.was_executed());
            assert!(output.was_aborted());
            assert_eq!(output.results(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ExecOutput::decode(b"_\r\n").unwrap();
            assert!(!output.was_executed());
            assert!(output.was_aborted());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ExecOutput::decode(b"-ERR EXEC without MULTI\r\n").unwrap_err();
            assert!(err.to_string().contains("EXEC"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = ExecInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_warns() {
            let input = ExecInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ExecInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ExecInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Exec);
        }

        #[test]
        fn test_serialize_input() {
            let input = ExecInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"EXEC\"") || json.contains("\"type\":\"Exec\""));
        }

        #[test]
        fn test_serialize_output_with_results() {
            let output = ExecOutput::new(Some(vec![RedisJsonValue::String("OK".into())]));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"results\""));
        }

        #[test]
        fn test_serialize_output_null() {
            let output = ExecOutput::new(None);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"results\":null"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::lib::string::incr::IncrInput;
        use crate::api::{MultiInput, SetInput, WatchInput};
        use crate::test_utils::*;
        use redis_core::RedisClient;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_without_multi_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let err = ExecOutput::decode(&result);
                    assert!(err.is_err(), "EXEC without MULTI should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_empty_transaction() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Execute without queuing any commands
                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result).expect("decode failed");
                    assert!(output.was_executed());
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_with_commands() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Start transaction
                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue SET command
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("exec_test".into()),
                            value: RedisJsonValue::String("value1".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Queue another SET command
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("exec_test2".into()),
                            value: RedisJsonValue::String("value2".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set2 failed");

                    // Execute
                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result).expect("decode failed");
                    assert!(output.was_executed());
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_aborted_by_watch() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Use a single connection for watch+transaction
                    let mut conn = ctx.pinned_connection().await.expect("failed to get conn");

                    // Set initial value
                    conn.send_command_raw(
                        &SetInput {
                            key: RedisKey::String("watched_key".into()),
                            value: RedisJsonValue::String("initial".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Watch the key
                    conn.send_command_raw(&WatchInput { keys: vec![RedisKey::String("watched_key".into())] }.command())
                        .await
                        .expect("watch failed");

                    // Modify the watched key from a DIFFERENT connection
                    // (simulating concurrent modification)
                    {
                        let mut client = RedisClient::connect(&ctx.connection_config()).await.expect("Failed to connect");

                        client
                            .send_command_raw(
                                &SetInput {
                                    key: RedisKey::String("watched_key".into()),
                                    value: RedisJsonValue::String("modified".into()),
                                    ..Default::default()
                                }
                                .command(),
                            )
                            .await
                            .expect("Set command failed");
                    }

                    // Start transaction
                    conn.send_command_raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue a command
                    conn.send_command_raw(
                        &SetInput {
                            key: RedisKey::String("watched_key".into()),
                            value: RedisJsonValue::String("tx_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("set failed");

                    // Execute - should be aborted due to WATCH
                    let (result, _latency) = conn.send_command_raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result.to_bytes()).expect("decode failed");
                    assert!(output.was_aborted(), "Transaction should be aborted");
                    assert!(!output.was_executed());

                    // Verify the key still has the modified value
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("watched_key".into()) }.command()).await.expect("get failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode get");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::String("modified".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp2_exec".into()),
                    value: RedisJsonValue::String("value".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ExecOutput::decode(&result).expect("decode failed");
            assert!(output.was_executed());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("resp3_exec".into()),
                    value: RedisJsonValue::String("value".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("set failed");

            let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

            let output = ExecOutput::decode(&result).expect("decode failed");
            assert!(output.was_executed());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exec_pipeline_results() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial counter
                    ctx.write(SetInput {
                        key: RedisKey::String("counter".into()),
                        value: RedisJsonValue::String("0".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.raw(&MultiInput {}.command()).await.expect("multi failed");

                    // Queue multiple INCR commands
                    ctx.raw(&IncrInput { key: RedisKey::String("counter".into()) }.command()).await.expect("incr1 failed");

                    ctx.raw(&IncrInput { key: RedisKey::String("counter".into()) }.command()).await.expect("incr2 failed");

                    ctx.raw(&IncrInput { key: RedisKey::String("counter".into()) }.command()).await.expect("incr3 failed");

                    let result = ctx.raw(&ExecInput {}.command()).await.expect("raw failed");

                    let output = ExecOutput::decode(&result).expect("decode failed");
                    assert!(output.was_executed());
                    assert_eq!(output.len(), 3);

                    // Verify results are 1, 2, 3
                    let results = output.results().expect("should have results");
                    assert_eq!(results[0], RedisJsonValue::Integer(1));
                    assert_eq!(results[1], RedisJsonValue::Integer(2));
                    assert_eq!(results[2], RedisJsonValue::Integer(3));
                })
            })
            .await;
        }
    }
}
