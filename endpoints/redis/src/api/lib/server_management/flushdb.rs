#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
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

const API_INFO: ApiInfo<RedisApi, FlushdbInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Flushdb, "Removes all keys from the current database", ReqType::Write, true);

/// Flush mode for FLUSHDB command
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Mode {
    /// Flush synchronously (blocking)
    #[default]
    SYNC,
    /// Flush asynchronously (non-blocking)
    ASYNC,
}

/// See official Redis documentation for `FLUSHDB`
/// https://redis.io/docs/latest/commands/flushdb/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FlushdbInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    mode: Option<Mode>,
}

impl Serialize for FlushdbInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 1;
        if self.mode.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("FlushdbInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(mode) = &self.mode {
            state.serialize_field("mode", mode)?;
        }
        state.end()
    }
}

impl_redis_operation!(FlushdbInput, API_INFO, { mode });

impl RedisCommandInput for FlushdbInput {
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
                Mode::SYNC => command.arg("SYNC"),
                Mode::ASYNC => command.arg("ASYNC"),
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
                "SYNC" => Some(Mode::SYNC),
                "ASYNC" => Some(Mode::ASYNC),
                _ => None,
            };
        }

        Ok(Self { mode })
    }
}

/// Output for Redis FLUSHDB command
///
/// Returns OK when all keys have been flushed from the current database.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FlushdbOutput {
    success: bool,
}

impl Serialize for FlushdbOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FlushdbOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FlushdbOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the flush was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FLUSHDB response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FLUSHDB response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_mode() {
            let input = FlushdbInput { mode: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$7\r\nFLUSHDB\r\n");
        }

        #[test]
        fn test_encode_command_sync() {
            let input = FlushdbInput { mode: Some(Mode::SYNC) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FLUSHDB"));
            assert!(cmd_str.contains("SYNC"));
        }

        #[test]
        fn test_encode_command_async() {
            let input = FlushdbInput { mode: Some(Mode::ASYNC) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FLUSHDB"));
            assert!(cmd_str.contains("ASYNC"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FlushdbOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FlushdbOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = FlushdbInput::decode(vec![]).unwrap();
            assert!(input.mode.is_none());
        }

        #[test]
        fn test_decode_input_sync() {
            let input = FlushdbInput::decode(vec![RedisJsonValue::String("SYNC".into())]).unwrap();
            assert!(matches!(input.mode, Some(Mode::SYNC)));
        }

        #[test]
        fn test_decode_input_async() {
            let input = FlushdbInput::decode(vec![RedisJsonValue::String("ASYNC".into())]).unwrap();
            assert!(matches!(input.mode, Some(Mode::ASYNC)));
        }

        #[test]
        fn test_decode_input_async_lowercase() {
            let input = FlushdbInput::decode(vec![RedisJsonValue::String("async".into())]).unwrap();
            assert!(matches!(input.mode, Some(Mode::ASYNC)));
        }

        #[test]
        fn test_decode_input_invalid_mode() {
            let input = FlushdbInput::decode(vec![RedisJsonValue::String("INVALID".into())]).unwrap();
            assert!(input.mode.is_none());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FlushdbInput::default();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = FlushdbInput::default();
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Flushdb);
        }

        #[test]
        fn test_serialize_input_no_mode() {
            let input = FlushdbInput { mode: None };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("FLUSHDB") || json.contains("Flushdb"));
            assert!(!json.contains("mode"));
        }

        #[test]
        fn test_serialize_input_with_mode() {
            let input = FlushdbInput { mode: Some(Mode::ASYNC) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("mode"));
            assert!(json.contains("ASYNC"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FlushdbOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_req_type_is_write() {
            assert_eq!(API_INFO.request_type, ReqType::Write);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_flushdb_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set some keys
                    for i in 0..3 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("flushdb_key{}", i)),
                            value: RedisJsonValue::String("value".into()),
                            ..Default::default()
                        })
                        .await;
                    }

                    // Verify keys exist (DBSIZE > 0)
                    let dbsize_before = ctx.raw(b"*1\r\n$6\r\nDBSIZE\r\n").await.expect("dbsize failed");
                    assert!(!dbsize_before.starts_with(b":0"), "should have keys before flush");

                    // FLUSHDB
                    let result = ctx.raw(&FlushdbInput::default().command()).await.expect("raw failed");

                    let output = FlushdbOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify database is empty
                    let dbsize_after = ctx.raw(b"*1\r\n$6\r\nDBSIZE\r\n").await.expect("dbsize failed");
                    assert!(dbsize_after.starts_with(b":0"), "should have 0 keys after flush");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_flushdb_async_mode() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set a key
                    ctx.write(SetInput {
                        key: RedisKey::String("async_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // FLUSHDB ASYNC
                    let result = ctx.raw(&FlushdbInput { mode: Some(Mode::ASYNC) }.command()).await.expect("raw failed");

                    let output = FlushdbOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_flushdb_sync_mode() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set a key
                    ctx.write(SetInput {
                        key: RedisKey::String("sync_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // FLUSHDB SYNC
                    let result = ctx.raw(&FlushdbInput { mode: Some(Mode::SYNC) }.command()).await.expect("raw failed");

                    let output = FlushdbOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());

                    // Verify empty after sync flush
                    let dbsize = ctx.raw(b"*1\r\n$6\r\nDBSIZE\r\n").await.expect("dbsize failed");
                    assert!(dbsize.starts_with(b":0"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_flushdb_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&FlushdbInput::default().command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 should return simple string");
            let output = FlushdbOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_flushdb_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&FlushdbInput::default().command()).await.expect("raw failed");

            let output = FlushdbOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }
    }
}
