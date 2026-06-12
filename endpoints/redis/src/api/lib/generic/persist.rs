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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, PersistInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Persist,
    "Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to persistent (a key that will never expire as no timeout is associated)",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PERSIST`
/// https://redis.io/docs/latest/commands/persist/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PersistInput {
    pub(crate) key: RedisKey,
}

impl Serialize for PersistInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PersistInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(PersistInput, API_INFO, { key });

impl RedisCommandInput for PersistInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("PERSIST requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "PERSIST takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis PERSIST command
///
/// Returns whether the timeout was successfully removed from the key.
/// - `true` (1): timeout was removed
/// - `false` (0): key does not exist or has no associated timeout
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PersistOutput {
    /// Whether the timeout was removed (true = removed, false = no timeout or key missing)
    removed: bool,
}

impl PersistOutput {
    pub fn new(removed: bool) -> Self {
        Self { removed }
    }
}

impl Serialize for PersistOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PersistOutput", 1)?;
        state.serialize_field("removed", &self.removed)?;
        state.end()
    }
}

impl PersistOutput {
    /// Check if the timeout was successfully removed
    pub fn was_removed(&self) -> bool {
        self.removed
    }

    /// Alias for was_removed() for semantic clarity
    pub fn success(&self) -> bool {
        self.removed
    }

    /// Decode the Redis protocol response into a PersistOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let removed = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i == 1,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PERSIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data == 1,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PERSIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { removed })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = PersistInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nPERSIST\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_success() {
            // RESP2 integer 1 = timeout removed
            let output = PersistOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_removed());
            assert!(output.success());
        }

        #[test]
        fn test_decode_no_ttl() {
            // RESP2 integer 0 = no timeout or key missing
            let output = PersistOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_removed());
            assert!(!output.success());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = PersistOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_serialize_input() {
            let input = PersistInput { key: RedisKey::String("testkey".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("\"type\":\"PERSIST\"") || json.contains("\"type\":\"Persist\""));
            assert!(json.contains("testkey"));
        }

        #[test]
        fn test_serialize_output() {
            let output = PersistOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"removed\":true"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let result = PersistInput::decode(vec![]);
            assert!(result.is_err());
        }

        #[test]
        fn test_decode_input_valid() {
            let result = PersistInput::decode(vec![RedisJsonValue::String("mykey".into())]);
            assert!(result.is_ok());
            let input = result.unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetexInput;
        use crate::api::lib::generic::expire::ExpireInput;
        use crate::api::lib::generic::ttl::TtlInput;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_removes_ttl() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a key with expiration
                    ctx.write(SetexInput {
                        key: RedisKey::String("persist_test".into()),
                        seconds: 300.into(),
                        value: RedisJsonValue::String("value".into()),
                    })
                    .await;

                    // Remove the timeout
                    let result =
                        ctx.raw(&PersistInput { key: RedisKey::String("persist_test".into()) }.command()).await.expect("raw failed");

                    let output = PersistOutput::decode(&result).expect("decode failed");
                    assert!(output.was_removed(), "PERSIST should return 1 when TTL removed");

                    // Verify TTL is gone (TTL returns -1 for keys with no expiry)
                    let ttl_result =
                        ctx.raw(&TtlInput { key: RedisKey::String("persist_test".into()) }.command()).await.expect("raw failed");
                    // TTL should be -1 (no expiry)
                    assert!(ttl_result.starts_with(b":-1"), "TTL should be -1 after PERSIST");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_no_ttl() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a key without expiration
                    ctx.write(SetInput {
                        key: RedisKey::String("no_ttl_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Try to persist (should return 0 - no timeout existed)
                    let result = ctx.raw(&PersistInput { key: RedisKey::String("no_ttl_key".into()) }.command()).await.expect("raw failed");

                    let output = PersistOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_removed(), "PERSIST should return 0 when no TTL exists");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&PersistInput { key: RedisKey::String("nonexistent_key".into()) }.command()).await.expect("raw failed");

                    let output = PersistOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_removed(), "PERSIST should return 0 for nonexistent key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First set up a key with TTL using SETEX
                    ctx.write(SetexInput {
                        key: RedisKey::String("pipe_key".into()),
                        seconds: 300.into(),
                        value: RedisJsonValue::String("pipe_value".into()),
                    })
                    .await;

                    // Pipeline: PERSIST + TTL to verify
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&PersistInput { key: RedisKey::String("pipe_key".into()) }.command());
                    pipeline.extend_from_slice(&TtlInput { key: RedisKey::String("pipe_key".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let persist_output = PersistOutput::decode(responses[0]).expect("decode PERSIST");
                    assert!(persist_output.was_removed());

                    // TTL should be -1 after PERSIST
                    assert!(responses[1].starts_with(b":-1"), "TTL should be -1 after PERSIST");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_after_expire() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a key without expiration
                    ctx.write(SetInput {
                        key: RedisKey::String("expire_persist".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Add expiration using EXPIRE
                    ctx.write(ExpireInput {
                        key: RedisKey::String("expire_persist".into()),
                        seconds: 200,
                        ..Default::default()
                    })
                    .await;

                    // Now persist
                    let result =
                        ctx.raw(&PersistInput { key: RedisKey::String("expire_persist".into()) }.command()).await.expect("raw failed");

                    let output = PersistOutput::decode(&result).expect("decode failed");
                    assert!(output.was_removed(), "PERSIST should remove EXPIRE timeout");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetexInput {
                key: RedisKey::String("resp2_persist".into()),
                seconds: 100.into(),
                value: RedisJsonValue::String("value".into()),
            })
            .await;

            let result = ctx.raw(&PersistInput { key: RedisKey::String("resp2_persist".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = PersistOutput::decode(&result).expect("decode failed");
            assert!(output.was_removed());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_persist_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetexInput {
                key: RedisKey::String("resp3_persist".into()),
                seconds: 100.into(),
                value: RedisJsonValue::String("value".into()),
            })
            .await;

            let result = ctx.raw(&PersistInput { key: RedisKey::String("resp3_persist".into()) }.command()).await.expect("raw failed");

            // RESP3 uses same integer format for simple integers
            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = PersistOutput::decode(&result).expect("decode failed");
            assert!(output.was_removed());
            ctx.stop().await;
        }
    }
}
