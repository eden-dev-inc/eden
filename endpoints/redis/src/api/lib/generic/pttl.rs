use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{PttlResult, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, PttlInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pttl,
    "Returns the remaining time to live of a key that has a timeout, in milliseconds. Returns -2 if the key does not exist, -1 if the key exists but has no associated expire.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PTTL`
/// https://redis.io/docs/latest/commands/pttl/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PttlInput {
    pub(crate) key: RedisKey,
}

impl Serialize for PttlInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PttlInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(PttlInput, API_INFO, { key });

impl RedisCommandInput for PttlInput {
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
            return Err(EpError::parse("PTTL requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "PTTL takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis PTTL command
///
/// Returns the remaining time to live of a key in milliseconds,
/// or special values indicating key doesn't exist or has no TTL.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PttlOutput {
    result: PttlResult,
}

impl PttlOutput {
    pub fn new(result: PttlResult) -> Self {
        Self { result }
    }

    /// Get the result
    pub fn result(&self) -> &PttlResult {
        &self.result
    }

    /// Get the TTL in milliseconds, if available
    pub fn ttl_ms(&self) -> Option<i64> {
        match &self.result {
            PttlResult::Milliseconds(ms) => Some(*ms),
            _ => None,
        }
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        !matches!(self.result, PttlResult::KeyNotFound)
    }

    /// Check if the key has an expiration set
    pub fn has_expire(&self) -> bool {
        matches!(self.result, PttlResult::Milliseconds(_))
    }

    /// Decode the Redis protocol response into a PttlOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PTTL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PTTL response: {:?}", other)));
                }
            },
        };

        let result = match value {
            -2 => PttlResult::KeyNotFound,
            -1 => PttlResult::NoExpire,
            ms => PttlResult::Milliseconds(ms),
        };

        Ok(Self { result })
    }
}

impl Serialize for PttlOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("PttlOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = PttlInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nPTTL\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_key_not_found() {
            let output = PttlOutput::decode(b":-2\r\n").unwrap();
            assert!(!output.exists());
            assert!(!output.has_expire());
            assert_eq!(output.ttl_ms(), None);
            assert_eq!(output.result(), &PttlResult::KeyNotFound);
        }

        #[test]
        fn test_decode_no_expire() {
            let output = PttlOutput::decode(b":-1\r\n").unwrap();
            assert!(output.exists());
            assert!(!output.has_expire());
            assert_eq!(output.ttl_ms(), None);
            assert_eq!(output.result(), &PttlResult::NoExpire);
        }

        #[test]
        fn test_decode_with_ttl() {
            let output = PttlOutput::decode(b":5000\r\n").unwrap();
            assert!(output.exists());
            assert!(output.has_expire());
            assert_eq!(output.ttl_ms(), Some(5000));
            assert_eq!(output.result(), &PttlResult::Milliseconds(5000));
        }

        #[test]
        fn test_decode_zero_ttl() {
            let output = PttlOutput::decode(b":0\r\n").unwrap();
            assert!(output.exists());
            assert!(output.has_expire());
            assert_eq!(output.ttl_ms(), Some(0));
        }

        #[test]
        fn test_decode_large_ttl() {
            // 30 days in milliseconds
            let output = PttlOutput::decode(b":2592000000\r\n").unwrap();
            assert_eq!(output.ttl_ms(), Some(2592000000));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = PttlOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = PttlInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PttlInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PttlInput { key: RedisKey::String("testkey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::SetexInput;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&PttlInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = PttlOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return -2");
                    assert_eq!(output.result(), &PttlResult::KeyNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_no_expire() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SET without expiration
                    ctx.write(SetInput {
                        key: RedisKey::String("noexpire".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&PttlInput { key: RedisKey::String("noexpire".into()) }.command()).await.expect("raw failed");

                    let output = PttlOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(!output.has_expire());
                    assert_eq!(output.result(), &PttlResult::NoExpire);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_with_expiration() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SETEX with 10 second TTL
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("expiring".into()),
                            seconds: 10.into(),
                            value: RedisJsonValue::String("value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&PttlInput { key: RedisKey::String("expiring".into()) }.command()).await.expect("raw failed");

                    let output = PttlOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.has_expire());

                    // Should be close to 10000ms (within 500ms tolerance)
                    let ttl = output.ttl_ms().expect("should have TTL");
                    assert!(ttl > 9500 && ttl <= 10000, "TTL should be ~10000ms, got {}", ttl);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_precision() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set key with 5 second TTL
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("precision".into()),
                            seconds: 5.into(),
                            value: RedisJsonValue::String("v".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Wait 100ms
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;

                    let result = ctx.raw(&PttlInput { key: RedisKey::String("precision".into()) }.command()).await.expect("raw failed");

                    let output = PttlOutput::decode(&result).expect("decode failed");
                    let ttl = output.ttl_ms().expect("should have TTL");

                    // Should be less than 5000ms but more than 4800ms
                    assert!(ttl < 5000 && ttl > 4800, "PTTL should reflect elapsed time, got {}", ttl);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_after_expiry() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("shortlived".into()),
                            seconds: 1.into(),
                            value: RedisJsonValue::String("v".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Wait for expiry
                    tokio::time::sleep(tokio::time::Duration::from_millis(1500)).await;

                    let result = ctx.raw(&PttlInput { key: RedisKey::String("shortlived".into()) }.command()).await.expect("raw failed");

                    let output = PttlOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "expired key should return -2");
                    assert_eq!(output.result(), &PttlResult::KeyNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup keys
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("p1".into()),
                            seconds: 100.into(),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("p2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    // Pipeline PTTL commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&PttlInput { key: RedisKey::String("p1".into()) }.command());
                    pipeline.extend_from_slice(&PttlInput { key: RedisKey::String("p2".into()) }.command());
                    pipeline.extend_from_slice(&PttlInput { key: RedisKey::String("missing".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = PttlOutput::decode(responses[0]).expect("decode p1");
                    assert!(out1.has_expire());

                    let out2 = PttlOutput::decode(responses[1]).expect("decode p2");
                    assert_eq!(out2.result(), &PttlResult::NoExpire);

                    let out3 = PttlOutput::decode(responses[2]).expect("decode missing");
                    assert_eq!(out3.result(), &PttlResult::KeyNotFound);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // Test -2 response
            let result = ctx.raw(&PttlInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");
            assert_eq!(&result[..], b":-2\r\n", "RESP2 integer format for -2");

            // Test -1 response
            ctx.write(SetInput {
                key: RedisKey::String("noexp".into()),
                value: RedisJsonValue::String("v".into()),
                ..Default::default()
            })
            .await;
            let result = ctx.raw(&PttlInput { key: RedisKey::String("noexp".into()) }.command()).await.expect("raw failed");
            assert_eq!(&result[..], b":-1\r\n", "RESP2 integer format for -1");

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pttl_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&PttlInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");
            assert_eq!(&result[..], b":-2\r\n", "RESP3 integer format for -2");

            ctx.stop().await;
        }
    }
}
