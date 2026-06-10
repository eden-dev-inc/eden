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

const API_INFO: ApiInfo<RedisApi, PexpiretimeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pexpiretime,
    "Returns the expiration time of a key as a Unix milliseconds timestamp. Returns -2 if the key does not exist, -1 if the key exists but has no associated expiration time.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PEXPIRETIME`
/// https://redis.io/docs/latest/commands/pexpiretime/
///
/// Note: This command is available since Redis 7.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PexpiretimeInput {
    pub(crate) key: RedisKey,
}

impl Serialize for PexpiretimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PexpiretimeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(PexpiretimeInput, API_INFO, { key });

impl RedisCommandInput for PexpiretimeInput {
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
            return Err(EpError::parse("PEXPIRETIME requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "PEXPIRETIME takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis PEXPIRETIME command
///
/// Returns the absolute Unix timestamp in milliseconds at which the key will expire.
/// - Returns -2 if the key does not exist
/// - Returns -1 if the key exists but has no associated expiration time
/// - Returns positive milliseconds timestamp otherwise
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PexpiretimeOutput {
    /// The expiration timestamp in milliseconds, or -2/-1 for special cases
    timestamp: i64,
}

impl PexpiretimeOutput {
    pub fn new(timestamp: i64) -> Self {
        Self { timestamp }
    }

    /// Get the raw timestamp value
    pub fn timestamp(&self) -> i64 {
        self.timestamp
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.timestamp != -2
    }

    /// Check if the key has an expiration time set
    pub fn has_expiry(&self) -> bool {
        self.timestamp > 0
    }

    /// Get the expiration time as an Option
    /// Returns None if key doesn't exist or has no expiry
    pub fn expiry_timestamp(&self) -> Option<i64> {
        if self.timestamp > 0 { Some(self.timestamp) } else { None }
    }

    /// Decode the Redis protocol response into a PexpiretimeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let timestamp = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIRETIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PEXPIRETIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { timestamp })
    }
}

impl Serialize for PexpiretimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PexpiretimeOutput", 1)?;
        state.serialize_field("timestamp", &self.timestamp)?;
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
            let input = PexpiretimeInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$11\r\nPEXPIRETIME\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_key_not_exists() {
            // RESP2 integer: :-2\r\n
            let output = PexpiretimeOutput::decode(b":-2\r\n").unwrap();
            assert_eq!(output.timestamp(), -2);
            assert!(!output.exists());
            assert!(!output.has_expiry());
            assert_eq!(output.expiry_timestamp(), None);
        }

        #[test]
        fn test_decode_no_expiry() {
            // RESP2 integer: :-1\r\n
            let output = PexpiretimeOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.timestamp(), -1);
            assert!(output.exists());
            assert!(!output.has_expiry());
            assert_eq!(output.expiry_timestamp(), None);
        }

        #[test]
        fn test_decode_with_expiry() {
            // RESP2 integer: :1234567890123\r\n
            let output = PexpiretimeOutput::decode(b":1234567890123\r\n").unwrap();
            assert_eq!(output.timestamp(), 1234567890123);
            assert!(output.exists());
            assert!(output.has_expiry());
            assert_eq!(output.expiry_timestamp(), Some(1234567890123));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = PexpiretimeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = PexpiretimeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PexpiretimeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PexpiretimeInput { key: RedisKey::String("testkey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = PexpiretimeOutput::new(1000);
            assert_eq!(output.timestamp(), 1000);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{SetInput, SetexInput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_nonexistent_key() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&PexpiretimeInput { key: RedisKey::String("nonexistent_pexpiretime".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = PexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.timestamp(), -2);
                    assert!(!output.exists());
                    assert!(!output.has_expiry());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_key_without_expiry() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Set a key without expiry
                    ctx.write(SetInput {
                        key: RedisKey::String("no_expiry_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&PexpiretimeInput { key: RedisKey::String("no_expiry_key".into()) }.command()).await.expect("raw failed");

                    let output = PexpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.timestamp(), -1);
                    assert!(output.exists());
                    assert!(!output.has_expiry());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_key_with_expiry() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Set a key with expiry using SETEX
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("expiry_key".into()),
                            seconds: 3600.into(), // 1 hour
                            value: RedisJsonValue::String("value".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&PexpiretimeInput { key: RedisKey::String("expiry_key".into()) }.command()).await.expect("raw failed");

                    let output = PexpiretimeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert!(output.has_expiry());

                    // Timestamp should be roughly now + 3600 seconds in milliseconds
                    let now_ms = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64;

                    let expected_min = now_ms + (3599 * 1000); // Allow 1 second tolerance
                    let expected_max = now_ms + (3601 * 1000);

                    assert!(
                        output.timestamp() >= expected_min && output.timestamp() <= expected_max,
                        "Expected timestamp between {} and {}, got {}",
                        expected_min,
                        expected_max,
                        output.timestamp()
                    );
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_pipeline() {
            test_all_protocols_min_version("7", |ctx| {
                Box::pin(async move {
                    // Setup: one key with expiry, one without
                    ctx.raw(
                        &SetexInput {
                            key: RedisKey::String("pipe_exp".into()),
                            seconds: 100.into(),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_noexp".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    // Pipeline: check both keys and a nonexistent one
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&PexpiretimeInput { key: RedisKey::String("pipe_exp".into()) }.command());
                    pipeline.extend_from_slice(&PexpiretimeInput { key: RedisKey::String("pipe_noexp".into()) }.command());
                    pipeline.extend_from_slice(&PexpiretimeInput { key: RedisKey::String("pipe_missing".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = PexpiretimeOutput::decode(responses[0]).expect("decode pipe_exp");
                    assert!(out1.exists());
                    assert!(out1.has_expiry());

                    let out2 = PexpiretimeOutput::decode(responses[1]).expect("decode pipe_noexp");
                    assert!(out2.exists());
                    assert!(!out2.has_expiry());
                    assert_eq!(out2.timestamp(), -1);

                    let out3 = PexpiretimeOutput::decode(responses[2]).expect("decode pipe_missing");
                    assert!(!out3.exists());
                    assert_eq!(out3.timestamp(), -2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_resp2_format() {
            // Skip if Redis version < 7
            for version in REDIS_VERSIONS {
                if version_is_earlier("7", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;

                // Set key without expiry
                ctx.write(SetInput {
                    key: RedisKey::String("resp2_pexp".into()),
                    value: RedisJsonValue::String("val".into()),
                    ..Default::default()
                })
                .await;

                let result = ctx.raw(&PexpiretimeInput { key: RedisKey::String("resp2_pexp".into()) }.command()).await.expect("raw failed");

                // RESP2 integer format: :-1\r\n
                assert_eq!(&result[..], b":-1\r\n", "RESP2 integer format for no expiry");
                let output = PexpiretimeOutput::decode(&result).expect("decode failed");
                assert_eq!(output.timestamp(), -1);

                ctx.stop().await;
                break; // Only need one version
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pexpiretime_resp3_format() {
            // Skip if Redis version < 7
            for version in REDIS_VERSIONS {
                if version_is_earlier("7", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;

                // Set key without expiry
                ctx.write(SetInput {
                    key: RedisKey::String("resp3_pexp".into()),
                    value: RedisJsonValue::String("val".into()),
                    ..Default::default()
                })
                .await;

                let result = ctx.raw(&PexpiretimeInput { key: RedisKey::String("resp3_pexp".into()) }.command()).await.expect("raw failed");

                // RESP3 also uses : for integers
                assert_eq!(&result[..], b":-1\r\n", "RESP3 integer format for no expiry");
                let output = PexpiretimeOutput::decode(&result).expect("decode failed");
                assert_eq!(output.timestamp(), -1);

                ctx.stop().await;
                break; // Only need one version
            }
        }
    }
}
