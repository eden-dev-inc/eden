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

const API_INFO: ApiInfo<RedisApi, ExpiretimeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Expiretime,
    "Returns the absolute Unix timestamp (since January 1, 1970) in seconds at which the given key will expire. Returns -1 if the key exists but has no associated expiration time, and -2 if the key does not exist.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `EXPIRETIME`
/// https://redis.io/docs/latest/commands/expiretime/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ExpiretimeInput {
    pub(crate) key: RedisKey,
}

impl Serialize for ExpiretimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ExpiretimeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ExpiretimeInput, API_INFO, { key });

impl RedisCommandInput for ExpiretimeInput {
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
            return Err(EpError::parse("EXPIRETIME requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "EXPIRETIME takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis EXPIRETIME command
///
/// Returns the absolute Unix timestamp in seconds when the key will expire,
/// -1 if the key has no expiry, or -2 if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ExpiretimeOutput {
    /// Unix timestamp in seconds, -1 (no expiry), or -2 (key missing)
    expiretime: i64,
}

impl ExpiretimeOutput {
    pub fn new(expiretime: i64) -> Self {
        Self { expiretime }
    }

    /// Get the expiration timestamp
    pub fn expiretime(&self) -> i64 {
        self.expiretime
    }

    /// Check if the key exists
    pub fn key_exists(&self) -> bool {
        self.expiretime != -2
    }

    /// Check if the key has an expiration set
    pub fn has_expiry(&self) -> bool {
        self.expiretime >= 0
    }

    /// Decode the Redis protocol response into an ExpiretimeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let expiretime = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIRETIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected EXPIRETIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { expiretime })
    }
}

impl Serialize for ExpiretimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ExpiretimeOutput", 1)?;
        state.serialize_field("expiretime", &self.expiretime)?;
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
            let input = ExpiretimeInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$10\r\nEXPIRETIME\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_positive_timestamp() {
            // :1234567890\r\n (integer response)
            let output = ExpiretimeOutput::decode(b":1234567890\r\n").unwrap();
            assert_eq!(output.expiretime(), 1234567890);
            assert!(output.key_exists());
            assert!(output.has_expiry());
        }

        #[test]
        fn test_decode_no_expiry() {
            // -1 means key exists but has no expiry
            let output = ExpiretimeOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.expiretime(), -1);
            assert!(output.key_exists());
            assert!(!output.has_expiry());
        }

        #[test]
        fn test_decode_key_missing() {
            // -2 means key does not exist
            let output = ExpiretimeOutput::decode(b":-2\r\n").unwrap();
            assert_eq!(output.expiretime(), -2);
            assert!(!output.key_exists());
            assert!(!output.has_expiry());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ExpiretimeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::lib::generic::expire::ExpireInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expiretime_nonexistent_key() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ExpiretimeInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = ExpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.expiretime(), -2);
                    assert!(!output.key_exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expiretime_key_without_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // Set a key without expiration
                    ctx.write(SetInput {
                        key: RedisKey::String("persistent".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ExpiretimeInput { key: RedisKey::String("persistent".into()) }.command()).await.expect("raw failed");

                    let output = ExpiretimeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.expiretime(), -1);
                    assert!(output.key_exists());
                    assert!(!output.has_expiry());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expiretime_key_with_expiry() {
            test_all_protocols_min_version("7.0", |ctx| {
                Box::pin(async move {
                    // Set a key
                    ctx.write(SetInput {
                        key: RedisKey::String("expiring".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Set expiration 1 hour from now
                    ctx.write(ExpireInput {
                        key: RedisKey::String("expiring".into()),
                        seconds: 3600,
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ExpiretimeInput { key: RedisKey::String("expiring".into()) }.command()).await.expect("raw failed");

                    let output = ExpiretimeOutput::decode(&result).expect("decode failed");
                    assert!(output.expiretime() > 0);
                    assert!(output.key_exists());
                    assert!(output.has_expiry());

                    // Verify timestamp is reasonable (within next 2 hours)
                    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64;
                    assert!(output.expiretime() > now);
                    assert!(output.expiretime() < now + 7200);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expiretime_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&ExpiretimeInput { key: RedisKey::String("r2key".into()) }.command()).await.expect("raw failed");

            // RESP2 integer format: :-1\r\n
            assert!(result.starts_with(b":"));
            let output = ExpiretimeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.expiretime(), -1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_expiretime_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&ExpiretimeInput { key: RedisKey::String("r3key".into()) }.command()).await.expect("raw failed");

            // RESP3 also uses : for integers
            assert!(result.starts_with(b":"));
            let output = ExpiretimeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.expiretime(), -1);

            ctx.stop().await;
        }
    }
}
