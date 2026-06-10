use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Ttl, key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
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

const API_INFO: ApiInfo<RedisApi, TtlInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Ttl, "Returns the expiration time in seconds of a key", ReqType::Read, true);

/// See official Redis documentation for `TTL`
/// https://redis.io/docs/latest/commands/ttl/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TtlInput {
    pub(crate) key: RedisKey,
}

impl Serialize for TtlInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TtlInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TtlInput, API_INFO, { key });

impl RedisCommandInput for TtlInput {
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
            return Err(EpError::parse("TTL requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "TTL takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis TTL command
///
/// Returns the remaining time to live of a key that has a timeout.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TtlOutput {
    ttl: Ttl,
}

impl Serialize for TtlOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TtlOutput", 1)?;
        state.serialize_field("ttl", &self.ttl)?;
        state.end()
    }
}

impl Serialize for Ttl {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Ttl::KeyDoesNotExist => serializer.serialize_i64(-2),
            Ttl::NoExpiration => serializer.serialize_i64(-1),
            Ttl::Seconds(n) => serializer.serialize_i64(*n),
        }
    }
}

impl TryFrom<i64> for TtlOutput {
    type Error = EpError;

    fn try_from(value: i64) -> Result<Self, Self::Error> {
        TtlOutput::match_ttl(value).map(|v| Self { ttl: v })
    }
}

impl TtlOutput {
    pub fn new(ttl: Ttl) -> Self {
        Self { ttl }
    }

    fn match_ttl(ttl: i64) -> ResultEP<Ttl> {
        Ok(match ttl {
            -2 => Ttl::KeyDoesNotExist,
            -1 => Ttl::NoExpiration,
            n if n >= 0 => Ttl::Seconds(n),
            _ => return Err(EpError::parse(format!("invalid TTL value: {}", ttl))),
        })
    }

    /// Get the TTL value
    pub fn ttl(&self) -> &Ttl {
        &self.ttl
    }

    /// Check if the key exists (value is not KeyDoesNotExist)
    pub fn key_exists(&self) -> bool {
        !matches!(self.ttl, Ttl::KeyDoesNotExist)
    }

    /// Check if the key has an expiration set
    pub fn has_expiration(&self) -> bool {
        matches!(self.ttl, Ttl::Seconds(_))
    }

    /// Get seconds remaining if expiration is set
    pub fn seconds(&self) -> Option<i64> {
        match &self.ttl {
            Ttl::Seconds(n) => Some(*n),
            _ => None,
        }
    }

    /// Decode the Redis protocol response into a TtlOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let ttl_value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected TTL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected TTL response: {:?}", other)));
                }
            },
        };

        let ttl = Self::match_ttl(ttl_value)?;
        Ok(Self { ttl })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = TtlInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nTTL\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_positive_ttl() {
            let output = TtlOutput::decode(b":3600\r\n").unwrap();
            assert_eq!(output.ttl(), &Ttl::Seconds(3600));
            assert!(output.key_exists());
            assert!(output.has_expiration());
            assert_eq!(output.seconds(), Some(3600));
        }

        #[test]
        fn test_decode_no_expiration() {
            let output = TtlOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.ttl(), &Ttl::NoExpiration);
            assert!(output.key_exists());
            assert!(!output.has_expiration());
            assert_eq!(output.seconds(), None);
        }

        #[test]
        fn test_decode_key_not_exists() {
            let output = TtlOutput::decode(b":-2\r\n").unwrap();
            assert_eq!(output.ttl(), &Ttl::KeyDoesNotExist);
            assert!(!output.key_exists());
            assert!(!output.has_expiration());
            assert_eq!(output.seconds(), None);
        }

        #[test]
        fn test_decode_zero_ttl() {
            let output = TtlOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.ttl(), &Ttl::Seconds(0));
            assert!(output.key_exists());
            assert!(output.has_expiration());
            assert_eq!(output.seconds(), Some(0));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = TtlOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_invalid_type_fails() {
            let err = TtlOutput::decode(b"+OK\r\n").unwrap_err();
            assert!(err.to_string().contains("unexpected"));
        }

        #[test]
        fn test_decode_invalid_negative_fails() {
            let err = TtlOutput::decode(b":-3\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid TTL"));
        }

        #[test]
        fn test_serialize_output() {
            let output = TtlOutput::new(Ttl::Seconds(120));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("120"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::generic::expire::ExpireInput;
        use crate::api::{SetInput, SetexInput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TtlInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = TtlOutput::decode(&result).expect("decode failed");
                    assert!(!output.key_exists(), "nonexistent key should return -2");
                    assert_eq!(output.ttl(), &Ttl::KeyDoesNotExist);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_no_expiration() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SET key without expiration
                    ctx.write(SetInput {
                        key: RedisKey::String("persistent".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&TtlInput { key: RedisKey::String("persistent".into()) }.command()).await.expect("raw failed");

                    let output = TtlOutput::decode(&result).expect("decode failed");
                    assert!(output.key_exists());
                    assert!(!output.has_expiration());
                    assert_eq!(output.ttl(), &Ttl::NoExpiration);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_with_expiration() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SET key with EX option (60 seconds)
                    ctx.write(SetexInput {
                        key: RedisKey::String("expiring".into()),
                        seconds: 60.into(),
                        value: RedisJsonValue::String("value".into()),
                    })
                    .await;

                    let result = ctx.raw(&TtlInput { key: RedisKey::String("expiring".into()) }.command()).await.expect("raw failed");

                    let output = TtlOutput::decode(&result).expect("decode failed");
                    assert!(output.key_exists());
                    assert!(output.has_expiration());
                    // TTL should be between 1 and 60 (allowing for test execution time)
                    let seconds = output.seconds().expect("should have seconds");
                    assert!(seconds > 0 && seconds <= 60, "TTL should be 1-60, got {}", seconds);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_after_expire() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // SET key without expiration (using SetInput)
                    ctx.write(SetInput {
                        key: RedisKey::String("willexpire".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // EXPIRE key 120
                    ctx.write(ExpireInput {
                        key: RedisKey::String("willexpire".into()),
                        seconds: 120,
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&TtlInput { key: RedisKey::String("willexpire".into()) }.command()).await.expect("raw failed");

                    let output = TtlOutput::decode(&result).expect("decode failed");
                    assert!(output.key_exists());
                    assert!(output.has_expiration());
                    let seconds = output.seconds().expect("should have seconds");
                    assert!(seconds > 0 && seconds <= 120);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: one key with TTL, one without, one missing
                    ctx.write(SetexInput {
                        key: RedisKey::String("ttl_key".into()),
                        seconds: 300.into(),
                        value: RedisJsonValue::String("v1".into()),
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("no_ttl_key".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    // Pipeline: TTL ttl_key, TTL no_ttl_key, TTL missing_key
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&TtlInput { key: RedisKey::String("ttl_key".into()) }.command());
                    pipeline.extend_from_slice(&TtlInput { key: RedisKey::String("no_ttl_key".into()) }.command());
                    pipeline.extend_from_slice(&TtlInput { key: RedisKey::String("missing_key".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = TtlOutput::decode(responses[0]).expect("decode ttl_key");
                    assert!(out1.has_expiration());

                    let out2 = TtlOutput::decode(responses[1]).expect("decode no_ttl_key");
                    assert_eq!(out2.ttl(), &Ttl::NoExpiration);

                    let out3 = TtlOutput::decode(responses[2]).expect("decode missing_key");
                    assert_eq!(out3.ttl(), &Ttl::KeyDoesNotExist);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&TtlInput { key: RedisKey::String("r2key".into()) }.command()).await.expect("raw failed");

            // RESP2 integer format: :-1\r\n
            assert!(result.starts_with(b":"), "RESP2 should return integer type");
            let output = TtlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.ttl(), &Ttl::NoExpiration);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ttl_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&TtlInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            // RESP3 also uses : for integers
            assert!(result.starts_with(b":"), "RESP3 should return integer type");
            let output = TtlOutput::decode(&result).expect("decode failed");
            assert_eq!(output.ttl(), &Ttl::KeyDoesNotExist);
            ctx.stop().await;
        }
    }
}
