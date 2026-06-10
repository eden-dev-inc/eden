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

const API_INFO: ApiInfo<RedisApi, VcardInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vcard, "Return the number of elements in a vector set", ReqType::Read, true);

/// See official Redis documentation for `VCARD`
/// https://redis.io/docs/latest/commands/vcard/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VcardInput {
    key: RedisKey,
}

impl VcardInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for VcardInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VcardInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(VcardInput, API_INFO, { key });

impl RedisCommandInput for VcardInput {
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
            return Err(EpError::request("VCARD requires 1 argument, given None"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "VCARD expects 1 argument, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis VCARD command
///
/// Returns the number of elements in the vector set, or 0 if the key does not exist.
///
/// See official Redis documentation for `VCARD`
/// https://redis.io/docs/latest/commands/vcard/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VcardOutput {
    /// Number of elements in the vector set
    count: i64,
}

impl VcardOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of elements in the vector set
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if the vector set is empty or doesn't exist
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Decode the Redis protocol response into a VcardOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VCARD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VCARD response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for VcardOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VcardOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = VcardInput { key: RedisKey::String("myvset".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nVCARD\r\n$6\r\nmyvset\r\n");
        }

        #[test]
        fn test_decode_integer() {
            let output = VcardOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.count(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_zero() {
            let output = VcardOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VcardOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = VcardInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = VcardInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VcardInput { key: RedisKey::String("test".into()) };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VCARD requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vcard_nonexistent_key() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VcardInput::new("nonexistent_vset").command()).await.expect("raw failed");

                    let output = VcardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vcard_after_vadd() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // Add elements to vector set
                    ctx.raw(b"*7\r\n$4\r\nVADD\r\n$9\r\ntest_vset\r\n$6\r\nVALUES\r\n$1\r\n2\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$4\r\nelem\r\n")
                        .await
                        .expect("vadd failed");

                    let result = ctx.raw(&VcardInput::new("test_vset").command()).await.expect("raw failed");

                    let output = VcardOutput::decode(&result).expect("decode failed");
                    println!("{output:?}");
                    assert!(output.count() >= 1);
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vcard_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VcardInput::new("empty_vset").command()).await.expect("raw failed");

            // RESP2 returns integer format
            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = VcardOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vcard_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VcardInput::new("empty_vset").command()).await.expect("raw failed");

            let output = VcardOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 0);

            ctx.stop().await;
        }
    }
}
