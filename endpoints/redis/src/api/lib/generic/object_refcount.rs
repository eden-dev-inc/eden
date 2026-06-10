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

const API_INFO: ApiInfo<RedisApi, ObjectRefcountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ObjectRefcount,
    "Returns the reference count of a value of a key",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `OBJECT REFCOUNT`
/// https://redis.io/docs/latest/commands/object-refcount/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ObjectRefcountInput {
    pub(crate) key: RedisKey,
}

impl Serialize for ObjectRefcountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ObjectRefcountInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ObjectRefcountInput, API_INFO, { key });

impl RedisCommandInput for ObjectRefcountInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        // OBJECT REFCOUNT is a subcommand: OBJECT REFCOUNT <key>
        let mut command = crate::command::cmd("OBJECT");
        command.arg("REFCOUNT");
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("OBJECT REFCOUNT requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "OBJECT REFCOUNT takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis OBJECT REFCOUNT command
///
/// Returns the reference count of the object stored at the key, or None if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ObjectRefcountOutput {
    /// The reference count, or None if key doesn't exist
    refcount: Option<i64>,
}

impl ObjectRefcountOutput {
    pub fn new(refcount: Option<i64>) -> Self {
        Self { refcount }
    }
}

impl Serialize for ObjectRefcountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ObjectRefcountOutput", 1)?;
        state.serialize_field("refcount", &self.refcount)?;
        state.end()
    }
}

impl ObjectRefcountOutput {
    /// Get the reference count from the output
    pub fn refcount(&self) -> Option<i64> {
        self.refcount
    }

    /// Check if the key exists (refcount is Some)
    pub fn exists(&self) -> bool {
        self.refcount.is_some()
    }

    /// Decode the Redis protocol response into an ObjectRefcountOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let refcount = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Some(n),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT REFCOUNT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Some(data),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT REFCOUNT response: {:?}", other)));
                }
            },
        };

        Ok(Self { refcount })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ObjectRefcountInput { key: RedisKey::String("mykey".into()) };
            // OBJECT REFCOUNT mykey -> *3 array with OBJECT, REFCOUNT, mykey
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nOBJECT\r\n$8\r\nREFCOUNT\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_integer() {
            // RESP2 integer: :1\r\n
            let output = ObjectRefcountOutput::decode(b":1\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.refcount(), Some(1));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = ObjectRefcountOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.refcount(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ObjectRefcountOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.refcount(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ObjectRefcountOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
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
        async fn test_object_refcount_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ObjectRefcountInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = ObjectRefcountOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return null");
                    assert_eq!(output.refcount(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_refcount_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("refkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ObjectRefcountInput { key: RedisKey::String("refkey".into()) }.command()).await.expect("raw failed");

                    let output = ObjectRefcountOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    // Refcount should be at least 1
                    assert!(output.refcount().unwrap() >= 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_refcount_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&ObjectRefcountInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            // RESP2 null bulk string format for nonexistent key
            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = ObjectRefcountOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_refcount_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&ObjectRefcountInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = ObjectRefcountOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
