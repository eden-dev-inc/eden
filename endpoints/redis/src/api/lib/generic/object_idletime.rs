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

const API_INFO: ApiInfo<RedisApi, ObjectIdletimeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ObjectIdletime,
    "Returns the number of seconds since the object stored at the specified key is idle (not requested by read or write operations). While the value is returned in seconds, the actual resolution is 10 seconds.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `OBJECT IDLETIME`
/// https://redis.io/docs/latest/commands/object-idletime/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ObjectIdletimeInput {
    pub(crate) key: RedisKey,
}

impl Serialize for ObjectIdletimeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ObjectIdletimeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ObjectIdletimeInput, API_INFO, { key });

impl RedisCommandInput for ObjectIdletimeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd("OBJECT");
        command.arg("IDLETIME");
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("OBJECT IDLETIME requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "OBJECT IDLETIME takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis OBJECT IDLETIME command
///
/// Returns the idle time in seconds, or None if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ObjectIdletimeOutput {
    /// The idle time in seconds, or None if key doesn't exist
    idle_time: Option<u64>,
}

impl ObjectIdletimeOutput {
    pub fn new(idle_time: Option<u64>) -> Self {
        Self { idle_time }
    }

    /// Get the idle time in seconds
    pub fn idle_time(&self) -> Option<u64> {
        self.idle_time
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.idle_time.is_some()
    }

    /// Decode the Redis protocol response into an ObjectIdletimeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let idle_time = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Some(n as u64),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT IDLETIME response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Some(data as u64),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT IDLETIME response: {:?}", other)));
                }
            },
        };

        Ok(Self { idle_time })
    }
}

impl Serialize for ObjectIdletimeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ObjectIdletimeOutput", 1)?;
        state.serialize_field("idle_time", &self.idle_time)?;
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
            let input = ObjectIdletimeInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nOBJECT\r\n$8\r\nIDLETIME\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_integer_resp2() {
            // :120\r\n represents integer 120
            let output = ObjectIdletimeOutput::decode(b":120\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.idle_time(), Some(120));
        }

        #[test]
        fn test_decode_zero_idle_time() {
            let output = ObjectIdletimeOutput::decode(b":0\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.idle_time(), Some(0));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = ObjectIdletimeOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.idle_time(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ObjectIdletimeOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.idle_time(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ObjectIdletimeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
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
        async fn test_object_idletime_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ObjectIdletimeInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = ObjectIdletimeOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return null");
                    assert_eq!(output.idle_time(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_idletime_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("idlekey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ObjectIdletimeInput { key: RedisKey::String("idlekey".into()) }.command()).await.expect("raw failed");

                    let output = ObjectIdletimeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    // Immediately after set, idle time should be 0 or very small
                    assert!(output.idle_time().unwrap() < 10);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_idletime_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&ObjectIdletimeInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            // RESP2 returns null bulk string for non-existent keys
            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = ObjectIdletimeOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_idletime_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&ObjectIdletimeInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            // RESP3 returns null type
            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = ObjectIdletimeOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
