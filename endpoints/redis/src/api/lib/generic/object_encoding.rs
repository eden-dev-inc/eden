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

const API_INFO: ApiInfo<RedisApi, ObjectEncodingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::ObjectEncoding,
    "Returns the internal encoding of a Redis object stored at the specified key",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `OBJECT ENCODING`
/// https://redis.io/docs/latest/commands/object-encoding/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ObjectEncodingInput {
    pub(crate) key: RedisKey,
}

impl Serialize for ObjectEncodingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ObjectEncodingInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ObjectEncodingInput, API_INFO, { key });

impl RedisCommandInput for ObjectEncodingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        // OBJECT ENCODING is a two-word command
        let mut command = crate::command::cmd("OBJECT");
        command.arg("ENCODING");
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("OBJECT ENCODING requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "OBJECT ENCODING takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis OBJECT ENCODING command
///
/// Returns the encoding of the object stored at key, or None if the key does not exist.
/// Possible encodings include: raw, int, embstr, ziplist, linkedlist, intset, hashtable,
/// skiplist, quicklist, listpack, etc.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ObjectEncodingOutput {
    /// The encoding type, or None if key doesn't exist
    encoding: Option<String>,
}

impl ObjectEncodingOutput {
    pub fn new(encoding: Option<String>) -> Self {
        Self { encoding }
    }

    /// Get the encoding from the output
    pub fn encoding(&self) -> Option<&str> {
        self.encoding.as_deref()
    }

    /// Check if the key exists (encoding is Some)
    pub fn exists(&self) -> bool {
        self.encoding.is_some()
    }
}

impl Serialize for ObjectEncodingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ObjectEncodingOutput", 1)?;
        state.serialize_field("encoding", &self.encoding)?;
        state.end()
    }
}

impl ObjectEncodingOutput {
    /// Decode the Redis protocol response into an ObjectEncodingOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let encoding = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(s) => Some(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT ENCODING response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected OBJECT ENCODING response: {:?}", other)));
                }
            },
        };

        Ok(Self { encoding })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ObjectEncodingInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nOBJECT\r\n$8\r\nENCODING\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = ObjectEncodingOutput::decode(b"$6\r\nembstr\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.encoding(), Some("embstr"));
        }

        #[test]
        fn test_decode_int_encoding() {
            let output = ObjectEncodingOutput::decode(b"$3\r\nint\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.encoding(), Some("int"));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = ObjectEncodingOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.encoding(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = ObjectEncodingOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.encoding(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ObjectEncodingOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ObjectEncodingInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = ObjectEncodingOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return null");
                    assert_eq!(output.encoding(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("strkey".into()),
                        value: RedisJsonValue::String("hello world".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ObjectEncodingInput { key: RedisKey::String("strkey".into()) }.command()).await.expect("raw failed");

                    let output = ObjectEncodingOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    // String encoding is typically "embstr" or "raw"
                    let enc = output.encoding().unwrap();
                    assert!(enc == "embstr" || enc == "raw", "unexpected string encoding: {}", enc);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_integer() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("intkey".into()),
                        value: RedisJsonValue::String("12345".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ObjectEncodingInput { key: RedisKey::String("intkey".into()) }.command()).await.expect("raw failed");

                    let output = ObjectEncodingOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.encoding(), Some("int"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&ObjectEncodingInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = ObjectEncodingOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_object_encoding_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&ObjectEncodingInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = ObjectEncodingOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
