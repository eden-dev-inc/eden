use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{RedisDataType, key::RedisKey, value::RedisJsonValue};
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
use std::str::FromStr;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TypeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Type, "Determines the type of value stored at a key", ReqType::Read, true);

/// See official Redis documentation for `TYPE`
/// https://redis.io/docs/latest/commands/type/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TypeInput {
    pub(crate) key: RedisKey,
}

impl Serialize for TypeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TypeInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TypeInput, API_INFO, { key });

impl RedisCommandInput for TypeInput {
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
            return Err(EpError::parse("TYPE requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "TYPE takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis TYPE command
///
/// Returns the type of value stored at key, or None if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TypeOutput {
    /// The type of the value stored at the key
    key_type: RedisDataType,
}

impl TypeOutput {
    pub fn new(key_type: RedisDataType) -> Self {
        Self { key_type }
    }

    /// Get the type of the key
    pub fn key_type(&self) -> RedisDataType {
        self.key_type
    }

    /// Check if the key exists (type is not None)
    pub fn exists(&self) -> bool {
        self.key_type != RedisDataType::None
    }

    /// Decode the Redis protocol response into a TypeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let type_str = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected TYPE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected TYPE response: {:?}", other)));
                }
            },
        };

        let key_type = RedisDataType::from_str(&type_str)?;
        Ok(Self { key_type })
    }
}

impl Serialize for TypeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TypeOutput", 1)?;
        state.serialize_field("key_type", &self.key_type)?;
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
            let input = TypeInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nTYPE\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_string_type() {
            let output = TypeOutput::decode(b"+string\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::String);
        }

        #[test]
        fn test_decode_list_type() {
            let output = TypeOutput::decode(b"+list\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::List);
        }

        #[test]
        fn test_decode_set_type() {
            let output = TypeOutput::decode(b"+set\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::Set);
        }

        #[test]
        fn test_decode_zset_type() {
            let output = TypeOutput::decode(b"+zset\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::ZSet);
        }

        #[test]
        fn test_decode_hash_type() {
            let output = TypeOutput::decode(b"+hash\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::Hash);
        }

        #[test]
        fn test_decode_stream_type() {
            let output = TypeOutput::decode(b"+stream\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.key_type(), RedisDataType::Stream);
        }

        #[test]
        fn test_decode_none_type() {
            let output = TypeOutput::decode(b"+none\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.key_type(), RedisDataType::None);
        }

        #[test]
        fn test_decode_bulk_string_response() {
            // Some Redis versions may return bulk string
            let output = TypeOutput::decode(b"$6\r\nstring\r\n").unwrap();
            assert_eq!(output.key_type(), RedisDataType::String);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = TypeOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = TypeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TypeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TypeInput { key: RedisKey::String("testkey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TypeOutput::new(RedisDataType::String);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("key_type"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TypeInput { key: RedisKey::String("missing_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return none");
                    assert_eq!(output.key_type(), RedisDataType::None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("string_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("string_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::String);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a list using RPUSH
                    ctx.raw(b"*3\r\n$5\r\nRPUSH\r\n$8\r\nlist_key\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("list_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::List);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a set using SADD
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$7\r\nset_key\r\n$6\r\nmember\r\n").await.expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("set_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::Set);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_zset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a sorted set using ZADD
                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$8\r\nzset_key\r\n$1\r\n1\r\n$6\r\nmember\r\n").await.expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("zset_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::ZSet);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a hash using HSET
                    ctx.raw(b"*4\r\n$4\r\nHSET\r\n$8\r\nhash_key\r\n$5\r\nfield\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("hash_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::Hash);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_stream() {
            // Streams require Redis 5.0+
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create a stream using XADD
                    ctx.raw(b"*5\r\n$4\r\nXADD\r\n$10\r\nstream_key\r\n$1\r\n*\r\n$5\r\nfield\r\n$5\r\nvalue\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("stream_key".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.key_type(), RedisDataType::Stream);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_pipeline_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup different types
                    ctx.write(SetInput {
                        key: RedisKey::String("p_string".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.raw(b"*3\r\n$5\r\nRPUSH\r\n$6\r\np_list\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&TypeInput { key: RedisKey::String("p_string".into()) }.command());
                    pipeline.extend_from_slice(&TypeInput { key: RedisKey::String("p_list".into()) }.command());
                    pipeline.extend_from_slice(&TypeInput { key: RedisKey::String("p_missing".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = TypeOutput::decode(responses[0]).expect("decode p_string");
                    assert_eq!(out1.key_type(), RedisDataType::String);

                    let out2 = TypeOutput::decode(responses[1]).expect("decode p_list");
                    assert_eq!(out2.key_type(), RedisDataType::List);

                    let out3 = TypeOutput::decode(responses[2]).expect("decode p_missing");
                    assert_eq!(out3.key_type(), RedisDataType::None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*3\r\n$3\r\nSET\r\n$9\r\nresp2_key\r\n$3\r\nval\r\n").await.expect("raw failed");

            let result = ctx.raw(&TypeInput { key: RedisKey::String("resp2_key".into()) }.command()).await.expect("raw failed");

            // TYPE returns simple string in both RESP2 and RESP3
            assert_eq!(&result[..], b"+string\r\n", "RESP2 simple string format");
            let output = TypeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.key_type(), RedisDataType::String);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*3\r\n$3\r\nSET\r\n$9\r\nresp3_key\r\n$3\r\nval\r\n").await.expect("raw failed");

            let result = ctx.raw(&TypeInput { key: RedisKey::String("resp3_key".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+string\r\n", "RESP3 simple string format");
            let output = TypeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.key_type(), RedisDataType::String);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_type_after_key_deleted() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create and then delete a key
                    ctx.write(SetInput {
                        key: RedisKey::String("delete_me".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // Delete the key
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\ndelete_me\r\n").await.expect("raw failed");

                    let result = ctx.raw(&TypeInput { key: RedisKey::String("delete_me".into()) }.command()).await.expect("raw failed");

                    let output = TypeOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                    assert_eq!(output.key_type(), RedisDataType::None);
                })
            })
            .await;
        }
    }
}
