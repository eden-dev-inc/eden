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

const API_INFO: ApiInfo<RedisApi, GetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Get,
    "Get the value of key. If the key does not exist the special value nil is returned. An error is returned if the value stored at key is not a string, because GET only handles string values",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GET`
/// https://redis.io/docs/latest/commands/get/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetInput {
    pub(crate) key: RedisKey,
}

impl Serialize for GetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(GetInput, API_INFO, { key });

impl RedisCommandInput for GetInput {
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
            return Err(EpError::parse("GET requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "GET takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis GET command
///
/// Returns the value of the key if it exists, or None if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetOutput {
    /// The value stored at the key, or None if key doesn't exist
    value: Option<RedisJsonValue>,
}

impl GetOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }
}

impl Serialize for GetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("GetOutput", 1)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl GetOutput {
    /// Get the value from the output
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if the key exists (value is Some)
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Decode the Redis protocol response into a GetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => {
                    // Try UTF-8 decode, fallback to raw bytes if invalid
                    Some(match String::from_utf8(bytes.clone()) {
                        Ok(s) => RedisJsonValue::from(s),
                        Err(_) => RedisJsonValue::Bytes(bytes),
                    })
                }
                Resp2Frame::SimpleString(s) => Some(match String::from_utf8(s.clone()) {
                    Ok(s) => RedisJsonValue::from(s),
                    Err(_) => RedisJsonValue::Bytes(s),
                }),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => {
                    // Try UTF-8 decode, fallback to raw bytes if invalid
                    Some(match String::from_utf8(data.clone()) {
                        Ok(s) => RedisJsonValue::from(s),
                        Err(_) => RedisJsonValue::Bytes(data),
                    })
                }
                Resp3Frame::SimpleString { data, .. } => Some(match String::from_utf8(data.clone()) {
                    Ok(s) => RedisJsonValue::from(s),
                    Err(_) => RedisJsonValue::Bytes(data),
                }),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected GET response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = GetInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = GetOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("hello")));
        }

        #[test]
        fn test_decode_empty_string() {
            let output = GetOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("")));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = GetOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = GetOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
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
        async fn test_get_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&GetInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

                    let output = GetOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent key should return null");
                    assert_eq!(output.value(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("k".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&GetInput { key: RedisKey::String("k".into()) }.command()).await.expect("raw failed");

                    let output = GetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("v")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_empty_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("empty".into()),
                        value: RedisJsonValue::String("".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&GetInput { key: RedisKey::String("empty".into()) }.command()).await.expect("raw failed");

                    let output = GetOutput::decode(&result).expect("decode failed");
                    assert!(output.exists(), "empty string should exist");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_pipeline_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("p1".into()),
                        value: RedisJsonValue::String("val1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("p2".into()),
                        value: RedisJsonValue::String("val2".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("p1".into()) }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("missing".into()) }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("p2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = GetOutput::decode(responses[0]).expect("decode p1");
                    assert!(out1.exists());
                    assert_eq!(out1.value(), Some(&RedisJsonValue::from("val1")));

                    let out2 = GetOutput::decode(responses[1]).expect("decode missing");
                    assert!(!out2.exists());

                    let out3 = GetOutput::decode(responses[2]).expect("decode p2");
                    assert!(out3.exists());
                    assert_eq!(out3.value(), Some(&RedisJsonValue::from("val2")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_pipeline_set_then_get() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pkey".into()),
                            value: RedisJsonValue::String("pval".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pkey".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    // SET response: +OK\r\n
                    assert!(responses[0].starts_with(b"+OK") || responses[0].starts_with(b"$2\r\nOK"));

                    let get_output = GetOutput::decode(responses[1]).expect("decode GET");
                    assert!(get_output.exists());
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("pval")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&GetInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = GetOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_get_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&GetInput { key: RedisKey::String("missing".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = GetOutput::decode(&result).expect("decode failed");
            assert!(!output.exists());
            ctx.stop().await;
        }
    }
}
