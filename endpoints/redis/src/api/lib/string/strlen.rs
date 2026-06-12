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

const API_INFO: ApiInfo<RedisApi, StrlenInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Strlen,
    "Returns the length of the string value stored at key. An error is returned when key holds a non-string value",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `STRLEN`
/// https://redis.io/docs/latest/commands/strlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct StrlenInput {
    pub(crate) key: RedisKey,
}

impl Serialize for StrlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("StrlenInput", 2)?;

        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(StrlenInput, API_INFO, { key });

impl RedisCommandInput for StrlenInput {
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
            return Err(EpError::parse("STRLEN requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "STRLEN takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis STRLEN command
///
/// Returns the length of the string stored at key.
/// Returns 0 if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct StrlenOutput {
    /// The length of the string, or 0 if key doesn't exist
    length: i64,
}

impl StrlenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the string length
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the key exists (non-zero length)
    /// Note: This assumes empty strings return 0, so a zero
    /// length could mean either missing key or empty string.
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into a StrlenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected STRLEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected STRLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for StrlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("StrlenOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = StrlenInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nSTRLEN\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_positive_length() {
            let output = StrlenOutput::decode(b":11\r\n").unwrap();
            assert_eq!(output.length(), 11);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_zero_length() {
            let output = StrlenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = StrlenOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = StrlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = StrlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = StrlenInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
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
        async fn test_strlen_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("strlen_key".into()),
                            value: RedisJsonValue::String("Hello World".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&StrlenInput { key: RedisKey::String("strlen_key".into()) }.command()).await.expect("raw failed");

                    let output = StrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 11);
                    assert!(!output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_strlen_missing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&StrlenInput { key: RedisKey::String("strlen_missing".into()) }.command()).await.expect("raw failed");

                    let output = StrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_strlen_empty_string() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("strlen_empty".into()),
                            value: RedisJsonValue::String("".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&StrlenInput { key: RedisKey::String("strlen_empty".into()) }.command()).await.expect("raw failed");

                    let output = StrlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_strlen_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sl1".into()),
                            value: RedisJsonValue::String("short".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("sl2".into()),
                            value: RedisJsonValue::String("this is longer".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&StrlenInput { key: RedisKey::String("sl1".into()) }.command());
                    pipeline.extend_from_slice(&StrlenInput { key: RedisKey::String("sl2".into()) }.command());
                    pipeline.extend_from_slice(&StrlenInput { key: RedisKey::String("sl_missing".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = StrlenOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.length(), 5);

                    let out2 = StrlenOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.length(), 14);

                    let out3 = StrlenOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_strlen_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("test".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&StrlenInput { key: RedisKey::String("r2key".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":4\r\n", "RESP2 integer format");
            let output = StrlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 4);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_strlen_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r3key".into()),
                    value: RedisJsonValue::String("hello".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&StrlenInput { key: RedisKey::String("r3key".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":5\r\n", "RESP3 integer format");
            let output = StrlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 5);

            ctx.stop().await;
        }
    }
}
