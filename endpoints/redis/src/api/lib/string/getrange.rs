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

const API_INFO: ApiInfo<RedisApi, GetrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Getrange,
    "Returns the substring of the string value stored at key, determined by the offsets start and end (both are inclusive). Negative offsets can be used in order to provide an offset starting from the end of the string. So -1 means the last character, -2 the penultimate and so forth",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `GETRANGE`
/// https://redis.io/docs/latest/commands/getrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetrangeInput {
    pub(crate) key: RedisKey,
    pub(crate) start: RedisJsonValue,
    pub(crate) end: RedisJsonValue,
}

impl Serialize for GetrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GetrangeInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("end", &self.end)?;
        state.end()
    }
}

impl_redis_operation!(
    GetrangeInput,
    API_INFO,
    {key, start, end}
);

impl RedisCommandInput for GetrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.start).arg(&self.end);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("GETRANGE requires 3 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GETRANGE expects 3 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            start: args[1].clone(),
            end: args[2].clone(),
        })
    }
}

/// Output for Redis GETRANGE command
///
/// Returns the substring of the string value stored at key.
/// Returns empty string if key doesn't exist or range is out of bounds.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetrangeOutput {
    /// The substring value
    value: RedisJsonValue,
}

impl GetrangeOutput {
    pub fn new(value: RedisJsonValue) -> Self {
        Self { value }
    }

    /// Get the substring value
    pub fn value(&self) -> &RedisJsonValue {
        &self.value
    }

    /// Get the substring as a string if possible
    pub fn as_str(&self) -> Option<&str> {
        match &self.value {
            RedisJsonValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        match &self.value {
            RedisJsonValue::String(s) => s.is_empty(),
            RedisJsonValue::Bytes(b) => b.is_empty(),
            _ => false,
        }
    }

    /// Decode the Redis protocol response into a GetrangeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp2Frame::Null => RedisJsonValue::String("".into()),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GETRANGE response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::Null => RedisJsonValue::String("".into()),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected GETRANGE response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for GetrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GetrangeOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = GetrangeInput {
                key: RedisKey::String("mykey".into()),
                start: RedisJsonValue::Integer(0),
                end: RedisJsonValue::Integer(4),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$8\r\nGETRANGE\r\n$5\r\nmykey\r\n$1\r\n0\r\n$1\r\n4\r\n");
        }

        #[test]
        fn test_encode_command_negative_indices() {
            let input = GetrangeInput {
                key: RedisKey::String("key".into()),
                start: RedisJsonValue::Integer(-5),
                end: RedisJsonValue::Integer(-1),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$8\r\nGETRANGE\r\n"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = GetrangeOutput::decode(b"$5\r\nHello\r\n").unwrap();
            assert_eq!(output.as_str(), Some("Hello"));
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_empty_string() {
            let output = GetrangeOutput::decode(b"$0\r\n\r\n").unwrap();
            assert_eq!(output.as_str(), Some(""));
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetrangeOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(10),
            ];
            let input = GetrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.start, RedisJsonValue::Integer(0));
            assert_eq!(input.end, RedisJsonValue::Integer(10));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::Integer(0)];
            let err = GetrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GetrangeInput {
                key: RedisKey::String("mykey".into()),
                start: RedisJsonValue::Integer(0),
                end: RedisJsonValue::Integer(5),
            };
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
        async fn test_getrange_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getrange_key".into()),
                            value: RedisJsonValue::String("Hello World".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &GetrangeInput {
                                key: RedisKey::String("getrange_key".into()),
                                start: RedisJsonValue::Integer(0),
                                end: RedisJsonValue::Integer(4),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("Hello"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_negative_indices() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getrange_neg".into()),
                            value: RedisJsonValue::String("Hello World".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Last 5 characters
                    let result = ctx
                        .raw(
                            &GetrangeInput {
                                key: RedisKey::String("getrange_neg".into()),
                                start: RedisJsonValue::Integer(-5),
                                end: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.as_str(), Some("World"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_missing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GetrangeInput {
                                key: RedisKey::String("getrange_missing".into()),
                                start: RedisJsonValue::Integer(0),
                                end: RedisJsonValue::Integer(10),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_out_of_range() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getrange_range".into()),
                            value: RedisJsonValue::String("Hi".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Request range beyond string length
                    let result = ctx
                        .raw(
                            &GetrangeInput {
                                key: RedisKey::String("getrange_range".into()),
                                start: RedisJsonValue::Integer(0),
                                end: RedisJsonValue::Integer(100),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetrangeOutput::decode(&result).expect("decode failed");
                    // Should return the full string, not error
                    assert_eq!(output.as_str(), Some("Hi"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getrange_pipe".into()),
                            value: RedisJsonValue::String("ABCDEFGHIJ".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &GetrangeInput {
                            key: RedisKey::String("getrange_pipe".into()),
                            start: RedisJsonValue::Integer(0),
                            end: RedisJsonValue::Integer(2),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetrangeInput {
                            key: RedisKey::String("getrange_pipe".into()),
                            start: RedisJsonValue::Integer(3),
                            end: RedisJsonValue::Integer(5),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetrangeInput {
                            key: RedisKey::String("getrange_pipe".into()),
                            start: RedisJsonValue::Integer(-3),
                            end: RedisJsonValue::Integer(-1),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = GetrangeOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.as_str(), Some("ABC"));

                    let out2 = GetrangeOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.as_str(), Some("DEF"));

                    let out3 = GetrangeOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.as_str(), Some("HIJ"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("testing".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &GetrangeInput {
                        key: RedisKey::String("r2key".into()),
                        start: RedisJsonValue::Integer(0),
                        end: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = GetrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_str(), Some("test"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getrange_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r3key".into()),
                    value: RedisJsonValue::String("testing".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &GetrangeInput {
                        key: RedisKey::String("r3key".into()),
                        start: RedisJsonValue::Integer(4),
                        end: RedisJsonValue::Integer(6),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = GetrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.as_str(), Some("ing"));

            ctx.stop().await;
        }
    }
}
