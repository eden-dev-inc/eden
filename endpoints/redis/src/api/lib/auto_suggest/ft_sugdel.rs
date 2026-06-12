use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSugdelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtSugdel, "Deletes a string from a suggestion index", ReqType::Write, true);

/// See official Redis documentation for `FT.SUGDEL`
/// https://redis.io/docs/latest/commands/ft.sugdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, PartialEq)]
pub struct FtSugdelInput {
    pub(crate) key: RedisKey,
    pub(crate) string: RedisJsonValue,
}

impl Serialize for FtSugdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSugdelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("string", &self.string)?;
        state.end()
    }
}

impl_redis_operation!(FtSugdelInput, API_INFO, { key, string });

impl RedisCommandInput for FtSugdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.string);
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("FT.SUGDEL requires 2 arguments, given {}", args.len())));
        }

        Ok(FtSugdelInput { key: args[0].clone().try_into()?, string: args[1].clone() })
    }
}

/// Output for Redis FT.SUGDEL command
///
/// Returns 1 if the string was found and deleted, 0 otherwise.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSugdelOutput {
    /// 1 if deleted, 0 if not found
    deleted: i64,
}

impl FtSugdelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Check if the string was deleted
    pub fn was_deleted(&self) -> bool {
        self.deleted == 1
    }

    /// Get the raw result (1 = deleted, 0 = not found)
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into a FtSugdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGDEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGDEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for FtSugdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FtSugdelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
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
            let input = FtSugdelInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$9\r\nFT.SUGDEL\r\n$6\r\nmydict\r\n$5\r\nhello\r\n");
        }

        #[test]
        fn test_encode_command_with_spaces() {
            let input = FtSugdelInput {
                key: RedisKey::String("sug".into()),
                string: RedisJsonValue::String("hello world".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$9\r\nFT.SUGDEL\r\n$3\r\nsug\r\n$11\r\nhello world\r\n");
        }

        #[test]
        fn test_decode_deleted() {
            let output = FtSugdelOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_deleted());
            assert_eq!(output.deleted(), 1);
        }

        #[test]
        fn test_decode_not_found() {
            let output = FtSugdelOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_deleted());
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FtSugdelOutput::decode(b"-ERR unknown key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mydict".into()), RedisJsonValue::String("hello".into())];
            let input = FtSugdelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mydict".into()));
            assert_eq!(input.string, RedisJsonValue::String("hello".into()));
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mydict".into())];
            let err = FtSugdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = FtSugdelInput {
                key: RedisKey::String("testkey".into()),
                string: RedisJsonValue::String("test".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{FtSugaddInput, FtSuglenInput, FtSuglenOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugdel_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSugdelInput {
                                key: RedisKey::String("missing_sug".into()),
                                string: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSugdelOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_deleted());
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugdel_existing() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add a suggestion first
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("del_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Verify it exists
                    let len_result =
                        ctx.raw(&FtSuglenInput { key: RedisKey::String("del_sug".into()) }.command()).await.expect("raw failed");

                    let len_output = FtSuglenOutput::decode(&len_result).expect("decode len failed");
                    assert_eq!(len_output.length(), 1);

                    // Delete it
                    let result = ctx
                        .raw(
                            &FtSugdelInput {
                                key: RedisKey::String("del_sug".into()),
                                string: RedisJsonValue::String("hello".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSugdelOutput::decode(&result).expect("decode failed");
                    assert!(output.was_deleted());

                    // Verify it's gone
                    let len_result =
                        ctx.raw(&FtSuglenInput { key: RedisKey::String("del_sug".into()) }.command()).await.expect("raw failed");

                    let len_output = FtSuglenOutput::decode(&len_result).expect("decode len failed");
                    assert_eq!(len_output.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugdel_pipeline() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add suggestions
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("pipe_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("pipe_sug".into()),
                            string: RedisJsonValue::String("world".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Pipeline: delete both, then check length
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &FtSugdelInput {
                            key: RedisKey::String("pipe_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &FtSugdelInput {
                            key: RedisKey::String("pipe_sug".into()),
                            string: RedisJsonValue::String("world".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&FtSuglenInput { key: RedisKey::String("pipe_sug".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let del1 = FtSugdelOutput::decode(responses[0]).expect("decode del1");
                    assert!(del1.was_deleted());

                    let del2 = FtSugdelOutput::decode(responses[1]).expect("decode del2");
                    assert!(del2.was_deleted());

                    let len = FtSuglenOutput::decode(responses[2]).expect("decode len");
                    assert_eq!(len.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugdel_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp2_del".into()),
                    string: RedisJsonValue::String("test".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &FtSugdelInput {
                        key: RedisKey::String("resp2_del".into()),
                        string: RedisJsonValue::String("test".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = FtSugdelOutput::decode(&result).expect("decode failed");
            assert!(output.was_deleted());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugdel_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp3_del".into()),
                    string: RedisJsonValue::String("test".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &FtSugdelInput {
                        key: RedisKey::String("resp3_del".into()),
                        string: RedisJsonValue::String("test".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FtSugdelOutput::decode(&result).expect("decode failed");
            assert!(output.was_deleted());
            ctx.stop().await;
        }
    }
}
