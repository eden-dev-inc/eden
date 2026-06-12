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

const API_INFO: ApiInfo<RedisApi, BfAddInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::BfAdd, "Adds an item to a Bloom Filter", ReqType::Write, true);

/// See official Redis documentation for `BF.ADD`
/// https://redis.io/docs/latest/commands/bf.add/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfAddInput {
    pub(crate) key: RedisKey,
    pub(crate) item: RedisJsonValue,
}

impl Serialize for BfAddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfAddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(BfAddInput, API_INFO, { key, item });

impl RedisCommandInput for BfAddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.item);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("BF.ADD requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.ADD expects 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis BF.ADD command
///
/// Returns 1 if the item was newly added, 0 if it may have existed previously.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfAddOutput {
    /// 1 if newly added, 0 if possibly existed
    result: i64,
}

impl BfAddOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Get the result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Check if the item was newly added
    pub fn was_added(&self) -> bool {
        self.result == 1
    }

    /// Decode the Redis protocol response into a BfAddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BF.ADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Boolean { data, .. } => data as i64,
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BF.ADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for BfAddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfAddOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = BfAddInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("item1".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$6\r\nBF.ADD\r\n"));
        }

        #[test]
        fn test_decode_integer_1() {
            let output = BfAddOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_added());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_integer_0() {
            let output = BfAddOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_added());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfAddOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::String("item".into())];
            let input = BfAddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let err = BfAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfAddInput {
                key: RedisKey::String("testkey".into()),
                item: RedisJsonValue::String("val".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_add_new_item() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfAddInput {
                                key: RedisKey::String("bf_add_test".into()),
                                item: RedisJsonValue::String("item1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfAddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added(), "new item should return 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_add_existing_item() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add item first time
                    ctx.raw(
                        &BfAddInput {
                            key: RedisKey::String("bf_add_dup".into()),
                            item: RedisJsonValue::String("dup_item".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add same item again
                    let result = ctx
                        .raw(
                            &BfAddInput {
                                key: RedisKey::String("bf_add_dup".into()),
                                item: RedisJsonValue::String("dup_item".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfAddOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_added(), "existing item should return 0");
                })
            })
            .await;
        }
    }
}
