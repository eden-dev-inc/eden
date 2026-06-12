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

const API_INFO: ApiInfo<RedisApi, BfExistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BfExists,
    "Checks whether an item exists in a Bloom Filter",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `BF.EXISTS`
/// https://redis.io/docs/latest/commands/bf.exists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfExistsInput {
    pub(crate) key: RedisKey,
    pub(crate) item: RedisJsonValue,
}

impl Serialize for BfExistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfExistsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(BfExistsInput, API_INFO, { key, item });

impl RedisCommandInput for BfExistsInput {
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
            return Err(EpError::parse(format!("BF.EXISTS requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.EXISTS expects 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis BF.EXISTS command
///
/// Returns 1 if the item may exist, 0 if the item definitely does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfExistsOutput {
    /// 1 if may exist, 0 if definitely not
    result: i64,
}

impl BfExistsOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Get the result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Check if the item may exist (note: false positives are possible)
    pub fn may_exist(&self) -> bool {
        self.result == 1
    }

    /// Check if the item definitely does not exist
    pub fn definitely_not_exists(&self) -> bool {
        self.result == 0
    }

    /// Decode the Redis protocol response into a BfExistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BF.EXISTS response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected BF.EXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for BfExistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfExistsOutput", 1)?;
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
            let input = BfExistsInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("item1".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$9\r\nBF.EXISTS\r\n"));
        }

        #[test]
        fn test_decode_integer_1() {
            let output = BfExistsOutput::decode(b":1\r\n").unwrap();
            assert!(output.may_exist());
            assert!(!output.definitely_not_exists());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_integer_0() {
            let output = BfExistsOutput::decode(b":0\r\n").unwrap();
            assert!(!output.may_exist());
            assert!(output.definitely_not_exists());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfExistsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::String("item".into())];
            let input = BfExistsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let err = BfExistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfExistsInput {
                key: RedisKey::String("testkey".into()),
                item: RedisJsonValue::String("val".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    // #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_add::BfAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_exists_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfExistsInput {
                                key: RedisKey::String("bf_exists_missing".into()),
                                item: RedisJsonValue::String("missing".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfExistsOutput::decode(&result).expect("decode failed");
                    assert!(output.definitely_not_exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_exists_after_add() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add item first
                    ctx.raw(
                        &BfAddInput {
                            key: RedisKey::String("bf_exists_test".into()),
                            item: RedisJsonValue::String("exists_item".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Check exists
                    let result = ctx
                        .raw(
                            &BfExistsInput {
                                key: RedisKey::String("bf_exists_test".into()),
                                item: RedisJsonValue::String("exists_item".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfExistsOutput::decode(&result).expect("decode failed");
                    assert!(output.may_exist());
                })
            })
            .await;
        }
    }
}
