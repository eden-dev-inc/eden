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

const API_INFO: ApiInfo<RedisApi, BfCardInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::BfCard, "Returns the cardinality of a Bloom filter", ReqType::Read, true);

/// See official Redis documentation for `BF.CARD`
/// https://redis.io/docs/latest/commands/bf.card/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfCardInput {
    pub(crate) key: RedisKey,
}

impl Serialize for BfCardInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfCardInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(BfCardInput, API_INFO, { key });

impl RedisCommandInput for BfCardInput {
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
            return Err(EpError::parse("BF.CARD requires 1 argument, given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.CARD expects 1 argument, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis BF.CARD command
///
/// Returns the cardinality (number of items added) of the Bloom filter,
/// or 0 if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfCardOutput {
    /// Number of items in the filter
    cardinality: i64,
}

impl BfCardOutput {
    pub fn new(cardinality: i64) -> Self {
        Self { cardinality }
    }

    /// Get the cardinality value
    pub fn cardinality(&self) -> i64 {
        self.cardinality
    }

    /// Check if the filter is empty or doesn't exist
    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Decode the Redis protocol response into a BfCardOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let cardinality = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BF.CARD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BF.CARD response: {:?}", other)));
                }
            },
        };

        Ok(Self { cardinality })
    }
}

impl Serialize for BfCardOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfCardOutput", 1)?;
        state.serialize_field("cardinality", &self.cardinality)?;
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
            let input = BfCardInput { key: RedisKey::String("myfilter".into()) };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n$7\r\nBF.CARD\r\n"));
        }

        #[test]
        fn test_decode_integer() {
            let output = BfCardOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.cardinality(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_zero() {
            let output = BfCardOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.cardinality(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfCardOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("filter".into())];
            let input = BfCardInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = BfCardInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfCardInput { key: RedisKey::String("testkey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_add::BfAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_card_nonexistent() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&BfCardInput { key: RedisKey::String("bf_card_missing".into()) }.command()).await.expect("raw failed");

                    let output = BfCardOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_card_after_adds() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add items
                    for i in 0..5 {
                        ctx.raw(
                            &BfAddInput {
                                key: RedisKey::String("bf_card_test".into()),
                                item: RedisJsonValue::String(format!("item{}", i)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    }

                    let result =
                        ctx.raw(&BfCardInput { key: RedisKey::String("bf_card_test".into()) }.command()).await.expect("raw failed");

                    let output = BfCardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 5);
                })
            })
            .await;
        }
    }
}
