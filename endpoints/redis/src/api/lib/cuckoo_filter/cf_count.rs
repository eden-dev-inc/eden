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

const API_INFO: ApiInfo<RedisApi, CfCountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfCount,
    "Returns the number of times an item may be in a Cuckoo Filter",
    ReqType::Read,
    true,
);

/// Input for Redis `CF.COUNT` command.
///
/// Returns an estimate of the number of times an item may be in the filter.
/// Because this is a probabilistic data structure, the count may not be exact.
///
/// See official Redis documentation for `CF.COUNT`:
/// https://redis.io/docs/latest/commands/cf.count/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfCountInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The item to count
    item: RedisJsonValue,
}

impl CfCountInput {
    pub fn new(key: impl Into<RedisKey>, item: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), item: item.into() }
    }
}

impl Serialize for CfCountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfCountInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(CfCountInput, API_INFO, { key, item });

impl RedisCommandInput for CfCountInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("CF.COUNT requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis `CF.COUNT` command.
///
/// Returns an estimate of the number of times an item is in the filter.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfCountOutput {
    /// Estimated count of the item in the filter
    count: i64,
}

impl CfCountOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the estimated count
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Returns true if the item is not in the filter
    pub fn is_zero(&self) -> bool {
        self.count == 0
    }

    /// Decode the Redis protocol response into a CfCountOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CF.COUNT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CF.COUNT response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for CfCountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfCountOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = CfCountInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("myitem".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.COUNT"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("myitem"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfCountInput::new("filter1", "item1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfCountInput::new("testfilter", "testitem");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("myitem".into())];
            let input = CfCountInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfCountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = CfCountOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.is_zero());
        }

        #[test]
        fn test_decode_output_one() {
            let output = CfCountOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.count(), 1);
            assert!(!output.is_zero());
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = CfCountOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfCountOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfCountOutput::new(3);
            assert_eq!(output.count(), 3);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::CfAddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_count_not_in_filter() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // Create filter
            let add_result = ctx.raw(&CfAddInput::new("cf_count_test", "other_item").command()).await;

            match add_result {
                Ok(_) => {
                    let result = ctx.raw(&CfCountInput::new("cf_count_test", "missing_item").command()).await.expect("raw failed");

                    let output = CfCountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.is_zero());
                }
                Err(e) => {
                    if e.to_string().contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    } else {
                        panic!("Unexpected error: {}", e);
                    }
                }
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_count_in_filter() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let add_result = ctx.raw(&CfAddInput::new("cf_count_test2", "counted_item").command()).await;

            match add_result {
                Ok(_) => {
                    let result = ctx.raw(&CfCountInput::new("cf_count_test2", "counted_item").command()).await.expect("raw failed");

                    let output = CfCountOutput::decode(&result).expect("decode failed");
                    assert!(output.count() >= 1);
                }
                Err(e) => {
                    if e.to_string().contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    } else {
                        panic!("Unexpected error: {}", e);
                    }
                }
            }

            ctx.stop().await;
        }
    }
}
