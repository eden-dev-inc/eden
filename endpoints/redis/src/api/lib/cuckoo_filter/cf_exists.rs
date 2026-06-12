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

const API_INFO: ApiInfo<RedisApi, CfExistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfExists,
    "Checks whether an item exists in a Cuckoo Filter",
    ReqType::Read,
    true,
);

/// Input for Redis `CF.EXISTS` command.
///
/// Checks whether an item may exist in the Cuckoo Filter.
///
/// See official Redis documentation for `CF.EXISTS`:
/// https://redis.io/docs/latest/commands/cf.exists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfExistsInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The item to check
    item: RedisJsonValue,
}

impl CfExistsInput {
    pub fn new(key: impl Into<RedisKey>, item: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), item: item.into() }
    }
}

impl Serialize for CfExistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfExistsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(CfExistsInput, API_INFO, { key, item });

impl RedisCommandInput for CfExistsInput {
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
            return Err(EpError::request(format!("CF.EXISTS requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis `CF.EXISTS` command.
///
/// Returns 1 if the item may exist in the filter, 0 if it definitely does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfExistsOutput {
    /// 1 if item may exist, 0 if definitely not present
    exists: i64,
}

impl CfExistsOutput {
    pub fn new(exists: i64) -> Self {
        Self { exists }
    }

    /// Returns true if the item may exist in the filter
    pub fn may_exist(&self) -> bool {
        self.exists == 1
    }

    /// Returns true if the item definitely does not exist
    pub fn not_found(&self) -> bool {
        self.exists == 0
    }

    /// Decode the Redis protocol response into a CfExistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let exists = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CF.EXISTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CF.EXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { exists })
    }
}

impl Serialize for CfExistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfExistsOutput", 1)?;
        state.serialize_field("exists", &self.exists)?;
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
            let input = CfExistsInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("myitem".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.EXISTS"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("myitem"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfExistsInput::new("filter1", "item1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfExistsInput::new("testfilter", "testitem");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("myitem".into())];
            let input = CfExistsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfExistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = CfExistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_exists() {
            let output = CfExistsOutput::decode(b":1\r\n").unwrap();
            assert!(output.may_exist());
            assert!(!output.not_found());
        }

        #[test]
        fn test_decode_output_not_found() {
            let output = CfExistsOutput::decode(b":0\r\n").unwrap();
            assert!(!output.may_exist());
            assert!(output.not_found());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfExistsOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfExistsOutput::new(1);
            assert!(output.may_exist());

            let output = CfExistsOutput::new(0);
            assert!(output.not_found());
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
        async fn test_cf_exists_not_found() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // First create a filter
            let add_result = ctx.raw(&CfAddInput::new("cf_exists_test", "known_item").command()).await;

            match add_result {
                Ok(_) => {
                    // Now check for an item that doesn't exist
                    let result = ctx.raw(&CfExistsInput::new("cf_exists_test", "unknown_item").command()).await.expect("raw failed");

                    let output = CfExistsOutput::decode(&result).expect("decode failed");
                    assert!(output.not_found());
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
        async fn test_cf_exists_found() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let add_result = ctx.raw(&CfAddInput::new("cf_exists_test2", "myitem").command()).await;

            match add_result {
                Ok(_) => {
                    let result = ctx.raw(&CfExistsInput::new("cf_exists_test2", "myitem").command()).await.expect("raw failed");

                    let output = CfExistsOutput::decode(&result).expect("decode failed");
                    assert!(output.may_exist());
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
