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

const API_INFO: ApiInfo<RedisApi, CfDelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::CfDel, "Deletes an item from a Cuckoo Filter", ReqType::Write, true);

/// Input for Redis `CF.DEL` command.
///
/// Deletes an item once from the filter. If the item exists multiple times,
/// only one instance is deleted.
///
/// See official Redis documentation for `CF.DEL`:
/// https://redis.io/docs/latest/commands/cf.del/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfDelInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The item to delete from the filter
    item: RedisJsonValue,
}

impl CfDelInput {
    pub fn new(key: impl Into<RedisKey>, item: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), item: item.into() }
    }
}

impl Serialize for CfDelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfDelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(CfDelInput, API_INFO, { key, item });

impl RedisCommandInput for CfDelInput {
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
            return Err(EpError::request(format!("CF.DEL requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis `CF.DEL` command.
///
/// Returns 1 if the item was deleted, 0 if the item was not found.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfDelOutput {
    /// 1 if deleted, 0 if not found
    deleted: i64,
}

impl CfDelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Returns true if the item was deleted
    pub fn was_deleted(&self) -> bool {
        self.deleted == 1
    }

    /// Returns true if the item was not found
    pub fn not_found(&self) -> bool {
        self.deleted == 0
    }

    /// Decode the Redis protocol response into a CfDelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CF.DEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CF.DEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for CfDelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfDelOutput", 1)?;
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
            let input = CfDelInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("myitem".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.DEL"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("myitem"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfDelInput::new("filter1", "item1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfDelInput::new("testfilter", "testitem");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("myitem".into())];
            let input = CfDelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_deleted() {
            let output = CfDelOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_deleted());
            assert!(!output.not_found());
        }

        #[test]
        fn test_decode_output_not_found() {
            let output = CfDelOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_deleted());
            assert!(output.not_found());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfDelOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfDelOutput::new(1);
            assert!(output.was_deleted());

            let output = CfDelOutput::new(0);
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
        async fn test_cf_del_existing_item() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // First add an item
            let add_result = ctx.raw(&CfAddInput::new("cf_del_test", "item_to_delete").command()).await;

            match add_result {
                Ok(_) => {
                    // Now delete it
                    let result = ctx.raw(&CfDelInput::new("cf_del_test", "item_to_delete").command()).await.expect("raw failed");

                    let output = CfDelOutput::decode(&result).expect("decode failed");
                    assert!(output.was_deleted());
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
        async fn test_cf_del_missing_item() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // Create filter with different item
            let add_result = ctx.raw(&CfAddInput::new("cf_del_test2", "other_item").command()).await;

            match add_result {
                Ok(_) => {
                    // Try to delete non-existent item
                    let result = ctx.raw(&CfDelInput::new("cf_del_test2", "missing_item").command()).await.expect("raw failed");

                    let output = CfDelOutput::decode(&result).expect("decode failed");
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
    }
}
