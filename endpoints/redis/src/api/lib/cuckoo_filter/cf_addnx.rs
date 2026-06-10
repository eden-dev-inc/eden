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

const API_INFO: ApiInfo<RedisApi, CfAddnxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfAddnx,
    "Adds an item to a Cuckoo Filter only if the item did not exist previously",
    ReqType::Write,
    true,
);

/// Input for Redis `CF.ADDNX` command.
///
/// Adds an item to a Cuckoo Filter only if it does not already exist.
/// This is the "add if not exists" variant of CF.ADD.
///
/// See official Redis documentation for `CF.ADDNX`:
/// https://redis.io/docs/latest/commands/cf.addnx/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfAddnxInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The item to add if not already present
    item: RedisJsonValue,
}

impl CfAddnxInput {
    pub fn new(key: impl Into<RedisKey>, item: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), item: item.into() }
    }
}

impl Serialize for CfAddnxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfAddnxInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(CfAddnxInput, API_INFO, { key, item });

impl RedisCommandInput for CfAddnxInput {
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
            return Err(EpError::request(format!("CF.ADDNX requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis `CF.ADDNX` command.
///
/// Returns 1 if the item was added, 0 if the item already existed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfAddnxOutput {
    /// 1 if added, 0 if already existed
    added: i64,
}

impl CfAddnxOutput {
    pub fn new(added: i64) -> Self {
        Self { added }
    }

    /// Returns true if the item was newly added
    pub fn was_added(&self) -> bool {
        self.added == 1
    }

    /// Returns true if the item already existed
    pub fn already_existed(&self) -> bool {
        self.added == 0
    }

    /// Decode the Redis protocol response into a CfAddnxOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let added = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CF.ADDNX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CF.ADDNX response: {:?}", other)));
                }
            },
        };

        Ok(Self { added })
    }
}

impl Serialize for CfAddnxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfAddnxOutput", 1)?;
        state.serialize_field("added", &self.added)?;
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
            let input = CfAddnxInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("myitem".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.ADDNX"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("myitem"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfAddnxInput::new("filter1", "item1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfAddnxInput::new("testfilter", "testitem");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("myitem".into())];
            let input = CfAddnxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfAddnxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_added() {
            let output = CfAddnxOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_added());
            assert!(!output.already_existed());
        }

        #[test]
        fn test_decode_output_already_existed() {
            let output = CfAddnxOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_added());
            assert!(output.already_existed());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfAddnxOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfAddnxOutput::new(1);
            assert!(output.was_added());

            let output = CfAddnxOutput::new(0);
            assert!(output.already_existed());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_addnx_new_item() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_addnx_test_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            let result = ctx.raw(&CfAddnxInput::new(&key, "newitem").command()).await;

            match result {
                Ok(bytes) => {
                    let output = CfAddnxOutput::decode(&bytes).expect("decode failed");
                    assert!(output.was_added());
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
        async fn test_cf_addnx_existing_item() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_addnx_dup_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            // First add
            let result = ctx.raw(&CfAddnxInput::new(&key, "duplicate").command()).await;

            match result {
                Ok(bytes) => {
                    let output1 = CfAddnxOutput::decode(&bytes).expect("decode first");
                    assert!(output1.was_added());

                    // Second add of same item
                    let result2 = ctx.raw(&CfAddnxInput::new(&key, "duplicate").command()).await.expect("raw failed");

                    let output2 = CfAddnxOutput::decode(&result2).expect("decode second");
                    assert!(output2.already_existed());
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
