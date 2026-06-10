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

const API_INFO: ApiInfo<RedisApi, CfAddInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::CfAdd, "Adds an item to a Cuckoo Filter", ReqType::Write, true);

/// Input for Redis `CF.ADD` command.
///
/// Adds an item to a Cuckoo Filter, creating the filter if it does not exist.
///
/// See official Redis documentation for `CF.ADD`:
/// https://redis.io/docs/latest/commands/cf.add/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfAddInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The item to add to the filter
    item: RedisJsonValue,
}

impl CfAddInput {
    pub fn new(key: impl Into<RedisKey>, item: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), item: item.into() }
    }
}

impl Serialize for CfAddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfAddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("item", &self.item)?;
        state.end()
    }
}

impl_redis_operation!(CfAddInput, API_INFO, { key, item });

impl RedisCommandInput for CfAddInput {
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
            return Err(EpError::request(format!("CF.ADD requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, item: args[1].clone() })
    }
}

/// Output for Redis `CF.ADD` command.
///
/// Returns 1 if the item was successfully added to the filter.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfAddOutput {
    /// 1 if the item was added successfully
    added: i64,
}

impl CfAddOutput {
    pub fn new(added: i64) -> Self {
        Self { added }
    }

    /// Returns true if the item was added successfully
    pub fn was_added(&self) -> bool {
        self.added == 1
    }

    /// Decode the Redis protocol response into a CfAddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let added = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected CF.ADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected CF.ADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { added })
    }
}

impl Serialize for CfAddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfAddOutput", 1)?;
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
            let input = CfAddInput {
                key: RedisKey::String("myfilter".into()),
                item: RedisJsonValue::String("myitem".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.ADD"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("myitem"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfAddInput::new("filter1", "item1");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfAddInput::new("testfilter", "testitem");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("myitem".into())];
            let input = CfAddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = CfAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_success() {
            let output = CfAddOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_added());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfAddOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfAddOutput::new(1);
            assert!(output.was_added());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: CF.ADD requires Redis Stack with RedisBloom module
        // These tests will be skipped on standard Redis installations

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_add_basic() {
            // This test requires Redis Stack - skip on standard Redis
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let result = ctx.raw(&CfAddInput::new("cf_test", "item1").command()).await;

            // If module not loaded, skip test gracefully
            match result {
                Ok(bytes) => {
                    let output = CfAddOutput::decode(&bytes).expect("decode failed");
                    assert!(output.was_added());
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("unknown command") || err_str.contains("ERR") {
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
        async fn test_cf_add_pipeline() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let mut pipeline = Vec::new();
            pipeline.extend_from_slice(&CfAddInput::new("cf_pipe", "item1").command());
            pipeline.extend_from_slice(&CfAddInput::new("cf_pipe", "item2").command());

            let result = ctx.raw(&pipeline).await;

            match result {
                Ok(bytes) => {
                    let responses = crate::protocol::RedisProtocol::parse_pipeline_response_zerocopy(&bytes).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);
                    for resp in responses {
                        let output = CfAddOutput::decode(resp).expect("decode");
                        assert!(output.was_added());
                    }
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
