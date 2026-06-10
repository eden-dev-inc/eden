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

const API_INFO: ApiInfo<RedisApi, CfMexistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfMexists,
    "Checks whether one or more items exist in a Cuckoo Filter",
    ReqType::Read,
    true,
);

/// Input for Redis `CF.MEXISTS` command.
///
/// Checks whether one or more items may exist in the Cuckoo Filter.
/// This is the multi-item version of CF.EXISTS.
///
/// See official Redis documentation for `CF.MEXISTS`:
/// https://redis.io/docs/latest/commands/cf.mexists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfMexistsInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// The items to check
    items: Vec<RedisJsonValue>,
}

impl CfMexistsInput {
    pub fn new(key: impl Into<RedisKey>, items: Vec<impl Into<RedisJsonValue>>) -> Self {
        Self {
            key: key.into(),
            items: items.into_iter().map(|i| i.into()).collect(),
        }
    }

    /// Add an item to check
    pub fn with_item(mut self, item: impl Into<RedisJsonValue>) -> Self {
        self.items.push(item.into());
        self
    }
}

impl Serialize for CfMexistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfMexistsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(CfMexistsInput, API_INFO, { key, items });

impl RedisCommandInput for CfMexistsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.items);
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("CF.MEXISTS requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let items = args[1..].to_vec();

        Ok(Self { key, items })
    }
}

/// Output for Redis `CF.MEXISTS` command.
///
/// Returns an array of integers, where 1 means the item may exist
/// and 0 means it definitely does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfMexistsOutput {
    /// Results for each item: 1 if may exist, 0 if definitely not
    results: Vec<i64>,
}

impl CfMexistsOutput {
    pub fn new(results: Vec<i64>) -> Self {
        Self { results }
    }

    /// Get the results array
    pub fn results(&self) -> &[i64] {
        &self.results
    }

    /// Check if a specific item at index may exist
    pub fn may_exist(&self, index: usize) -> Option<bool> {
        self.results.get(index).map(|&r| r == 1)
    }

    /// Count how many items may exist
    pub fn count_existing(&self) -> usize {
        self.results.iter().filter(|&&r| r == 1).count()
    }

    /// Check if all items may exist
    pub fn all_may_exist(&self) -> bool {
        self.results.iter().all(|&r| r == 1)
    }

    /// Check if no items exist
    pub fn none_exist(&self) -> bool {
        self.results.iter().all(|&r| r == 0)
    }

    /// Decode the Redis protocol response into a CfMexistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut results = Vec::with_capacity(arr.len());
                for item in arr {
                    match item {
                        Resp2Frame::Integer(n) => results.push(n),
                        _ => return Err(EpError::parse("expected integer in array")),
                    }
                }
                Ok(Self { results })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected CF.MEXISTS response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data: arr, .. } => {
                let mut results = Vec::with_capacity(arr.len());
                for item in arr {
                    match item {
                        Resp3Frame::Number { data, .. } => results.push(data),
                        _ => return Err(EpError::parse("expected integer in array")),
                    }
                }
                Ok(Self { results })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected CF.MEXISTS response: {:?}", other))),
        }
    }
}

impl Serialize for CfMexistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfMexistsOutput", 1)?;
        state.serialize_field("results", &self.results)?;
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
            let input = CfMexistsInput {
                key: RedisKey::String("myfilter".into()),
                items: vec![RedisJsonValue::String("item1".into()), RedisJsonValue::String("item2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.MEXISTS"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("item1"));
            assert!(cmd_str.contains("item2"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfMexistsInput::new("filter1", vec!["item1", "item2"]);
            assert_eq!(input.key, RedisKey::String("filter1".into()));
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_with_item() {
            let input = CfMexistsInput::new("filter1", vec!["item1"]).with_item("item2").with_item("item3");
            assert_eq!(input.items.len(), 3);
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfMexistsInput::new("testfilter", vec!["a"]);
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::String("item1".into()),
                RedisJsonValue::String("item2".into()),
            ];
            let input = CfMexistsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into())];
            let err = CfMexistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_all_exist() {
            let output = CfMexistsOutput::decode(b"*3\r\n:1\r\n:1\r\n:1\r\n").unwrap();
            assert_eq!(output.results().len(), 3);
            assert!(output.all_may_exist());
            assert_eq!(output.count_existing(), 3);
        }

        #[test]
        fn test_decode_output_none_exist() {
            let output = CfMexistsOutput::decode(b"*3\r\n:0\r\n:0\r\n:0\r\n").unwrap();
            assert!(output.none_exist());
            assert_eq!(output.count_existing(), 0);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = CfMexistsOutput::decode(b"*3\r\n:1\r\n:0\r\n:1\r\n").unwrap();
            assert!(!output.all_may_exist());
            assert!(!output.none_exist());
            assert_eq!(output.count_existing(), 2);
            assert_eq!(output.may_exist(0), Some(true));
            assert_eq!(output.may_exist(1), Some(false));
            assert_eq!(output.may_exist(2), Some(true));
            assert_eq!(output.may_exist(3), None);
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfMexistsOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfMexistsOutput::new(vec![1, 0, 1]);
            assert_eq!(output.results(), &[1, 0, 1]);
            assert_eq!(output.count_existing(), 2);
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
        async fn test_cf_mexists_mixed() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // Add some items
            let add_result = ctx.raw(&CfAddInput::new("cf_mexists_test", "known1").command()).await;

            match add_result {
                Ok(_) => {
                    ctx.raw(&CfAddInput::new("cf_mexists_test", "known2").command()).await.expect("add known2");

                    // Check mix of known and unknown items
                    let result = ctx
                        .raw(&CfMexistsInput::new("cf_mexists_test", vec!["known1", "unknown", "known2"]).command())
                        .await
                        .expect("raw failed");

                    let output = CfMexistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 3);
                    assert_eq!(output.may_exist(0), Some(true)); // known1
                    assert_eq!(output.may_exist(1), Some(false)); // unknown
                    assert_eq!(output.may_exist(2), Some(true)); // known2
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
        async fn test_cf_mexists_all_unknown() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // Create filter with different items
            let add_result = ctx.raw(&CfAddInput::new("cf_mexists_test2", "other").command()).await;

            match add_result {
                Ok(_) => {
                    let result =
                        ctx.raw(&CfMexistsInput::new("cf_mexists_test2", vec!["a", "b", "c"]).command()).await.expect("raw failed");

                    let output = CfMexistsOutput::decode(&result).expect("decode failed");
                    assert!(output.none_exist());
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
