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

const API_INFO: ApiInfo<RedisApi, CfInsertInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfInsert,
    "Adds one or more items to a Cuckoo Filter, creating the filter if it does not exist",
    ReqType::Write,
    true,
);

/// Input for Redis `CF.INSERT` command.
///
/// Adds one or more items to a Cuckoo Filter. The filter will be created
/// if it does not exist (unless NOCREATE is specified).
///
/// See official Redis documentation for `CF.INSERT`:
/// https://redis.io/docs/latest/commands/cf.insert/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfInsertInput {
    /// The name of the Cuckoo Filter
    key: RedisKey,
    /// Optional capacity for auto-created filter
    capacity: Option<RedisJsonValue>,
    /// If true, don't create filter if it doesn't exist
    no_create: Option<bool>,
    /// The items to add
    items: Vec<RedisJsonValue>,
}

impl CfInsertInput {
    pub fn new(key: impl Into<RedisKey>, items: Vec<impl Into<RedisJsonValue>>) -> Self {
        Self {
            key: key.into(),
            capacity: None,
            no_create: None,
            items: items.into_iter().map(|i| i.into()).collect(),
        }
    }

    /// Set the capacity for auto-created filter
    pub fn with_capacity(mut self, capacity: impl Into<RedisJsonValue>) -> Self {
        self.capacity = Some(capacity.into());
        self
    }

    /// Set NOCREATE flag to prevent auto-creation
    pub fn with_nocreate(mut self) -> Self {
        self.no_create = Some(true);
        self
    }

    /// Add an item to insert
    pub fn with_item(mut self, item: impl Into<RedisJsonValue>) -> Self {
        self.items.push(item.into());
        self
    }
}

impl Serialize for CfInsertInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, items
        if self.capacity.is_some() {
            fields += 1;
        }
        if self.no_create.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CfInsertInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        if let Some(capacity) = &self.capacity {
            state.serialize_field("capacity", capacity)?;
        }
        if let Some(no_create) = &self.no_create {
            state.serialize_field("no_create", no_create)?;
        }
        state.end()
    }
}

impl_redis_operation!(CfInsertInput, API_INFO, { key, capacity, no_create, items });

impl RedisCommandInput for CfInsertInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(capacity) = &self.capacity {
            command.arg("CAPACITY").arg(capacity);
        }

        if let Some(true) = &self.no_create {
            command.arg("NOCREATE");
        }

        command.arg("ITEMS").arg(&self.items);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("CF.INSERT requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let mut capacity = None;
        let mut no_create = None;
        let mut items_start = 1;

        let mut i = 1;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "CAPACITY" if i + 1 < args.len() => {
                        capacity = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "NOCREATE" => {
                        no_create = Some(true);
                        i += 1;
                    }
                    "ITEMS" => {
                        items_start = i + 1;
                        break;
                    }
                    _ => {
                        items_start = i;
                        break;
                    }
                }
            } else {
                items_start = i;
                break;
            }
        }

        if items_start >= args.len() {
            return Err(EpError::request("CF.INSERT requires at least one item"));
        }

        let items = args[items_start..].to_vec();

        Ok(Self { key, capacity, no_create, items })
    }
}

/// Output for Redis `CF.INSERT` command.
///
/// Returns an array of integers, where 1 means the item was added
/// and 0 means the item may already exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfInsertOutput {
    /// Results for each item: 1 if added, 0 if may already exist
    results: Vec<i64>,
}

impl CfInsertOutput {
    pub fn new(results: Vec<i64>) -> Self {
        Self { results }
    }

    /// Get the results array
    pub fn results(&self) -> &[i64] {
        &self.results
    }

    /// Check if a specific item at index was added
    pub fn was_added(&self, index: usize) -> Option<bool> {
        self.results.get(index).map(|&r| r == 1)
    }

    /// Count how many items were added
    pub fn count_added(&self) -> usize {
        self.results.iter().filter(|&&r| r == 1).count()
    }

    /// Check if all items were added
    pub fn all_added(&self) -> bool {
        self.results.iter().all(|&r| r == 1)
    }

    /// Decode the Redis protocol response into a CfInsertOutput
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
            other => Err(EpError::parse(format!("unexpected CF.INSERT response: {:?}", other))),
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
            other => Err(EpError::parse(format!("unexpected CF.INSERT response: {:?}", other))),
        }
    }
}

impl Serialize for CfInsertOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfInsertOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = CfInsertInput::new("myfilter", vec!["item1", "item2"]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.INSERT"));
            assert!(cmd_str.contains("myfilter"));
            assert!(cmd_str.contains("ITEMS"));
            assert!(cmd_str.contains("item1"));
            assert!(cmd_str.contains("item2"));
        }

        #[test]
        fn test_encode_command_with_capacity() {
            let input = CfInsertInput::new("myfilter", vec!["item1"]).with_capacity(1000i64);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CAPACITY"));
            assert!(cmd_str.contains("1000"));
        }

        #[test]
        fn test_encode_command_with_nocreate() {
            let input = CfInsertInput::new("myfilter", vec!["item1"]).with_nocreate();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NOCREATE"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfInsertInput::new("filter1", vec!["item1", "item2"]);
            assert_eq!(input.key, RedisKey::String("filter1".into()));
            assert_eq!(input.items.len(), 2);
            assert!(input.capacity.is_none());
            assert!(input.no_create.is_none());
        }

        #[test]
        fn test_builder_methods() {
            let input = CfInsertInput::new("filter1", vec!["a"]).with_capacity(500i64).with_nocreate().with_item("b");

            assert!(input.capacity.is_some());
            assert_eq!(input.no_create, Some(true));
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfInsertInput::new("testfilter", vec!["item"]);
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::String("ITEMS".into()),
                RedisJsonValue::String("item1".into()),
                RedisJsonValue::String("item2".into()),
            ];
            let input = CfInsertInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::String("CAPACITY".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("NOCREATE".into()),
                RedisJsonValue::String("ITEMS".into()),
                RedisJsonValue::String("item1".into()),
            ];
            let input = CfInsertInput::decode(args).unwrap();
            assert!(input.capacity.is_some());
            assert_eq!(input.no_create, Some(true));
            assert_eq!(input.items.len(), 1);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::String("ITEMS".into())];
            let err = CfInsertInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least"));
        }

        #[test]
        fn test_decode_output_all_added() {
            let output = CfInsertOutput::decode(b"*3\r\n:1\r\n:1\r\n:1\r\n").unwrap();
            assert_eq!(output.results().len(), 3);
            assert!(output.all_added());
            assert_eq!(output.count_added(), 3);
        }

        #[test]
        fn test_decode_output_mixed() {
            let output = CfInsertOutput::decode(b"*3\r\n:1\r\n:0\r\n:1\r\n").unwrap();
            assert!(!output.all_added());
            assert_eq!(output.count_added(), 2);
            assert_eq!(output.was_added(0), Some(true));
            assert_eq!(output.was_added(1), Some(false));
            assert_eq!(output.was_added(2), Some(true));
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfInsertOutput::decode(b"-ERR not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfInsertOutput::new(vec![1, 1, 0]);
            assert_eq!(output.results(), &[1, 1, 0]);
            assert_eq!(output.count_added(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_insert_basic() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_insert_test_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            let result = ctx.raw(&CfInsertInput::new(&key, vec!["item1", "item2", "item3"]).command()).await;

            match result {
                Ok(bytes) => {
                    let output = CfInsertOutput::decode(&bytes).expect("decode failed");
                    assert_eq!(output.results().len(), 3);
                    assert!(output.all_added());
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
        async fn test_cf_insert_with_capacity() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_insert_cap_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            let result = ctx.raw(&CfInsertInput::new(&key, vec!["item1"]).with_capacity(5000i64).command()).await;

            match result {
                Ok(bytes) => {
                    let output = CfInsertOutput::decode(&bytes).expect("decode failed");
                    assert!(output.all_added());
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
        async fn test_cf_insert_nocreate_nonexistent() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let key = format!(
                "cf_insert_nocreate_{}",
                std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
            );

            // Try to insert with NOCREATE into non-existent filter
            let result = ctx.raw(&CfInsertInput::new(&key, vec!["item1"]).with_nocreate().command()).await;

            match result {
                Ok(bytes) => {
                    // Should return error because filter doesn't exist
                    let decode_result = CfInsertOutput::decode(&bytes);
                    // Error expected
                    assert!(decode_result.is_err() || !decode_result.unwrap().all_added());
                }
                Err(e) => {
                    let err_str = e.to_string();
                    if err_str.contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    }
                    // Error is expected for non-existent filter with NOCREATE
                }
            }

            ctx.stop().await;
        }
    }
}
