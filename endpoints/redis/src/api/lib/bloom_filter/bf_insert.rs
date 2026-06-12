use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, BfInsertInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BfInsert,
    "Adds one or more items to a Bloom Filter. A filter will be created if it does not exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `BF.INSERT`
/// https://redis.io/docs/latest/commands/bf.insert/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfInsertInput {
    pub(crate) key: RedisKey,
    pub(crate) capacity: Option<RedisJsonValue>,
    pub(crate) error: Option<RedisJsonValue>,
    pub(crate) expansion: Option<RedisJsonValue>,
    pub(crate) no_create: Option<bool>,
    pub(crate) non_scaling: Option<bool>,
    pub(crate) items: Vec<RedisJsonValue>,
}

impl Serialize for BfInsertInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.capacity.is_some() {
            fields += 1;
        }
        if self.error.is_some() {
            fields += 1;
        }
        if self.expansion.is_some() {
            fields += 1;
        }
        if self.no_create.is_some() {
            fields += 1;
        }
        if self.non_scaling.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BfInsertInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(capacity) = &self.capacity {
            state.serialize_field("capacity", capacity)?;
        }
        if let Some(error) = &self.error {
            state.serialize_field("error", error)?;
        }
        if let Some(expansion) = &self.expansion {
            state.serialize_field("expansion", expansion)?;
        }
        if let Some(no_create) = &self.no_create {
            state.serialize_field("no_create", no_create)?;
        }
        if let Some(non_scaling) = &self.non_scaling {
            state.serialize_field("non_scaling", non_scaling)?;
        }
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(
    BfInsertInput,
    API_INFO,
    { key, capacity, error, expansion, no_create, non_scaling, items }
);

impl RedisCommandInput for BfInsertInput {
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

        if let Some(error) = &self.error {
            command.arg("ERROR").arg(error);
        }

        if let Some(expansion) = &self.expansion {
            command.arg("EXPANSION").arg(expansion);
        }

        if let Some(true) = self.no_create {
            // Fixed: was no_create.clone()
            command.arg("NOCREATE");
        }

        if let Some(true) = self.non_scaling {
            command.arg("NONSCALING");
        }

        command.arg("ITEMS");
        for item in &self.items {
            command.arg(item);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse("BF.INSERT requires at least key ITEMS item"));
        }

        let key = args[0].clone().try_into()?;

        // Find ITEMS position (required)
        let items_pos = args
            .iter()
            .position(|arg| matches!(arg, RedisJsonValue::String(s) if s.to_uppercase() == "ITEMS"))
            .ok_or_else(|| EpError::parse("ITEMS parameter is required"))?;

        if items_pos + 1 >= args.len() {
            return Err(EpError::parse("ITEMS requires at least one value"));
        }

        // Parse optional parameters between key and ITEMS
        let mut capacity = None;
        let mut error = None;
        let mut expansion = None;
        let mut no_create = None;
        let mut non_scaling = None;

        let mut i = 1;
        while i < items_pos {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "CAPACITY" => {
                        capacity = Some(args.get(i + 1).ok_or_else(|| EpError::parse("CAPACITY requires a value"))?.clone());
                        i += 2;
                    }
                    "ERROR" => {
                        error = Some(args.get(i + 1).ok_or_else(|| EpError::parse("ERROR requires a value"))?.clone());
                        i += 2;
                    }
                    "EXPANSION" => {
                        expansion = Some(args.get(i + 1).ok_or_else(|| EpError::parse("EXPANSION requires a value"))?.clone());
                        i += 2;
                    }
                    "NOCREATE" => {
                        no_create = Some(true);
                        i += 1;
                    }
                    "NONSCALING" => {
                        non_scaling = Some(true);
                        i += 1;
                    }
                    _ => return Err(EpError::parse(format!("Unknown parameter: {}", s))),
                },
                _ => return Err(EpError::parse("Parameters must be strings")),
            }
        }

        Ok(BfInsertInput {
            key,
            capacity,
            error,
            expansion,
            no_create,
            non_scaling,
            items: args[items_pos + 1..].to_vec(),
        })
    }
}

/// Output for Redis BF.INSERT command
///
/// Returns an array of integers, where each integer indicates whether
/// the corresponding item was newly added (1) or may have existed (0).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfInsertOutput {
    /// Results for each item: 1 if newly added, 0 if may have existed
    results: Vec<i64>,
}

impl BfInsertOutput {
    pub fn new(results: Vec<i64>) -> Self {
        Self { results }
    }

    /// Get the results array
    pub fn results(&self) -> &[i64] {
        &self.results
    }

    /// Get result for a specific index
    pub fn get(&self, index: usize) -> Option<i64> {
        self.results.get(index).copied()
    }

    /// Check if item at index was newly added
    pub fn was_added(&self, index: usize) -> Option<bool> {
        self.results.get(index).map(|&r| r == 1)
    }

    /// Count how many items were newly added
    pub fn added_count(&self) -> usize {
        self.results.iter().filter(|&&r| r == 1).count()
    }

    /// Decode the Redis protocol response into a BfInsertOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut results = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::Integer(i) => results.push(i),
                            Resp2Frame::SimpleString(_) => results.push(0), // Filter is full
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in BF.INSERT response: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected BF.INSERT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut results = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::Boolean { data, .. } => results.push(data as i64),
                            Resp3Frame::SimpleString { data: _data, .. } => results.push(0), // Filter is full
                            Resp3Frame::Number { data, .. } => results.push(data),
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in BF.INSERT response: {:?}", other)));
                            }
                        }
                    }
                    results
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected BF.INSERT response: {:?}", other)));
                }
            },
        };

        Ok(Self { results })
    }
}

impl Serialize for BfInsertOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfInsertOutput", 1)?;
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
            let input = BfInsertInput {
                key: RedisKey::String("myfilter".into()),
                capacity: None,
                error: None,
                expansion: None,
                no_create: None,
                non_scaling: None,
                items: vec![RedisJsonValue::String("item1".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$9\r\nBF.INSERT\r\n"));
            assert!(cmd.windows(5).any(|w| w == b"ITEMS"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = BfInsertInput {
                key: RedisKey::String("myfilter".into()),
                capacity: Some(RedisJsonValue::Integer(1000)),
                error: Some(RedisJsonValue::String("0.01".into())),
                expansion: None,
                no_create: Some(true),
                non_scaling: None,
                items: vec![RedisJsonValue::String("item1".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"CAPACITY"));
            assert!(cmd.windows(5).any(|w| w == b"ERROR"));
            assert!(cmd.windows(8).any(|w| w == b"NOCREATE"));
        }

        #[test]
        fn test_decode_array_response() {
            let output = BfInsertOutput::decode(b"*2\r\n:1\r\n:0\r\n").unwrap();
            assert_eq!(output.results(), &[1, 0]);
            assert_eq!(output.was_added(0), Some(true));
            assert_eq!(output.was_added(1), Some(false));
            assert_eq!(output.added_count(), 1);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = BfInsertOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("ITEMS".into()),
                RedisJsonValue::String("item1".into()),
            ];
            let input = BfInsertInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
            assert_eq!(input.items.len(), 1);
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("CAPACITY".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("NOCREATE".into()),
                RedisJsonValue::String("ITEMS".into()),
                RedisJsonValue::String("item1".into()),
            ];
            let input = BfInsertInput::decode(args).unwrap();
            assert!(input.capacity.is_some());
            assert_eq!(input.no_create, Some(true));
        }

        #[test]
        fn test_decode_input_missing_items() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("CAPACITY".into()),
                RedisJsonValue::Integer(1000),
            ];
            let err = BfInsertInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("ITEMS"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfInsertInput {
                key: RedisKey::String("testkey".into()),
                capacity: None,
                error: None,
                expansion: None,
                no_create: None,
                non_scaling: None,
                items: vec![RedisJsonValue::String("item".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_exists::BfExistsInput;
        use crate::api::lib::bloom_filter::bf_exists::BfExistsOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_insert_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfInsertInput {
                                key: RedisKey::String("bf_insert_test".into()),
                                capacity: None,
                                error: None,
                                expansion: None,
                                no_create: None,
                                non_scaling: None,
                                items: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfInsertOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.results().len(), 2);
                    assert_eq!(output.added_count(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_insert_with_capacity() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfInsertInput {
                                key: RedisKey::String("bf_insert_cap".into()),
                                capacity: Some(RedisJsonValue::Integer(100)),
                                error: Some(RedisJsonValue::String("0.01".into())),
                                expansion: None,
                                no_create: None,
                                non_scaling: None,
                                items: vec![RedisJsonValue::String("item".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfInsertOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added(0).unwrap());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_insert_nocreate_fails() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // NOCREATE should fail if filter doesn't exist
                    let result = ctx
                        .raw(
                            &BfInsertInput {
                                key: RedisKey::String("bf_insert_nocreate".into()),
                                capacity: None,
                                error: None,
                                expansion: None,
                                no_create: Some(true),
                                non_scaling: None,
                                items: vec![RedisJsonValue::String("item".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = BfInsertOutput::decode(&result);
                    assert!(err.is_err());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_insert_then_exists() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &BfInsertInput {
                            key: RedisKey::String("bf_insert_exists".into()),
                            capacity: None,
                            error: None,
                            expansion: None,
                            no_create: None,
                            non_scaling: None,
                            items: vec![RedisJsonValue::String("check_me".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &BfExistsInput {
                                key: RedisKey::String("bf_insert_exists".into()),
                                item: RedisJsonValue::String("check_me".into()),
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
