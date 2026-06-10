use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use borsh::{BorshDeserialize, BorshSerialize};
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

const API_INFO: ApiInfo<RedisApi, TopkIncrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TopkIncrby,
    "Increases the count of one or more items by increment",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `TOPK.INCRBY`
/// https://redis.io/docs/latest/commands/topk.incrby/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkIncrbyInput {
    key: RedisKey,
    items: Vec<TopkIncrbyItem>,
}

impl Serialize for TopkIncrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TopkIncrbyInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

/// Item with increment for TOPK.INCRBY
/// Note: Redis command format is TOPK.INCRBY key item increment [item increment ...]
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct TopkIncrbyItem {
    /// The item to increment
    item: RedisJsonValue,
    /// The increment amount
    increment: RedisJsonValue,
}

impl TopkIncrbyItem {
    pub fn new(item: impl Into<RedisJsonValue>, increment: impl Into<RedisJsonValue>) -> Self {
        Self { item: item.into(), increment: increment.into() }
    }
}

impl_redis_operation!(
    TopkIncrbyInput,
    API_INFO,
    { key, items }
);

impl RedisCommandInput for TopkIncrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        // Redis format: TOPK.INCRBY key item increment [item increment ...]
        for item in &self.items {
            command.arg(&item.item).arg(&item.increment);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request("TOPK.INCRBY requires at least 3 arguments (key, item, increment)"));
        }

        if !(args.len() - 1).is_multiple_of(2) {
            return Err(EpError::request("TOPK.INCRBY requires item/increment pairs after key"));
        }

        let key = args[0].clone().try_into()?;
        let mut items = Vec::new();

        // Parse pairs: item, increment, item, increment, ...
        for chunk in args[1..].chunks(2) {
            if chunk.len() != 2 {
                return Err(EpError::request("Invalid item/increment pair"));
            }
            items.push(TopkIncrbyItem { item: chunk[0].clone(), increment: chunk[1].clone() });
        }

        Ok(TopkIncrbyInput { key, items })
    }
}

/// Output for Redis TOPK.INCRBY command
///
/// Returns an array of items that were dropped from the Top-K to make room
/// (or null if no item was dropped for that position).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkIncrbyOutput {
    /// Items dropped from the Top-K (null if no item was dropped)
    dropped: Vec<Option<RedisJsonValue>>,
}

impl TopkIncrbyOutput {
    pub fn new(dropped: Vec<Option<RedisJsonValue>>) -> Self {
        Self { dropped }
    }

    /// Get the items that were dropped
    pub fn dropped(&self) -> &[Option<RedisJsonValue>] {
        &self.dropped
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.dropped.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.dropped.is_empty()
    }

    /// Check if any items were dropped
    pub fn has_dropped(&self) -> bool {
        self.dropped.iter().any(|d| d.is_some())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let dropped = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => arr
                .into_iter()
                .map(|f| match f {
                    Resp2Frame::Null => Ok(None),
                    Resp2Frame::BulkString(b) => Ok(Some(RedisJsonValue::String(String::from_utf8_lossy(&b).into()))),
                    Resp2Frame::SimpleString(b) => Ok(Some(RedisJsonValue::String(String::from_utf8_lossy(&b).into()))),
                    Resp2Frame::Integer(i) => Ok(Some(RedisJsonValue::Integer(i))),
                    _ => Err(EpError::parse("unexpected value in TOPK.INCRBY response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data
                .into_iter()
                .map(|f| match f {
                    Resp3Frame::Null => Ok(None),
                    Resp3Frame::BlobString { data, .. } => Ok(Some(RedisJsonValue::String(String::from_utf8_lossy(&data).into()))),
                    Resp3Frame::SimpleString { data, .. } => Ok(Some(RedisJsonValue::String(String::from_utf8_lossy(&data).into()))),
                    Resp3Frame::Number { data, .. } => Ok(Some(RedisJsonValue::Integer(data))),
                    _ => Err(EpError::parse("unexpected value in TOPK.INCRBY response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.INCRBY response")),
        };

        Ok(Self { dropped })
    }
}

impl Serialize for TopkIncrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkIncrbyOutput", 1)?;
        state.serialize_field("dropped", &self.dropped)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_item() {
            let input = TopkIncrbyInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![TopkIncrbyItem::new("item1", 5)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.INCRBY"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("item1"));
            assert!(cmd_str.contains("5"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = TopkIncrbyInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![
                    TopkIncrbyItem::new("a", 1),
                    TopkIncrbyItem::new("b", 2),
                    TopkIncrbyItem::new("c", 3),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.INCRBY"));
            // Verify order: item then increment
            let pos_a = cmd_str.find('a').unwrap();
            let pos_1 = cmd_str.rfind('1').unwrap();
            assert!(pos_a < pos_1);
        }

        #[test]
        fn test_decode_input_single_pair() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("item1".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = TopkIncrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.items.len(), 1);
            assert_eq!(input.items[0].item, RedisJsonValue::String("item1".into()));
            assert_eq!(input.items[0].increment, RedisJsonValue::Integer(5));
        }

        #[test]
        fn test_decode_input_multiple_pairs() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::Integer(2),
            ];
            let input = TopkIncrbyInput::decode(args).unwrap();
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_key_only_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TopkIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_odd_args_fails() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("item".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("orphan".into()), // Missing increment
            ];
            let err = TopkIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("pairs"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkIncrbyInput {
                key: RedisKey::String("mykey".into()),
                items: vec![TopkIncrbyItem::new("item", 1)],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_all_nulls() {
            let bytes = b"*2\r\n$-1\r\n$-1\r\n";
            let output = TopkIncrbyOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 2);
            assert!(!output.has_dropped());
        }

        #[test]
        fn test_output_decode_with_dropped() {
            let bytes = b"*2\r\n$-1\r\n$3\r\nold\r\n";
            let output = TopkIncrbyOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 2);
            assert!(output.has_dropped());
            assert!(output.dropped()[0].is_none());
            assert!(output.dropped()[1].is_some());
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkIncrbyOutput::decode(b"-ERR key not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_topk_incrby_item_new() {
            let item = TopkIncrbyItem::new("test", 10);
            assert_eq!(item.item, RedisJsonValue::String("test".into()));
            assert_eq!(item.increment, RedisJsonValue::Integer(10));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_incrby_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nincrby_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$15\r\nincrby_test_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    let result = ctx
                        .raw(
                            &TopkIncrbyInput {
                                key: RedisKey::String("incrby_test_key".into()),
                                items: vec![TopkIncrbyItem::new("apple", 3), TopkIncrbyItem::new("banana", 2)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkIncrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_incrby_large_increment() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nincrby_large_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$16\r\nincrby_large_key\r\n$1\r\n3\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add items with large increments
                    let result = ctx
                        .raw(
                            &TopkIncrbyInput {
                                key: RedisKey::String("incrby_large_key".into()),
                                items: vec![TopkIncrbyItem::new("heavy", 1000), TopkIncrbyItem::new("light", 1)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkIncrbyOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }
    }
}
