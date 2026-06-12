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

const API_INFO: ApiInfo<RedisApi, TopkListInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TopkList, "Returns full list of items in Top K list", ReqType::Read, true);

/// See official Redis documentation for `TOPK.LIST`
/// https://redis.io/docs/latest/commands/topk.list/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkListInput {
    key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_count: Option<bool>,
}

impl TopkListInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into(), with_count: None }
    }

    pub fn with_count(mut self) -> Self {
        self.with_count = Some(true);
        self
    }
}

impl Serialize for TopkListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, key
        if self.with_count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TopkListInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;

        if let Some(with_count) = &self.with_count {
            state.serialize_field("with_count", with_count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TopkListInput,
    API_INFO,
    { key, with_count }
);

impl RedisCommandInput for TopkListInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(true) = self.with_count {
            command.arg("WITHCOUNT");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TOPK.LIST requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let with_count = if args.len() > 1 {
            if let RedisJsonValue::String(s) = &args[1] {
                if s.to_uppercase() == "WITHCOUNT" {
                    Some(true)
                } else {
                    return Err(EpError::request(format!("Unknown TOPK.LIST option: {}", s)));
                }
            } else {
                return Err(EpError::request("TOPK.LIST options must be strings"));
            }
        } else {
            None
        };

        Ok(TopkListInput { key, with_count })
    }
}

/// A Top-K item with its count
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct TopkListItem {
    /// The item value
    pub item: RedisJsonValue,
    /// The count (only present if WITHCOUNT was used)
    pub count: Option<i64>,
}

/// Output for Redis TOPK.LIST command
///
/// Returns the list of items in the Top-K, optionally with counts.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkListOutput {
    /// Items in the Top-K list
    items: Vec<TopkListItem>,
}

impl TopkListOutput {
    pub fn new(items: Vec<TopkListItem>) -> Self {
        Self { items }
    }

    /// Get the items
    pub fn items(&self) -> &[TopkListItem] {
        &self.items
    }

    /// Get the number of items
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Check if the list is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get item at index
    pub fn get(&self, index: usize) -> Option<&TopkListItem> {
        self.items.get(index)
    }

    /// Decode response without WITHCOUNT (just items)
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let items = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => arr
                .into_iter()
                .map(|f| {
                    let item = match f {
                        Resp2Frame::BulkString(b) => RedisJsonValue::String(String::from_utf8_lossy(&b).into()),
                        Resp2Frame::SimpleString(b) => RedisJsonValue::String(String::from_utf8_lossy(&b).into()),
                        Resp2Frame::Integer(i) => RedisJsonValue::Integer(i),
                        Resp2Frame::Null => RedisJsonValue::Null,
                        _ => return Err(EpError::parse("unexpected value in TOPK.LIST")),
                    };
                    Ok(TopkListItem { item, count: None })
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data
                .into_iter()
                .map(|f| {
                    let item = match f {
                        Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(&data).into()),
                        Resp3Frame::SimpleString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(&data).into()),
                        Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(data),
                        Resp3Frame::Null => RedisJsonValue::Null,
                        _ => return Err(EpError::parse("unexpected value in TOPK.LIST")),
                    };
                    Ok(TopkListItem { item, count: None })
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.LIST response")),
        };

        Ok(Self { items })
    }

    /// Decode response with WITHCOUNT (items and counts interleaved)
    pub fn decode_with_count(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let items = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_with_count_resp2(&arr)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_with_count_resp3(&data)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.LIST response")),
        };

        Ok(Self { items })
    }

    fn parse_with_count_resp2(arr: &[Resp2Frame]) -> Result<Vec<TopkListItem>, EpError> {
        if !arr.len().is_multiple_of(2) {
            return Err(EpError::parse("TOPK.LIST WITHCOUNT should return item/count pairs"));
        }

        arr.chunks(2)
            .map(|chunk| {
                let item = match &chunk[0] {
                    Resp2Frame::BulkString(b) => RedisJsonValue::String(String::from_utf8_lossy(b).into()),
                    Resp2Frame::SimpleString(b) => RedisJsonValue::String(String::from_utf8_lossy(b).into()),
                    _ => return Err(EpError::parse("expected string item")),
                };

                let count = match &chunk[1] {
                    Resp2Frame::Integer(i) => *i,
                    Resp2Frame::BulkString(b) => String::from_utf8_lossy(b).parse::<i64>().map_err(|_| EpError::parse("invalid count"))?,
                    _ => return Err(EpError::parse("expected integer count")),
                };

                Ok(TopkListItem { item, count: Some(count) })
            })
            .collect()
    }

    fn parse_with_count_resp3(arr: &[Resp3Frame]) -> Result<Vec<TopkListItem>, EpError> {
        if !arr.len().is_multiple_of(2) {
            return Err(EpError::parse("TOPK.LIST WITHCOUNT should return item/count pairs"));
        }

        arr.chunks(2)
            .map(|chunk| {
                let item = match &chunk[0] {
                    Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).into()),
                    Resp3Frame::SimpleString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).into()),
                    _ => return Err(EpError::parse("expected string item")),
                };

                let count = match &chunk[1] {
                    Resp3Frame::Number { data, .. } => *data,
                    Resp3Frame::BlobString { data, .. } => {
                        String::from_utf8_lossy(data).parse::<i64>().map_err(|_| EpError::parse("invalid count"))?
                    }
                    _ => return Err(EpError::parse("expected integer count")),
                };

                Ok(TopkListItem { item, count: Some(count) })
            })
            .collect()
    }
}

impl Serialize for TopkListOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkListOutput", 1)?;
        state.serialize_field("items", &self.items)?;
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
            let input = TopkListInput { key: RedisKey::String("mytopk".into()), with_count: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.LIST"));
            assert!(cmd_str.contains("mytopk"));
            assert!(!cmd_str.contains("WITHCOUNT"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = TopkListInput::new(RedisKey::String("mytopk".into())).with_count();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.LIST"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("WITHCOUNT"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TopkListInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.with_count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("WITHCOUNT".into())];
            let input = TopkListInput::decode(args).unwrap();
            assert_eq!(input.with_count, Some(true));
        }

        #[test]
        fn test_decode_input_with_count_lowercase() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("withcount".into())];
            let input = TopkListInput::decode(args).unwrap();
            assert_eq!(input.with_count, Some(true));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkListInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_decode_input_unknown_option_fails() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("INVALID".into())];
            let err = TopkListInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkListInput { key: RedisKey::String("mykey".into()), with_count: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_items() {
            let bytes = b"*3\r\n$5\r\napple\r\n$6\r\nbanana\r\n$6\r\ncherry\r\n";
            let output = TopkListOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.items()[0].count.is_none());
        }

        #[test]
        fn test_output_decode_empty() {
            let bytes = b"*0\r\n";
            let output = TopkListOutput::decode(bytes).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_output_decode_with_count() {
            // item1, count1, item2, count2
            let bytes = b"*4\r\n$5\r\napple\r\n:10\r\n$6\r\nbanana\r\n:5\r\n";
            let output = TopkListOutput::decode_with_count(bytes).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.items()[0].item, RedisJsonValue::String("apple".into()));
            assert_eq!(output.items()[0].count, Some(10));
            assert_eq!(output.items()[1].item, RedisJsonValue::String("banana".into()));
            assert_eq!(output.items()[1].count, Some(5));
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkListOutput::decode(b"-ERR key not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_get() {
            let output = TopkListOutput::new(vec![
                TopkListItem { item: RedisJsonValue::String("first".into()), count: None },
                TopkListItem { item: RedisJsonValue::String("second".into()), count: None },
            ]);
            assert!(output.get(0).is_some());
            assert!(output.get(1).is_some());
            assert!(output.get(2).is_none());
        }

        #[test]
        fn test_builder_pattern() {
            let input = TopkListInput::new("mykey").with_count();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.with_count, Some(true));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_list_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nlist_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$13\r\nlist_test_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add items
                    ctx.raw(b"*4\r\n$8\r\nTOPK.ADD\r\n$13\r\nlist_test_key\r\n$5\r\napple\r\n$6\r\nbanana\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkListInput {
                                key: RedisKey::String("list_test_key".into()),
                                with_count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkListOutput::decode(&result).expect("decode failed");
                    assert!(output.len() >= 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_list_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nlist_count_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$19\r\nlist_count_test_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add items multiple times
                    ctx.raw(b"*5\r\n$8\r\nTOPK.ADD\r\n$19\r\nlist_count_test_key\r\n$5\r\napple\r\n$5\r\napple\r\n$6\r\nbanana\r\n")
                        .await
                        .ok();

                    let result = ctx
                        .raw(&TopkListInput::new(RedisKey::String("list_count_test_key".into())).with_count().command())
                        .await
                        .expect("raw failed");

                    let output = TopkListOutput::decode_with_count(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    // All items should have counts
                    for item in output.items() {
                        assert!(item.count.is_some());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_list_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nlist_empty_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$14\r\nlist_empty_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    let result = ctx
                        .raw(
                            &TopkListInput {
                                key: RedisKey::String("list_empty_key".into()),
                                with_count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkListOutput::decode(&result).expect("decode failed");
                    // Empty TopK should return empty or sparse array
                    assert!(output.len() <= 5);
                })
            })
            .await;
        }
    }
}
