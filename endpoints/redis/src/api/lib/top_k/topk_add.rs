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

const API_INFO: ApiInfo<RedisApi, TopkAddInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TopkAdd, "Adds one or more items to a Top-K filter", ReqType::Write, true);

/// See official Redis documentation for `TOPK.ADD`
/// https://redis.io/docs/latest/commands/topk.add/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkAddInput {
    key: RedisKey,
    items: Vec<RedisJsonValue>,
}

impl Serialize for TopkAddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TopkAddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(
    TopkAddInput,
    API_INFO,
    { key, items }
);

impl RedisCommandInput for TopkAddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);
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
        if args.len() < 2 {
            return Err(EpError::request("TOPK.ADD requires at least 2 arguments (key, item...)"));
        }

        let key = args[0].clone().try_into()?;
        let items = args[1..].to_vec();

        Ok(TopkAddInput { key, items })
    }
}

/// Output for Redis TOPK.ADD command
///
/// Returns an array of items that were dropped from the Top-K to make room
/// for the new items (or null if no item was dropped for that position).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkAddOutput {
    /// Items dropped from the Top-K (null if no item was dropped)
    dropped: Vec<Option<RedisJsonValue>>,
}

impl TopkAddOutput {
    pub fn new(dropped: Vec<Option<RedisJsonValue>>) -> Self {
        Self { dropped }
    }

    /// Get the items that were dropped
    pub fn dropped(&self) -> &[Option<RedisJsonValue>] {
        &self.dropped
    }

    /// Get the number of results (one per added item)
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
                    _ => Err(EpError::parse("unexpected value in TOPK.ADD response")),
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
                    _ => Err(EpError::parse("unexpected value in TOPK.ADD response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.ADD response")),
        };

        Ok(Self { dropped })
    }
}

impl Serialize for TopkAddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkAddOutput", 1)?;
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
            let input = TopkAddInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![RedisJsonValue::String("item1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.ADD"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("item1"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = TopkAddInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![
                    RedisJsonValue::String("item1".into()),
                    RedisJsonValue::String("item2".into()),
                    RedisJsonValue::String("item3".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.ADD"));
            assert!(cmd_str.contains("item1"));
            assert!(cmd_str.contains("item2"));
            assert!(cmd_str.contains("item3"));
        }

        #[test]
        fn test_decode_input_single_item() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("item1".into())];
            let input = TopkAddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.items.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_items() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let input = TopkAddInput::decode(args).unwrap();
            assert_eq!(input.items.len(), 3);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_key_only_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TopkAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkAddInput {
                key: RedisKey::String("mykey".into()),
                items: vec![RedisJsonValue::String("item".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_all_nulls() {
            // All nulls means nothing was dropped
            let bytes = b"*3\r\n$-1\r\n$-1\r\n$-1\r\n";
            let output = TopkAddOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 3);
            assert!(!output.has_dropped());
        }

        #[test]
        fn test_output_decode_with_dropped() {
            // Mix of null and dropped items
            let bytes = b"*3\r\n$-1\r\n$5\r\nitem1\r\n$-1\r\n";
            let output = TopkAddOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.has_dropped());
            assert!(output.dropped()[0].is_none());
            assert!(output.dropped()[1].is_some());
            assert!(output.dropped()[2].is_none());
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkAddOutput::decode(b"-ERR key not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let dropped = vec![None, Some(RedisJsonValue::String("old".into())), None];
            let output = TopkAddOutput::new(dropped);
            assert_eq!(output.len(), 3);
            assert!(output.has_dropped());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_add_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Setup: create TopK
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nadd_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$12\r\nadd_test_key\r\n$1\r\n3\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return; // RedisBloom not available
                    }

                    let result = ctx
                        .raw(
                            &TopkAddInput {
                                key: RedisKey::String("add_test_key".into()),
                                items: vec![RedisJsonValue::String("apple".into()), RedisJsonValue::String("banana".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkAddOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_add_overflow() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nadd_overflow_key\r\n").await.ok();

                    // Create TopK with k=2
                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$16\r\nadd_overflow_key\r\n$1\r\n2\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add items to fill the TopK
                    ctx.raw(
                        &TopkAddInput {
                            key: RedisKey::String("add_overflow_key".into()),
                            items: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("b".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Adding a new item may cause drops
                    let result = ctx
                        .raw(
                            &TopkAddInput {
                                key: RedisKey::String("add_overflow_key".into()),
                                items: vec![
                                    RedisJsonValue::String("c".into()),
                                    RedisJsonValue::String("c".into()),
                                    RedisJsonValue::String("c".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkAddOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    // May or may not have dropped items depending on algorithm
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_add_nonexistent_key_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nnonexistent_topk\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkAddInput {
                                key: RedisKey::String("nonexistent_topk".into()),
                                items: vec![RedisJsonValue::String("item".into())],
                            }
                            .command(),
                        )
                        .await;

                    // Should fail or return error for nonexistent key
                    if let Ok(bytes) = result
                        && !bytes.starts_with(b"-ERR unknown")
                    {
                        // Either error on decode or key doesn't exist error
                        assert!(bytes.starts_with(b"-") || TopkAddOutput::decode(&bytes).is_err());
                    }
                })
            })
            .await;
        }
    }
}
