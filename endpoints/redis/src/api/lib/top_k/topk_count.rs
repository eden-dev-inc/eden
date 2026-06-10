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

const API_INFO: ApiInfo<RedisApi, TopkCountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TopkCount,
    "Returns the count for one or more items in a sketch",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `TOPK.COUNT`
/// https://redis.io/docs/latest/commands/topk.count/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkCountInput {
    key: RedisKey,
    items: Vec<RedisJsonValue>,
}

impl Serialize for TopkCountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TopkCountInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(
    TopkCountInput,
    API_INFO,
    { key, items }
);

impl RedisCommandInput for TopkCountInput {
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
            return Err(EpError::request("TOPK.COUNT requires at least 2 arguments (key, item...)"));
        }

        let key = args[0].clone().try_into()?;
        let items = args[1..].to_vec();

        Ok(TopkCountInput { key, items })
    }
}

/// Output for Redis TOPK.COUNT command
///
/// Returns an array of integers representing the count of each queried item.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkCountOutput {
    /// Array of counts for each queried item
    counts: Vec<i64>,
}

impl TopkCountOutput {
    pub fn new(counts: Vec<i64>) -> Self {
        Self { counts }
    }

    /// Get the counts
    pub fn counts(&self) -> &[i64] {
        &self.counts
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Get count for item at index
    pub fn count_at(&self, index: usize) -> Option<i64> {
        self.counts.get(index).copied()
    }

    /// Get total count across all items
    pub fn total(&self) -> i64 {
        self.counts.iter().sum()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let counts = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => arr
                .into_iter()
                .map(|f| match f {
                    Resp2Frame::Integer(i) => Ok(i),
                    Resp2Frame::BulkString(b) => {
                        String::from_utf8_lossy(&b).parse::<i64>().map_err(|_| EpError::parse("invalid count value"))
                    }
                    _ => Err(EpError::parse("expected integer in TOPK.COUNT response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                // If the key doesn't exist, return empty counts (all zeros would be returned if key existed but was empty)
                // However, since we don't know how many items were requested, we return an error
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data
                .into_iter()
                .map(|f| match f {
                    Resp3Frame::Number { data, .. } => Ok(data),
                    Resp3Frame::BlobString { data, .. } => {
                        String::from_utf8_lossy(&data).parse::<i64>().map_err(|_| EpError::parse("invalid count value"))
                    }
                    _ => Err(EpError::parse("expected integer in TOPK.COUNT response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.COUNT response")),
        };

        Ok(Self { counts })
    }
}

impl Serialize for TopkCountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkCountOutput", 1)?;
        state.serialize_field("counts", &self.counts)?;
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
            let input = TopkCountInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![RedisJsonValue::String("item1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.COUNT"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("item1"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = TopkCountInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![
                    RedisJsonValue::String("a".into()),
                    RedisJsonValue::String("b".into()),
                    RedisJsonValue::String("c".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.COUNT"));
            assert!(cmd_str.contains("a"));
            assert!(cmd_str.contains("b"));
            assert!(cmd_str.contains("c"));
        }

        #[test]
        fn test_decode_input_single_item() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("item1".into())];
            let input = TopkCountInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.items.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_items() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
            ];
            let input = TopkCountInput::decode(args).unwrap();
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkCountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_key_only_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TopkCountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkCountInput {
                key: RedisKey::String("mykey".into()),
                items: vec![RedisJsonValue::String("item".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_counts() {
            let bytes = b"*3\r\n:5\r\n:10\r\n:0\r\n";
            let output = TopkCountOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.counts()[0], 5);
            assert_eq!(output.counts()[1], 10);
            assert_eq!(output.counts()[2], 0);
        }

        #[test]
        fn test_output_decode_single_count() {
            let bytes = b"*1\r\n:42\r\n";
            let output = TopkCountOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.count_at(0), Some(42));
        }

        #[test]
        fn test_output_total() {
            let output = TopkCountOutput::new(vec![5, 10, 15]);
            assert_eq!(output.total(), 30);
        }

        #[test]
        fn test_output_count_at() {
            let output = TopkCountOutput::new(vec![1, 2, 3]);
            assert_eq!(output.count_at(0), Some(1));
            assert_eq!(output.count_at(1), Some(2));
            assert_eq!(output.count_at(2), Some(3));
            assert_eq!(output.count_at(3), None);
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkCountOutput::decode(b"-ERR key not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = TopkCountOutput::new(vec![100, 200]);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_count_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\ncount_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$14\r\ncount_test_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add items with different frequencies
                    ctx.raw(b"*5\r\n$8\r\nTOPK.ADD\r\n$14\r\ncount_test_key\r\n$5\r\napple\r\n$5\r\napple\r\n$6\r\nbanana\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkCountInput {
                                key: RedisKey::String("count_test_key".into()),
                                items: vec![
                                    RedisJsonValue::String("apple".into()),
                                    RedisJsonValue::String("banana".into()),
                                    RedisJsonValue::String("cherry".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkCountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    // apple was added twice, banana once, cherry never
                    assert!(output.count_at(0).unwrap() >= 2);
                    assert!(output.count_at(1).unwrap() >= 1);
                    assert_eq!(output.count_at(2).unwrap(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_count_empty_topk() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\ncount_empty_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$15\r\ncount_empty_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add one item to initialize the TOPK structure
                    ctx.raw(b"*3\r\n$8\r\nTOPK.ADD\r\n$15\r\ncount_empty_key\r\n$4\r\ntest\r\n").await.ok();

                    // Count an item that was never added - should return 0
                    let result = ctx
                        .raw(
                            &TopkCountInput {
                                key: RedisKey::String("count_empty_key".into()),
                                items: vec![RedisJsonValue::String("anything".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkCountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.count_at(0).unwrap(), 0);
                })
            })
            .await;
        }
    }
}
