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

const API_INFO: ApiInfo<RedisApi, TopkQueryInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TopkQuery,
    "Checks whether one or more items are in a sketch",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `TOPK.QUERY`
/// https://redis.io/docs/latest/commands/topk.query/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkQueryInput {
    key: RedisKey,
    items: Vec<RedisJsonValue>,
}

impl Serialize for TopkQueryInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TopkQueryInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(
    TopkQueryInput,
    API_INFO,
    { key, items }
);

impl RedisCommandInput for TopkQueryInput {
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
            return Err(EpError::request("TOPK.QUERY requires at least 2 arguments (key, item...)"));
        }

        let key = args[0].clone().try_into()?;
        let items = args[1..].to_vec();

        Ok(TopkQueryInput { key, items })
    }
}

/// Output for Redis TOPK.QUERY command
///
/// Returns an array of integers (0 or 1) indicating whether each item
/// is in the Top-K list.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkQueryOutput {
    /// Array of booleans indicating if each item is in the Top-K
    results: Vec<bool>,
}

impl TopkQueryOutput {
    pub fn new(results: Vec<bool>) -> Self {
        Self { results }
    }

    /// Get the query results
    pub fn results(&self) -> &[bool] {
        &self.results
    }

    /// Get the number of results
    pub fn len(&self) -> usize {
        self.results.len()
    }

    /// Check if result is empty
    pub fn is_empty(&self) -> bool {
        self.results.is_empty()
    }

    /// Check if item at index is in Top-K
    pub fn is_in_topk(&self, index: usize) -> Option<bool> {
        self.results.get(index).copied()
    }

    /// Count how many items are in the Top-K
    pub fn count_in_topk(&self) -> usize {
        self.results.iter().filter(|&&b| b).count()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let results = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => arr
                .into_iter()
                .map(|f| match f {
                    Resp2Frame::Integer(i) => Ok(i != 0),
                    Resp2Frame::BulkString(b) => {
                        let s = String::from_utf8_lossy(&b);
                        Ok(s == "1" || s.to_lowercase() == "true")
                    }
                    _ => Err(EpError::parse("expected integer in TOPK.QUERY response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data
                .into_iter()
                .map(|f| match f {
                    Resp3Frame::Number { data, .. } => Ok(data != 0),
                    Resp3Frame::Boolean { data, .. } => Ok(data),
                    Resp3Frame::BlobString { data, .. } => {
                        let s = String::from_utf8_lossy(&data);
                        Ok(s == "1" || s.to_lowercase() == "true")
                    }
                    _ => Err(EpError::parse("expected integer in TOPK.QUERY response")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            _ => return Err(EpError::parse("expected array for TOPK.QUERY response")),
        };

        Ok(Self { results })
    }
}

impl Serialize for TopkQueryOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkQueryOutput", 1)?;
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
        fn test_encode_command_single_item() {
            let input = TopkQueryInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![RedisJsonValue::String("item1".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.QUERY"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("item1"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = TopkQueryInput {
                key: RedisKey::String("mytopk".into()),
                items: vec![
                    RedisJsonValue::String("a".into()),
                    RedisJsonValue::String("b".into()),
                    RedisJsonValue::String("c".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.QUERY"));
            assert!(cmd_str.contains("a"));
            assert!(cmd_str.contains("b"));
            assert!(cmd_str.contains("c"));
        }

        #[test]
        fn test_decode_input_single_item() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("item1".into())];
            let input = TopkQueryInput::decode(args).unwrap();
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
            let input = TopkQueryInput::decode(args).unwrap();
            assert_eq!(input.items.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkQueryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_key_only_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TopkQueryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkQueryInput {
                key: RedisKey::String("mykey".into()),
                items: vec![RedisJsonValue::String("item".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_all_false() {
            let bytes = b"*3\r\n:0\r\n:0\r\n:0\r\n";
            let output = TopkQueryOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.count_in_topk(), 0);
            assert!(!output.results()[0]);
            assert!(!output.results()[1]);
            assert!(!output.results()[2]);
        }

        #[test]
        fn test_output_decode_all_true() {
            let bytes = b"*2\r\n:1\r\n:1\r\n";
            let output = TopkQueryOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.count_in_topk(), 2);
            assert!(output.results()[0]);
            assert!(output.results()[1]);
        }

        #[test]
        fn test_output_decode_mixed() {
            let bytes = b"*4\r\n:1\r\n:0\r\n:1\r\n:0\r\n";
            let output = TopkQueryOutput::decode(bytes).unwrap();
            assert_eq!(output.len(), 4);
            assert_eq!(output.count_in_topk(), 2);
            assert!(output.is_in_topk(0).unwrap());
            assert!(!output.is_in_topk(1).unwrap());
            assert!(output.is_in_topk(2).unwrap());
            assert!(!output.is_in_topk(3).unwrap());
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkQueryOutput::decode(b"-ERR key not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = TopkQueryOutput::new(vec![true, false, true]);
            assert_eq!(output.len(), 3);
            assert_eq!(output.count_in_topk(), 2);
        }

        #[test]
        fn test_output_is_in_topk_out_of_bounds() {
            let output = TopkQueryOutput::new(vec![true]);
            assert!(output.is_in_topk(0).is_some());
            assert!(output.is_in_topk(1).is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_query_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nquery_test_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$14\r\nquery_test_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add some items
                    ctx.raw(b"*4\r\n$8\r\nTOPK.ADD\r\n$14\r\nquery_test_key\r\n$5\r\napple\r\n$6\r\nbanana\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkQueryInput {
                                key: RedisKey::String("query_test_key".into()),
                                items: vec![RedisJsonValue::String("apple".into()), RedisJsonValue::String("cherry".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkQueryOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    // apple should be in top-k, cherry should not
                    assert!(output.is_in_topk(0).unwrap());
                    assert!(!output.is_in_topk(1).unwrap());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_query_empty_topk() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nquery_empty_key\r\n").await.ok();

                    let reserve = ctx.raw(b"*3\r\n$12\r\nTOPK.RESERVE\r\n$15\r\nquery_empty_key\r\n$1\r\n5\r\n").await;

                    if reserve.is_err() || reserve.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Add one item to initialize the TOPK structure
                    ctx.raw(b"*3\r\n$8\r\nTOPK.ADD\r\n$15\r\nquery_empty_key\r\n$4\r\ntest\r\n").await.ok();

                    // Query for a different item - should return false
                    let result = ctx
                        .raw(
                            &TopkQueryInput {
                                key: RedisKey::String("query_empty_key".into()),
                                items: vec![RedisJsonValue::String("anything".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TopkQueryOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert!(!output.is_in_topk(0).unwrap());
                })
            })
            .await;
        }
    }
}
