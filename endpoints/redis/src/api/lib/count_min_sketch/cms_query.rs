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

const API_INFO: ApiInfo<RedisApi, CmsQueryInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CmsQuery,
    "Returns the count for one or more items in a sketch",
    ReqType::Read,
    true,
);

/// Input for Redis `CMS.QUERY` command.
///
/// Returns the count for one or more items in a Count-Min Sketch.
///
/// See official Redis documentation for `CMS.QUERY`:
/// https://redis.io/docs/latest/commands/cms.query/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CmsQueryInput {
    /// The key name for the Count-Min Sketch
    key: RedisKey,
    /// The items to query
    items: Vec<RedisJsonValue>,
}

impl Serialize for CmsQueryInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CmsQueryInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("items", &self.items)?;
        state.end()
    }
}

impl_redis_operation!(CmsQueryInput, API_INFO, { key, items });

impl RedisCommandInput for CmsQueryInput {
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

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("CMS.QUERY requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let items = args[1..].to_vec();

        Ok(Self { key, items })
    }
}

/// Output for Redis `CMS.QUERY` command.
///
/// Returns an array of counts for the queried items.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CmsQueryOutput {
    /// The count values for each queried item
    counts: Vec<i64>,
}

impl CmsQueryOutput {
    /// Create a new CmsQueryOutput
    pub fn new(counts: Vec<i64>) -> Self {
        Self { counts }
    }

    /// Get the counts
    pub fn counts(&self) -> &[i64] {
        &self.counts
    }

    /// Get a specific count by index
    pub fn get(&self, index: usize) -> Option<i64> {
        self.counts.get(index).copied()
    }

    /// Get the number of counts
    pub fn len(&self) -> usize {
        self.counts.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.counts.is_empty()
    }

    /// Decode the Redis protocol response into a CmsQueryOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        Self::parse_frame(frame)
    }

    fn parse_frame(frame: DecoderRespFrame) -> Result<Self, EpError> {
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame),
        }
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let counts = arr.into_iter().map(Self::extract_integer_resp2).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { counts })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected CMS.QUERY response: {:?}", other))),
        }
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let counts = data.into_iter().map(Self::extract_integer_resp3).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { counts })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected CMS.QUERY response: {:?}", other))),
        }
    }

    fn extract_integer_resp2(frame: Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(n),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data.to_vec()).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_integer_resp3(frame: Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data.to_vec()).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }
}

impl Serialize for CmsQueryOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CmsQueryOutput", 1)?;
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
            let input = CmsQueryInput {
                key: RedisKey::String("cms_key".into()),
                items: vec![RedisJsonValue::String("foo".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.QUERY"));
            assert!(cmd_str.contains("cms_key"));
            assert!(cmd_str.contains("foo"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = CmsQueryInput {
                key: RedisKey::String("cms_key".into()),
                items: vec![
                    RedisJsonValue::String("foo".into()),
                    RedisJsonValue::String("bar".into()),
                    RedisJsonValue::String("baz".into()),
                ],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.QUERY"));
            assert!(cmd_str.contains("foo"));
            assert!(cmd_str.contains("bar"));
            assert!(cmd_str.contains("baz"));
        }

        #[test]
        fn test_decode_output_single() {
            let output = CmsQueryOutput::decode(b"*1\r\n:5\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.get(0), Some(5));
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = CmsQueryOutput::decode(b"*3\r\n:5\r\n:10\r\n:0\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.counts(), &[5, 10, 0]);
        }

        #[test]
        fn test_decode_output_zeros() {
            let output = CmsQueryOutput::decode(b"*2\r\n:0\r\n:0\r\n").unwrap();
            assert_eq!(output.counts(), &[0, 0]);
        }

        #[test]
        fn test_decode_output_error() {
            let err = CmsQueryOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("foo".into())];
            let input = CmsQueryInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.items.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_items() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("foo".into()),
                RedisJsonValue::String("bar".into()),
                RedisJsonValue::String("baz".into()),
            ];
            let input = CmsQueryInput::decode(args).unwrap();
            assert_eq!(input.items.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = CmsQueryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = CmsQueryInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = CmsQueryInput {
                key: RedisKey::String("mykey".into()),
                items: vec![RedisJsonValue::String("foo".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = CmsQueryOutput::new(vec![5, 10, 15]);
            assert_eq!(output.len(), 3);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_empty() {
            let output = CmsQueryOutput::new(vec![]);
            assert!(output.is_empty());
        }

        #[test]
        fn test_output_serialize() {
            let output = CmsQueryOutput::new(vec![5, 10]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("counts"));
            assert!(json.contains("[5,10]"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::Incrby;
        use crate::api::lib::count_min_sketch::cms_incrby::CmsIncrbyInput;
        use crate::api::lib::count_min_sketch::cms_initbydim::CmsInitbydimInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_query_basic() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    // Create sketch first
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_query_test".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create_result) = create_result {
                        if create_result.starts_with(b"-") {
                            return; // Module not loaded
                        }

                        // Add some items
                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_query_test".into()),
                                incrby: vec![
                                    Incrby {
                                        item: RedisJsonValue::String("foo".into()),
                                        increment: RedisJsonValue::Integer(5),
                                    },
                                    Incrby {
                                        item: RedisJsonValue::String("bar".into()),
                                        increment: RedisJsonValue::Integer(10),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby failed");

                        // Query the items
                        let result = ctx
                            .raw(
                                &CmsQueryInput {
                                    key: RedisKey::String("cms_query_test".into()),
                                    items: vec![RedisJsonValue::String("foo".into()), RedisJsonValue::String("bar".into())],
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = CmsQueryOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 2);
                        assert_eq!(output.get(0), Some(5));
                        assert_eq!(output.get(1), Some(10));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_query_nonexistent_items() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_query_nonexist".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create_result) = create_result {
                        if create_result.starts_with(b"-") {
                            return;
                        }

                        // Query items that were never added
                        let result = ctx
                            .raw(
                                &CmsQueryInput {
                                    key: RedisKey::String("cms_query_nonexist".into()),
                                    items: vec![
                                        RedisJsonValue::String("never_added".into()),
                                        RedisJsonValue::String("also_not_added".into()),
                                    ],
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = CmsQueryOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 2);
                        assert_eq!(output.get(0), Some(0));
                        assert_eq!(output.get(1), Some(0));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_query_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_query_r2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                ctx.raw(
                    &CmsIncrbyInput {
                        key: RedisKey::String("cms_query_r2".into()),
                        incrby: vec![Incrby {
                            item: RedisJsonValue::String("test".into()),
                            increment: RedisJsonValue::Integer(7),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("incrby failed");

                let result = ctx
                    .raw(
                        &CmsQueryInput {
                            key: RedisKey::String("cms_query_r2".into()),
                            items: vec![RedisJsonValue::String("test".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                assert!(result.starts_with(b"*"), "RESP2 should return array");
                let output = CmsQueryOutput::decode(&result).expect("decode failed");
                assert_eq!(output.get(0), Some(7));
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_query_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_query_r3".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                ctx.raw(
                    &CmsIncrbyInput {
                        key: RedisKey::String("cms_query_r3".into()),
                        incrby: vec![Incrby {
                            item: RedisJsonValue::String("test".into()),
                            increment: RedisJsonValue::Integer(7),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("incrby failed");

                let result = ctx
                    .raw(
                        &CmsQueryInput {
                            key: RedisKey::String("cms_query_r3".into()),
                            items: vec![RedisJsonValue::String("test".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                let output = CmsQueryOutput::decode(&result).expect("decode failed");
                assert_eq!(output.get(0), Some(7));
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_query_pipeline() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_query_pipe".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create_result) = create_result {
                        if create_result.starts_with(b"-") {
                            return;
                        }

                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_query_pipe".into()),
                                incrby: vec![
                                    Incrby {
                                        item: RedisJsonValue::String("a".into()),
                                        increment: RedisJsonValue::Integer(1),
                                    },
                                    Incrby {
                                        item: RedisJsonValue::String("b".into()),
                                        increment: RedisJsonValue::Integer(2),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby failed");

                        let mut pipeline = Vec::new();
                        pipeline.extend_from_slice(
                            &CmsQueryInput {
                                key: RedisKey::String("cms_query_pipe".into()),
                                items: vec![RedisJsonValue::String("a".into())],
                            }
                            .command(),
                        );
                        pipeline.extend_from_slice(
                            &CmsQueryInput {
                                key: RedisKey::String("cms_query_pipe".into()),
                                items: vec![RedisJsonValue::String("b".into())],
                            }
                            .command(),
                        );

                        let result = ctx.raw(&pipeline).await.expect("raw failed");
                        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                        assert_eq!(responses.len(), 2);

                        let out1 = CmsQueryOutput::decode(responses[0]).expect("decode first");
                        assert_eq!(out1.get(0), Some(1));

                        let out2 = CmsQueryOutput::decode(responses[1]).expect("decode second");
                        assert_eq!(out2.get(0), Some(2));
                    }
                })
            })
            .await;
        }
    }
}
