use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Incrby, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, CmsIncrbyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CmsIncrby,
    "Increases the count of one or more items by increment",
    ReqType::Write,
    true,
);

/// Input for Redis `CMS.INCRBY` command.
///
/// Increases the count of one or more items in a Count-Min Sketch.
///
/// See official Redis documentation for `CMS.INCRBY`:
/// https://redis.io/docs/latest/commands/cms.incrby/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CmsIncrbyInput {
    /// The key name for the Count-Min Sketch
    pub(crate) key: RedisKey,
    /// List of item/increment pairs
    pub(crate) incrby: Vec<Incrby>,
}

impl Serialize for CmsIncrbyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CmsIncrbyInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("incrby", &self.incrby)?;
        state.end()
    }
}

impl_redis_operation!(CmsIncrbyInput, API_INFO, { key, incrby });

impl RedisCommandInput for CmsIncrbyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        for i in self.incrby.iter() {
            command.arg(&i.item).arg(&i.increment);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("CMS.INCRBY requires at least 3 arguments, given {}", args.len())));
        }

        if !(args.len() - 1).is_multiple_of(2) {
            return Err(EpError::parse("CMS.INCRBY requires pairs of item and increment values"));
        }

        let key = args[0].clone().try_into()?;
        let mut incrby = Vec::new();

        for chunk in args[1..].chunks(2) {
            incrby.push(Incrby { item: chunk[0].clone(), increment: chunk[1].clone() });
        }

        Ok(Self { key, incrby })
    }
}

/// Output for Redis `CMS.INCRBY` command.
///
/// Returns an array of counts after the increment operations.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CmsIncrbyOutput {
    /// The count values after incrementing (one per item)
    counts: Vec<i64>,
}

impl CmsIncrbyOutput {
    /// Create a new CmsIncrbyOutput
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

    /// Decode the Redis protocol response into a CmsIncrbyOutput
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
            other => Err(EpError::parse(format!("unexpected CMS.INCRBY response: {:?}", other))),
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
            other => Err(EpError::parse(format!("unexpected CMS.INCRBY response: {:?}", other))),
        }
    }

    fn extract_integer_resp2(frame: Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(n),
            Resp2Frame::BulkString(data) => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_integer_resp3(frame: Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(data),
            Resp3Frame::BlobString { data, .. } => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                s.parse().map_err(|_| EpError::parse("invalid integer"))
            }
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }
}

impl Serialize for CmsIncrbyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CmsIncrbyOutput", 1)?;
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
            let input = CmsIncrbyInput {
                key: RedisKey::String("cms_key".into()),
                incrby: vec![Incrby {
                    item: RedisJsonValue::String("foo".into()),
                    increment: RedisJsonValue::Integer(5),
                }],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.INCRBY"));
            assert!(cmd_str.contains("cms_key"));
            assert!(cmd_str.contains("foo"));
            assert!(cmd_str.contains("5"));
        }

        #[test]
        fn test_encode_command_multiple_items() {
            let input = CmsIncrbyInput {
                key: RedisKey::String("cms_key".into()),
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
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.INCRBY"));
            assert!(cmd_str.contains("foo"));
            assert!(cmd_str.contains("bar"));
        }

        #[test]
        fn test_decode_output_single() {
            let output = CmsIncrbyOutput::decode(b"*1\r\n:5\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.get(0), Some(5));
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = CmsIncrbyOutput::decode(b"*3\r\n:5\r\n:10\r\n:15\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.counts(), &[5, 10, 15]);
        }

        #[test]
        fn test_decode_output_error() {
            let err = CmsIncrbyOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("foo".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = CmsIncrbyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.incrby.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_pairs() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("foo".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("bar".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = CmsIncrbyInput::decode(args).unwrap();
            assert_eq!(input.incrby.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("foo".into())];
            let err = CmsIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_uneven_pairs() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("foo".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("bar".into()),
            ];
            let err = CmsIncrbyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("pairs"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = CmsIncrbyInput {
                key: RedisKey::String("mykey".into()),
                incrby: vec![Incrby {
                    item: RedisJsonValue::String("foo".into()),
                    increment: RedisJsonValue::Integer(5),
                }],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = CmsIncrbyOutput::new(vec![5, 10, 15]);
            assert_eq!(output.len(), 3);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_serialize() {
            let output = CmsIncrbyOutput::new(vec![5, 10]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("counts"));
            assert!(json.contains("[5,10]"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::count_min_sketch::cms_initbydim::CmsInitbydimInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_basic() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    // Create sketch first
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_incrby_test".into()),
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

                        let result = ctx
                            .raw(
                                &CmsIncrbyInput {
                                    key: RedisKey::String("cms_incrby_test".into()),
                                    incrby: vec![Incrby {
                                        item: RedisJsonValue::String("foo".into()),
                                        increment: RedisJsonValue::Integer(5),
                                    }],
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = CmsIncrbyOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 1);
                        assert_eq!(output.get(0), Some(5));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_multiple_items() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_incrby_multi".into()),
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

                        let result = ctx
                            .raw(
                                &CmsIncrbyInput {
                                    key: RedisKey::String("cms_incrby_multi".into()),
                                    incrby: vec![
                                        Incrby {
                                            item: RedisJsonValue::String("foo".into()),
                                            increment: RedisJsonValue::Integer(5),
                                        },
                                        Incrby {
                                            item: RedisJsonValue::String("bar".into()),
                                            increment: RedisJsonValue::Integer(10),
                                        },
                                        Incrby {
                                            item: RedisJsonValue::String("baz".into()),
                                            increment: RedisJsonValue::Integer(3),
                                        },
                                    ],
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = CmsIncrbyOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.len(), 3);
                        assert_eq!(output.counts(), &[5, 10, 3]);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_accumulates() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_incrby_accum".into()),
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

                        // First increment
                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_incrby_accum".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("foo".into()),
                                    increment: RedisJsonValue::Integer(5),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("first incrby failed");

                        // Second increment
                        let result = ctx
                            .raw(
                                &CmsIncrbyInput {
                                    key: RedisKey::String("cms_incrby_accum".into()),
                                    incrby: vec![Incrby {
                                        item: RedisJsonValue::String("foo".into()),
                                        increment: RedisJsonValue::Integer(3),
                                    }],
                                }
                                .command(),
                            )
                            .await
                            .expect("second incrby failed");

                        let output = CmsIncrbyOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.get(0), Some(8)); // 5 + 3
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_incrby_r2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                let result = ctx
                    .raw(
                        &CmsIncrbyInput {
                            key: RedisKey::String("cms_incrby_r2".into()),
                            incrby: vec![Incrby {
                                item: RedisJsonValue::String("foo".into()),
                                increment: RedisJsonValue::Integer(7),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                assert!(result.starts_with(b"*"), "RESP2 should return array");
                let output = CmsIncrbyOutput::decode(&result).expect("decode failed");
                assert_eq!(output.get(0), Some(7));
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let create_result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_incrby_r3".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create_result) = create_result
                && !create_result.starts_with(b"-")
            {
                let result = ctx
                    .raw(
                        &CmsIncrbyInput {
                            key: RedisKey::String("cms_incrby_r3".into()),
                            incrby: vec![Incrby {
                                item: RedisJsonValue::String("foo".into()),
                                increment: RedisJsonValue::Integer(7),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                let output = CmsIncrbyOutput::decode(&result).expect("decode failed");
                assert_eq!(output.get(0), Some(7));
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_incrby_pipeline() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create_result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_incrby_pipe".into()),
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

                        let mut pipeline = Vec::new();
                        pipeline.extend_from_slice(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_incrby_pipe".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("a".into()),
                                    increment: RedisJsonValue::Integer(1),
                                }],
                            }
                            .command(),
                        );
                        pipeline.extend_from_slice(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_incrby_pipe".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("b".into()),
                                    increment: RedisJsonValue::Integer(2),
                                }],
                            }
                            .command(),
                        );

                        let result = ctx.raw(&pipeline).await.expect("raw failed");
                        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                        assert_eq!(responses.len(), 2);

                        let out1 = CmsIncrbyOutput::decode(responses[0]).expect("decode first");
                        assert_eq!(out1.get(0), Some(1));

                        let out2 = CmsIncrbyOutput::decode(responses[1]).expect("decode second");
                        assert_eq!(out2.get(0), Some(2));
                    }
                })
            })
            .await;
        }
    }
}
