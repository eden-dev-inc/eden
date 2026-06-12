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

const API_INFO: ApiInfo<RedisApi, CmsMergeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CmsMerge,
    "Merges several Count-Min Sketches into one sketch",
    ReqType::Write,
    true,
);

/// Input for Redis `CMS.MERGE` command.
///
/// Merges multiple Count-Min Sketches into a destination sketch.
///
/// See official Redis documentation for `CMS.MERGE`:
/// https://redis.io/docs/latest/commands/cms.merge/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CmsMergeInput {
    /// The destination key for the merged sketch
    destination: RedisKey,
    /// Number of source sketches to merge
    numkeys: RedisJsonValue,
    /// Source sketch keys
    sources: Vec<RedisKey>,
    /// Optional weights for each source sketch
    #[serde(skip_serializing_if = "Option::is_none")]
    weights: Option<Vec<RedisJsonValue>>,
}

impl Serialize for CmsMergeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.weights.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("CmsMergeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("destination", &self.destination)?;
        state.serialize_field("numkeys", &self.numkeys)?;
        state.serialize_field("sources", &self.sources)?;
        if let Some(weights) = &self.weights {
            state.serialize_field("weights", weights)?;
        }
        state.end()
    }
}

impl_redis_operation!(CmsMergeInput, API_INFO, { destination, numkeys, sources, weights });

impl RedisCommandInput for CmsMergeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut keys = vec![self.destination.clone()];
        keys.extend(self.sources.clone());
        keys
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.destination).arg(&self.numkeys).arg(&self.sources);

        if let Some(weights) = &self.weights {
            command.arg("WEIGHTS").arg(weights);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("CMS.MERGE requires at least 3 arguments, given {}", args.len())));
        }

        let destination = args[0].clone().try_into()?;
        let numkeys = args[1].clone();

        let num_sources = match &numkeys {
            RedisJsonValue::Integer(n) => *n as usize,
            RedisJsonValue::String(s) => s.parse::<usize>().map_err(|_| EpError::parse("numkeys must be an integer"))?,
            _ => return Err(EpError::parse("numkeys must be an integer")),
        };

        if args.len() < 2 + num_sources {
            return Err(EpError::parse("Insufficient source keys"));
        }

        let mut sources = vec![];
        for source in args[2..2 + num_sources].iter() {
            sources.push(source.clone().try_into()?);
        }

        let weights = if args.len() > 2 + num_sources {
            if let RedisJsonValue::String(cmd) = &args[2 + num_sources] {
                if cmd.to_uppercase() == "WEIGHTS" {
                    if args.len() >= 3 + num_sources + num_sources {
                        Some(args[3 + num_sources..3 + num_sources + num_sources].to_vec())
                    } else {
                        return Err(EpError::parse("Insufficient weights"));
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { destination, numkeys, sources, weights })
    }
}

/// Output for Redis `CMS.MERGE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CmsMergeOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl CmsMergeOutput {
    /// Create a new CmsMergeOutput
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation succeeded
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a CmsMergeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CMS.MERGE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CMS.MERGE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for CmsMergeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CmsMergeOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = CmsMergeInput {
                destination: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(2),
                sources: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
                weights: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.MERGE"));
            assert!(cmd_str.contains("dest"));
            assert!(cmd_str.contains("src1"));
            assert!(cmd_str.contains("src2"));
            assert!(!cmd_str.contains("WEIGHTS"));
        }

        #[test]
        fn test_encode_command_with_weights() {
            let input = CmsMergeInput {
                destination: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(2),
                sources: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
                weights: Some(vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)]),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.MERGE"));
            assert!(cmd_str.contains("WEIGHTS"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = CmsMergeOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CmsMergeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let input = CmsMergeInput::decode(args).unwrap();
            assert_eq!(input.destination, RedisKey::String("dest".into()));
            assert_eq!(input.sources.len(), 2);
            assert!(input.weights.is_none());
        }

        #[test]
        fn test_decode_input_with_weights() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
                RedisJsonValue::String("WEIGHTS".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(2),
            ];
            let input = CmsMergeInput::decode(args).unwrap();
            assert_eq!(input.sources.len(), 2);
            assert!(input.weights.is_some());
            assert_eq!(input.weights.unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("dest".into()), RedisJsonValue::Integer(2)];
            let err = CmsMergeInput::decode(args).unwrap_err();
            println!("{err}");
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_insufficient_sources() {
            let args = vec![
                RedisJsonValue::String("dest".into()),
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("src1".into()),
                RedisJsonValue::String("src2".into()),
            ];
            let err = CmsMergeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Insufficient"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = CmsMergeInput {
                destination: RedisKey::String("dest".into()),
                numkeys: RedisJsonValue::Integer(2),
                sources: vec![RedisKey::String("src1".into()), RedisKey::String("src2".into())],
                weights: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 3); // destination + 2 sources
            assert_eq!(keys[0], RedisKey::String("dest".into()));
            assert_eq!(keys[1], RedisKey::String("src1".into()));
            assert_eq!(keys[2], RedisKey::String("src2".into()));
        }

        #[test]
        fn test_output_new() {
            let output = CmsMergeOutput::new(true);
            assert!(output.is_success());
        }

        #[test]
        fn test_output_serialize() {
            let output = CmsMergeOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::Incrby;
        use crate::api::lib::count_min_sketch::cms_incrby::CmsIncrbyInput;
        use crate::api::lib::count_min_sketch::cms_info::{CmsInfoInput, CmsInfoOutput};
        use crate::api::lib::count_min_sketch::cms_initbydim::CmsInitbydimInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_merge_basic() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create1 = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_merge_src1".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create1) = create1 {
                        if create1.starts_with(b"-") {
                            return;
                        }

                        ctx.raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_merge_src2".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("create src2 failed");

                        ctx.raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_merge_dest".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("create dest failed");

                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_merge_src1".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("foo".into()),
                                    increment: RedisJsonValue::Integer(5),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby src1 failed");

                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_merge_src2".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("foo".into()),
                                    increment: RedisJsonValue::Integer(10),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby src2 failed");

                        let result = ctx
                            .raw(
                                &CmsMergeInput {
                                    destination: RedisKey::String("cms_merge_dest".into()),
                                    numkeys: RedisJsonValue::Integer(2),
                                    sources: vec![RedisKey::String("cms_merge_src1".into()), RedisKey::String("cms_merge_src2".into())],
                                    weights: None,
                                }
                                .command(),
                            )
                            .await
                            .expect("merge failed");

                        let output = CmsMergeOutput::decode(&result).expect("decode failed");
                        assert!(output.is_success());

                        let info_result =
                            ctx.raw(&CmsInfoInput { key: RedisKey::String("cms_merge_dest".into()) }.command()).await.expect("info failed");

                        let info = CmsInfoOutput::decode(&info_result).expect("decode info failed");
                        assert_eq!(info.count(), 15);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_merge_with_weights() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let create1 = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_wmerge_src1".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(create1) = create1 {
                        if create1.starts_with(b"-") {
                            return;
                        }

                        ctx.raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_wmerge_src2".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("create src2 failed");

                        ctx.raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_wmerge_dest".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await
                        .expect("create dest failed");

                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_wmerge_src1".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("item".into()),
                                    increment: RedisJsonValue::Integer(10),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby src1 failed");

                        ctx.raw(
                            &CmsIncrbyInput {
                                key: RedisKey::String("cms_wmerge_src2".into()),
                                incrby: vec![Incrby {
                                    item: RedisJsonValue::String("item".into()),
                                    increment: RedisJsonValue::Integer(10),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("incrby src2 failed");

                        let result = ctx
                            .raw(
                                &CmsMergeInput {
                                    destination: RedisKey::String("cms_wmerge_dest".into()),
                                    numkeys: RedisJsonValue::Integer(2),
                                    sources: vec![
                                        RedisKey::String("cms_wmerge_src1".into()),
                                        RedisKey::String("cms_wmerge_src2".into()),
                                    ],
                                    weights: Some(vec![RedisJsonValue::Integer(2), RedisJsonValue::Integer(3)]),
                                }
                                .command(),
                            )
                            .await
                            .expect("merge failed");

                        let output = CmsMergeOutput::decode(&result).expect("decode failed");
                        assert!(output.is_success());

                        let info_result = ctx
                            .raw(&CmsInfoInput { key: RedisKey::String("cms_wmerge_dest".into()) }.command())
                            .await
                            .expect("info failed");

                        let info = CmsInfoOutput::decode(&info_result).expect("decode info failed");
                        assert_eq!(info.count(), 50);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_merge_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let create1 = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_merge_r2_1".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create1) = create1
                && !create1.starts_with(b"-")
            {
                ctx.raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_merge_r2_2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await
                .expect("create src2 failed");

                let result = ctx
                    .raw(
                        &CmsMergeInput {
                            destination: RedisKey::String("cms_merge_r2_dest".into()),
                            numkeys: RedisJsonValue::Integer(2),
                            sources: vec![RedisKey::String("cms_merge_r2_1".into()), RedisKey::String("cms_merge_r2_2".into())],
                            weights: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("merge failed");

                assert!(result.starts_with(b"+"), "RESP2 should return simple string");
                let output = CmsMergeOutput::decode(&result).expect("decode failed");
                assert!(output.is_success());
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_merge_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let create1 = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_merge_r3_1".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(create1) = create1
                && !create1.starts_with(b"-")
            {
                ctx.raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_merge_r3_2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await
                .expect("create src2 failed");

                let result = ctx
                    .raw(
                        &CmsMergeInput {
                            destination: RedisKey::String("cms_merge_r3_dest".into()),
                            numkeys: RedisJsonValue::Integer(2),
                            sources: vec![RedisKey::String("cms_merge_r3_1".into()), RedisKey::String("cms_merge_r3_2".into())],
                            weights: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("merge failed");

                let output = CmsMergeOutput::decode(&result).expect("decode failed");
                assert!(output.is_success());
            }

            ctx.stop().await;
        }
    }
}
