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

const API_INFO: ApiInfo<RedisApi, XdelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xdel,
    "Removes the specified entries from a stream, and returns the number of entries deleted.",
    ReqType::Write,
    true,
);

/// Input for Redis `XDEL` command.
///
/// Removes one or more entries from a stream.
///
/// See official Redis documentation for `XDEL`:
/// https://redis.io/docs/latest/commands/xdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XdelInput {
    /// The key of the stream
    key: RedisKey,
    /// The IDs of the entries to delete
    ids: Vec<RedisJsonValue>,
}

impl Serialize for XdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XdelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("ids", &self.ids)?;
        state.end()
    }
}

impl_redis_operation!(XdelInput, API_INFO, { key, ids });

impl RedisCommandInput for XdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        for id in &self.ids {
            command.arg(id);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!(
                "XDEL requires at least 2 arguments (key and at least one ID), given {}",
                args.len()
            )));
        }

        Ok(Self { key: args[0].clone().try_into()?, ids: args[1..].to_vec() })
    }
}

/// Output for Redis `XDEL` command.
///
/// Returns the number of entries actually deleted from the stream.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XdelOutput {
    /// The number of entries that were deleted
    deleted: i64,
}

impl XdelOutput {
    /// Create a new XdelOutput
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of deleted entries
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into an XdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XDEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XDEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for XdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XdelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_id() {
            let input = XdelInput {
                key: RedisKey::String("mystream".into()),
                ids: vec![RedisJsonValue::String("1234567890123-0".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n"));
            assert!(cmd.windows(4).any(|w| w == b"XDEL"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_encode_command_multiple_ids() {
            let input = XdelInput {
                key: RedisKey::String("mystream".into()),
                ids: vec![
                    RedisJsonValue::String("1234567890123-0".into()),
                    RedisJsonValue::String("1234567890124-0".into()),
                    RedisJsonValue::String("1234567890125-0".into()),
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n")); // XDEL + key + 3 IDs
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XdelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_output_one() {
            let output = XdelOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.deleted(), 1);
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = XdelOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.deleted(), 5);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XdelOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XdelInput {
                key: RedisKey::String("mystream".into()),
                ids: vec![RedisJsonValue::String("1-0".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_single_id() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("1234567890123-0".into()),
            ];
            let input = XdelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.ids.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_ids() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("1-0".into()),
                RedisJsonValue::String("2-0".into()),
                RedisJsonValue::String("3-0".into()),
            ];
            let input = XdelInput::decode(args).unwrap();
            assert_eq!(input.ids.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_output_new() {
            let output = XdelOutput::new(3);
            assert_eq!(output.deleted(), 3);
        }

        #[test]
        fn test_output_serialize() {
            let output = XdelOutput::new(2);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"deleted\":2"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Helper to create a stream entry using XADD
        async fn xadd_entry(ctx: &mut TestContext, key: &str, field: &str, value: &str) -> String {
            let cmd = format!(
                "*5\r\n$4\r\nXADD\r\n${}\r\n{}\r\n$1\r\n*\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                field.len(),
                field,
                value.len(),
                value
            );
            let result = ctx.raw(cmd.as_bytes()).await.expect("XADD failed");
            // Parse the returned ID from bulk string response
            let response = String::from_utf8_lossy(&result);
            // Extract ID from response like "$15\r\n1234567890123-0\r\n" or "+1234567890123-0\r\n"
            if response.starts_with('$') {
                response.lines().nth(1).unwrap_or("").trim().to_string()
            } else if let Some(stripped) = response.strip_prefix('+') {
                stripped.trim().to_string()
            } else {
                response.trim().to_string()
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_nonexistent_stream() {
            // Streams require Redis 5.0+
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("nonexistent_stream".into()),
                                ids: vec![RedisJsonValue::String("1234567890123-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0, "deleting from nonexistent stream should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_nonexistent_id() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream with one entry
                    xadd_entry(ctx, "xdel_test_stream", "field", "value").await;

                    // Try to delete a non-existent ID
                    let result = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_test_stream".into()),
                                ids: vec![RedisJsonValue::String("9999999999999-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_single_entry() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream with one entry
                    let id = xadd_entry(ctx, "xdel_single", "field", "value").await;

                    // Delete the entry
                    let result = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_single".into()),
                                ids: vec![RedisJsonValue::String(id)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_multiple_entries() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream with multiple entries
                    let id1 = xadd_entry(ctx, "xdel_multi", "field1", "value1").await;
                    let _id2 = xadd_entry(ctx, "xdel_multi", "field2", "value2").await;
                    let id3 = xadd_entry(ctx, "xdel_multi", "field3", "value3").await;

                    // Delete two entries
                    let result = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_multi".into()),
                                ids: vec![RedisJsonValue::String(id1), RedisJsonValue::String(id3)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 2);

                    // Verify only id2 remains by checking stream length
                    let xlen_result =
                        ctx.raw("*2\r\n$4\r\nXLEN\r\n$10\r\nxdel_multi\r\n".to_string().as_bytes()).await.expect("XLEN failed");

                    // Parse integer response
                    let len_str = String::from_utf8_lossy(&xlen_result);
                    assert!(len_str.contains(":1\r\n"), "stream should have 1 entry remaining");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_idempotent() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream with one entry
                    let id = xadd_entry(ctx, "xdel_idemp", "field", "value").await;

                    // First delete
                    let result1 = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_idemp".into()),
                                ids: vec![RedisJsonValue::String(id.clone())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output1 = XdelOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.deleted(), 1);

                    // Second delete (already gone)
                    let result2 = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_idemp".into()),
                                ids: vec![RedisJsonValue::String(id)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output2 = XdelOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_mixed_existing_nonexisting() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream with entries
                    let id1 = xadd_entry(ctx, "xdel_mixed", "field1", "value1").await;
                    let _id2 = xadd_entry(ctx, "xdel_mixed", "field2", "value2").await;

                    // Delete one existing and one non-existing
                    let result = ctx
                        .raw(
                            &XdelInput {
                                key: RedisKey::String("xdel_mixed".into()),
                                ids: vec![RedisJsonValue::String(id1), RedisJsonValue::String("9999999999999-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XdelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1, "should only count actually deleted entries");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Create stream entry
            let id = xadd_entry(&mut ctx, "xdel_r2", "field", "value").await;

            let result = ctx
                .raw(
                    &XdelInput {
                        key: RedisKey::String("xdel_r2".into()),
                        ids: vec![RedisJsonValue::String(id)],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = XdelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Create stream entry
            let id = xadd_entry(&mut ctx, "xdel_r3", "field", "value").await;

            let result = ctx
                .raw(
                    &XdelInput {
                        key: RedisKey::String("xdel_r3".into()),
                        ids: vec![RedisJsonValue::String(id)],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XdelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xdel_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create entries
                    let id1 = xadd_entry(ctx, "xdel_pipe", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xdel_pipe", "f2", "v2").await;

                    // Pipeline two XDEL commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XdelInput {
                            key: RedisKey::String("xdel_pipe".into()),
                            ids: vec![RedisJsonValue::String(id1)],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XdelInput {
                            key: RedisKey::String("xdel_pipe".into()),
                            ids: vec![RedisJsonValue::String(id2)],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XdelOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.deleted(), 1);

                    let out2 = XdelOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.deleted(), 1);
                })
            })
            .await;
        }
    }
}
