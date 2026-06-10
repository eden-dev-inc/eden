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

const API_INFO: ApiInfo<RedisApi, XackInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xack,
    "Acknowledges one or more messages as processed, removing them from the pending entries list (PEL) of the consumer group.",
    ReqType::Write,
    true,
);

/// Input for Redis `XACK` command.
///
/// Acknowledges messages in a consumer group, marking them as successfully processed.
///
/// See official Redis documentation for `XACK`:
/// https://redis.io/docs/latest/commands/xack/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XackInput {
    /// The key of the stream
    key: RedisKey,
    /// The consumer group name
    group: RedisJsonValue,
    /// The message IDs to acknowledge
    ids: Vec<RedisJsonValue>,
}

impl Serialize for XackInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XackInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("ids", &self.ids)?;
        state.end()
    }
}

impl_redis_operation!(XackInput, API_INFO, { key, group, ids });

impl RedisCommandInput for XackInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.group);
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
        if args.len() < 3 {
            return Err(EpError::parse(format!(
                "XACK requires at least 3 arguments (key, group, and at least one ID), given {}",
                args.len()
            )));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            group: args[1].clone(),
            ids: args[2..].to_vec(),
        })
    }
}

/// Output for Redis `XACK` command.
///
/// Returns the number of messages successfully acknowledged.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XackOutput {
    /// The number of messages that were successfully acknowledged
    acknowledged: i64,
}

impl XackOutput {
    /// Create a new XackOutput
    pub fn new(acknowledged: i64) -> Self {
        Self { acknowledged }
    }

    /// Get the number of acknowledged messages
    pub fn acknowledged(&self) -> i64 {
        self.acknowledged
    }

    /// Decode the Redis protocol response into an XackOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let acknowledged = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XACK response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XACK response: {:?}", other)));
                }
            },
        };

        Ok(Self { acknowledged })
    }
}

impl Serialize for XackOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XackOutput", 1)?;
        state.serialize_field("acknowledged", &self.acknowledged)?;
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
            let input = XackInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                ids: vec![RedisJsonValue::String("1234567890123-0".into())],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            assert!(cmd.windows(4).any(|w| w == b"XACK"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
            assert!(cmd.windows(7).any(|w| w == b"mygroup"));
        }

        #[test]
        fn test_encode_command_multiple_ids() {
            let input = XackInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                ids: vec![
                    RedisJsonValue::String("1-0".into()),
                    RedisJsonValue::String("2-0".into()),
                    RedisJsonValue::String("3-0".into()),
                ],
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n")); // XACK + key + group + 3 IDs
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XackOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.acknowledged(), 0);
        }

        #[test]
        fn test_decode_output_one() {
            let output = XackOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.acknowledged(), 1);
        }

        #[test]
        fn test_decode_output_multiple() {
            let output = XackOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.acknowledged(), 5);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XackOutput::decode(b"-NOGROUP No such consumer group\r\n").unwrap_err();
            assert!(err.to_string().contains("NOGROUP"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XackInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
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
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("1234567890123-0".into()),
            ];
            let input = XackInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.group, RedisJsonValue::String("mygroup".into()));
            assert_eq!(input.ids.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_ids() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("1-0".into()),
                RedisJsonValue::String("2-0".into()),
                RedisJsonValue::String("3-0".into()),
            ];
            let input = XackInput::decode(args).unwrap();
            assert_eq!(input.ids.len(), 3);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let err = XackInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XackInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_output_new() {
            let output = XackOutput::new(3);
            assert_eq!(output.acknowledged(), 3);
        }

        #[test]
        fn test_output_serialize() {
            let output = XackOutput::new(2);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"acknowledged\":2"));
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
            let response = String::from_utf8_lossy(&result);
            if response.starts_with('$') {
                response.lines().nth(1).unwrap_or("").trim().to_string()
            } else if let Some(stripped) = response.strip_prefix('+') {
                stripped.trim().to_string()
            } else {
                response.trim().to_string()
            }
        }

        // Helper to create a consumer group starting from ID 0 (includes all existing entries)
        // Uses MKSTREAM to create the stream if it doesn't exist
        async fn create_group(ctx: &mut TestContext, key: &str, group: &str) {
            let cmd = format!(
                "*6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n0\r\n$8\r\nMKSTREAM\r\n",
                key.len(),
                key,
                group.len(),
                group
            );
            // Ignore errors if group already exists
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        // Helper to read from a consumer group (to put message in PEL)
        async fn xreadgroup(ctx: &mut TestContext, group: &str, consumer: &str, key: &str, id: &str) {
            let cmd = format!(
                "*7\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n${}\r\n{}\r\n${}\r\n{}\r\n$7\r\nSTREAMS\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                group.len(),
                group,
                consumer.len(),
                consumer,
                key.len(),
                key,
                id.len(),
                id
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_no_group() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream first
                    xadd_entry(ctx, "xack_nogroup", "field", "value").await;

                    // Try to ack on non-existent group
                    let result = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_nogroup".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                                ids: vec![RedisJsonValue::String("1-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // XACK returns 0 for non-existent group (doesn't error)
                    let output = XackOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_nonexistent_id() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream and group
                    xadd_entry(ctx, "xack_noid", "field", "value").await;
                    create_group(ctx, "xack_noid", "testgroup").await;

                    // Try to ack a non-existent ID
                    let result = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_noid".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![RedisJsonValue::String("9999999999999-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XackOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_single_message() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream, group, and add entry
                    let id = xadd_entry(ctx, "xack_single", "field", "value").await;
                    create_group(ctx, "xack_single", "testgroup").await;

                    // Read to put in PEL
                    xreadgroup(ctx, "testgroup", "consumer1", "xack_single", ">").await;

                    // Acknowledge the message
                    let result = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_single".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![RedisJsonValue::String(id)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XackOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.acknowledged(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_multiple_messages() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream and group
                    let id1 = xadd_entry(ctx, "xack_multi", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xack_multi", "f2", "v2").await;
                    let id3 = xadd_entry(ctx, "xack_multi", "f3", "v3").await;
                    create_group(ctx, "xack_multi", "testgroup").await;

                    // Read all to put in PEL
                    xreadgroup(ctx, "testgroup", "consumer1", "xack_multi", ">").await;

                    // Acknowledge multiple messages
                    let result = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_multi".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![
                                    RedisJsonValue::String(id1),
                                    RedisJsonValue::String(id2),
                                    RedisJsonValue::String(id3),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XackOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.acknowledged(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_idempotent() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream and group
                    let id = xadd_entry(ctx, "xack_idemp", "field", "value").await;
                    create_group(ctx, "xack_idemp", "testgroup").await;

                    // Read to put in PEL
                    xreadgroup(ctx, "testgroup", "consumer1", "xack_idemp", ">").await;

                    // First ack
                    let result1 = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_idemp".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![RedisJsonValue::String(id.clone())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output1 = XackOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.acknowledged(), 1);

                    // Second ack (already acknowledged)
                    let result2 = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_idemp".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![RedisJsonValue::String(id)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output2 = XackOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.acknowledged(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_mixed_existing_nonexisting() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream and group
                    let id1 = xadd_entry(ctx, "xack_mixed", "f1", "v1").await;
                    let _id2 = xadd_entry(ctx, "xack_mixed", "f2", "v2").await;
                    create_group(ctx, "xack_mixed", "testgroup").await;

                    // Read to put in PEL
                    xreadgroup(ctx, "testgroup", "consumer1", "xack_mixed", ">").await;

                    // Ack one real and one fake ID
                    let result = ctx
                        .raw(
                            &XackInput {
                                key: RedisKey::String("xack_mixed".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                ids: vec![RedisJsonValue::String(id1), RedisJsonValue::String("9999999999999-0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XackOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.acknowledged(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            // Setup
            let id = xadd_entry(&mut ctx, "xack_r2", "field", "value").await;
            create_group(&mut ctx, "xack_r2", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xack_r2", ">").await;

            let result = ctx
                .raw(
                    &XackInput {
                        key: RedisKey::String("xack_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        ids: vec![RedisJsonValue::String(id)],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = XackOutput::decode(&result).expect("decode failed");
            assert_eq!(output.acknowledged(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            // Setup
            let id = xadd_entry(&mut ctx, "xack_r3", "field", "value").await;
            create_group(&mut ctx, "xack_r3", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xack_r3", ">").await;

            let result = ctx
                .raw(
                    &XackInput {
                        key: RedisKey::String("xack_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        ids: vec![RedisJsonValue::String(id)],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XackOutput::decode(&result).expect("decode failed");
            assert_eq!(output.acknowledged(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xack_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Setup
                    let id1 = xadd_entry(ctx, "xack_pipe", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xack_pipe", "f2", "v2").await;
                    create_group(ctx, "xack_pipe", "testgroup").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xack_pipe", ">").await;

                    // Pipeline two XACK commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XackInput {
                            key: RedisKey::String("xack_pipe".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            ids: vec![RedisJsonValue::String(id1)],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XackInput {
                            key: RedisKey::String("xack_pipe".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            ids: vec![RedisJsonValue::String(id2)],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XackOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.acknowledged(), 1);

                    let out2 = XackOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.acknowledged(), 1);
                })
            })
            .await;
        }
    }
}
