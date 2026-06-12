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

const API_INFO: ApiInfo<RedisApi, XgroupCreateconsumerInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XgroupCreateconsumer,
    "Creates a consumer in a consumer group",
    ReqType::Write,
    true,
);

/// Input for Redis `XGROUP CREATECONSUMER` command.
///
/// Creates a consumer in the specified consumer group. Consumers are automatically
/// created when reading from a group, but this command allows explicit creation.
///
/// See official Redis documentation for `XGROUP CREATECONSUMER`:
/// https://redis.io/docs/latest/commands/xgroup-createconsumer/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XgroupCreateconsumerInput {
    /// The key of the stream
    key: RedisKey,
    /// The name of the consumer group
    group: RedisJsonValue,
    /// The name of the consumer to create
    consumer: RedisJsonValue,
}

impl Serialize for XgroupCreateconsumerInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupCreateconsumerInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("consumer", &self.consumer)?;
        state.end()
    }
}

impl_redis_operation!(
    XgroupCreateconsumerInput,
    API_INFO,
    {key, group, consumer}
);

impl RedisCommandInput for XgroupCreateconsumerInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        // XGROUP CREATECONSUMER is a subcommand: XGROUP CREATECONSUMER <key> <group> <consumer>
        let mut command = crate::command::cmd("XGROUP");
        command.arg("CREATECONSUMER");
        command.arg(&self.key).arg(&self.group).arg(&self.consumer);

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::parse(format!(
                "XGROUP CREATECONSUMER requires exactly 3 arguments (key, group, consumer), given {}",
                args.len()
            )));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            group: args[1].clone(),
            consumer: args[2].clone(),
        })
    }
}

/// Output for Redis `XGROUP CREATECONSUMER` command.
///
/// Returns 1 if the consumer was created, 0 if it already existed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XgroupCreateconsumerOutput {
    /// Whether a new consumer was created (1) or already existed (0)
    created: i64,
}

impl XgroupCreateconsumerOutput {
    /// Create a new XgroupCreateconsumerOutput
    pub fn new(created: i64) -> Self {
        Self { created }
    }

    /// Get the created count (1 if new consumer, 0 if existed)
    pub fn created(&self) -> i64 {
        self.created
    }

    /// Check if a new consumer was created
    pub fn was_created(&self) -> bool {
        self.created == 1
    }

    /// Decode the Redis protocol response into an XgroupCreateconsumerOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let created = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XGROUP CREATECONSUMER response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XGROUP CREATECONSUMER response: {:?}", other)));
                }
            },
        };

        Ok(Self { created })
    }
}

impl Serialize for XgroupCreateconsumerOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupCreateconsumerOutput", 1)?;
        state.serialize_field("created", &self.created)?;
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
            let input = XgroupCreateconsumerInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
            };
            let cmd = input.command();

            // Verify against manually constructed protocol
            // *5\r\n$6\r\nXGROUP\r\n$14\r\nCREATECONSUMER\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$10\r\nmyconsumer\r\n
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$14\r\nCREATECONSUMER\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$10\r\nmyconsumer\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_encode_command_short_names() {
            let input = XgroupCreateconsumerInput {
                key: RedisKey::String("s".into()),
                group: RedisJsonValue::String("g".into()),
                consumer: RedisJsonValue::String("c".into()),
            };
            let cmd = input.command();

            // *5\r\n$6\r\nXGROUP\r\n$14\r\nCREATECONSUMER\r\n$1\r\ns\r\n$1\r\ng\r\n$1\r\nc\r\n
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$14\r\nCREATECONSUMER\r\n$1\r\ns\r\n$1\r\ng\r\n$1\r\nc\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_decode_output_one() {
            let output = XgroupCreateconsumerOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.created(), 1);
            assert!(output.was_created());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XgroupCreateconsumerOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.created(), 0);
            assert!(!output.was_created());
        }

        #[test]
        fn test_decode_output_error_nogroup() {
            let err =
                XgroupCreateconsumerOutput::decode(b"-NOGROUP No such consumer group 'mygroup' for key name 'mystream'\r\n").unwrap_err();
            assert!(err.to_string().contains("NOGROUP"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XgroupCreateconsumerInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
            ];
            let input = XgroupCreateconsumerInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.group, RedisJsonValue::String("mygroup".into()));
            assert_eq!(input.consumer, RedisJsonValue::String("myconsumer".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let err = XgroupCreateconsumerInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = XgroupCreateconsumerInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XgroupCreateconsumerInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_output_new() {
            let output = XgroupCreateconsumerOutput::new(1);
            assert_eq!(output.created(), 1);
            assert!(output.was_created());
        }

        #[test]
        fn test_output_serialize() {
            let output = XgroupCreateconsumerOutput::new(1);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"created\":1"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

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

        async fn create_group(ctx: &mut TestContext, key: &str, group: &str) {
            let cmd = format!(
                "*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n$\r\n",
                key.len(),
                key,
                group.len(),
                group
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_basic() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgcc_basic", "field", "value").await;
                    create_group(ctx, "xgcc_basic", "testgroup").await;

                    let result = ctx
                        .raw(
                            &XgroupCreateconsumerInput {
                                key: RedisKey::String("xgcc_basic".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupCreateconsumerOutput::decode(&result).expect("decode failed");
                    assert!(output.was_created());
                    assert_eq!(output.created(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_already_exists() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgcc_exists", "field", "value").await;
                    create_group(ctx, "xgcc_exists", "testgroup").await;

                    ctx.raw(
                        &XgroupCreateconsumerInput {
                            key: RedisKey::String("xgcc_exists".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            consumer: RedisJsonValue::String("consumer1".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &XgroupCreateconsumerInput {
                                key: RedisKey::String("xgcc_exists".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupCreateconsumerOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_created());
                    assert_eq!(output.created(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_no_group() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgcc_nogroup", "field", "value").await;

                    let result = ctx
                        .raw(
                            &XgroupCreateconsumerInput {
                                key: RedisKey::String("xgcc_nogroup".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XgroupCreateconsumerOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("NOGROUP"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xgcc_r2", "field", "value").await;
            create_group(&mut ctx, "xgcc_r2", "testgroup").await;

            let result = ctx
                .raw(
                    &XgroupCreateconsumerInput {
                        key: RedisKey::String("xgcc_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer1".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = XgroupCreateconsumerOutput::decode(&result).expect("decode failed");
            assert!(output.was_created());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xgcc_r3", "field", "value").await;
            create_group(&mut ctx, "xgcc_r3", "testgroup").await;

            let result = ctx
                .raw(
                    &XgroupCreateconsumerInput {
                        key: RedisKey::String("xgcc_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer1".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XgroupCreateconsumerOutput::decode(&result).expect("decode failed");
            assert!(output.was_created());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_createconsumer_pipeline() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgcc_pipe", "field", "value").await;
                    create_group(ctx, "xgcc_pipe", "testgroup").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XgroupCreateconsumerInput {
                            key: RedisKey::String("xgcc_pipe".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            consumer: RedisJsonValue::String("consumer1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XgroupCreateconsumerInput {
                            key: RedisKey::String("xgcc_pipe".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            consumer: RedisJsonValue::String("consumer2".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XgroupCreateconsumerOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.was_created());

                    let out2 = XgroupCreateconsumerOutput::decode(responses[1]).expect("decode second");
                    assert!(out2.was_created());
                })
            })
            .await;
        }
    }
}
