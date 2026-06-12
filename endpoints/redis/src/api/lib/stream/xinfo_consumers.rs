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

const API_INFO: ApiInfo<RedisApi, XinfoConsumersInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XinfoConsumers,
    "Returns a list of the consumers in a consumer group",
    ReqType::Read,
    true,
);

/// Input for Redis `XINFO CONSUMERS` command.
///
/// Returns information about the consumers in a consumer group.
///
/// See official Redis documentation for `XINFO CONSUMERS`:
/// https://redis.io/docs/latest/commands/xinfo-consumers/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XinfoConsumersInput {
    /// The key of the stream
    key: RedisKey,
    /// The consumer group name
    group: RedisJsonValue,
}

impl Serialize for XinfoConsumersInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XinfoConsumersInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.end()
    }
}

impl_redis_operation!(XinfoConsumersInput, API_INFO, { key, group });

impl RedisCommandInput for XinfoConsumersInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.group);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::parse(format!("XINFO CONSUMERS requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, group: args[1].clone() })
    }
}

/// Information about a single consumer
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ConsumerInfo {
    /// The consumer name
    pub name: String,
    /// The number of pending messages for this consumer
    pub pending: i64,
    /// Milliseconds since last interaction (XREADGROUP, XCLAIM, etc.)
    pub idle: i64,
    /// Milliseconds since last successful interaction (Redis 7.2+)
    pub inactive: Option<i64>,
}

impl Serialize for ConsumerInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut fields = 3;
        if self.inactive.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ConsumerInfo", fields)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("pending", &self.pending)?;
        state.serialize_field("idle", &self.idle)?;
        if let Some(inactive) = &self.inactive {
            state.serialize_field("inactive", inactive)?;
        }
        state.end()
    }
}

/// Output for Redis `XINFO CONSUMERS` command.
///
/// Returns a list of consumers in the specified consumer group.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XinfoConsumersOutput {
    /// The list of consumers
    consumers: Vec<ConsumerInfo>,
}

impl XinfoConsumersOutput {
    /// Create a new XinfoConsumersOutput
    pub fn new(consumers: Vec<ConsumerInfo>) -> Self {
        Self { consumers }
    }

    /// Get the list of consumers
    pub fn consumers(&self) -> &[ConsumerInfo] {
        &self.consumers
    }

    /// Get the number of consumers
    pub fn count(&self) -> usize {
        self.consumers.len()
    }

    /// Check if there are no consumers
    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }

    /// Decode the Redis protocol response into an XinfoConsumersOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let consumers = Self::parse_consumers(frame)?;
        Ok(Self { consumers })
    }

    fn parse_consumers(frame: DecoderRespFrame) -> Result<Vec<ConsumerInfo>, EpError> {
        let array = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(data) => data.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XINFO CONSUMERS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XINFO CONSUMERS response: {:?}", other)));
                }
            },
        };

        let mut consumers = Vec::new();
        for item in array {
            consumers.push(Self::parse_consumer_info(item)?);
        }
        Ok(consumers)
    }

    fn parse_consumer_info(frame: DecoderRespFrame) -> Result<ConsumerInfo, EpError> {
        let mut name = String::new();
        let mut pending = 0;
        let mut idle = 0;
        let mut inactive = None;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::extract_string_resp2(&arr[i])?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp2(&arr[i + 1])?,
                        "pending" => pending = Self::extract_int_resp2(&arr[i + 1])?,
                        "idle" => idle = Self::extract_int_resp2(&arr[i + 1])?,
                        "inactive" => inactive = Self::extract_optional_int_resp2(&arr[i + 1]),
                        _ => {}
                    }
                    i += 2;
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                for (k, v) in data {
                    let key = Self::extract_string_resp3(k)?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp3(v)?,
                        "pending" => pending = Self::extract_int_resp3(v)?,
                        "idle" => idle = Self::extract_int_resp3(v)?,
                        "inactive" => inactive = Self::extract_optional_int_resp3(v),
                        _ => {}
                    }
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut i = 0;
                while i + 1 < data.len() {
                    let key = Self::extract_string_resp3(data[i].clone())?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp3(data[i + 1].clone())?,
                        "pending" => pending = Self::extract_int_resp3(data[i + 1].clone())?,
                        "idle" => idle = Self::extract_int_resp3(data[i + 1].clone())?,
                        "inactive" => inactive = Self::extract_optional_int_resp3(data[i + 1].clone()),
                        _ => {}
                    }
                    i += 2;
                }
            }
            other => {
                return Err(EpError::parse(format!("unexpected consumer info format: {:?}", other)));
            }
        }

        Ok(ConsumerInfo { name, pending, idle, inactive })
    }

    fn extract_string_resp2(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_int_resp2(frame: &Resp2Frame) -> Result<i64, EpError> {
        match frame {
            Resp2Frame::Integer(n) => Ok(*n),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_optional_int_resp2(frame: &Resp2Frame) -> Option<i64> {
        match frame {
            Resp2Frame::Integer(n) => Some(*n),
            Resp2Frame::Null => None,
            _ => None,
        }
    }

    fn extract_string_resp3(frame: Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_int_resp3(frame: Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(data),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_optional_int_resp3(frame: Resp3Frame) -> Option<i64> {
        match frame {
            Resp3Frame::Number { data, .. } => Some(data),
            Resp3Frame::Null => None,
            _ => None,
        }
    }
}

impl Serialize for XinfoConsumersOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XinfoConsumersOutput", 1)?;
        state.serialize_field("consumers", &self.consumers)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = XinfoConsumersInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"XINFO"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
            assert!(cmd.windows(7).any(|w| w == b"mygroup"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XinfoConsumersInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let input = XinfoConsumersInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.group, RedisJsonValue::String("mygroup".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XinfoConsumersInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("stream".into()),
                RedisJsonValue::String("group".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = XinfoConsumersInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_output_new() {
            let consumers = vec![ConsumerInfo {
                name: "consumer1".into(),
                pending: 5,
                idle: 1000,
                inactive: Some(500),
            }];
            let output = XinfoConsumersOutput::new(consumers);
            assert_eq!(output.count(), 1);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_empty() {
            let output = XinfoConsumersOutput::new(vec![]);
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_empty_array() {
            let output = XinfoConsumersOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
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
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        // Helper to read from a consumer group (creates consumer)
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
        async fn test_xinfo_consumers_no_consumers() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    create_group(ctx, "xinfo_cons_none", "mygroup").await;

                    let result = ctx
                        .raw(
                            &XinfoConsumersInput {
                                key: RedisKey::String("xinfo_cons_none".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XinfoConsumersOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_single_consumer() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xinfo_cons_single", "field", "value").await;
                    create_group(ctx, "xinfo_cons_single", "mygroup").await;
                    xreadgroup(ctx, "mygroup", "consumer1", "xinfo_cons_single", ">").await;

                    let result = ctx
                        .raw(
                            &XinfoConsumersInput {
                                key: RedisKey::String("xinfo_cons_single".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XinfoConsumersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.consumers()[0].name, "consumer1");
                    assert_eq!(output.consumers()[0].pending, 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_multiple_consumers() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create group first, then add message and have each consumer read it
                    // Each consumer needs to read at least one message to be registered
                    create_group(ctx, "xinfo_cons_multi", "mygroup").await;

                    xadd_entry(ctx, "xinfo_cons_multi", "f1", "v1").await;
                    xreadgroup(ctx, "mygroup", "consumer1", "xinfo_cons_multi", ">").await;

                    xadd_entry(ctx, "xinfo_cons_multi", "f2", "v2").await;
                    xreadgroup(ctx, "mygroup", "consumer2", "xinfo_cons_multi", ">").await;

                    xadd_entry(ctx, "xinfo_cons_multi", "f3", "v3").await;
                    xreadgroup(ctx, "mygroup", "consumer3", "xinfo_cons_multi", ">").await;

                    let result = ctx
                        .raw(
                            &XinfoConsumersInput {
                                key: RedisKey::String("xinfo_cons_multi".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XinfoConsumersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);

                    let names: Vec<&str> = output.consumers().iter().map(|c| c.name.as_str()).collect();
                    assert!(names.contains(&"consumer1"));
                    assert!(names.contains(&"consumer2"));
                    assert!(names.contains(&"consumer3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_nonexistent_group() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xinfo_cons_nogroup", "field", "value").await;

                    let result = ctx
                        .raw(
                            &XinfoConsumersInput {
                                key: RedisKey::String("xinfo_cons_nogroup".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XinfoConsumersOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("NOGROUP"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xinfo_cons_r2", "field", "value").await;
            create_group(&mut ctx, "xinfo_cons_r2", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xinfo_cons_r2", ">").await;

            let result = ctx
                .raw(
                    &XinfoConsumersInput {
                        key: RedisKey::String("xinfo_cons_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XinfoConsumersOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xinfo_cons_r3", "field", "value").await;
            create_group(&mut ctx, "xinfo_cons_r3", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xinfo_cons_r3", ">").await;

            let result = ctx
                .raw(
                    &XinfoConsumersInput {
                        key: RedisKey::String("xinfo_cons_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XinfoConsumersOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_consumers_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create groups first, then add messages for each consumer to read
                    create_group(ctx, "xinfo_cons_pipe1", "group1").await;
                    create_group(ctx, "xinfo_cons_pipe2", "group2").await;

                    // pipe1: one consumer
                    xadd_entry(ctx, "xinfo_cons_pipe1", "f", "v").await;
                    xreadgroup(ctx, "group1", "c1", "xinfo_cons_pipe1", ">").await;

                    // pipe2: two consumers - each needs their own message
                    xadd_entry(ctx, "xinfo_cons_pipe2", "f1", "v1").await;
                    xreadgroup(ctx, "group2", "c2", "xinfo_cons_pipe2", ">").await;
                    xadd_entry(ctx, "xinfo_cons_pipe2", "f2", "v2").await;
                    xreadgroup(ctx, "group2", "c3", "xinfo_cons_pipe2", ">").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XinfoConsumersInput {
                            key: RedisKey::String("xinfo_cons_pipe1".into()),
                            group: RedisJsonValue::String("group1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XinfoConsumersInput {
                            key: RedisKey::String("xinfo_cons_pipe2".into()),
                            group: RedisJsonValue::String("group2".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XinfoConsumersOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.count(), 1);

                    let out2 = XinfoConsumersOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.count(), 2);
                })
            })
            .await;
        }
    }
}
