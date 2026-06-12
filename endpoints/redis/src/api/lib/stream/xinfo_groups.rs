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

const API_INFO: ApiInfo<RedisApi, XinfoGroupsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XinfoGroups,
    "Returns a list of the consumer groups of a stream",
    ReqType::Read,
    true,
);

/// Input for Redis `XINFO GROUPS` command.
///
/// Returns information about the consumer groups of a stream.
///
/// See official Redis documentation for `XINFO GROUPS`:
/// https://redis.io/docs/latest/commands/xinfo-groups/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XinfoGroupsInput {
    /// The key of the stream
    key: RedisKey,
}

impl Serialize for XinfoGroupsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XinfoGroupsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(XinfoGroupsInput, API_INFO, { key });

impl RedisCommandInput for XinfoGroupsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::parse(format!("XINFO GROUPS requires 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Information about a single consumer group
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GroupInfo {
    /// The consumer group name
    pub name: String,
    /// The number of consumers in the group
    pub consumers: i64,
    /// The number of pending messages
    pub pending: i64,
    /// The last delivered ID
    pub last_delivered_id: String,
    /// The number of entries read from the stream (Redis 7.0+)
    pub entries_read: Option<i64>,
    /// The logical lag of the group (Redis 7.0+)
    pub lag: Option<i64>,
}

impl Serialize for GroupInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut fields = 4;
        if self.entries_read.is_some() {
            fields += 1;
        }
        if self.lag.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("GroupInfo", fields)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("consumers", &self.consumers)?;
        state.serialize_field("pending", &self.pending)?;
        state.serialize_field("last_delivered_id", &self.last_delivered_id)?;
        if let Some(entries_read) = &self.entries_read {
            state.serialize_field("entries_read", entries_read)?;
        }
        if let Some(lag) = &self.lag {
            state.serialize_field("lag", lag)?;
        }
        state.end()
    }
}

/// Output for Redis `XINFO GROUPS` command.
///
/// Returns a list of consumer groups associated with the stream.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XinfoGroupsOutput {
    /// The list of consumer groups
    groups: Vec<GroupInfo>,
}

impl XinfoGroupsOutput {
    /// Create a new XinfoGroupsOutput
    pub fn new(groups: Vec<GroupInfo>) -> Self {
        Self { groups }
    }

    /// Get the list of groups
    pub fn groups(&self) -> &[GroupInfo] {
        &self.groups
    }

    /// Get the number of groups
    pub fn count(&self) -> usize {
        self.groups.len()
    }

    /// Check if there are no groups
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }

    /// Decode the Redis protocol response into an XinfoGroupsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let groups = Self::parse_groups(frame)?;
        Ok(Self { groups })
    }

    fn parse_groups(frame: DecoderRespFrame) -> Result<Vec<GroupInfo>, EpError> {
        let array = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XINFO GROUPS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XINFO GROUPS response: {:?}", other)));
                }
            },
        };

        let mut groups = Vec::new();
        for item in array {
            groups.push(Self::parse_group_info(item)?);
        }
        Ok(groups)
    }

    fn parse_group_info(frame: DecoderRespFrame) -> Result<GroupInfo, EpError> {
        // XINFO GROUPS returns arrays of key-value pairs (RESP2) or maps (RESP3)
        let mut name = String::new();
        let mut consumers = 0;
        let mut pending = 0;
        let mut last_delivered_id = String::new();
        let mut entries_read = None;
        let mut lag = None;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::extract_string_resp2(&arr[i])?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp2(&arr[i + 1])?,
                        "consumers" => consumers = Self::extract_int_resp2(&arr[i + 1])?,
                        "pending" => pending = Self::extract_int_resp2(&arr[i + 1])?,
                        "last-delivered-id" => last_delivered_id = Self::extract_string_resp2(&arr[i + 1])?,
                        "entries-read" => entries_read = Self::extract_optional_int_resp2(&arr[i + 1]),
                        "lag" => lag = Self::extract_optional_int_resp2(&arr[i + 1]),
                        _ => {}
                    }
                    i += 2;
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                for (k, v) in data {
                    let key = Self::extract_string_resp3(&k)?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp3(&v)?,
                        "consumers" => consumers = Self::extract_int_resp3(&v)?,
                        "pending" => pending = Self::extract_int_resp3(&v)?,
                        "last-delivered-id" => last_delivered_id = Self::extract_string_resp3(&v)?,
                        "entries-read" => entries_read = Self::extract_optional_int_resp3(&v),
                        "lag" => lag = Self::extract_optional_int_resp3(&v),
                        _ => {}
                    }
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut i = 0;
                while i + 1 < data.len() {
                    let key = Self::extract_string_resp3(&data[i])?;
                    match key.as_str() {
                        "name" => name = Self::extract_string_resp3(&data[i + 1])?,
                        "consumers" => consumers = Self::extract_int_resp3(&data[i + 1])?,
                        "pending" => pending = Self::extract_int_resp3(&data[i + 1])?,
                        "last-delivered-id" => last_delivered_id = Self::extract_string_resp3(&data[i + 1])?,
                        "entries-read" => entries_read = Self::extract_optional_int_resp3(&data[i + 1]),
                        "lag" => lag = Self::extract_optional_int_resp3(&data[i + 1]),
                        _ => {}
                    }
                    i += 2;
                }
            }
            other => {
                return Err(EpError::parse(format!("unexpected group info format: {:?}", other)));
            }
        }

        Ok(GroupInfo {
            name,
            consumers,
            pending,
            last_delivered_id,
            entries_read,
            lag,
        })
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

    fn extract_string_resp3(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),

            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_int_resp3(frame: &Resp3Frame) -> Result<i64, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(*data),
            other => Err(EpError::parse(format!("expected integer, got {:?}", other))),
        }
    }

    fn extract_optional_int_resp3(frame: &Resp3Frame) -> Option<i64> {
        match frame {
            Resp3Frame::Number { data, .. } => Some(*data),
            Resp3Frame::Null => None,
            _ => None,
        }
    }
}

impl Serialize for XinfoGroupsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XinfoGroupsOutput", 1)?;
        state.serialize_field("groups", &self.groups)?;
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
            let input = XinfoGroupsInput { key: RedisKey::String("mystream".into()) };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"XINFO"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XinfoGroupsInput { key: RedisKey::String("mystream".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let input = XinfoGroupsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XinfoGroupsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("stream1".into()), RedisJsonValue::String("extra".into())];
            let err = XinfoGroupsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_output_new() {
            let groups = vec![GroupInfo {
                name: "mygroup".into(),
                consumers: 2,
                pending: 5,
                last_delivered_id: "1234-0".into(),
                entries_read: Some(10),
                lag: Some(3),
            }];
            let output = XinfoGroupsOutput::new(groups);
            assert_eq!(output.count(), 1);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_empty() {
            let output = XinfoGroupsOutput::new(vec![]);
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_empty_array() {
            // RESP2 empty array
            let output = XinfoGroupsOutput::decode(b"*0\r\n").unwrap();
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

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_no_groups() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream without groups
                    xadd_entry(ctx, "xinfo_groups_none", "field", "value").await;

                    let result = ctx
                        .raw(&XinfoGroupsInput { key: RedisKey::String("xinfo_groups_none".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = XinfoGroupsOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_single_group() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    create_group(ctx, "xinfo_groups_single", "mygroup").await;

                    let result = ctx
                        .raw(&XinfoGroupsInput { key: RedisKey::String("xinfo_groups_single".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = XinfoGroupsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert_eq!(output.groups()[0].name, "mygroup");
                    assert_eq!(output.groups()[0].consumers, 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_multiple_groups() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    create_group(ctx, "xinfo_groups_multi", "group1").await;
                    create_group(ctx, "xinfo_groups_multi", "group2").await;
                    create_group(ctx, "xinfo_groups_multi", "group3").await;

                    let result = ctx
                        .raw(&XinfoGroupsInput { key: RedisKey::String("xinfo_groups_multi".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = XinfoGroupsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);

                    let names: Vec<&str> = output.groups().iter().map(|g| g.name.as_str()).collect();
                    assert!(names.contains(&"group1"));
                    assert!(names.contains(&"group2"));
                    assert!(names.contains(&"group3"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_nonexistent_stream() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&XinfoGroupsInput { key: RedisKey::String("nonexistent_stream".into()) }.command())
                        .await
                        .expect("raw failed");

                    let err = XinfoGroupsOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("no such key") || err.to_string().contains("ERR"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            create_group(&mut ctx, "xinfo_groups_r2", "testgroup").await;

            let result =
                ctx.raw(&XinfoGroupsInput { key: RedisKey::String("xinfo_groups_r2".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XinfoGroupsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            create_group(&mut ctx, "xinfo_groups_r3", "testgroup").await;

            let result =
                ctx.raw(&XinfoGroupsInput { key: RedisKey::String("xinfo_groups_r3".into()) }.command()).await.expect("raw failed");

            let output = XinfoGroupsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_groups_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    create_group(ctx, "xinfo_pipe1", "group1").await;
                    create_group(ctx, "xinfo_pipe2", "group2").await;
                    create_group(ctx, "xinfo_pipe2", "group3").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&XinfoGroupsInput { key: RedisKey::String("xinfo_pipe1".into()) }.command());
                    pipeline.extend_from_slice(&XinfoGroupsInput { key: RedisKey::String("xinfo_pipe2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XinfoGroupsOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.count(), 1);

                    let out2 = XinfoGroupsOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.count(), 2);
                })
            })
            .await;
        }
    }
}
