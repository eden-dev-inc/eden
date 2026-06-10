use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, XinfoStreamInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::XinfoStream, "Returns information about the stream", ReqType::Read, true);

/// Input for Redis `XINFO STREAM` command.
///
/// Returns detailed information about a stream including its length,
/// radix-tree information, groups, and optionally the first/last entries.
///
/// See official Redis documentation for `XINFO STREAM`:
/// https://redis.io/docs/latest/commands/xinfo-stream/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XinfoStreamInput {
    /// The key of the stream
    key: RedisKey,
    /// Optional FULL modifier with COUNT
    #[serde(skip_serializing_if = "Option::is_none")]
    full: Option<Full>,
}

impl Serialize for XinfoStreamInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if let Some(full) = &self.full {
            fields += 1;
            if full.count.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("XinfoStreamInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(full) = &self.full {
            state.serialize_field("full", &true)?;
            if let Some(count) = &full.count {
                state.serialize_field("count", count)?;
            }
        }
        state.end()
    }
}

/// FULL modifier options for XINFO STREAM
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema)]
pub struct Full {
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
}

impl_redis_operation!(XinfoStreamInput, API_INFO, { key, full });

impl RedisCommandInput for XinfoStreamInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);

        if let Some(full) = &self.full {
            command.arg("FULL");
            if let Some(count) = &full.count {
                command.arg("COUNT").arg(count);
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse(format!("XINFO STREAM requires at least 1 argument, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let mut full = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i]
                && s.to_uppercase() == "FULL"
            {
                let mut count = None;
                i += 1;

                if i + 1 < args.len()
                    && let RedisJsonValue::String(s) = &args[i]
                    && s.to_uppercase() == "COUNT"
                {
                    count = Some(args[i + 1].clone());
                    #[allow(unused_assignments)]
                    {
                        i += 2;
                    }
                }

                full = Some(Full { count });
                break;
            }
            i += 1;
        }

        Ok(Self { key, full })
    }
}

/// Basic stream information
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct StreamInfo {
    pub length: i64,
    pub radix_tree_keys: i64,
    pub radix_tree_nodes: i64,
    pub last_generated_id: String,
    pub max_deleted_entry_id: Option<String>,
    pub entries_added: Option<i64>,
    pub recorded_first_entry_id: Option<String>,
    pub first_entry: Option<StreamEntry>,
    pub last_entry: Option<StreamEntry>,
    pub groups: i64,
}

/// A stream entry with ID and fields
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct StreamEntry {
    pub id: String,
    pub fields: Vec<(String, String)>,
}

impl Serialize for StreamEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("StreamEntry", 2)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("fields", &self.fields)?;
        state.end()
    }
}

impl Serialize for StreamInfo {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("StreamInfo", 10)?;
        state.serialize_field("length", &self.length)?;
        state.serialize_field("radix_tree_keys", &self.radix_tree_keys)?;
        state.serialize_field("radix_tree_nodes", &self.radix_tree_nodes)?;
        state.serialize_field("last_generated_id", &self.last_generated_id)?;
        state.serialize_field("max_deleted_entry_id", &self.max_deleted_entry_id)?;
        state.serialize_field("entries_added", &self.entries_added)?;
        state.serialize_field("recorded_first_entry_id", &self.recorded_first_entry_id)?;
        state.serialize_field("first_entry", &self.first_entry)?;
        state.serialize_field("last_entry", &self.last_entry)?;
        state.serialize_field("groups", &self.groups)?;
        state.end()
    }
}

/// Output for Redis `XINFO STREAM` command.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XinfoStreamOutput {
    info: StreamInfo,
}

impl XinfoStreamOutput {
    pub fn new(info: StreamInfo) -> Self {
        Self { info }
    }

    pub fn info(&self) -> &StreamInfo {
        &self.info
    }

    pub fn length(&self) -> i64 {
        self.info.length
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let info = Self::parse_stream_info(frame)?;
        Ok(Self { info })
    }

    fn parse_stream_info(frame: DecoderRespFrame) -> Result<StreamInfo, EpError> {
        let mut length = 0;
        let mut radix_tree_keys = 0;
        let mut radix_tree_nodes = 0;
        let mut last_generated_id = String::new();
        let mut max_deleted_entry_id = None;
        let mut entries_added = None;
        let mut recorded_first_entry_id = None;
        let mut first_entry = None;
        let mut last_entry = None;
        let mut groups = 0;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::extract_string_resp2(&arr[i])?;
                    match key.as_str() {
                        "length" => length = Self::extract_int_resp2(&arr[i + 1])?,
                        "radix-tree-keys" => radix_tree_keys = Self::extract_int_resp2(&arr[i + 1])?,
                        "radix-tree-nodes" => radix_tree_nodes = Self::extract_int_resp2(&arr[i + 1])?,
                        "last-generated-id" => last_generated_id = Self::extract_string_resp2(&arr[i + 1])?,
                        "max-deleted-entry-id" => max_deleted_entry_id = Self::extract_optional_string_resp2(&arr[i + 1])?,
                        "entries-added" => entries_added = Self::extract_optional_int_resp2(&arr[i + 1]),
                        "recorded-first-entry-id" => recorded_first_entry_id = Self::extract_optional_string_resp2(&arr[i + 1])?,
                        "first-entry" => first_entry = Self::parse_entry_resp2(&arr[i + 1])?,
                        "last-entry" => last_entry = Self::parse_entry_resp2(&arr[i + 1])?,
                        "groups" => groups = Self::extract_int_resp2(&arr[i + 1])?,
                        _ => {}
                    }
                    i += 2;
                }
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                for (k, v) in data {
                    let key = Self::extract_string_resp3(&k)?;
                    match key.as_str() {
                        "length" => length = Self::extract_int_resp3(&v)?,
                        "radix-tree-keys" => radix_tree_keys = Self::extract_int_resp3(&v)?,
                        "radix-tree-nodes" => radix_tree_nodes = Self::extract_int_resp3(&v)?,
                        "last-generated-id" => last_generated_id = Self::extract_string_resp3(&v)?,
                        "max-deleted-entry-id" => max_deleted_entry_id = Self::extract_optional_string_resp3(&v)?,
                        "entries-added" => entries_added = Self::extract_optional_int_resp3(&v),
                        "recorded-first-entry-id" => recorded_first_entry_id = Self::extract_optional_string_resp3(&v)?,
                        "first-entry" => first_entry = Self::parse_entry_resp3(&v)?,
                        "last-entry" => last_entry = Self::parse_entry_resp3(&v)?,
                        "groups" => groups = Self::extract_int_resp3(&v)?,
                        _ => {}
                    }
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data: arr, .. }) => {
                // Handle RESP3 Array (same structure as RESP2 Array - flat key-value pairs)
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::extract_string_resp3(&arr[i])?;
                    match key.as_str() {
                        "length" => length = Self::extract_int_resp3(&arr[i + 1])?,
                        "radix-tree-keys" => radix_tree_keys = Self::extract_int_resp3(&arr[i + 1])?,
                        "radix-tree-nodes" => radix_tree_nodes = Self::extract_int_resp3(&arr[i + 1])?,
                        "last-generated-id" => last_generated_id = Self::extract_string_resp3(&arr[i + 1])?,
                        "max-deleted-entry-id" => max_deleted_entry_id = Self::extract_optional_string_resp3(&arr[i + 1])?,
                        "entries-added" => entries_added = Self::extract_optional_int_resp3(&arr[i + 1]),
                        "recorded-first-entry-id" => recorded_first_entry_id = Self::extract_optional_string_resp3(&arr[i + 1])?,
                        "first-entry" => first_entry = Self::parse_entry_resp3(&arr[i + 1])?,
                        "last-entry" => last_entry = Self::parse_entry_resp3(&arr[i + 1])?,
                        "groups" => groups = Self::extract_int_resp3(&arr[i + 1])?,
                        _ => {}
                    }
                    i += 2;
                }
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected XINFO STREAM response: {:?}", other)));
            }
        }

        Ok(StreamInfo {
            length,
            radix_tree_keys,
            radix_tree_nodes,
            last_generated_id,
            max_deleted_entry_id,
            entries_added,
            recorded_first_entry_id,
            first_entry,
            last_entry,
            groups,
        })
    }

    fn parse_entry_resp2(frame: &Resp2Frame) -> Result<Option<StreamEntry>, EpError> {
        match frame {
            Resp2Frame::Null => Ok(None),
            Resp2Frame::Array(arr) if arr.len() >= 2 => {
                let id = Self::extract_string_resp2(&arr[0])?;
                let mut fields = Vec::new();

                if let Resp2Frame::Array(field_arr) = &arr[1] {
                    let mut j = 0;
                    while j + 1 < field_arr.len() {
                        let field = Self::extract_string_resp2(&field_arr[j])?;
                        let value = Self::extract_string_resp2(&field_arr[j + 1])?;
                        fields.push((field, value));
                        j += 2;
                    }
                }

                Ok(Some(StreamEntry { id, fields }))
            }
            _ => Ok(None),
        }
    }

    fn parse_entry_resp3(frame: &Resp3Frame) -> Result<Option<StreamEntry>, EpError> {
        match frame {
            Resp3Frame::Null => Ok(None),
            Resp3Frame::Array { data, .. } if data.len() >= 2 => {
                let id = Self::extract_string_resp3(&data[0])?;
                let mut fields = Vec::new();

                if let Resp3Frame::Array { data: field_arr, .. } = &data[1] {
                    let mut j = 0;
                    while j + 1 < field_arr.len() {
                        let field = Self::extract_string_resp3(&field_arr[j])?;
                        let value = Self::extract_string_resp3(&field_arr[j + 1])?;
                        fields.push((field, value));
                        j += 2;
                    }
                }

                Ok(Some(StreamEntry { id, fields }))
            }
            _ => Ok(None),
        }
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

    fn extract_optional_string_resp2(frame: &Resp2Frame) -> ResultEP<Option<String>> {
        Ok(match frame {
            Resp2Frame::BulkString(data) => Some(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(data) => Some(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            _ => None,
        })
    }

    fn extract_optional_int_resp2(frame: &Resp2Frame) -> Option<i64> {
        match frame {
            Resp2Frame::Integer(n) => Some(*n),
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

    fn extract_optional_string_resp3(frame: &Resp3Frame) -> ResultEP<Option<String>> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),

            _ => None,
        })
    }

    fn extract_optional_int_resp3(frame: &Resp3Frame) -> Option<i64> {
        match frame {
            Resp3Frame::Number { data, .. } => Some(*data),
            _ => None,
        }
    }
}

impl Serialize for XinfoStreamOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XinfoStreamOutput", 1)?;
        state.serialize_field("info", &self.info)?;
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
            let input = XinfoStreamInput { key: RedisKey::String("mystream".into()), full: None };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"XINFO"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
            assert!(!cmd.windows(4).any(|w| w == b"FULL"));
        }

        #[test]
        fn test_encode_command_with_full() {
            let input = XinfoStreamInput {
                key: RedisKey::String("mystream".into()),
                full: Some(Full { count: None }),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"FULL"));
        }

        #[test]
        fn test_encode_command_with_full_count() {
            let input = XinfoStreamInput {
                key: RedisKey::String("mystream".into()),
                full: Some(Full { count: Some(RedisJsonValue::Integer(10)) }),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"FULL"));
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XinfoStreamInput { key: RedisKey::String("mystream".into()), full: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let input = XinfoStreamInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(input.full.is_none());
        }

        #[test]
        fn test_decode_input_with_full() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("FULL".into())];
            let input = XinfoStreamInput::decode(args).unwrap();
            assert!(input.full.is_some());
        }

        #[test]
        fn test_decode_input_with_full_count() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("FULL".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = XinfoStreamInput::decode(args).unwrap();
            assert!(input.full.is_some());
            assert_eq!(input.full.as_ref().unwrap().count, Some(RedisJsonValue::Integer(10)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XinfoStreamInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_full_default() {
            let full = Full::default();
            assert!(full.count.is_none());
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
        async fn test_xinfo_stream_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xinfo_stream_basic", "field", "value").await;

                    let result = ctx
                        .raw(
                            &XinfoStreamInput {
                                key: RedisKey::String("xinfo_stream_basic".into()),
                                full: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XinfoStreamOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_stream_with_groups() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xinfo_stream_groups", "field", "value").await;
                    create_group(ctx, "xinfo_stream_groups", "group1").await;
                    create_group(ctx, "xinfo_stream_groups", "group2").await;

                    let result = ctx
                        .raw(
                            &XinfoStreamInput {
                                key: RedisKey::String("xinfo_stream_groups".into()),
                                full: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XinfoStreamOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.info().groups, 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_stream_nonexistent() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XinfoStreamInput {
                                key: RedisKey::String("nonexistent_stream".into()),
                                full: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XinfoStreamOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("no such key") || err.to_string().contains("ERR"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_stream_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xinfo_stream_r2", "field", "value").await;

            let result = ctx
                .raw(&XinfoStreamInput { key: RedisKey::String("xinfo_stream_r2".into()), full: None }.command())
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XinfoStreamOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_stream_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xinfo_stream_r3", "field", "value").await;

            let result = ctx
                .raw(&XinfoStreamInput { key: RedisKey::String("xinfo_stream_r3".into()), full: None }.command())
                .await
                .expect("raw failed");

            let output = XinfoStreamOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xinfo_stream_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xinfo_str_pipe1", "f", "v").await;
                    xadd_entry(ctx, "xinfo_str_pipe2", "f", "v").await;
                    xadd_entry(ctx, "xinfo_str_pipe2", "f", "v").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&XinfoStreamInput { key: RedisKey::String("xinfo_str_pipe1".into()), full: None }.command());
                    pipeline.extend_from_slice(&XinfoStreamInput { key: RedisKey::String("xinfo_str_pipe2".into()), full: None }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XinfoStreamOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.length(), 1);

                    let out2 = XinfoStreamOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.length(), 2);
                })
            })
            .await;
        }
    }
}
