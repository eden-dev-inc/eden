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

const API_INFO: ApiInfo<RedisApi, XautoclaimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xautoclaim,
    "Automatically claims messages from pending entries that have been idle for the specified time.",
    ReqType::Write,
    true,
);

/// Input for Redis `XAUTOCLAIM` command.
///
/// See official Redis documentation for `XAUTOCLAIM`:
/// https://redis.io/docs/latest/commands/xautoclaim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XautoclaimInput {
    /// The key of the stream
    key: RedisKey,
    /// The consumer group name
    group: RedisJsonValue,
    /// The consumer claiming the messages
    consumer: RedisJsonValue,
    /// Minimum idle time in milliseconds
    min_idle_time: RedisJsonValue,
    /// The start ID for scanning
    start: RedisJsonValue,
    /// Maximum number of entries to claim
    count: Option<RedisJsonValue>,
    /// If true, only return message IDs
    just_id: Option<bool>,
}

impl Serialize for XautoclaimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 6;
        if self.count.is_some() {
            fields += 1;
        }
        if self.just_id.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XautoclaimInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("consumer", &self.consumer)?;
        state.serialize_field("min_idle_time", &self.min_idle_time)?;
        state.serialize_field("start", &self.start)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(just_id) = &self.just_id {
            state.serialize_field("just_id", just_id)?;
        }
        state.end()
    }
}

impl_redis_operation!(XautoclaimInput, API_INFO, { key, group, consumer, min_idle_time, start, count, just_id });

impl RedisCommandInput for XautoclaimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.group).arg(&self.consumer).arg(&self.min_idle_time).arg(&self.start);

        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }

        if let Some(true) = &self.just_id {
            command.arg("JUSTID");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::parse(format!("XAUTOCLAIM requires at least 5 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let group = args[1].clone();
        let consumer = args[2].clone();
        let min_idle_time = args[3].clone();
        let start = args[4].clone();
        let mut count = None;
        let mut just_id = None;
        let mut i = 5;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "COUNT" && i + 1 < args.len() {
                    count = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "JUSTID" {
                    just_id = Some(true);
                    i += 1;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, group, consumer, min_idle_time, start, count, just_id })
    }
}

/// A claimed stream entry
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, Serialize)]
pub struct ClaimedEntry {
    pub id: String,
    pub fields: Option<Vec<(String, RedisJsonValue)>>,
}

/// Output for Redis `XAUTOCLAIM` command.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XautoclaimOutput {
    next_id: String,
    entries: Vec<ClaimedEntry>,
    deleted_ids: Vec<String>,
}

impl XautoclaimOutput {
    pub fn new(next_id: String, entries: Vec<ClaimedEntry>, deleted_ids: Vec<String>) -> Self {
        Self { next_id, entries, deleted_ids }
    }

    pub fn next_id(&self) -> &str {
        &self.next_id
    }

    pub fn entries(&self) -> &[ClaimedEntry] {
        &self.entries
    }

    pub fn claimed_count(&self) -> usize {
        self.entries.len()
    }

    pub fn deleted_ids(&self) -> &[String] {
        &self.deleted_ids
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                if arr.len() < 2 {
                    return Err(EpError::parse("XAUTOCLAIM response too short"));
                }

                let next_id = Self::extract_string_resp2(&arr[0])?;
                let entries = Self::parse_entries_resp2(&arr[1])?;
                let deleted_ids = if arr.len() > 2 {
                    Self::parse_string_array_resp2(&arr[2])?
                } else {
                    vec![]
                };

                Ok(Self { next_id, entries, deleted_ids })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected XAUTOCLAIM response: {:?}", other))),
        }
    }

    fn extract_string_resp2(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_entries_resp2(frame: &Resp2Frame) -> Result<Vec<ClaimedEntry>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut entries = Vec::new();
                for item in arr {
                    match item {
                        Resp2Frame::Array(entry_arr) if entry_arr.len() >= 2 => {
                            let id = Self::extract_string_resp2(&entry_arr[0])?;
                            let fields = Self::parse_fields_resp2(&entry_arr[1])?;
                            entries.push(ClaimedEntry { id, fields: Some(fields) });
                        }
                        Resp2Frame::BulkString(_) | Resp2Frame::SimpleString(_) => {
                            let id = Self::extract_string_resp2(item)?;
                            entries.push(ClaimedEntry { id, fields: None });
                        }
                        _ => {}
                    }
                }
                Ok(entries)
            }
            Resp2Frame::Null => Ok(vec![]),
            other => Err(EpError::parse(format!("expected array, got {:?}", other))),
        }
    }

    fn parse_fields_resp2(frame: &Resp2Frame) -> Result<Vec<(String, RedisJsonValue)>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut fields = Vec::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    let key = Self::extract_string_resp2(&arr[i])?;
                    let value = match &arr[i + 1] {
                        Resp2Frame::BulkString(data) => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
                        Resp2Frame::Integer(n) => RedisJsonValue::Integer(*n),
                        _ => RedisJsonValue::Null,
                    };
                    fields.push((key, value));
                    i += 2;
                }
                Ok(fields)
            }
            _ => Ok(vec![]),
        }
    }

    fn parse_string_array_resp2(frame: &Resp2Frame) -> Result<Vec<String>, EpError> {
        match frame {
            Resp2Frame::Array(arr) => arr.iter().map(Self::extract_string_resp2).collect(),
            Resp2Frame::Null => Ok(vec![]),
            other => Err(EpError::parse(format!("expected array, got {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.len() < 2 {
                    return Err(EpError::parse("XAUTOCLAIM response too short"));
                }

                let next_id = Self::extract_string_resp3(&data[0])?;
                let entries = Self::parse_entries_resp3(&data[1])?;
                let deleted_ids = if data.len() > 2 {
                    Self::parse_string_array_resp3(&data[2])?
                } else {
                    vec![]
                };

                Ok(Self { next_id, entries, deleted_ids })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected XAUTOCLAIM response: {:?}", other))),
        }
    }

    fn extract_string_resp3(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn parse_entries_resp3(frame: &Resp3Frame) -> Result<Vec<ClaimedEntry>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut entries = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::Array { data: entry_arr, .. } if entry_arr.len() >= 2 => {
                            let id = Self::extract_string_resp3(&entry_arr[0])?;
                            let fields = Self::parse_fields_resp3(&entry_arr[1])?;
                            entries.push(ClaimedEntry { id, fields: Some(fields) });
                        }
                        Resp3Frame::BlobString { .. } | Resp3Frame::SimpleString { .. } => {
                            let id = Self::extract_string_resp3(item)?;
                            entries.push(ClaimedEntry { id, fields: None });
                        }
                        _ => {}
                    }
                }
                Ok(entries)
            }
            Resp3Frame::Null => Ok(vec![]),
            other => Err(EpError::parse(format!("expected array, got {:?}", other))),
        }
    }

    fn parse_fields_resp3(frame: &Resp3Frame) -> Result<Vec<(String, RedisJsonValue)>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut fields = Vec::new();
                let mut i = 0;
                while i + 1 < data.len() {
                    let key = Self::extract_string_resp3(&data[i])?;
                    let value = match &data[i + 1] {
                        Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
                        Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
                        _ => RedisJsonValue::Null,
                    };
                    fields.push((key, value));
                    i += 2;
                }
                Ok(fields)
            }
            Resp3Frame::Map { data, .. } => {
                let mut fields = Vec::new();
                for (k, v) in data {
                    let key = Self::extract_string_resp3(k)?;
                    let value = match v {
                        Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
                        Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
                        _ => RedisJsonValue::Null,
                    };
                    fields.push((key, value));
                }
                Ok(fields)
            }
            _ => Ok(vec![]),
        }
    }

    fn parse_string_array_resp3(frame: &Resp3Frame) -> Result<Vec<String>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => data.iter().map(Self::extract_string_resp3).collect(),
            Resp3Frame::Null => Ok(vec![]),
            other => Err(EpError::parse(format!("expected array, got {:?}", other))),
        }
    }
}

impl Serialize for XautoclaimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XautoclaimOutput", 3)?;
        state.serialize_field("next_id", &self.next_id)?;
        state.serialize_field("entries", &self.entries)?;
        state.serialize_field("deleted_ids", &self.deleted_ids)?;
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
            let input = XautoclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                start: RedisJsonValue::String("0-0".into()),
                count: None,
                just_id: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(10).any(|w| w == b"XAUTOCLAIM"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = XautoclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                start: RedisJsonValue::String("0-0".into()),
                count: Some(RedisJsonValue::Integer(10)),
                just_id: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_encode_command_with_justid() {
            let input = XautoclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                start: RedisJsonValue::String("0-0".into()),
                count: None,
                just_id: Some(true),
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"JUSTID"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XautoclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                start: RedisJsonValue::String("0-0".into()),
                count: None,
                just_id: None,
            };
            assert_eq!(input.keys().len(), 1);
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("0-0".into()),
            ];
            let input = XautoclaimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let err = XautoclaimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 5"));
        }

        #[test]
        fn test_output_new() {
            let output = XautoclaimOutput::new("0-0".to_string(), vec![], vec![]);
            assert_eq!(output.next_id(), "0-0");
            assert_eq!(output.claimed_count(), 0);
        }

        #[test]
        fn test_output_serialize() {
            let output = XautoclaimOutput::new("0-0".to_string(), vec![], vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"next_id\":\"0-0\""));
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
            } else {
                response.trim_start_matches('+').trim().to_string()
            }
        }

        async fn create_group(ctx: &mut TestContext, key: &str, group: &str, start_id: &str) {
            let cmd = format!(
                "*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n{}\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                group.len(),
                group,
                start_id.len(),
                start_id
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

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
        async fn test_xautoclaim_no_pending() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xauto_empty", "field", "value").await;
                    create_group(ctx, "xauto_empty", "testgroup", "0").await;

                    let result = ctx
                        .raw(
                            &XautoclaimInput {
                                key: RedisKey::String("xauto_empty".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                start: RedisJsonValue::String("0-0".into()),
                                count: None,
                                just_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XautoclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xautoclaim_with_pending() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let id = xadd_entry(ctx, "xauto_pending", "field", "value").await;
                    create_group(ctx, "xauto_pending", "testgroup", "0").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xauto_pending", ">").await;

                    // Small delay to ensure idle time
                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                    let result = ctx
                        .raw(
                            &XautoclaimInput {
                                key: RedisKey::String("xauto_pending".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer2".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                start: RedisJsonValue::String("0-0".into()),
                                count: None,
                                just_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XautoclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 1);
                    assert_eq!(output.entries()[0].id, id);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xautoclaim_justid() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xauto_justid", "field", "value").await;
                    create_group(ctx, "xauto_justid", "testgroup", "0").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xauto_justid", ">").await;

                    tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

                    let result = ctx
                        .raw(
                            &XautoclaimInput {
                                key: RedisKey::String("xauto_justid".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer2".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                start: RedisJsonValue::String("0-0".into()),
                                count: None,
                                just_id: Some(true),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XautoclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 1);
                    assert!(output.entries()[0].fields.is_none());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xautoclaim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xauto_r2", "field", "value").await;
            create_group(&mut ctx, "xauto_r2", "testgroup", "0").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xauto_r2", ">").await;

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            let result = ctx
                .raw(
                    &XautoclaimInput {
                        key: RedisKey::String("xauto_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer2".into()),
                        min_idle_time: RedisJsonValue::Integer(0),
                        start: RedisJsonValue::String("0-0".into()),
                        count: None,
                        just_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XautoclaimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.claimed_count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xautoclaim_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xauto_r3", "field", "value").await;
            create_group(&mut ctx, "xauto_r3", "testgroup", "0").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xauto_r3", ">").await;

            tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;

            let result = ctx
                .raw(
                    &XautoclaimInput {
                        key: RedisKey::String("xauto_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer2".into()),
                        min_idle_time: RedisJsonValue::Integer(0),
                        start: RedisJsonValue::String("0-0".into()),
                        count: None,
                        just_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XautoclaimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.claimed_count(), 1);

            ctx.stop().await;
        }
    }
}
