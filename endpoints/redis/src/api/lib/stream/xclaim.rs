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

const API_INFO: ApiInfo<RedisApi, XclaimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xclaim,
    "Claims ownership of one or more pending messages in a consumer group.",
    ReqType::Write,
    true,
);

/// Input for Redis `XCLAIM` command.
///
/// Changes ownership of pending stream entries to a different consumer.
///
/// See official Redis documentation for `XCLAIM`:
/// https://redis.io/docs/latest/commands/xclaim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XclaimInput {
    /// The key of the stream
    key: RedisKey,
    /// The consumer group name
    group: RedisJsonValue,
    /// The consumer claiming the messages
    consumer: RedisJsonValue,
    /// Minimum idle time in milliseconds
    min_idle_time: RedisJsonValue,
    /// The message IDs to claim
    ids: Vec<RedisJsonValue>,
    /// Set the idle time (ms) of the message
    idle_ms: Option<RedisJsonValue>,
    /// Set the idle time to a specific Unix time (ms)
    time: Option<RedisJsonValue>,
    /// Set the retry counter
    retry_count: Option<RedisJsonValue>,
    /// Create the pending entry if it doesn't exist
    force: Option<bool>,
    /// Only return message IDs, not the messages
    just_id: Option<bool>,
    /// Update the last ID of the consumer group
    last_id: Option<RedisJsonValue>,
}

impl Serialize for XclaimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 6;
        if self.idle_ms.is_some() {
            fields += 1;
        }
        if self.time.is_some() {
            fields += 1;
        }
        if self.retry_count.is_some() {
            fields += 1;
        }
        if self.force.is_some() {
            fields += 1;
        }
        if self.just_id.is_some() {
            fields += 1;
        }
        if self.last_id.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XclaimInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("consumer", &self.consumer)?;
        state.serialize_field("min_idle_time", &self.min_idle_time)?;
        state.serialize_field("ids", &self.ids)?;
        if let Some(idle_ms) = &self.idle_ms {
            state.serialize_field("idle_ms", idle_ms)?;
        }
        if let Some(time) = &self.time {
            state.serialize_field("time", time)?;
        }
        if let Some(retry_count) = &self.retry_count {
            state.serialize_field("retry_count", retry_count)?;
        }
        if let Some(force) = &self.force {
            state.serialize_field("force", force)?;
        }
        if let Some(just_id) = &self.just_id {
            state.serialize_field("just_id", just_id)?;
        }
        if let Some(last_id) = &self.last_id {
            state.serialize_field("last_id", last_id)?;
        }
        state.end()
    }
}

impl_redis_operation!(XclaimInput, API_INFO, { key, group, consumer, min_idle_time, ids, idle_ms, time, retry_count, force, just_id, last_id });

impl RedisCommandInput for XclaimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.group).arg(&self.consumer).arg(&self.min_idle_time);

        for id in &self.ids {
            command.arg(id);
        }

        if let Some(idle_ms) = &self.idle_ms {
            command.arg("IDLE").arg(idle_ms);
        }

        if let Some(time) = &self.time {
            command.arg("TIME").arg(time);
        }

        if let Some(retry_count) = &self.retry_count {
            command.arg("RETRYCOUNT").arg(retry_count);
        }

        if let Some(true) = &self.force {
            command.arg("FORCE");
        }

        if let Some(true) = &self.just_id {
            command.arg("JUSTID");
        }

        if let Some(last_id) = &self.last_id {
            command.arg("LASTID").arg(last_id);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::parse(format!(
                "XCLAIM requires at least 5 arguments (key, group, consumer, min-idle-time, id), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let group = args[1].clone();
        let consumer = args[2].clone();
        let min_idle_time = args[3].clone();
        let mut ids = Vec::new();
        let mut idle_ms = None;
        let mut time = None;
        let mut retry_count = None;
        let mut force = None;
        let mut just_id = None;
        let mut last_id = None;
        let mut i = 4;

        // Parse IDs first (until we hit a keyword)
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if matches!(upper.as_str(), "IDLE" | "TIME" | "RETRYCOUNT" | "FORCE" | "JUSTID" | "LASTID") {
                    break;
                }
            }
            ids.push(args[i].clone());
            i += 1;
        }

        // Parse optional parameters
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                match upper.as_str() {
                    "IDLE" if i + 1 < args.len() => {
                        idle_ms = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "TIME" if i + 1 < args.len() => {
                        time = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "RETRYCOUNT" if i + 1 < args.len() => {
                        retry_count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FORCE" => {
                        force = Some(true);
                        i += 1;
                    }
                    "JUSTID" => {
                        just_id = Some(true);
                        i += 1;
                    }
                    "LASTID" if i + 1 < args.len() => {
                        last_id = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }

        if ids.is_empty() {
            return Err(EpError::parse("XCLAIM requires at least one message ID"));
        }

        Ok(Self {
            key,
            group,
            consumer,
            min_idle_time,
            ids,
            idle_ms,
            time,
            retry_count,
            force,
            just_id,
            last_id,
        })
    }
}

/// A claimed stream entry
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, Serialize)]
pub struct ClaimedMessage {
    pub id: String,
    pub fields: Option<Vec<(String, RedisJsonValue)>>,
}

/// Output for Redis `XCLAIM` command.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XclaimOutput {
    messages: Vec<ClaimedMessage>,
}

impl XclaimOutput {
    pub fn new(messages: Vec<ClaimedMessage>) -> Self {
        Self { messages }
    }

    pub fn messages(&self) -> &[ClaimedMessage] {
        &self.messages
    }

    pub fn claimed_count(&self) -> usize {
        self.messages.len()
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
                let mut messages = Vec::new();
                for item in arr {
                    match item {
                        // Full message: [id, [field, value, ...]]
                        Resp2Frame::Array(msg_arr) if msg_arr.len() >= 2 => {
                            let id = Self::extract_string_resp2(&msg_arr[0])?;
                            let fields = Self::parse_fields_resp2(&msg_arr[1])?;
                            messages.push(ClaimedMessage { id, fields: Some(fields) });
                        }
                        // JUSTID: just the ID string
                        Resp2Frame::BulkString(_) | Resp2Frame::SimpleString(_) => {
                            let id = Self::extract_string_resp2(&item)?;
                            messages.push(ClaimedMessage { id, fields: None });
                        }
                        Resp2Frame::Null => {
                            // Null entry means message doesn't exist, skip
                        }
                        _ => {}
                    }
                }
                Ok(Self { messages })
            }
            Resp2Frame::Null => Ok(Self { messages: vec![] }),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected XCLAIM response: {:?}", other))),
        }
    }

    fn extract_string_resp2(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
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
            Resp2Frame::Null => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut messages = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::Array { data: msg_arr, .. } if msg_arr.len() >= 2 => {
                            let id = Self::extract_string_resp3(&msg_arr[0])?;
                            let fields = Self::parse_fields_resp3(&msg_arr[1])?;
                            messages.push(ClaimedMessage { id, fields: Some(fields) });
                        }
                        Resp3Frame::BlobString { .. } | Resp3Frame::SimpleString { .. } => {
                            let id = Self::extract_string_resp3(&item)?;
                            messages.push(ClaimedMessage { id, fields: None });
                        }
                        Resp3Frame::Null => {}
                        _ => {}
                    }
                }
                Ok(Self { messages })
            }
            Resp3Frame::Null => Ok(Self { messages: vec![] }),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected XCLAIM response: {:?}", other))),
        }
    }

    fn extract_string_resp3(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
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
            Resp3Frame::Null => Ok(vec![]),
            _ => Ok(vec![]),
        }
    }
}

impl Serialize for XclaimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XclaimOutput", 1)?;
        state.serialize_field("messages", &self.messages)?;
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
            let input = XclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                ids: vec![RedisJsonValue::String("1-0".into())],
                idle_ms: None,
                time: None,
                retry_count: None,
                force: None,
                just_id: None,
                last_id: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"XCLAIM"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = XclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                ids: vec![RedisJsonValue::String("1-0".into())],
                idle_ms: Some(RedisJsonValue::Integer(5000)),
                time: None,
                retry_count: Some(RedisJsonValue::Integer(3)),
                force: Some(true),
                just_id: Some(true),
                last_id: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"IDLE"));
            assert!(cmd.windows(10).any(|w| w == b"RETRYCOUNT"));
            assert!(cmd.windows(5).any(|w| w == b"FORCE"));
            assert!(cmd.windows(6).any(|w| w == b"JUSTID"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XclaimInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
                min_idle_time: RedisJsonValue::Integer(10000),
                ids: vec![RedisJsonValue::String("1-0".into())],
                idle_ms: None,
                time: None,
                retry_count: None,
                force: None,
                just_id: None,
                last_id: None,
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
                RedisJsonValue::String("1-0".into()),
            ];
            let input = XclaimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.ids.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_ids() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("1-0".into()),
                RedisJsonValue::String("2-0".into()),
                RedisJsonValue::String("3-0".into()),
            ];
            let input = XclaimInput::decode(args).unwrap();
            assert_eq!(input.ids.len(), 3);
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
                RedisJsonValue::Integer(10000),
                RedisJsonValue::String("1-0".into()),
                RedisJsonValue::String("IDLE".into()),
                RedisJsonValue::Integer(5000),
                RedisJsonValue::String("FORCE".into()),
                RedisJsonValue::String("JUSTID".into()),
            ];
            let input = XclaimInput::decode(args).unwrap();
            assert_eq!(input.idle_ms, Some(RedisJsonValue::Integer(5000)));
            assert_eq!(input.force, Some(true));
            assert_eq!(input.just_id, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let err = XclaimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 5"));
        }

        #[test]
        fn test_output_new() {
            let output = XclaimOutput::new(vec![ClaimedMessage {
                id: "1-0".to_string(),
                fields: Some(vec![("f".to_string(), RedisJsonValue::String("v".into()))]),
            }]);
            assert_eq!(output.claimed_count(), 1);
        }

        #[test]
        fn test_output_serialize() {
            let output = XclaimOutput::new(vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"messages\":[]"));
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
        async fn test_xclaim_no_pending() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xclaim_empty", "field", "value").await;
                    create_group(ctx, "xclaim_empty", "testgroup", "0").await;

                    let result = ctx
                        .raw(
                            &XclaimInput {
                                key: RedisKey::String("xclaim_empty".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                ids: vec![RedisJsonValue::String("9999999999999-0".into())],
                                idle_ms: None,
                                time: None,
                                retry_count: None,
                                force: None,
                                just_id: None,
                                last_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xclaim_single_message() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let id = xadd_entry(ctx, "xclaim_single", "field", "value").await;
                    create_group(ctx, "xclaim_single", "testgroup", "0").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xclaim_single", ">").await;

                    let result = ctx
                        .raw(
                            &XclaimInput {
                                key: RedisKey::String("xclaim_single".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer2".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                ids: vec![RedisJsonValue::String(id.clone())],
                                idle_ms: None,
                                time: None,
                                retry_count: None,
                                force: None,
                                just_id: None,
                                last_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 1);
                    assert_eq!(output.messages()[0].id, id);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xclaim_multiple_messages() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let id1 = xadd_entry(ctx, "xclaim_multi", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xclaim_multi", "f2", "v2").await;
                    create_group(ctx, "xclaim_multi", "testgroup", "0").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xclaim_multi", ">").await;

                    let result = ctx
                        .raw(
                            &XclaimInput {
                                key: RedisKey::String("xclaim_multi".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer2".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                ids: vec![RedisJsonValue::String(id1), RedisJsonValue::String(id2)],
                                idle_ms: None,
                                time: None,
                                retry_count: None,
                                force: None,
                                just_id: None,
                                last_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xclaim_justid() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let id = xadd_entry(ctx, "xclaim_justid", "field", "value").await;
                    create_group(ctx, "xclaim_justid", "testgroup", "0").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xclaim_justid", ">").await;

                    let result = ctx
                        .raw(
                            &XclaimInput {
                                key: RedisKey::String("xclaim_justid".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer2".into()),
                                min_idle_time: RedisJsonValue::Integer(0),
                                ids: vec![RedisJsonValue::String(id)],
                                idle_ms: None,
                                time: None,
                                retry_count: None,
                                force: None,
                                just_id: Some(true),
                                last_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XclaimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.claimed_count(), 1);
                    assert!(output.messages()[0].fields.is_none());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xclaim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let id = xadd_entry(&mut ctx, "xclaim_r2", "field", "value").await;
            create_group(&mut ctx, "xclaim_r2", "testgroup", "0").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xclaim_r2", ">").await;

            let result = ctx
                .raw(
                    &XclaimInput {
                        key: RedisKey::String("xclaim_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer2".into()),
                        min_idle_time: RedisJsonValue::Integer(0),
                        ids: vec![RedisJsonValue::String(id)],
                        idle_ms: None,
                        time: None,
                        retry_count: None,
                        force: None,
                        just_id: None,
                        last_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XclaimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.claimed_count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xclaim_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let id = xadd_entry(&mut ctx, "xclaim_r3", "field", "value").await;
            create_group(&mut ctx, "xclaim_r3", "testgroup", "0").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xclaim_r3", ">").await;

            let result = ctx
                .raw(
                    &XclaimInput {
                        key: RedisKey::String("xclaim_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer2".into()),
                        min_idle_time: RedisJsonValue::Integer(0),
                        ids: vec![RedisJsonValue::String(id)],
                        idle_ms: None,
                        time: None,
                        retry_count: None,
                        force: None,
                        just_id: None,
                        last_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XclaimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.claimed_count(), 1);

            ctx.stop().await;
        }
    }
}
