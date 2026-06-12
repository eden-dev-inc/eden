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

const API_INFO: ApiInfo<RedisApi, XpendingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xpending,
    "Returns the information and entries from a stream consumer group's pending entries list",
    ReqType::Read,
    true,
);

/// Input for Redis `XPENDING` command.
///
/// Returns information about pending messages in a consumer group.
/// The command has two forms:
/// - Summary form: XPENDING key group
/// - Extended form: XPENDING key group [IDLE min-idle-time] start end count [consumer]
///
/// See official Redis documentation for `XPENDING`:
/// https://redis.io/docs/latest/commands/xpending/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XpendingInput {
    /// The key of the stream
    key: RedisKey,
    /// The consumer group name
    group: RedisJsonValue,
    /// Optional extended form parameters
    #[serde(skip_serializing_if = "Option::is_none")]
    filters: Option<PendingFilters>,
}

impl Serialize for XpendingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, group
        if let Some(filters) = &self.filters {
            fields += 3; // start, end, count
            if filters.idle.is_some() {
                fields += 1;
            }
            if filters.consumer.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("XpendingInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        if let Some(filters) = &self.filters {
            if let Some(idle) = &filters.idle {
                state.serialize_field("idle", idle)?;
            }
            state.serialize_field("start", &filters.start)?;
            state.serialize_field("end", &filters.end)?;
            state.serialize_field("count", &filters.count)?;
            if let Some(consumer) = &filters.consumer {
                state.serialize_field("consumer", consumer)?;
            }
        }
        state.end()
    }
}

/// Extended form filters for XPENDING
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema)]
pub struct PendingFilters {
    /// Optional IDLE filter (milliseconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    idle: Option<RedisJsonValue>,
    /// Start of the range (use "-" for minimum)
    start: RedisJsonValue,
    /// End of the range (use "+" for maximum)
    end: RedisJsonValue,
    /// Maximum number of entries to return
    count: RedisJsonValue,
    /// Optional consumer name filter
    #[serde(skip_serializing_if = "Option::is_none")]
    consumer: Option<RedisJsonValue>,
}

impl PendingFilters {
    fn cmd(&self, command: &mut crate::command::Cmd) {
        if let Some(idle) = &self.idle {
            command.arg("IDLE").arg(idle);
        }

        command.arg(&self.start).arg(&self.end).arg(&self.count);

        if let Some(consumer) = &self.consumer {
            command.arg(consumer);
        }
    }
}

impl_redis_operation!(XpendingInput, API_INFO, { key, group, filters });

impl RedisCommandInput for XpendingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.group);

        if let Some(filters) = &self.filters {
            filters.cmd(&mut command);
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
                "XPENDING requires at least 2 arguments (key and group), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let group = args[1].clone();

        // If only key and group, it's the summary form
        if args.len() == 2 {
            return Ok(Self { key, group, filters: None });
        }

        // Extended form parsing
        let mut i = 2;
        let mut idle = None;

        // Check for IDLE
        if let Some(RedisJsonValue::String(s)) = args.get(i)
            && s.to_uppercase() == "IDLE"
        {
            if i + 1 >= args.len() {
                return Err(EpError::parse("IDLE requires a value"));
            }
            idle = Some(args[i + 1].clone());
            i += 2;
        }

        // Need at least start, end, count
        if i + 2 >= args.len() {
            return Err(EpError::parse("Extended XPENDING requires start, end, and count"));
        }

        let start = args[i].clone();
        let end = args[i + 1].clone();
        let count = args[i + 2].clone();
        i += 3;

        // Optional consumer
        let consumer = if i < args.len() { Some(args[i].clone()) } else { None };

        Ok(Self {
            key,
            group,
            filters: Some(PendingFilters { idle, start, end, count, consumer }),
        })
    }
}

/// Summary information returned by XPENDING (without range parameters)
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PendingSummary {
    /// Total number of pending messages
    pub count: i64,
    /// Smallest pending message ID
    pub min_id: Option<String>,
    /// Greatest pending message ID
    pub max_id: Option<String>,
    /// List of consumers with pending message counts
    pub consumers: Vec<ConsumerPending>,
}

/// Consumer pending message count
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ConsumerPending {
    /// Consumer name
    pub name: String,
    /// Number of pending messages
    pub count: i64,
}

impl Serialize for ConsumerPending {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ConsumerPending", 2)?;
        state.serialize_field("name", &self.name)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

impl Serialize for PendingSummary {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PendingSummary", 4)?;
        state.serialize_field("count", &self.count)?;
        state.serialize_field("min_id", &self.min_id)?;
        state.serialize_field("max_id", &self.max_id)?;
        state.serialize_field("consumers", &self.consumers)?;
        state.end()
    }
}

/// Extended pending entry information
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PendingEntry {
    /// Message ID
    pub id: String,
    /// Consumer name
    pub consumer: String,
    /// Milliseconds since last delivery
    pub idle_time: i64,
    /// Number of times delivered
    pub delivery_count: i64,
}

impl Serialize for PendingEntry {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PendingEntry", 4)?;
        state.serialize_field("id", &self.id)?;
        state.serialize_field("consumer", &self.consumer)?;
        state.serialize_field("idle_time", &self.idle_time)?;
        state.serialize_field("delivery_count", &self.delivery_count)?;
        state.end()
    }
}

/// Output for Redis `XPENDING` command.
///
/// Can contain either summary information or extended entry details
/// depending on the command form used.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum XpendingOutput {
    /// Summary form output
    Summary(PendingSummary),
    /// Extended form output (list of pending entries)
    Extended(Vec<PendingEntry>),
}

impl XpendingOutput {
    /// Create a summary output
    pub fn summary(summary: PendingSummary) -> Self {
        Self::Summary(summary)
    }

    /// Create an extended output
    pub fn extended(entries: Vec<PendingEntry>) -> Self {
        Self::Extended(entries)
    }

    /// Get summary if this is a summary response
    pub fn as_summary(&self) -> Option<&PendingSummary> {
        match self {
            Self::Summary(s) => Some(s),
            _ => None,
        }
    }

    /// Get entries if this is an extended response
    pub fn as_extended(&self) -> Option<&[PendingEntry]> {
        match self {
            Self::Extended(e) => Some(e),
            _ => None,
        }
    }

    /// Decode summary form response
    pub fn decode_summary(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let summary = Self::parse_summary(frame)?;
        Ok(Self::Summary(summary))
    }

    /// Decode extended form response
    pub fn decode_extended(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let entries = Self::parse_extended(frame)?;
        Ok(Self::Extended(entries))
    }

    fn parse_summary(frame: DecoderRespFrame) -> Result<PendingSummary, EpError> {
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() < 4 {
                    return Ok(PendingSummary { count: 0, min_id: None, max_id: None, consumers: vec![] });
                }

                let count = Self::extract_int_resp2(&arr[0])?;
                let min_id = Self::extract_optional_string_resp2(&arr[1])?;
                let max_id = Self::extract_optional_string_resp2(&arr[2])?;

                let mut consumers = Vec::new();
                if let Resp2Frame::Array(consumer_arr) = arr[3].clone() {
                    for item in consumer_arr {
                        if let Resp2Frame::Array(pair) = item
                            && pair.len() >= 2
                        {
                            let name = Self::extract_string_resp2(&pair[0])?;
                            let cnt_str = Self::extract_string_resp2(&pair[1])?;
                            let cnt = cnt_str.parse().unwrap_or(0);
                            consumers.push(ConsumerPending { name, count: cnt });
                        }
                    }
                }

                Ok(PendingSummary { count, min_id, max_id, consumers })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() < 4 {
                    return Ok(PendingSummary { count: 0, min_id: None, max_id: None, consumers: vec![] });
                }

                let count = Self::extract_int_resp3(&data[0])?;
                let min_id = Self::extract_optional_string_resp3(&data[1])?;
                let max_id = Self::extract_optional_string_resp3(&data[2])?;

                let mut consumers = Vec::new();
                if let Resp3Frame::Array { data: consumer_arr, .. } = &data[3] {
                    for item in consumer_arr {
                        if let Resp3Frame::Array { data: pair, .. } = item
                            && pair.len() >= 2
                        {
                            let name = Self::extract_string_resp3(&pair[0])?;
                            let cnt_str = Self::extract_string_resp3(&pair[1])?;
                            let cnt = cnt_str.parse().unwrap_or(0);
                            consumers.push(ConsumerPending { name, count: cnt });
                        }
                    }
                }

                Ok(PendingSummary { count, min_id, max_id, consumers })
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected XPENDING response: {:?}", other))),
        }
    }

    fn parse_extended(frame: DecoderRespFrame) -> Result<Vec<PendingEntry>, EpError> {
        let array = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                arr.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>()
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>()
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected XPENDING response: {:?}", other)));
            }
        };

        let mut entries = Vec::new();
        for item in array {
            match item {
                DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) if arr.len() >= 4 => {
                    let id = Self::extract_string_resp2(&arr[0])?;
                    let consumer = Self::extract_string_resp2(&arr[1])?;
                    let idle_time = Self::extract_int_resp2(&arr[2])?;
                    let delivery_count = Self::extract_int_resp2(&arr[3])?;
                    entries.push(PendingEntry { id, consumer, idle_time, delivery_count });
                }
                DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) if data.len() >= 4 => {
                    let id = Self::extract_string_resp3(&data[0])?;
                    let consumer = Self::extract_string_resp3(&data[1])?;
                    let idle_time = Self::extract_int_resp3(&data[2])?;
                    let delivery_count = Self::extract_int_resp3(&data[3])?;
                    entries.push(PendingEntry { id, consumer, idle_time, delivery_count });
                }
                _ => {}
            }
        }

        Ok(entries)
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
}

impl Serialize for XpendingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            Self::Summary(s) => s.serialize(serializer),
            Self::Extended(e) => e.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_summary() {
            let input = XpendingInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                filters: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"XPENDING"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
            assert!(cmd.windows(7).any(|w| w == b"mygroup"));
        }

        #[test]
        fn test_encode_command_extended() {
            let input = XpendingInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                filters: Some(PendingFilters {
                    idle: None,
                    start: RedisJsonValue::String("-".into()),
                    end: RedisJsonValue::String("+".into()),
                    count: RedisJsonValue::Integer(10),
                    consumer: None,
                }),
            };
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"XPENDING"));
        }

        #[test]
        fn test_encode_command_with_idle() {
            let input = XpendingInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                filters: Some(PendingFilters {
                    idle: Some(RedisJsonValue::Integer(5000)),
                    start: RedisJsonValue::String("-".into()),
                    end: RedisJsonValue::String("+".into()),
                    count: RedisJsonValue::Integer(10),
                    consumer: None,
                }),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"IDLE"));
        }

        #[test]
        fn test_encode_command_with_consumer() {
            let input = XpendingInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                filters: Some(PendingFilters {
                    idle: None,
                    start: RedisJsonValue::String("-".into()),
                    end: RedisJsonValue::String("+".into()),
                    count: RedisJsonValue::Integer(10),
                    consumer: Some(RedisJsonValue::String("consumer1".into())),
                }),
            };
            let cmd = input.command();
            assert!(cmd.windows(9).any(|w| w == b"consumer1"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XpendingInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                filters: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_summary() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let input = XpendingInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(input.filters.is_none());
        }

        #[test]
        fn test_decode_input_extended() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = XpendingInput::decode(args).unwrap();
            assert!(input.filters.is_some());
            let filters = input.filters.unwrap();
            assert!(filters.idle.is_none());
            assert_eq!(filters.count, RedisJsonValue::Integer(10));
        }

        #[test]
        fn test_decode_input_with_idle() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("IDLE".into()),
                RedisJsonValue::Integer(5000),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
                RedisJsonValue::Integer(10),
            ];
            let input = XpendingInput::decode(args).unwrap();
            assert!(input.filters.is_some());
            let filters = input.filters.unwrap();
            assert_eq!(filters.idle, Some(RedisJsonValue::Integer(5000)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XpendingInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_pending_filters_default() {
            let filters = PendingFilters::default();
            assert!(filters.idle.is_none());
            assert!(filters.consumer.is_none());
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
        async fn test_xpending_summary_no_pending() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    create_group(ctx, "xpend_no_pend", "mygroup").await;

                    let result = ctx
                        .raw(
                            &XpendingInput {
                                key: RedisKey::String("xpend_no_pend".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                                filters: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XpendingOutput::decode_summary(&result).expect("decode failed");
                    let summary = output.as_summary().unwrap();
                    assert_eq!(summary.count, 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_summary_with_pending() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xpend_pending", "field", "value").await;
                    create_group(ctx, "xpend_pending", "mygroup").await;
                    xreadgroup(ctx, "mygroup", "consumer1", "xpend_pending", ">").await;

                    let result = ctx
                        .raw(
                            &XpendingInput {
                                key: RedisKey::String("xpend_pending".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                                filters: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XpendingOutput::decode_summary(&result).expect("decode failed");
                    let summary = output.as_summary().unwrap();
                    assert_eq!(summary.count, 1);
                    assert!(summary.min_id.is_some());
                    assert!(summary.max_id.is_some());
                    assert_eq!(summary.consumers.len(), 1);
                    assert_eq!(summary.consumers[0].name, "consumer1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_extended() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xpend_extended", "field", "value").await;
                    create_group(ctx, "xpend_extended", "mygroup").await;
                    xreadgroup(ctx, "mygroup", "consumer1", "xpend_extended", ">").await;

                    let result = ctx
                        .raw(
                            &XpendingInput {
                                key: RedisKey::String("xpend_extended".into()),
                                group: RedisJsonValue::String("mygroup".into()),
                                filters: Some(PendingFilters {
                                    idle: None,
                                    start: RedisJsonValue::String("-".into()),
                                    end: RedisJsonValue::String("+".into()),
                                    count: RedisJsonValue::Integer(10),
                                    consumer: None,
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XpendingOutput::decode_extended(&result).expect("decode failed");
                    let entries = output.as_extended().unwrap();
                    assert_eq!(entries.len(), 1);
                    assert_eq!(entries[0].consumer, "consumer1");
                    assert_eq!(entries[0].delivery_count, 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_nonexistent_group() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xpend_nogroup", "field", "value").await;

                    let result = ctx
                        .raw(
                            &XpendingInput {
                                key: RedisKey::String("xpend_nogroup".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                                filters: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XpendingOutput::decode_summary(&result).unwrap_err();
                    assert!(err.to_string().contains("NOGROUP"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xpend_r2", "field", "value").await;
            create_group(&mut ctx, "xpend_r2", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xpend_r2", ">").await;

            let result = ctx
                .raw(
                    &XpendingInput {
                        key: RedisKey::String("xpend_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        filters: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XpendingOutput::decode_summary(&result).expect("decode failed");
            assert_eq!(output.as_summary().unwrap().count, 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xpend_r3", "field", "value").await;
            create_group(&mut ctx, "xpend_r3", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xpend_r3", ">").await;

            let result = ctx
                .raw(
                    &XpendingInput {
                        key: RedisKey::String("xpend_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        filters: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XpendingOutput::decode_summary(&result).expect("decode failed");
            assert_eq!(output.as_summary().unwrap().count, 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xpending_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xpend_pipe1", "f", "v").await;
                    xadd_entry(ctx, "xpend_pipe2", "f", "v").await;
                    create_group(ctx, "xpend_pipe1", "group1").await;
                    create_group(ctx, "xpend_pipe2", "group2").await;
                    xreadgroup(ctx, "group1", "c1", "xpend_pipe1", ">").await;
                    xreadgroup(ctx, "group2", "c2", "xpend_pipe2", ">").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XpendingInput {
                            key: RedisKey::String("xpend_pipe1".into()),
                            group: RedisJsonValue::String("group1".into()),
                            filters: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XpendingInput {
                            key: RedisKey::String("xpend_pipe2".into()),
                            group: RedisJsonValue::String("group2".into()),
                            filters: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XpendingOutput::decode_summary(responses[0]).expect("decode first");
                    assert_eq!(out1.as_summary().unwrap().count, 1);

                    let out2 = XpendingOutput::decode_summary(responses[1]).expect("decode second");
                    assert_eq!(out2.as_summary().unwrap().count, 1);
                })
            })
            .await;
        }
    }
}
