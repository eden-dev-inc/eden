use crate::api::lib::server_management::acl::Events;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use redis_protocol::resp3::types::FrameMap;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, AclLogInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclLog,
    "Lists recent security events generated due to ACL rules",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL LOG`
/// https://redis.io/docs/latest/commands/acl-log/
///
/// When `events` is `Some(Events::RESET)`, the log is reset.
/// When `events` is `Some(Events::COUNT(n))`, returns up to n entries.
/// When `events` is `None`, returns the default number of entries.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclLogInput {
    events: Option<Events>,
}

impl Serialize for AclLogInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;

        if self.events.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("AclLogInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(events) = &self.events {
            state.serialize_field("events", events)?;
        }
        state.end()
    }
}

impl_redis_operation!(AclLogInput, API_INFO, { events });

impl RedisCommandInput for AclLogInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(events) = &self.events {
            match events {
                Events::COUNT(count) => {
                    command.arg(count);
                }
                Events::RESET => {
                    command.arg("RESET");
                }
            };
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "ACL LOG expected 0 or 1 argument, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self {
            events: args.first().map(|e| match e {
                RedisJsonValue::Integer(count) => Events::COUNT(RedisJsonValue::Integer(*count)),
                RedisJsonValue::Float(count) => Events::COUNT(RedisJsonValue::Float(*count)),
                RedisJsonValue::String(s) if s.eq_ignore_ascii_case("reset") => Events::RESET,
                other => Events::COUNT(other.clone()),
            }),
        })
    }
}

/// A single ACL log entry
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, Serialize)]
pub(crate) struct AclLogEntry {
    /// Number of similar events
    pub count: i64,
    /// Reason for the log entry (e.g., "command", "key", "channel")
    pub reason: String,
    /// Context of the event
    pub context: String,
    /// The object (command, key, or channel) that triggered the event
    pub object: String,
    /// Username that triggered the event
    pub username: String,
    /// Age of the entry in seconds
    pub age_seconds: f64,
    /// Client info string
    pub client_info: String,
    /// Entry ID
    pub entry_id: i64,
    /// Timestamp in milliseconds
    pub timestamp_created: i64,
    /// Raw properties
    pub properties: HashMap<String, RedisJsonValue>,
}

/// Output for Redis ACL LOG command
///
/// Returns a list of ACL log entries or OK for RESET.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclLogOutput {
    /// List of log entries (empty if RESET was called)
    entries: Vec<AclLogEntry>,
    /// Whether this was a RESET response
    was_reset: bool,
}

impl AclLogOutput {
    pub fn new(entries: Vec<AclLogEntry>) -> Self {
        Self { entries, was_reset: false }
    }

    pub fn new_reset() -> Self {
        Self { entries: Vec::new(), was_reset: true }
    }

    /// Get the log entries
    pub fn entries(&self) -> &[AclLogEntry] {
        &self.entries
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if there are no entries
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Check if this was a RESET response
    pub fn was_reset(&self) -> bool {
        self.was_reset
    }

    /// Decode the Redis protocol response into an AclLogOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::SimpleString(bytes) => {
                let s = String::from_utf8(bytes).map_err(EpError::parse)?;
                if s == "OK" {
                    Ok(Self::new_reset())
                } else {
                    Err(EpError::parse(format!("unexpected response: {}", s)))
                }
            }
            Resp2Frame::Array(arr) => {
                let mut entries = Vec::with_capacity(arr.len());
                for item in arr {
                    if let Resp2Frame::Array(entry_arr) = item {
                        entries.push(Self::parse_entry_resp2(entry_arr)?);
                    }
                }
                Ok(Self::new(entries))
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected ACL LOG response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::SimpleString { data, .. } => {
                let s = String::from_utf8(data).map_err(EpError::parse)?;
                if s == "OK" {
                    Ok(Self::new_reset())
                } else {
                    Err(EpError::parse(format!("unexpected response: {}", s)))
                }
            }
            Resp3Frame::Array { data, .. } => {
                let mut entries = Vec::with_capacity(data.len());
                for item in data {
                    match item {
                        Resp3Frame::Map { data: map_data, .. } => {
                            entries.push(Self::parse_entry_resp3_map(map_data)?);
                        }
                        Resp3Frame::Array { data: arr_data, .. } => {
                            entries.push(Self::parse_entry_resp3_array(arr_data)?);
                        }
                        _ => {}
                    }
                }
                Ok(Self::new(entries))
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected ACL LOG response: {:?}", other))),
        }
    }

    fn parse_entry_resp2(arr: Vec<Resp2Frame>) -> Result<AclLogEntry, EpError> {
        let mut properties = HashMap::new();
        let mut count = 0i64;
        let mut reason = String::new();
        let mut context = String::new();
        let mut object = String::new();
        let mut username = String::new();
        let mut age_seconds = 0.0f64;
        let mut client_info = String::new();
        let mut entry_id = 0i64;
        let mut timestamp_created = 0i64;

        let mut iter = arr.into_iter();
        while let Some(key_frame) = iter.next() {
            let key = match key_frame {
                Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                _ => continue,
            };

            if let Some(value_frame) = iter.next() {
                match key.as_str() {
                    "count" => {
                        if let Resp2Frame::Integer(n) = &value_frame {
                            count = *n;
                        }
                    }
                    "reason" => {
                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                            reason = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "context" => {
                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                            context = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "object" => {
                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                            object = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "username" => {
                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                            username = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "age-seconds" => {
                        if let Resp2Frame::BulkString(bytes) = &value_frame {
                            let s = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                            age_seconds = s.parse().unwrap_or(0.0);
                        }
                    }
                    "client-info" => {
                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                            client_info = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "entry-id" => {
                        if let Resp2Frame::Integer(n) = &value_frame {
                            entry_id = *n;
                        }
                    }
                    "timestamp-created" => {
                        if let Resp2Frame::Integer(n) = &value_frame {
                            timestamp_created = *n;
                        }
                    }
                    _ => {}
                }
                properties.insert(key, Self::frame_to_json_value_resp2(value_frame)?);
            }
        }

        Ok(AclLogEntry {
            count,
            reason,
            context,
            object,
            username,
            age_seconds,
            client_info,
            entry_id,
            timestamp_created,
            properties,
        })
    }

    fn parse_entry_resp3_map(data: FrameMap<Resp3Frame, Resp3Frame>) -> Result<AclLogEntry, EpError> {
        let mut properties = HashMap::new();
        let mut count = 0i64;
        let mut reason = String::new();
        let mut context = String::new();
        let mut object = String::new();
        let mut username = String::new();
        let mut age_seconds = 0.0f64;
        let mut client_info = String::new();
        let mut entry_id = 0i64;
        let mut timestamp_created = 0i64;

        for (key_frame, value_frame) in data {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    String::from_utf8(data).map_err(EpError::parse)?
                }
                _ => continue,
            };

            match key.as_str() {
                "count" => {
                    if let Resp3Frame::Number { data, .. } = &value_frame {
                        count = *data;
                    }
                }
                "reason" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                        reason = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                    }
                }
                "context" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                        context = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                    }
                }
                "object" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                        object = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                    }
                }
                "username" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                        username = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                    }
                }
                "age-seconds" => {
                    if let Resp3Frame::Double { data, .. } = &value_frame {
                        age_seconds = *data;
                    } else if let Resp3Frame::BlobString { data, .. } = &value_frame {
                        let s = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        age_seconds = s.parse().unwrap_or(0.0);
                    }
                }
                "client-info" => {
                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                        client_info = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                    }
                }
                "entry-id" => {
                    if let Resp3Frame::Number { data, .. } = &value_frame {
                        entry_id = *data;
                    }
                }
                "timestamp-created" => {
                    if let Resp3Frame::Number { data, .. } = &value_frame {
                        timestamp_created = *data;
                    }
                }
                _ => {}
            }
            properties.insert(key, Self::frame_to_json_value_resp3(value_frame)?);
        }

        Ok(AclLogEntry {
            count,
            reason,
            context,
            object,
            username,
            age_seconds,
            client_info,
            entry_id,
            timestamp_created,
            properties,
        })
    }

    fn parse_entry_resp3_array(arr: Vec<Resp3Frame>) -> Result<AclLogEntry, EpError> {
        let mut properties = HashMap::new();
        let mut count = 0i64;
        let mut reason = String::new();
        let mut context = String::new();
        let mut object = String::new();
        let mut username = String::new();
        let mut age_seconds = 0.0f64;
        let mut client_info = String::new();
        let mut entry_id = 0i64;
        let mut timestamp_created = 0i64;

        let mut iter = arr.into_iter();
        while let Some(key_frame) = iter.next() {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                    String::from_utf8(data).map_err(EpError::parse)?
                }
                _ => continue,
            };

            if let Some(value_frame) = iter.next() {
                match key.as_str() {
                    "count" => {
                        if let Resp3Frame::Number { data, .. } = &value_frame {
                            count = *data;
                        }
                    }
                    "reason" => {
                        if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                            reason = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "context" => {
                        if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                            context = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "object" => {
                        if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                            object = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "username" => {
                        if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                            username = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "age-seconds" => {
                        if let Resp3Frame::Double { data, .. } = &value_frame {
                            age_seconds = *data;
                        }
                    }
                    "client-info" => {
                        if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                            client_info = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                        }
                    }
                    "entry-id" => {
                        if let Resp3Frame::Number { data, .. } = &value_frame {
                            entry_id = *data;
                        }
                    }
                    "timestamp-created" => {
                        if let Resp3Frame::Number { data, .. } = &value_frame {
                            timestamp_created = *data;
                        }
                    }
                    _ => {}
                }
                properties.insert(key, Self::frame_to_json_value_resp3(value_frame)?);
            }
        }

        Ok(AclLogEntry {
            count,
            reason,
            context,
            object,
            username,
            age_seconds,
            client_info,
            entry_id,
            timestamp_created,
            properties,
        })
    }

    fn frame_to_json_value_resp2(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => {
                Ok(RedisJsonValue::String(String::from_utf8(bytes).map_err(EpError::parse)?))
            }
            Resp2Frame::Integer(n) => Ok(RedisJsonValue::Integer(n)),
            Resp2Frame::Array(arr) => {
                let items: Result<Vec<_>, _> = arr.into_iter().map(Self::frame_to_json_value_resp2).collect();
                Ok(RedisJsonValue::Array(items?))
            }
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
        }
    }

    fn frame_to_json_value_resp3(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)
            }
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(data),
            Resp3Frame::Double { data, .. } => RedisJsonValue::Float(data),
            Resp3Frame::Array { data, .. } => {
                let items: Result<Vec<_>, _> = data.into_iter().map(Self::frame_to_json_value_resp3).collect();
                RedisJsonValue::Array(items?)
            }
            Resp3Frame::Set { data, .. } => {
                let items: Result<Vec<_>, _> = data.into_iter().map(Self::frame_to_json_value_resp3).collect();
                RedisJsonValue::Array(items?)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }
}

impl Serialize for AclLogOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclLogOutput", 2)?;
        state.serialize_field("entries", &self.entries)?;
        state.serialize_field("was_reset", &self.was_reset)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_default() {
            let input = AclLogInput { events: None };
            // ACL LOG splits into: ACL, LOG
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$3\r\nLOG\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = AclLogInput { events: Some(Events::COUNT(RedisJsonValue::Integer(5))) };
            // ACL LOG splits into: ACL, LOG, 5
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$3\r\nLOG\r\n$1\r\n5\r\n");
        }

        #[test]
        fn test_encode_command_reset() {
            let input = AclLogInput { events: Some(Events::RESET) };
            // ACL LOG splits into: ACL, LOG, RESET
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$3\r\nLOG\r\n$5\r\nRESET\r\n");
        }

        #[test]
        fn test_decode_ok_reset() {
            let output = AclLogOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.was_reset());
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_empty_array() {
            let output = AclLogOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.was_reset());
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclLogOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = AclLogInput::decode(vec![]).unwrap();
            assert!(input.events.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::Integer(10)];
            let input = AclLogInput::decode(args).unwrap();
            assert!(matches!(input.events, Some(Events::COUNT(_))));
        }

        #[test]
        fn test_decode_input_with_reset() {
            let args = vec![RedisJsonValue::String("RESET".into())];
            let input = AclLogInput::decode(args).unwrap();
            assert!(matches!(input.events, Some(Events::RESET)));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclLogInput { events: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclLogInput { events: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclLog);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_log_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclLogInput { events: None }.command()).await.expect("raw failed");

                    let output = AclLogOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_reset());
                    // Log may be empty on fresh instance
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_log_reset() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclLogInput { events: Some(Events::RESET) }.command()).await.expect("raw failed");

                    let output = AclLogOutput::decode(&result).expect("decode failed");
                    assert!(output.was_reset());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_log_with_count() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&AclLogInput { events: Some(Events::COUNT(RedisJsonValue::Integer(5))) }.command())
                        .await
                        .expect("raw failed");

                    let output = AclLogOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_reset());
                    assert!(output.len() <= 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_log_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclLogInput { events: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = AclLogOutput::decode(&result).expect("decode failed");
            assert!(!output.was_reset());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_log_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclLogInput { events: None }.command()).await.expect("raw failed");

            let output = AclLogOutput::decode(&result).expect("decode failed");
            assert!(!output.was_reset());

            ctx.stop().await;
        }
    }
}
