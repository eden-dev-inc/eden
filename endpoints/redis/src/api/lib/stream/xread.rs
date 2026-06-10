use crate::api::lib::stream::xrange::StreamEntry;
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

const API_INFO: ApiInfo<RedisApi, XreadInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xread,
    "Returns messages from multiple streams with IDs greater than the ones requested. Blocks until a message is available otherwise",
    ReqType::Read,
    false,
);

/// Input for Redis `XREAD` command.
///
/// Read data from one or multiple streams, only returning entries with
/// IDs greater than the last received IDs.
///
/// See official Redis documentation for `XREAD`:
/// https://redis.io/docs/latest/commands/xread/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XreadInput {
    /// Optional maximum number of entries per stream
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
    /// Optional blocking timeout in milliseconds (0 = block forever)
    #[serde(skip_serializing_if = "Option::is_none")]
    block: Option<RedisJsonValue>,
    /// The stream keys to read from
    keys: Vec<RedisKey>,
    /// The IDs to start reading from (use "$" for new entries only, "0" for all)
    ids: Vec<RedisJsonValue>,
}

impl Serialize for XreadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, keys, ids
        if self.count.is_some() {
            fields += 1;
        }
        if self.block.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XreadInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(block) = &self.block {
            state.serialize_field("block", block)?;
        }
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("ids", &self.ids)?;
        state.end()
    }
}

impl_redis_operation!(XreadInput, API_INFO, { count, block, keys, ids });

impl RedisCommandInput for XreadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(c) = &self.count {
            command.arg("COUNT").arg(c);
        }

        if let Some(b) = &self.block {
            command.arg("BLOCK").arg(b);
        }

        command.arg("STREAMS");
        for key in &self.keys {
            command.arg(key);
        }
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
            return Err(EpError::parse(format!("XREAD requires at least 3 arguments, given {}", args.len())));
        }

        let mut count = None;
        let mut block = None;
        let mut keys = Vec::new();
        let mut i = 0;
        let mut streams_found = false;

        // Parse optional parameters first
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "COUNT" && i + 1 < args.len() {
                    count = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "BLOCK" && i + 1 < args.len() {
                    block = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "STREAMS" {
                    streams_found = true;
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        if !streams_found {
            return Err(EpError::parse("XREAD requires STREAMS keyword".to_string()));
        }

        // Split remaining args into keys and ids
        let remaining_args = &args[i..];
        let mid = remaining_args.len() / 2;
        for key in remaining_args[..mid].iter() {
            keys.push(key.try_into()?);
        }
        let ids = remaining_args[mid..].to_vec();

        Ok(Self { count, block, keys, ids })
    }
}

/// A stream with its entries from XREAD
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XreadStreamResult {
    /// The stream key
    pub key: String,
    /// The entries from this stream
    pub entries: Vec<StreamEntry>,
}

/// Output for Redis `XREAD` command.
///
/// Returns entries from multiple streams.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XreadOutput {
    /// Results from each stream (None if timeout or no data)
    streams: Option<Vec<XreadStreamResult>>,
}

impl XreadOutput {
    /// Create a new XreadOutput
    pub fn new(streams: Option<Vec<XreadStreamResult>>) -> Self {
        Self { streams }
    }

    /// Get the stream results
    pub fn streams(&self) -> Option<&[XreadStreamResult]> {
        self.streams.as_deref()
    }

    /// Check if no data was returned (timeout or empty)
    pub fn is_empty(&self) -> bool {
        self.streams.is_none() || self.streams.as_ref().is_none_or(|s| s.is_empty())
    }

    /// Get total entry count across all streams
    pub fn total_entries(&self) -> usize {
        self.streams.as_ref().map_or(0, |streams| streams.iter().map(|s| s.entries.len()).sum())
    }

    /// Decode the Redis protocol response into an XreadOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let streams = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(&resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(&resp3_frame)?,
        };

        Ok(Self { streams })
    }

    fn decode_resp2(frame: &Resp2Frame) -> Result<Option<Vec<XreadStreamResult>>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut streams = Vec::new();
                for item in items {
                    if let Resp2Frame::Array(stream_parts) = item
                        && stream_parts.len() >= 2
                    {
                        let key = Self::extract_string_resp2(&stream_parts[0])?;
                        let entries = Self::decode_entries_resp2(&stream_parts[1])?;
                        streams.push(XreadStreamResult { key, entries });
                    }
                }
                Ok(Some(streams))
            }
            Resp2Frame::Null => Ok(None),
            Resp2Frame::Error(e) => Err(EpError::parse(e.to_string())),
            other => Err(EpError::parse(format!("unexpected XREAD response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: &Resp3Frame) -> Result<Option<Vec<XreadStreamResult>>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut streams = Vec::new();
                for item in data {
                    if let Resp3Frame::Array { data: stream_parts, .. } = item
                        && stream_parts.len() >= 2
                    {
                        let key = Self::extract_string_resp3(&stream_parts[0])?;
                        let entries = Self::decode_entries_resp3(&stream_parts[1])?;
                        streams.push(XreadStreamResult { key, entries });
                    }
                }
                Ok(Some(streams))
            }
            Resp3Frame::Map { data, .. } => {
                let mut streams = Vec::new();
                for (k, v) in data {
                    let key = Self::extract_string_resp3(k)?;
                    let entries = Self::decode_entries_resp3(v)?;
                    streams.push(XreadStreamResult { key, entries });
                }
                Ok(Some(streams))
            }
            Resp3Frame::Null => Ok(None),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data.to_string())),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(data).to_string())),
            other => Err(EpError::parse(format!("unexpected XREAD response: {:?}", other))),
        }
    }

    fn decode_entries_resp2(frame: &Resp2Frame) -> Result<Vec<StreamEntry>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut entries = Vec::new();
                for item in items {
                    if let Resp2Frame::Array(entry_parts) = item
                        && entry_parts.len() >= 2
                    {
                        let id = Self::extract_string_resp2(&entry_parts[0])?;
                        let fields = Self::extract_fields_resp2(&entry_parts[1])?;
                        entries.push(StreamEntry { id, fields });
                    }
                }
                Ok(entries)
            }
            other => Err(EpError::parse(format!("expected array of entries, got {:?}", other))),
        }
    }

    fn decode_entries_resp3(frame: &Resp3Frame) -> Result<Vec<StreamEntry>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut entries = Vec::new();
                for item in data {
                    if let Resp3Frame::Array { data: entry_parts, .. } = item
                        && entry_parts.len() >= 2
                    {
                        let id = Self::extract_string_resp3(&entry_parts[0])?;
                        let fields = Self::extract_fields_resp3(&entry_parts[1])?;
                        entries.push(StreamEntry { id, fields });
                    }
                }
                Ok(entries)
            }
            other => Err(EpError::parse(format!("expected array of entries, got {:?}", other))),
        }
    }

    fn extract_string_resp2(frame: &Resp2Frame) -> Result<String, EpError> {
        match frame {
            Resp2Frame::BulkString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp2Frame::SimpleString(data) => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_string_resp3(frame: &Resp3Frame) -> Result<String, EpError> {
        match frame {
            Resp3Frame::BlobString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            Resp3Frame::SimpleString { data, .. } => Ok(String::from_utf8(data.to_vec()).map_err(EpError::parse)?),
            other => Err(EpError::parse(format!("expected string, got {:?}", other))),
        }
    }

    fn extract_fields_resp2(frame: &Resp2Frame) -> Result<Vec<(String, String)>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut fields = Vec::new();
                let mut i = 0;
                while i + 1 < items.len() {
                    let key = Self::extract_string_resp2(&items[i])?;
                    let value = Self::extract_string_resp2(&items[i + 1])?;
                    fields.push((key, value));
                    i += 2;
                }
                Ok(fields)
            }
            other => Err(EpError::parse(format!("expected array of fields, got {:?}", other))),
        }
    }

    fn extract_fields_resp3(frame: &Resp3Frame) -> Result<Vec<(String, String)>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut fields = Vec::new();
                let mut i = 0;
                while i + 1 < data.len() {
                    let key = Self::extract_string_resp3(&data[i])?;
                    let value = Self::extract_string_resp3(&data[i + 1])?;
                    fields.push((key, value));
                    i += 2;
                }
                Ok(fields)
            }
            Resp3Frame::Map { data, .. } => {
                let mut fields = Vec::new();
                for (k, v) in data {
                    let key = Self::extract_string_resp3(k)?;
                    let value = Self::extract_string_resp3(v)?;
                    fields.push((key, value));
                }
                Ok(fields)
            }
            other => Err(EpError::parse(format!("expected array of fields, got {:?}", other))),
        }
    }
}

impl Serialize for XreadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XreadOutput", 1)?;
        state.serialize_field("streams", &self.streams)?;
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
            let input = XreadInput {
                count: None,
                block: None,
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String("0".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"XREAD"));
            assert!(cmd.windows(7).any(|w| w == b"STREAMS"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = XreadInput {
                count: Some(RedisJsonValue::Integer(10)),
                block: None,
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String("0".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_encode_command_with_block() {
            let input = XreadInput {
                count: None,
                block: Some(RedisJsonValue::Integer(1000)),
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String("$".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"BLOCK"));
        }

        #[test]
        fn test_encode_command_multiple_streams() {
            let input = XreadInput {
                count: None,
                block: None,
                keys: vec![RedisKey::String("stream1".into()), RedisKey::String("stream2".into())],
                ids: vec![RedisJsonValue::String("0".into()), RedisJsonValue::String("0".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"stream1"));
            assert!(cmd.windows(7).any(|w| w == b"stream2"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XreadInput {
                count: None,
                block: None,
                keys: vec![RedisKey::String("stream1".into()), RedisKey::String("stream2".into())],
                ids: vec![RedisJsonValue::String("0".into()), RedisJsonValue::String("0".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("STREAMS".into()),
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("0".into()),
            ];
            let input = XreadInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.ids.len(), 1);
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
                RedisJsonValue::String("BLOCK".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("STREAMS".into()),
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("0".into()),
            ];
            let input = XreadInput::decode(args).unwrap();
            assert!(input.count.is_some());
            assert!(input.block.is_some());
        }

        #[test]
        fn test_decode_input_missing_streams() {
            // Need at least 3 args to pass the length check and reach the STREAMS check
            let args = vec![
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(10),
                RedisJsonValue::String("extra".into()),
            ];
            let err = XreadInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("STREAMS"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = XreadOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XreadOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = XreadOutput::new(Some(vec![XreadStreamResult { key: "mystream".to_string(), entries: vec![] }]));
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_total_entries() {
            let output = XreadOutput::new(Some(vec![
                XreadStreamResult {
                    key: "stream1".to_string(),
                    entries: vec![StreamEntry { id: "1-0".to_string(), fields: vec![] }],
                },
                XreadStreamResult {
                    key: "stream2".to_string(),
                    entries: vec![
                        StreamEntry { id: "2-0".to_string(), fields: vec![] },
                        StreamEntry { id: "2-1".to_string(), fields: vec![] },
                    ],
                },
            ]));
            assert_eq!(output.total_entries(), 3);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::stream::xadd::{Entry, Id, XaddInput, XaddOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        async fn xadd_entry(ctx: &mut TestContext, key: &str, field: &str, value: &str) -> String {
            let result = ctx
                .raw(
                    &XaddInput {
                        key: RedisKey::String(key.into()),
                        no_mk_stream: None,
                        trim: None,
                        id: Id::Auto,
                        entries: vec![Entry {
                            field: RedisJsonValue::String(field.into()),
                            value: RedisJsonValue::String(value.into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("XADD failed");

            XaddOutput::decode(&result).expect("decode XADD failed").id().unwrap().to_string()
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let _id = xadd_entry(ctx, "xread_basic", "f", "v").await;

                    let result = ctx
                        .raw(
                            &XreadInput {
                                count: None,
                                block: None,
                                keys: vec![RedisKey::String("xread_basic".into())],
                                ids: vec![RedisJsonValue::String("0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    let streams = output.streams().unwrap();
                    assert_eq!(streams.len(), 1);
                    assert_eq!(streams[0].key, "xread_basic");
                    assert_eq!(streams[0].entries.len(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_multiple_streams() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xread_multi1", "f1", "v1").await;
                    xadd_entry(ctx, "xread_multi2", "f2", "v2").await;

                    let result = ctx
                        .raw(
                            &XreadInput {
                                count: None,
                                block: None,
                                keys: vec![RedisKey::String("xread_multi1".into()), RedisKey::String("xread_multi2".into())],
                                ids: vec![RedisJsonValue::String("0".into()), RedisJsonValue::String("0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.total_entries(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_with_count() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    for i in 0..5 {
                        xadd_entry(ctx, "xread_count", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    let result = ctx
                        .raw(
                            &XreadInput {
                                count: Some(RedisJsonValue::Integer(2)),
                                block: None,
                                keys: vec![RedisKey::String("xread_count".into())],
                                ids: vec![RedisJsonValue::String("0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.total_entries(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_nonexistent_key() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XreadInput {
                                count: None,
                                block: None,
                                keys: vec![RedisKey::String("xread_nonexistent".into())],
                                ids: vec![RedisJsonValue::String("0".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xread_r2", "f", "v").await;

            let result = ctx
                .raw(
                    &XreadInput {
                        count: None,
                        block: None,
                        keys: vec![RedisKey::String("xread_r2".into())],
                        ids: vec![RedisJsonValue::String("0".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XreadOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xread_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xread_r3", "f", "v").await;

            let result = ctx
                .raw(
                    &XreadInput {
                        count: None,
                        block: None,
                        keys: vec![RedisKey::String("xread_r3".into())],
                        ids: vec![RedisJsonValue::String("0".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XreadOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }
    }
}
