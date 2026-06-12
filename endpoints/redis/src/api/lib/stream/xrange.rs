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

const API_INFO: ApiInfo<RedisApi, XrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xrange,
    "Returns the messages from a stream within a range of IDs",
    ReqType::Read,
    true,
);

/// Input for Redis `XRANGE` command.
///
/// Returns the stream entries matching a given range of IDs.
///
/// See official Redis documentation for `XRANGE`:
/// https://redis.io/docs/latest/commands/xrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XrangeInput {
    /// The key of the stream
    key: RedisKey,
    /// Start of the range (use "-" for minimum ID)
    start: RedisJsonValue,
    /// End of the range (use "+" for maximum ID)
    end: RedisJsonValue,
    /// Optional maximum number of entries to return
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
}

impl Serialize for XrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, start, end
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("end", &self.end)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(XrangeInput, API_INFO, { key, start, end, count });

impl RedisCommandInput for XrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.start).arg(&self.end);

        if let Some(c) = &self.count {
            command.arg("COUNT").arg(c);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("XRANGE requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let start = args[1].clone();
        let end = args[2].clone();
        let mut count = None;

        if args.len() >= 5
            && let RedisJsonValue::String(s) = &args[3]
            && s.to_uppercase() == "COUNT"
        {
            count = Some(args[4].clone());
        }

        Ok(Self { key, start, end, count })
    }
}

/// A single stream entry with ID and field-value pairs
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct StreamEntry {
    /// The entry ID (e.g., "1234567890123-0")
    pub id: String,
    /// The field-value pairs in this entry
    pub fields: Vec<(String, String)>,
}

/// Output for Redis `XRANGE` command.
///
/// Returns a list of stream entries within the specified range.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XrangeOutput {
    /// The entries returned from the stream
    entries: Vec<StreamEntry>,
}

impl XrangeOutput {
    /// Create a new XrangeOutput
    pub fn new(entries: Vec<StreamEntry>) -> Self {
        Self { entries }
    }

    /// Get the entries
    pub fn entries(&self) -> &[StreamEntry] {
        &self.entries
    }

    /// Check if the result is empty
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Decode the Redis protocol response into an XrangeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let entries = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame)?,
        };

        Ok(Self { entries })
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Vec<StreamEntry>, EpError> {
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
            Resp2Frame::Null => Ok(Vec::new()),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected XRANGE response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Vec<StreamEntry>, EpError> {
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
            Resp3Frame::Null => Ok(Vec::new()),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data.to_string())),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected XRANGE response: {:?}", other))),
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

impl Serialize for XrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XrangeOutput", 1)?;
        state.serialize_field("entries", &self.entries)?;
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
            let input = XrangeInput {
                key: RedisKey::String("mystream".into()),
                start: RedisJsonValue::String("-".into()),
                end: RedisJsonValue::String("+".into()),
                count: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            assert!(cmd.windows(6).any(|w| w == b"XRANGE"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = XrangeInput {
                key: RedisKey::String("mystream".into()),
                start: RedisJsonValue::String("-".into()),
                end: RedisJsonValue::String("+".into()),
                count: Some(RedisJsonValue::Integer(10)),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n")); // XRANGE key start end COUNT 10
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_encode_command_with_specific_ids() {
            let input = XrangeInput {
                key: RedisKey::String("mystream".into()),
                start: RedisJsonValue::String("1234567890123-0".into()),
                end: RedisJsonValue::String("1234567890124-0".into()),
                count: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(15).any(|w| w == b"1234567890123-0"));
            assert!(cmd.windows(15).any(|w| w == b"1234567890124-0"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XrangeInput {
                key: RedisKey::String("mystream".into()),
                start: RedisJsonValue::String("-".into()),
                end: RedisJsonValue::String("+".into()),
                count: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            ];
            let input = XrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = XrangeInput::decode(args).unwrap();
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("-".into())];
            let err = XrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3"));
        }

        #[test]
        fn test_decode_output_empty() {
            // Empty array response
            let output = XrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_null() {
            // Null response (non-existent key)
            let output = XrangeOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XrangeOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let entries = vec![StreamEntry {
                id: "1234-0".to_string(),
                fields: vec![("field".to_string(), "value".to_string())],
            }];
            let output = XrangeOutput::new(entries);
            assert_eq!(output.len(), 1);
            assert_eq!(output.entries()[0].id, "1234-0");
        }

        #[test]
        fn test_output_serialize() {
            let output = XrangeOutput::new(vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("entries"));
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
        async fn test_xrange_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries
                    let id1 = xadd_entry(ctx, "xrange_basic", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xrange_basic", "f2", "v2").await;

                    // Get all entries
                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_basic".into()),
                                start: RedisJsonValue::String("-".into()),
                                end: RedisJsonValue::String("+".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.entries()[0].id, id1);
                    assert_eq!(output.entries()[1].id, id2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_with_count() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add multiple entries
                    for i in 0..5 {
                        xadd_entry(ctx, "xrange_count", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Get only 2 entries
                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_count".into()),
                                start: RedisJsonValue::String("-".into()),
                                end: RedisJsonValue::String("+".into()),
                                count: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_specific_range() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries
                    let id1 = xadd_entry(ctx, "xrange_range", "f1", "v1").await;
                    let _id2 = xadd_entry(ctx, "xrange_range", "f2", "v2").await;
                    let _id3 = xadd_entry(ctx, "xrange_range", "f3", "v3").await;

                    // Get range from id1 to id1 (inclusive)
                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_range".into()),
                                start: RedisJsonValue::String(id1.clone()),
                                end: RedisJsonValue::String(id1.clone()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].id, id1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_empty_stream() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create empty stream using XADD + XDEL
                    let id = xadd_entry(ctx, "xrange_empty", "f", "v").await;
                    ctx.raw(format!("*3\r\n$4\r\nXDEL\r\n$12\r\nxrange_empty\r\n${}\r\n{}\r\n", id.len(), id).as_bytes())
                        .await
                        .expect("XDEL failed");

                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_empty".into()),
                                start: RedisJsonValue::String("-".into()),
                                end: RedisJsonValue::String("+".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_nonexistent_key() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_nonexistent".into()),
                                start: RedisJsonValue::String("-".into()),
                                end: RedisJsonValue::String("+".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_multiple_fields() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entry with multiple fields
                    let result = ctx
                        .raw(
                            &XaddInput {
                                key: RedisKey::String("xrange_multi".into()),
                                no_mk_stream: None,
                                trim: None,
                                id: Id::Auto,
                                entries: vec![
                                    Entry {
                                        field: RedisJsonValue::String("name".into()),
                                        value: RedisJsonValue::String("Alice".into()),
                                    },
                                    Entry {
                                        field: RedisJsonValue::String("age".into()),
                                        value: RedisJsonValue::String("30".into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("XADD failed");

                    let _ = XaddOutput::decode(&result).expect("decode XADD failed");

                    // Retrieve and verify
                    let result = ctx
                        .raw(
                            &XrangeInput {
                                key: RedisKey::String("xrange_multi".into()),
                                start: RedisJsonValue::String("-".into()),
                                end: RedisJsonValue::String("+".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].fields.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrange_r2", "field", "value").await;

            let result = ctx
                .raw(
                    &XrangeInput {
                        key: RedisKey::String("xrange_r2".into()),
                        start: RedisJsonValue::String("-".into()),
                        end: RedisJsonValue::String("+".into()),
                        count: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrange_r3", "field", "value").await;

            let result = ctx
                .raw(
                    &XrangeInput {
                        key: RedisKey::String("xrange_r3".into()),
                        start: RedisJsonValue::String("-".into()),
                        end: RedisJsonValue::String("+".into()),
                        count: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrange_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries to two streams
                    xadd_entry(ctx, "xrange_pipe1", "f1", "v1").await;
                    xadd_entry(ctx, "xrange_pipe2", "f2", "v2").await;

                    // Pipeline two XRANGE commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XrangeInput {
                            key: RedisKey::String("xrange_pipe1".into()),
                            start: RedisJsonValue::String("-".into()),
                            end: RedisJsonValue::String("+".into()),
                            count: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XrangeInput {
                            key: RedisKey::String("xrange_pipe2".into()),
                            start: RedisJsonValue::String("-".into()),
                            end: RedisJsonValue::String("+".into()),
                            count: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XrangeOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.len(), 1);

                    let out2 = XrangeOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.len(), 1);
                })
            })
            .await;
        }
    }
}
