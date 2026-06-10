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

const API_INFO: ApiInfo<RedisApi, XaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xadd,
    "Appends a new entry to the specified stream. Creates the stream if it doesn't exist.",
    ReqType::Write,
    true,
);

/// Input for Redis `XADD` command.
///
/// Appends a new entry to a stream with the specified field-value pairs.
///
/// See official Redis documentation for `XADD`:
/// https://redis.io/docs/latest/commands/xadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XaddInput {
    /// The key of the stream
    pub(crate) key: RedisKey,
    /// If true, don't create the stream if it doesn't exist
    pub(crate) no_mk_stream: Option<bool>,
    /// Optional trimming arguments (MAXLEN/MINID)
    pub(crate) trim: Option<TrimArgs>,
    /// The ID for the new entry (* for auto-generation)
    pub(crate) id: Id,
    /// The field-value pairs for the entry
    pub(crate) entries: Vec<Entry>,
}

impl Serialize for XaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, id, entries
        if self.no_mk_stream.is_some() {
            fields += 1;
        }
        if let Some(trim) = &self.trim {
            fields += 2; // strategy, threshold
            if trim.approx.is_some() {
                fields += 1;
            }
            if trim.limit.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("XaddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(no_mk_stream) = &self.no_mk_stream {
            state.serialize_field("no_mk_stream", no_mk_stream)?;
        }
        if let Some(trim) = &self.trim {
            state.serialize_field("strategy", &trim.strategy)?;
            if let Some(approx) = &trim.approx {
                state.serialize_field("approx", approx)?;
            }
            state.serialize_field("threshold", &trim.threshold)?;
            if let Some(limit) = &trim.limit {
                state.serialize_field("limit", limit)?;
            }
        }
        state.serialize_field("id", &self.id)?;
        state.serialize_field("entries", &self.entries)?;
        state.end()
    }
}

/// A field-value pair for a stream entry
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct Entry {
    /// The field name
    pub(crate) field: RedisJsonValue,
    /// The field value
    pub(crate) value: RedisJsonValue,
}

/// Trimming arguments for XADD
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct TrimArgs {
    /// Trimming strategy (MAXLEN or MINID)
    strategy: TrimStrategy,
    /// If Some(true), use approximate trimming (~)
    approx: Option<bool>,
    /// The threshold value for trimming
    threshold: RedisJsonValue,
    /// Optional LIMIT for approximate trimming
    limit: Option<RedisJsonValue>,
}

impl TrimArgs {
    fn cmd(&self, command: &mut crate::command::Cmd) {
        match self.strategy {
            TrimStrategy::MaxLen => command.arg("MAXLEN"),
            TrimStrategy::MinId => command.arg("MINID"),
        };

        if let Some(true) = self.approx {
            command.arg("~");
        } else if self.approx.is_none() || self.approx == Some(false) {
            // Exact matching - no modifier needed, but we can use = for clarity
        }

        command.arg(&self.threshold);

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(limit);
        }
    }
}

/// Trimming strategy
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum TrimStrategy {
    /// Trim by maximum length
    #[default]
    MaxLen,
    /// Trim by minimum ID
    MinId,
}

/// Stream entry ID specification
#[derive(Debug, Default, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Id {
    /// Auto-generate ID (*)
    #[default]
    Auto,
    /// Explicit ID value
    Explicit(RedisJsonValue),
}

impl Id {
    fn cmd(&self, command: &mut crate::command::Cmd) {
        match self {
            Self::Auto => command.arg("*"),
            Self::Explicit(id) => command.arg(id),
        };
    }
}

impl_redis_operation!(XaddInput, API_INFO, { key, no_mk_stream, trim, id, entries });

impl RedisCommandInput for XaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(true) = &self.no_mk_stream {
            command.arg("NOMKSTREAM");
        }

        if let Some(trim) = &self.trim {
            trim.cmd(&mut command);
        }

        self.id.cmd(&mut command);

        for entry in &self.entries {
            command.arg(&entry.field).arg(&entry.value);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::parse(format!(
                "XADD requires at least 4 arguments (key, id, field, value), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let mut i = 1;
        let mut no_mk_stream = None;
        let mut trim = None;
        let mut id = Id::Auto;
        let mut entries = Vec::new();

        // Parse optional NOMKSTREAM
        if i < args.len()
            && let RedisJsonValue::String(s) = &args[i]
            && s.to_uppercase() == "NOMKSTREAM"
        {
            no_mk_stream = Some(true);
            i += 1;
        }

        // Parse optional trimming args (MAXLEN/MINID)
        if i < args.len()
            && let RedisJsonValue::String(s) = &args[i]
        {
            let upper = s.to_uppercase();
            if upper == "MAXLEN" || upper == "MINID" {
                let strategy = if upper == "MAXLEN" {
                    TrimStrategy::MaxLen
                } else {
                    TrimStrategy::MinId
                };
                i += 1;

                let mut approx = None;
                let mut threshold = RedisJsonValue::String("0".to_string());
                let mut limit = None;

                // Check for = or ~ modifier
                if i < args.len()
                    && let RedisJsonValue::String(s) = &args[i]
                {
                    if s == "=" {
                        approx = Some(false);
                        i += 1;
                    } else if s == "~" {
                        approx = Some(true);
                        i += 1;
                    }
                }

                // Get threshold
                if i < args.len() {
                    threshold = args[i].clone();
                    i += 1;
                }

                // Check for LIMIT
                if i + 1 < args.len()
                    && let RedisJsonValue::String(s) = &args[i]
                    && s.to_uppercase() == "LIMIT"
                {
                    limit = Some(args[i + 1].clone());
                    i += 2;
                }

                trim = Some(TrimArgs { strategy, approx, threshold, limit });
            }
        }

        // Parse ID
        if i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                if s == "*" {
                    id = Id::Auto;
                } else {
                    id = Id::Explicit(args[i].clone());
                }
            } else {
                id = Id::Explicit(args[i].clone());
            }
            i += 1;
        }

        // Parse field-value pairs
        while i + 1 < args.len() {
            entries.push(Entry { field: args[i].clone(), value: args[i + 1].clone() });
            i += 2;
        }

        if entries.is_empty() {
            return Err(EpError::parse("XADD requires at least one field-value pair"));
        }

        Ok(Self { key, no_mk_stream, trim, id, entries })
    }
}

/// Output for Redis `XADD` command.
///
/// Returns the ID of the added entry.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XaddOutput {
    /// The ID of the added entry (None if NOMKSTREAM was used and stream doesn't exist)
    id: Option<String>,
}

impl XaddOutput {
    /// Create a new XaddOutput with an ID
    pub fn new(id: Option<String>) -> Self {
        Self { id }
    }

    /// Get the entry ID
    pub fn id(&self) -> Option<&str> {
        self.id.as_deref()
    }

    /// Check if the entry was added (returns false if stream didn't exist with NOMKSTREAM)
    pub fn was_added(&self) -> bool {
        self.id.is_some()
    }

    /// Decode the Redis protocol response into an XaddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let id = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(data) => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { id })
    }
}

impl Serialize for XaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XaddOutput", 1)?;
        state.serialize_field("id", &self.id)?;
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
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: None,
                trim: None,
                id: Id::Auto,
                entries: vec![Entry {
                    field: RedisJsonValue::String("field1".into()),
                    value: RedisJsonValue::String("value1".into()),
                }],
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"XADD"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_encode_command_with_nomkstream() {
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: Some(true),
                trim: None,
                id: Id::Auto,
                entries: vec![Entry {
                    field: RedisJsonValue::String("f".into()),
                    value: RedisJsonValue::String("v".into()),
                }],
            };
            let cmd = input.command();
            assert!(cmd.windows(10).any(|w| w == b"NOMKSTREAM"));
        }

        #[test]
        fn test_encode_command_with_maxlen() {
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: None,
                trim: Some(TrimArgs {
                    strategy: TrimStrategy::MaxLen,
                    approx: Some(true),
                    threshold: RedisJsonValue::Integer(1000),
                    limit: None,
                }),
                id: Id::Auto,
                entries: vec![Entry {
                    field: RedisJsonValue::String("f".into()),
                    value: RedisJsonValue::String("v".into()),
                }],
            };
            let cmd = input.command();
            assert!(cmd.windows(6).any(|w| w == b"MAXLEN"));
        }

        #[test]
        fn test_encode_command_with_explicit_id() {
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: None,
                trim: None,
                id: Id::Explicit(RedisJsonValue::String("1234567890123-0".into())),
                entries: vec![Entry {
                    field: RedisJsonValue::String("f".into()),
                    value: RedisJsonValue::String("v".into()),
                }],
            };
            let cmd = input.command();
            assert!(cmd.windows(15).any(|w| w == b"1234567890123-0"));
        }

        #[test]
        fn test_encode_command_multiple_entries() {
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: None,
                trim: None,
                id: Id::Auto,
                entries: vec![
                    Entry {
                        field: RedisJsonValue::String("f1".into()),
                        value: RedisJsonValue::String("v1".into()),
                    },
                    Entry {
                        field: RedisJsonValue::String("f2".into()),
                        value: RedisJsonValue::String("v2".into()),
                    },
                ],
            };
            let cmd = input.command();
            // *6: XADD key * f1 v1 f2 v2
            assert!(cmd.starts_with(b"*7\r\n"));
        }

        #[test]
        fn test_decode_output_bulk_string() {
            let output = XaddOutput::decode(b"$15\r\n1234567890123-0\r\n").unwrap();
            assert!(output.was_added());
            assert_eq!(output.id(), Some("1234567890123-0"));
        }

        #[test]
        fn test_decode_output_simple_string() {
            let output = XaddOutput::decode(b"+1234567890123-0\r\n").unwrap();
            assert!(output.was_added());
            assert_eq!(output.id(), Some("1234567890123-0"));
        }

        #[test]
        fn test_decode_output_null_resp2() {
            let output = XaddOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.was_added());
            assert_eq!(output.id(), None);
        }

        #[test]
        fn test_decode_output_null_resp3() {
            let output = XaddOutput::decode(b"_\r\n").unwrap();
            assert!(!output.was_added());
            assert_eq!(output.id(), None);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XaddOutput::decode(b"-ERR wrong number of arguments\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XaddInput {
                key: RedisKey::String("mystream".into()),
                no_mk_stream: None,
                trim: None,
                id: Id::Auto,
                entries: vec![Entry {
                    field: RedisJsonValue::String("f".into()),
                    value: RedisJsonValue::String("v".into()),
                }],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = XaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(matches!(input.id, Id::Auto));
            assert_eq!(input.entries.len(), 1);
        }

        #[test]
        fn test_decode_input_with_nomkstream() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("NOMKSTREAM".into()),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = XaddInput::decode(args).unwrap();
            assert_eq!(input.no_mk_stream, Some(true));
        }

        #[test]
        fn test_decode_input_with_maxlen() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("MAXLEN".into()),
                RedisJsonValue::String("~".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("*".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = XaddInput::decode(args).unwrap();
            assert!(input.trim.is_some());
            let trim = input.trim.unwrap();
            assert!(matches!(trim.strategy, TrimStrategy::MaxLen));
            assert_eq!(trim.approx, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("*".into())];
            let err = XaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least"));
        }

        #[test]
        fn test_output_new() {
            let output = XaddOutput::new(Some("123-0".to_string()));
            assert_eq!(output.id(), Some("123-0"));
        }

        #[test]
        fn test_output_serialize() {
            let output = XaddOutput::new(Some("123-0".to_string()));
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"id\":\"123-0\""));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XaddInput {
                                key: RedisKey::String("xadd_basic".into()),
                                no_mk_stream: None,
                                trim: None,
                                id: Id::Auto,
                                entries: vec![Entry {
                                    field: RedisJsonValue::String("field1".into()),
                                    value: RedisJsonValue::String("value1".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added());
                    assert!(output.id().is_some());
                    // ID format: timestamp-sequence
                    assert!(output.id().unwrap().contains('-'));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_with_explicit_id() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XaddInput {
                                key: RedisKey::String("xadd_explicit".into()),
                                no_mk_stream: None,
                                trim: None,
                                id: Id::Explicit(RedisJsonValue::String("9999999999999-0".into())),
                                entries: vec![Entry {
                                    field: RedisJsonValue::String("field1".into()),
                                    value: RedisJsonValue::String("value1".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added());
                    assert_eq!(output.id(), Some("9999999999999-0"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_multiple_fields() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XaddInput {
                                key: RedisKey::String("xadd_multi".into()),
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
                                    Entry {
                                        field: RedisJsonValue::String("city".into()),
                                        value: RedisJsonValue::String("NYC".into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_added());

                    // Verify stream length is 1
                    let xlen_result = ctx.raw(b"*2\r\n$4\r\nXLEN\r\n$10\r\nxadd_multi\r\n").await.expect("XLEN failed");
                    let len_str = String::from_utf8_lossy(&xlen_result);
                    assert!(len_str.contains(":1\r\n"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_with_maxlen() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add several entries
                    for i in 0..5 {
                        ctx.raw(
                            &XaddInput {
                                key: RedisKey::String("xadd_maxlen".into()),
                                no_mk_stream: None,
                                trim: Some(TrimArgs {
                                    strategy: TrimStrategy::MaxLen,
                                    approx: None,
                                    threshold: RedisJsonValue::Integer(3),
                                    limit: None,
                                }),
                                id: Id::Auto,
                                entries: vec![Entry {
                                    field: RedisJsonValue::String("idx".into()),
                                    value: RedisJsonValue::Integer(i),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    }

                    // Check stream length is capped
                    let xlen_result = ctx.raw(b"*2\r\n$4\r\nXLEN\r\n$11\r\nxadd_maxlen\r\n").await.expect("XLEN failed");
                    let len_str = String::from_utf8_lossy(&xlen_result);
                    assert!(len_str.contains(":3\r\n"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_nomkstream_nonexistent() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XaddInput {
                                key: RedisKey::String("xadd_nomk_new".into()),
                                no_mk_stream: Some(true),
                                trim: None,
                                id: Id::Auto,
                                entries: vec![Entry {
                                    field: RedisJsonValue::String("field".into()),
                                    value: RedisJsonValue::String("value".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XaddOutput::decode(&result).expect("decode failed");
                    // Should return null for non-existent stream with NOMKSTREAM
                    assert!(!output.was_added());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &XaddInput {
                        key: RedisKey::String("xadd_r2".into()),
                        no_mk_stream: None,
                        trim: None,
                        id: Id::Auto,
                        entries: vec![Entry {
                            field: RedisJsonValue::String("f".into()),
                            value: RedisJsonValue::String("v".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = XaddOutput::decode(&result).expect("decode failed");
            assert!(output.was_added());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &XaddInput {
                        key: RedisKey::String("xadd_r3".into()),
                        no_mk_stream: None,
                        trim: None,
                        id: Id::Auto,
                        entries: vec![Entry {
                            field: RedisJsonValue::String("f".into()),
                            value: RedisJsonValue::String("v".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XaddOutput::decode(&result).expect("decode failed");
            assert!(output.was_added());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xadd_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XaddInput {
                            key: RedisKey::String("xadd_pipe".into()),
                            no_mk_stream: None,
                            trim: None,
                            id: Id::Auto,
                            entries: vec![Entry {
                                field: RedisJsonValue::String("f1".into()),
                                value: RedisJsonValue::String("v1".into()),
                            }],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XaddInput {
                            key: RedisKey::String("xadd_pipe".into()),
                            no_mk_stream: None,
                            trim: None,
                            id: Id::Auto,
                            entries: vec![Entry {
                                field: RedisJsonValue::String("f2".into()),
                                value: RedisJsonValue::String("v2".into()),
                            }],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XaddOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.was_added());

                    let out2 = XaddOutput::decode(responses[1]).expect("decode second");
                    assert!(out2.was_added());

                    // Verify IDs are different
                    assert_ne!(out1.id(), out2.id());
                })
            })
            .await;
        }
    }
}
