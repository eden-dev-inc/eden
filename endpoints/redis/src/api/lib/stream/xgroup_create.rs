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

const API_INFO: ApiInfo<RedisApi, XgroupCreateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XgroupCreate,
    "Creates a consumer group for the specified stream",
    ReqType::Write,
    true,
);

/// Input for Redis `XGROUP CREATE` command.
///
/// Creates a new consumer group associated with a stream.
///
/// See official Redis documentation for `XGROUP CREATE`:
/// https://redis.io/docs/latest/commands/xgroup-create/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XgroupCreateInput {
    /// The key of the stream
    key: RedisKey,
    /// The name of the consumer group to create
    group: RedisJsonValue,
    /// The ID from which to start reading ($ for new messages, 0 for all, or specific ID)
    id: StreamId,
    /// If true, creates the stream if it doesn't exist
    mk_stream: Option<bool>,
    /// Optional number of entries already read (for lag tracking)
    entries_read: Option<RedisJsonValue>,
}

impl Serialize for XgroupCreateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, group, id
        if self.mk_stream.is_some() {
            fields += 1;
        }
        if self.entries_read.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XgroupCreateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("id", &self.id)?;
        if let Some(mk_stream) = &self.mk_stream {
            state.serialize_field("mk_stream", mk_stream)?;
        }
        if let Some(entries_read) = &self.entries_read {
            state.serialize_field("entries_read", entries_read)?;
        }
        state.end()
    }
}

/// Stream ID specification for consumer group operations
#[derive(Debug, Default, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum StreamId {
    /// $ - Only new messages arriving after group creation
    #[default]
    New,
    /// Explicit ID value (e.g., "0" for all messages, or specific entry ID)
    Explicit(RedisJsonValue),
}

impl StreamId {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        match self {
            Self::New => command.arg("$"),
            Self::Explicit(id) => command.arg(id),
        };
    }
}

impl_redis_operation!(
    XgroupCreateInput,
    API_INFO,
    {key, group, id, mk_stream, entries_read}
);

impl RedisCommandInput for XgroupCreateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        // XGROUP CREATE is a subcommand: XGROUP CREATE <key> <group> <id> [MKSTREAM] [ENTRIESREAD <n>]
        let mut command = crate::command::cmd("XGROUP");
        command.arg("CREATE");
        command.arg(&self.key).arg(&self.group);

        self.id.cmd(&mut command);

        if let Some(mk_stream) = &self.mk_stream
            && *mk_stream
        {
            command.arg("MKSTREAM");
        }

        if let Some(entries_read) = &self.entries_read {
            command.arg("ENTRIESREAD").arg(entries_read);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!(
                "XGROUP CREATE requires at least 3 arguments (key, group, id), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let group = args[1].clone();
        let id = if let RedisJsonValue::String(s) = &args[2] {
            if s == "$" {
                StreamId::New
            } else {
                StreamId::Explicit(args[2].clone())
            }
        } else {
            StreamId::Explicit(args[2].clone())
        };

        let mut mk_stream = None;
        let mut entries_read = None;
        let mut i = 3;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "MKSTREAM" {
                    mk_stream = Some(true);
                    i += 1;
                } else if upper == "ENTRIESREAD" && i + 1 < args.len() {
                    entries_read = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, group, id, mk_stream, entries_read })
    }
}

/// Output for Redis `XGROUP CREATE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XgroupCreateOutput {
    /// Whether the group was created successfully
    success: bool,
}

impl XgroupCreateOutput {
    /// Create a new XgroupCreateOutput
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the group was created successfully
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an XgroupCreateOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected XGROUP CREATE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected XGROUP CREATE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for XgroupCreateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupCreateOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = XgroupCreateInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::New,
                mk_stream: None,
                entries_read: None,
            };
            let cmd = input.command();

            // Verify against manually constructed protocol
            // *5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_encode_command_with_mkstream() {
            let input = XgroupCreateInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                mk_stream: Some(true),
                entries_read: None,
            };
            let cmd = input.command();

            // *6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n0\r\n$8\r\nMKSTREAM\r\n
            let expected = b"*6\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n0\r\n$8\r\nMKSTREAM\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_encode_command_with_entries_read() {
            let input = XgroupCreateInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::New,
                mk_stream: None,
                entries_read: Some(RedisJsonValue::Integer(100)),
            };
            let cmd = input.command();

            // *7\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n$11\r\nENTRIESREAD\r\n:100\r\n
            assert!(cmd.starts_with(b"*7\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n"));
            assert!(cmd.windows(11).any(|w| w == b"ENTRIESREAD"));
        }

        #[test]
        fn test_encode_command_explicit_id() {
            let input = XgroupCreateInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::Explicit(RedisJsonValue::String("1234567890123-0".into())),
                mk_stream: None,
                entries_read: None,
            };
            let cmd = input.command();

            // *5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$15\r\n1234567890123-0\r\n
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$15\r\n1234567890123-0\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = XgroupCreateInput {
                key: RedisKey::String("s".into()),
                group: RedisJsonValue::String("g".into()),
                id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                mk_stream: Some(true),
                entries_read: Some(RedisJsonValue::Integer(5)),
            };
            let cmd = input.command();

            // Verify structure: XGROUP CREATE s g 0 MKSTREAM ENTRIESREAD 5
            assert!(cmd.starts_with(b"*8\r\n")); // 8 elements
            assert!(cmd.windows(6).any(|w| w == b"XGROUP"));
            assert!(cmd.windows(6).any(|w| w == b"CREATE"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = XgroupCreateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_output_error_busygroup() {
            let err = XgroupCreateOutput::decode(b"-BUSYGROUP Consumer Group name already exists\r\n").unwrap_err();
            assert!(err.to_string().contains("BUSYGROUP"));
        }

        #[test]
        fn test_decode_output_error_nokey() {
            let err = XgroupCreateOutput::decode(b"-ERR The XGROUP subcommand requires the key to exist\r\n").unwrap_err();
            assert!(err.to_string().contains("key to exist"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XgroupCreateInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::New,
                mk_stream: None,
                entries_read: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("$".into()),
            ];
            let input = XgroupCreateInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert_eq!(input.group, RedisJsonValue::String("mygroup".into()));
            assert!(matches!(input.id, StreamId::New));
        }

        #[test]
        fn test_decode_input_with_mkstream() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("0".into()),
                RedisJsonValue::String("MKSTREAM".into()),
            ];
            let input = XgroupCreateInput::decode(args).unwrap();
            assert_eq!(input.mk_stream, Some(true));
        }

        #[test]
        fn test_decode_input_with_entries_read() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("$".into()),
                RedisJsonValue::String("ENTRIESREAD".into()),
                RedisJsonValue::Integer(50),
            ];
            let input = XgroupCreateInput::decode(args).unwrap();
            assert_eq!(input.entries_read, Some(RedisJsonValue::Integer(50)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let err = XgroupCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XgroupCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3 arguments"));
        }

        #[test]
        fn test_output_new() {
            let output = XgroupCreateOutput::new(true);
            assert!(output.success());
        }

        #[test]
        fn test_output_serialize() {
            let output = XgroupCreateOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }

        #[test]
        fn test_stream_id_default() {
            let id = StreamId::default();
            assert!(matches!(id, StreamId::New));
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

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgc_basic", "field", "value").await;

                    let result = ctx
                        .raw(
                            &XgroupCreateInput {
                                key: RedisKey::String("xgc_basic".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                id: StreamId::New,
                                mk_stream: None,
                                entries_read: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupCreateOutput::decode(&result).expect("decode failed");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_with_mkstream() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XgroupCreateInput {
                                key: RedisKey::String("xgc_mkstream".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                                mk_stream: Some(true),
                                entries_read: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupCreateOutput::decode(&result).expect("decode failed");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_duplicate_fails() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgc_dup", "field", "value").await;

                    ctx.raw(
                        &XgroupCreateInput {
                            key: RedisKey::String("xgc_dup".into()),
                            group: RedisJsonValue::String("testgroup".into()),
                            id: StreamId::New,
                            mk_stream: None,
                            entries_read: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &XgroupCreateInput {
                                key: RedisKey::String("xgc_dup".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                id: StreamId::New,
                                mk_stream: None,
                                entries_read: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XgroupCreateOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("BUSYGROUP"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xgc_r2", "field", "value").await;

            let result = ctx
                .raw(
                    &XgroupCreateInput {
                        key: RedisKey::String("xgc_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        id: StreamId::New,
                        mk_stream: None,
                        entries_read: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return simple string");
            let output = XgroupCreateOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xgc_r3", "field", "value").await;

            let result = ctx
                .raw(
                    &XgroupCreateInput {
                        key: RedisKey::String("xgc_r3".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        id: StreamId::New,
                        mk_stream: None,
                        entries_read: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XgroupCreateOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_create_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgc_pipe", "field", "value").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XgroupCreateInput {
                            key: RedisKey::String("xgc_pipe".into()),
                            group: RedisJsonValue::String("group1".into()),
                            id: StreamId::New,
                            mk_stream: None,
                            entries_read: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XgroupCreateInput {
                            key: RedisKey::String("xgc_pipe".into()),
                            group: RedisJsonValue::String("group2".into()),
                            id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                            mk_stream: None,
                            entries_read: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XgroupCreateOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.success());

                    let out2 = XgroupCreateOutput::decode(responses[1]).expect("decode second");
                    assert!(out2.success());
                })
            })
            .await;
        }
    }
}
