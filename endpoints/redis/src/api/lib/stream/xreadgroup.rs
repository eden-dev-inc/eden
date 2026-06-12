use crate::api::lib::stream::xread::{XreadOutput, XreadStreamResult};
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, XreadgroupInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xreadgroup,
    "Returns new or historical messages from a stream for a consumer in a group. Blocks until a message is available otherwise",
    ReqType::Read,
    false,
);

/// Input for Redis `XREADGROUP` command.
///
/// Read data from streams as a consumer in a consumer group.
///
/// See official Redis documentation for `XREADGROUP`:
/// https://redis.io/docs/latest/commands/xreadgroup/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XreadgroupInput {
    /// The consumer group name
    group: RedisJsonValue,
    /// The consumer name
    consumer: RedisJsonValue,
    /// Optional maximum number of entries per stream
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
    /// Optional blocking timeout in milliseconds (0 = block forever)
    #[serde(skip_serializing_if = "Option::is_none")]
    block: Option<RedisJsonValue>,
    /// If true, don't add messages to the PEL
    #[serde(skip_serializing_if = "Option::is_none")]
    no_ack: Option<bool>,
    /// The stream keys to read from
    keys: Vec<RedisKey>,
    /// The IDs to start reading from (use ">" for new messages only)
    ids: Vec<RedisJsonValue>,
}

impl Serialize for XreadgroupInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 5; // type, group, consumer, keys, ids
        if self.count.is_some() {
            fields += 1;
        }
        if self.block.is_some() {
            fields += 1;
        }
        if self.no_ack.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XreadgroupInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("consumer", &self.consumer)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(block) = &self.block {
            state.serialize_field("block", block)?;
        }
        if let Some(no_ack) = &self.no_ack {
            state.serialize_field("no_ack", no_ack)?;
        }
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("ids", &self.ids)?;
        state.end()
    }
}

impl_redis_operation!(XreadgroupInput, API_INFO, { group, consumer, count, block, no_ack, keys, ids });

impl RedisCommandInput for XreadgroupInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg("GROUP").arg(&self.group).arg(&self.consumer);

        if let Some(c) = &self.count {
            command.arg("COUNT").arg(c);
        }

        if let Some(b) = &self.block {
            command.arg("BLOCK").arg(b);
        }

        if let Some(true) = &self.no_ack {
            command.arg("NOACK");
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
        if args.len() < 5 {
            return Err(EpError::parse(format!("XREADGROUP requires at least 5 arguments, given {}", args.len())));
        }

        // Expect GROUP keyword first
        if let RedisJsonValue::String(s) = &args[0] {
            if s.to_uppercase() != "GROUP" {
                return Err(EpError::parse("XREADGROUP requires GROUP keyword".to_string()));
            }
        } else {
            return Err(EpError::parse("XREADGROUP requires GROUP keyword".to_string()));
        }

        let group = args[1].clone();
        let consumer = args[2].clone();
        let mut count = None;
        let mut block = None;
        let mut no_ack = None;
        let mut keys = Vec::new();
        let mut i = 3;
        let mut streams_found = false;

        // Parse optional parameters
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "COUNT" && i + 1 < args.len() {
                    count = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "BLOCK" && i + 1 < args.len() {
                    block = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "NOACK" {
                    no_ack = Some(true);
                    i += 1;
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
            return Err(EpError::parse("XREADGROUP requires STREAMS keyword".to_string()));
        }

        // Split remaining args into keys and ids
        let remaining_args = &args[i..];
        let mid = remaining_args.len() / 2;
        for key in remaining_args[..mid].iter() {
            keys.push(key.try_into()?);
        }
        let ids = remaining_args[mid..].to_vec();

        Ok(Self { group, consumer, count, block, no_ack, keys, ids })
    }
}

/// Output for Redis `XREADGROUP` command.
///
/// Returns entries from multiple streams for a consumer group.
/// Reuses XreadOutput structure since format is identical.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XreadgroupOutput {
    /// Results from each stream (None if timeout or no data)
    streams: Option<Vec<XreadStreamResult>>,
}

impl XreadgroupOutput {
    /// Create a new XreadgroupOutput
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

    /// Decode the Redis protocol response into an XreadgroupOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        // Reuse XreadOutput's decode since format is identical
        let xread_output = XreadOutput::decode(bytes)?;
        Ok(Self { streams: xread_output.streams().map(|s| s.to_vec()) })
    }
}

impl Serialize for XreadgroupOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XreadgroupOutput", 1)?;
        state.serialize_field("streams", &self.streams)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::stream::xrange::StreamEntry;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = XreadgroupInput {
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("consumer1".into()),
                count: None,
                block: None,
                no_ack: None,
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String(">".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(10).any(|w| w == b"XREADGROUP"));
            assert!(cmd.windows(5).any(|w| w == b"GROUP"));
            assert!(cmd.windows(7).any(|w| w == b"STREAMS"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = XreadgroupInput {
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("consumer1".into()),
                count: Some(RedisJsonValue::Integer(10)),
                block: None,
                no_ack: None,
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String(">".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_encode_command_with_block() {
            let input = XreadgroupInput {
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("consumer1".into()),
                count: None,
                block: Some(RedisJsonValue::Integer(1000)),
                no_ack: None,
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String(">".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"BLOCK"));
        }

        #[test]
        fn test_encode_command_with_noack() {
            let input = XreadgroupInput {
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("consumer1".into()),
                count: None,
                block: None,
                no_ack: Some(true),
                keys: vec![RedisKey::String("mystream".into())],
                ids: vec![RedisJsonValue::String(">".into())],
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"NOACK"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XreadgroupInput {
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("consumer1".into()),
                count: None,
                block: None,
                no_ack: None,
                keys: vec![RedisKey::String("stream1".into()), RedisKey::String("stream2".into())],
                ids: vec![RedisJsonValue::String(">".into()), RedisJsonValue::String(">".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("GROUP".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("consumer1".into()),
                RedisJsonValue::String("STREAMS".into()),
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String(">".into()),
            ];
            let input = XreadgroupInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.ids.len(), 1);
        }

        #[test]
        fn test_decode_input_missing_group() {
            let args = vec![
                RedisJsonValue::String("STREAMS".into()),
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String(">".into()),
            ];
            let err = XreadgroupInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("GROUP"));
        }

        #[test]
        fn test_decode_input_missing_streams() {
            // Need at least 5 args to pass the length check and reach the STREAMS check
            let args = vec![
                RedisJsonValue::String("GROUP".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("consumer1".into()),
                RedisJsonValue::String("extra1".into()),
                RedisJsonValue::String("extra2".into()),
            ];
            let err = XreadgroupInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("STREAMS"));
        }

        #[test]
        fn test_decode_output_null() {
            let output = XreadgroupOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XreadgroupOutput::decode(b"-NOGROUP No such group\r\n").unwrap_err();
            assert!(err.to_string().contains("NOGROUP"));
        }

        #[test]
        fn test_output_new() {
            let output = XreadgroupOutput::new(Some(vec![XreadStreamResult { key: "mystream".to_string(), entries: vec![] }]));
            assert!(!output.is_empty());
        }

        #[test]
        fn test_output_total_entries() {
            let output = XreadgroupOutput::new(Some(vec![XreadStreamResult {
                key: "stream1".to_string(),
                entries: vec![
                    StreamEntry { id: "1-0".to_string(), fields: vec![] },
                    StreamEntry { id: "1-1".to_string(), fields: vec![] },
                ],
            }]));
            assert_eq!(output.total_entries(), 2);
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
        async fn test_xreadgroup_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream and group
                    xadd_entry(ctx, "xrg_basic", "f", "v").await;
                    create_group(ctx, "xrg_basic", "mygroup").await;

                    let result = ctx
                        .raw(
                            &XreadgroupInput {
                                group: RedisJsonValue::String("mygroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                                count: None,
                                block: None,
                                no_ack: None,
                                keys: vec![RedisKey::String("xrg_basic".into())],
                                ids: vec![RedisJsonValue::String(">".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadgroupOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    assert_eq!(output.total_entries(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xreadgroup_with_count() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    for i in 0..5 {
                        xadd_entry(ctx, "xrg_count", &format!("f{}", i), &format!("v{}", i)).await;
                    }
                    create_group(ctx, "xrg_count", "mygroup").await;

                    let result = ctx
                        .raw(
                            &XreadgroupInput {
                                group: RedisJsonValue::String("mygroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                                count: Some(RedisJsonValue::Integer(2)),
                                block: None,
                                no_ack: None,
                                keys: vec![RedisKey::String("xrg_count".into())],
                                ids: vec![RedisJsonValue::String(">".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadgroupOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.total_entries(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xreadgroup_no_new_messages() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xrg_empty", "f", "v").await;
                    create_group(ctx, "xrg_empty", "mygroup").await;

                    // First read consumes the message
                    ctx.raw(
                        &XreadgroupInput {
                            group: RedisJsonValue::String("mygroup".into()),
                            consumer: RedisJsonValue::String("consumer1".into()),
                            count: None,
                            block: None,
                            no_ack: None,
                            keys: vec![RedisKey::String("xrg_empty".into())],
                            ids: vec![RedisJsonValue::String(">".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Second read should be empty (no new messages)
                    let result = ctx
                        .raw(
                            &XreadgroupInput {
                                group: RedisJsonValue::String("mygroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                                count: None,
                                block: None,
                                no_ack: None,
                                keys: vec![RedisKey::String("xrg_empty".into())],
                                ids: vec![RedisJsonValue::String(">".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XreadgroupOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xreadgroup_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrg_r2", "f", "v").await;
            create_group(&mut ctx, "xrg_r2", "mygroup").await;

            let result = ctx
                .raw(
                    &XreadgroupInput {
                        group: RedisJsonValue::String("mygroup".into()),
                        consumer: RedisJsonValue::String("consumer1".into()),
                        count: None,
                        block: None,
                        no_ack: None,
                        keys: vec![RedisKey::String("xrg_r2".into())],
                        ids: vec![RedisJsonValue::String(">".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XreadgroupOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xreadgroup_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrg_r3", "f", "v").await;
            create_group(&mut ctx, "xrg_r3", "mygroup").await;

            let result = ctx
                .raw(
                    &XreadgroupInput {
                        group: RedisJsonValue::String("mygroup".into()),
                        consumer: RedisJsonValue::String("consumer1".into()),
                        count: None,
                        block: None,
                        no_ack: None,
                        keys: vec![RedisKey::String("xrg_r3".into())],
                        ids: vec![RedisJsonValue::String(">".into())],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XreadgroupOutput::decode(&result).expect("decode failed");
            assert!(!output.is_empty());

            ctx.stop().await;
        }
    }
}
