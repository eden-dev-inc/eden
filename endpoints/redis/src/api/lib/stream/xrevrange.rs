use crate::api::lib::stream::xrange::{StreamEntry, XrangeOutput};
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

const API_INFO: ApiInfo<RedisApi, XrevrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xrevrange,
    "Returns the messages from a stream within a range of IDs in reverse order",
    ReqType::Read,
    true,
);

/// Input for Redis `XREVRANGE` command.
///
/// Returns the stream entries matching a given range of IDs in reverse order
/// (from higher to lower IDs).
///
/// See official Redis documentation for `XREVRANGE`:
/// https://redis.io/docs/latest/commands/xrevrange/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XrevrangeInput {
    /// The key of the stream
    key: RedisKey,
    /// End of the range (use "+" for maximum ID) - note: this is the first arg in XREVRANGE
    end: RedisJsonValue,
    /// Start of the range (use "-" for minimum ID) - note: this is the second arg in XREVRANGE
    start: RedisJsonValue,
    /// Optional maximum number of entries to return
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
}

impl Serialize for XrevrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, end, start
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XrevrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("end", &self.end)?;
        state.serialize_field("start", &self.start)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(XrevrangeInput, API_INFO, { key, end, start, count });

impl RedisCommandInput for XrevrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.end).arg(&self.start);

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
            return Err(EpError::parse(format!("XREVRANGE requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let end = args[1].clone();
        let start = args[2].clone();
        let mut count = None;

        if args.len() >= 5
            && let RedisJsonValue::String(s) = &args[3]
            && s.to_uppercase() == "COUNT"
        {
            count = Some(args[4].clone());
        }

        Ok(Self { key, end, start, count })
    }
}

/// Output for Redis `XREVRANGE` command.
///
/// Returns a list of stream entries within the specified range in reverse order.
/// Reuses the same structure as XrangeOutput since the format is identical.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XrevrangeOutput {
    /// The entries returned from the stream (in reverse order)
    entries: Vec<StreamEntry>,
}

impl XrevrangeOutput {
    /// Create a new XrevrangeOutput
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

    /// Decode the Redis protocol response into an XrevrangeOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        // Reuse XrangeOutput's decode since format is identical
        let xrange_output = XrangeOutput::decode(bytes)?;
        Ok(Self { entries: xrange_output.entries().to_vec() })
    }
}

impl Serialize for XrevrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XrevrangeOutput", 1)?;
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
            let input = XrevrangeInput {
                key: RedisKey::String("mystream".into()),
                end: RedisJsonValue::String("+".into()),
                start: RedisJsonValue::String("-".into()),
                count: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            assert!(cmd.windows(9).any(|w| w == b"XREVRANGE"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = XrevrangeInput {
                key: RedisKey::String("mystream".into()),
                end: RedisJsonValue::String("+".into()),
                start: RedisJsonValue::String("-".into()),
                count: Some(RedisJsonValue::Integer(10)),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*6\r\n")); // XREVRANGE key end start COUNT 10
            assert!(cmd.windows(5).any(|w| w == b"COUNT"));
        }

        #[test]
        fn test_encode_command_with_specific_ids() {
            let input = XrevrangeInput {
                key: RedisKey::String("mystream".into()),
                end: RedisJsonValue::String("1234567890124-0".into()),
                start: RedisJsonValue::String("1234567890123-0".into()),
                count: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(15).any(|w| w == b"1234567890123-0"));
            assert!(cmd.windows(15).any(|w| w == b"1234567890124-0"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XrevrangeInput {
                key: RedisKey::String("mystream".into()),
                end: RedisJsonValue::String("+".into()),
                start: RedisJsonValue::String("-".into()),
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
                RedisJsonValue::String("+".into()),
                RedisJsonValue::String("-".into()),
            ];
            let input = XrevrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("+".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("COUNT".into()),
                RedisJsonValue::Integer(5),
            ];
            let input = XrevrangeInput::decode(args).unwrap();
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("+".into())];
            let err = XrevrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = XrevrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_null() {
            let output = XrevrangeOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XrevrangeOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let entries = vec![StreamEntry {
                id: "1234-0".to_string(),
                fields: vec![("field".to_string(), "value".to_string())],
            }];
            let output = XrevrangeOutput::new(entries);
            assert_eq!(output.len(), 1);
            assert_eq!(output.entries()[0].id, "1234-0");
        }

        #[test]
        fn test_output_serialize() {
            let output = XrevrangeOutput::new(vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("entries"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::stream::xadd::{Entry, Id, XaddInput, XaddOutput};
        use crate::protocol::RedisProtocol;
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
        async fn test_xrevrange_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries
                    let id1 = xadd_entry(ctx, "xrevrange_basic", "f1", "v1").await;
                    let id2 = xadd_entry(ctx, "xrevrange_basic", "f2", "v2").await;

                    // Get all entries in reverse
                    let result = ctx
                        .raw(
                            &XrevrangeInput {
                                key: RedisKey::String("xrevrange_basic".into()),
                                end: RedisJsonValue::String("+".into()),
                                start: RedisJsonValue::String("-".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrevrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    // Reverse order: id2 first, then id1
                    assert_eq!(output.entries()[0].id, id2);
                    assert_eq!(output.entries()[1].id, id1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_with_count() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add multiple entries
                    for i in 0..5 {
                        xadd_entry(ctx, "xrevrange_count", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Get only 2 entries (should be the latest 2)
                    let result = ctx
                        .raw(
                            &XrevrangeInput {
                                key: RedisKey::String("xrevrange_count".into()),
                                end: RedisJsonValue::String("+".into()),
                                start: RedisJsonValue::String("-".into()),
                                count: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrevrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_specific_range() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries
                    let id1 = xadd_entry(ctx, "xrevrange_range", "f1", "v1").await;
                    let _id2 = xadd_entry(ctx, "xrevrange_range", "f2", "v2").await;
                    let _id3 = xadd_entry(ctx, "xrevrange_range", "f3", "v3").await;

                    // Get only first entry by its ID
                    let result = ctx
                        .raw(
                            &XrevrangeInput {
                                key: RedisKey::String("xrevrange_range".into()),
                                end: RedisJsonValue::String(id1.clone()),
                                start: RedisJsonValue::String(id1.clone()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrevrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.entries()[0].id, id1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_nonexistent_key() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XrevrangeInput {
                                key: RedisKey::String("xrevrange_nonexistent".into()),
                                end: RedisJsonValue::String("+".into()),
                                start: RedisJsonValue::String("-".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XrevrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrevrange_r2", "field", "value").await;

            let result = ctx
                .raw(
                    &XrevrangeInput {
                        key: RedisKey::String("xrevrange_r2".into()),
                        end: RedisJsonValue::String("+".into()),
                        start: RedisJsonValue::String("-".into()),
                        count: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = XrevrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xrevrange_r3", "field", "value").await;

            let result = ctx
                .raw(
                    &XrevrangeInput {
                        key: RedisKey::String("xrevrange_r3".into()),
                        end: RedisJsonValue::String("+".into()),
                        start: RedisJsonValue::String("-".into()),
                        count: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XrevrangeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xrevrange_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries to two streams
                    xadd_entry(ctx, "xrevrange_pipe1", "f1", "v1").await;
                    xadd_entry(ctx, "xrevrange_pipe2", "f2", "v2").await;

                    // Pipeline two XREVRANGE commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XrevrangeInput {
                            key: RedisKey::String("xrevrange_pipe1".into()),
                            end: RedisJsonValue::String("+".into()),
                            start: RedisJsonValue::String("-".into()),
                            count: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XrevrangeInput {
                            key: RedisKey::String("xrevrange_pipe2".into()),
                            end: RedisJsonValue::String("+".into()),
                            start: RedisJsonValue::String("-".into()),
                            count: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XrevrangeOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.len(), 1);

                    let out2 = XrevrangeOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.len(), 1);
                })
            })
            .await;
        }
    }
}
