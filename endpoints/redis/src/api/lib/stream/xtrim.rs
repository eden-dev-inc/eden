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

const API_INFO: ApiInfo<RedisApi, XtrimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xtrim,
    "Deletes messages from the beginning of a stream",
    ReqType::Write,
    true,
);

/// Input for Redis `XTRIM` command.
///
/// Trims a stream by evicting older entries (entries with lower IDs) if needed.
///
/// See official Redis documentation for `XTRIM`:
/// https://redis.io/docs/latest/commands/xtrim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XtrimInput {
    /// The key of the stream
    key: RedisKey,
    /// Trimming strategy (MAXLEN or MINID)
    strategy: TrimStrategy,
    /// Optional approximate trimming modifier (= for exact, ~ for approximate)
    #[serde(skip_serializing_if = "Option::is_none")]
    operator: Option<TrimOperator>,
    /// The threshold for trimming
    threshold: RedisJsonValue,
    /// Optional LIMIT for approximate trimming (Redis 6.2+)
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<RedisJsonValue>,
}

impl Serialize for XtrimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, strategy, threshold
        if self.operator.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XtrimInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("strategy", &self.strategy)?;
        if let Some(op) = &self.operator {
            state.serialize_field("operator", op)?;
        }
        state.serialize_field("threshold", &self.threshold)?;
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        state.end()
    }
}

/// Trimming strategy for XTRIM
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum TrimStrategy {
    /// Trim by maximum length
    #[default]
    MaxLen,
    /// Trim by minimum ID (Redis 6.2+)
    MinId,
}

impl TrimStrategy {
    fn as_str(&self) -> &'static str {
        match self {
            Self::MaxLen => "MAXLEN",
            Self::MinId => "MINID",
        }
    }
}

/// Trimming operator for exact or approximate trimming
#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum TrimOperator {
    /// Exact trimming (=)
    #[default]
    Exact,
    /// Approximate trimming (~)
    Approximate,
}

impl TrimOperator {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Exact => "=",
            Self::Approximate => "~",
        }
    }
}

impl_redis_operation!(XtrimInput, API_INFO, { key, strategy, operator, threshold, limit });

impl RedisCommandInput for XtrimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);
        command.arg(self.strategy.as_str());

        if let Some(op) = &self.operator {
            command.arg(op.as_str());
        }

        command.arg(&self.threshold);

        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(limit);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("XTRIM requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let strategy = if let RedisJsonValue::String(s) = &args[1] {
            match s.to_uppercase().as_str() {
                "MAXLEN" => TrimStrategy::MaxLen,
                "MINID" => TrimStrategy::MinId,
                _ => TrimStrategy::MaxLen,
            }
        } else {
            TrimStrategy::MaxLen
        };

        let mut operator = None;
        let mut threshold = args[2].clone();
        let mut limit = None;
        let mut i = 2;

        // Check for = or ~ operator
        if let RedisJsonValue::String(s) = &args[i] {
            if s == "=" {
                operator = Some(TrimOperator::Exact);
                i += 1;
                if i < args.len() {
                    threshold = args[i].clone();
                    i += 1;
                }
            } else if s == "~" {
                operator = Some(TrimOperator::Approximate);
                i += 1;
                if i < args.len() {
                    threshold = args[i].clone();
                    i += 1;
                }
            } else {
                i += 1;
            }
        } else {
            i += 1;
        }

        // Check for LIMIT
        if i + 1 < args.len()
            && let RedisJsonValue::String(s) = &args[i]
            && s.to_uppercase() == "LIMIT"
        {
            limit = Some(args[i + 1].clone());
        }

        Ok(Self { key, strategy, operator, threshold, limit })
    }
}

/// Output for Redis `XTRIM` command.
///
/// Returns the number of entries deleted from the stream.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XtrimOutput {
    /// The number of entries deleted from the stream
    deleted: i64,
}

impl XtrimOutput {
    /// Create a new XtrimOutput
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of deleted entries
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into an XtrimOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XTRIM response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XTRIM response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

impl Serialize for XtrimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XtrimOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic_maxlen() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MaxLen,
                operator: None,
                threshold: RedisJsonValue::Integer(100),
                limit: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n"));
            assert!(cmd.windows(5).any(|w| w == b"XTRIM"));
            assert!(cmd.windows(6).any(|w| w == b"MAXLEN"));
        }

        #[test]
        fn test_encode_command_with_approximate() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MaxLen,
                operator: Some(TrimOperator::Approximate),
                threshold: RedisJsonValue::Integer(100),
                limit: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n"));
            assert!(cmd.windows(1).any(|w| w == b"~"));
        }

        #[test]
        fn test_encode_command_with_exact() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MaxLen,
                operator: Some(TrimOperator::Exact),
                threshold: RedisJsonValue::Integer(100),
                limit: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(1).any(|w| w == b"="));
        }

        #[test]
        fn test_encode_command_minid() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MinId,
                operator: None,
                threshold: RedisJsonValue::String("1234567890123-0".into()),
                limit: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"MINID"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MaxLen,
                operator: Some(TrimOperator::Approximate),
                threshold: RedisJsonValue::Integer(100),
                limit: Some(RedisJsonValue::Integer(10)),
            };
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"LIMIT"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XtrimInput {
                key: RedisKey::String("mystream".into()),
                strategy: TrimStrategy::MaxLen,
                operator: None,
                threshold: RedisJsonValue::Integer(100),
                limit: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("MAXLEN".into()),
                RedisJsonValue::Integer(100),
            ];
            let input = XtrimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(matches!(input.strategy, TrimStrategy::MaxLen));
        }

        #[test]
        fn test_decode_input_with_approximate() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("MAXLEN".into()),
                RedisJsonValue::String("~".into()),
                RedisJsonValue::Integer(100),
            ];
            let input = XtrimInput::decode(args).unwrap();
            assert!(matches!(input.operator, Some(TrimOperator::Approximate)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("MAXLEN".into())];
            let err = XtrimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XtrimOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_output_positive() {
            let output = XtrimOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.deleted(), 5);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XtrimOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = XtrimOutput::new(10);
            assert_eq!(output.deleted(), 10);
        }

        #[test]
        fn test_output_serialize() {
            let output = XtrimOutput::new(5);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("deleted"));
            assert!(json.contains("5"));
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
        async fn test_xtrim_maxlen_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add multiple entries
                    for i in 0..10 {
                        xadd_entry(ctx, "xtrim_maxlen", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Trim to 5 entries
                    let result = ctx
                        .raw(
                            &XtrimInput {
                                key: RedisKey::String("xtrim_maxlen".into()),
                                strategy: TrimStrategy::MaxLen,
                                operator: None,
                                threshold: RedisJsonValue::Integer(5),
                                limit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 5);

                    // Verify stream length
                    let xlen_result = ctx.raw(b"*2\r\n$4\r\nXLEN\r\n$12\r\nxtrim_maxlen\r\n").await.expect("XLEN failed");
                    let len_str = String::from_utf8_lossy(&xlen_result);
                    assert!(len_str.contains(":5\r\n"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_no_delete_needed() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add a few entries
                    for i in 0..3 {
                        xadd_entry(ctx, "xtrim_nodelete", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Trim to 10 (more than we have)
                    let result = ctx
                        .raw(
                            &XtrimInput {
                                key: RedisKey::String("xtrim_nodelete".into()),
                                strategy: TrimStrategy::MaxLen,
                                operator: None,
                                threshold: RedisJsonValue::Integer(10),
                                limit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_approximate() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add multiple entries
                    for i in 0..20 {
                        xadd_entry(ctx, "xtrim_approx", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Trim with approximate (~)
                    let result = ctx
                        .raw(
                            &XtrimInput {
                                key: RedisKey::String("xtrim_approx".into()),
                                strategy: TrimStrategy::MaxLen,
                                operator: Some(TrimOperator::Approximate),
                                threshold: RedisJsonValue::Integer(5),
                                limit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XtrimOutput::decode(&result).expect("decode failed");
                    // With approximate, we may delete slightly more or less
                    assert!(output.deleted() >= 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_minid() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Add entries
                    let _id1 = xadd_entry(ctx, "xtrim_minid", "f1", "v1").await;
                    let _id2 = xadd_entry(ctx, "xtrim_minid", "f2", "v2").await;
                    let id3 = xadd_entry(ctx, "xtrim_minid", "f3", "v3").await;

                    // Trim everything before id3
                    let result = ctx
                        .raw(
                            &XtrimInput {
                                key: RedisKey::String("xtrim_minid".into()),
                                strategy: TrimStrategy::MinId,
                                operator: None,
                                threshold: RedisJsonValue::String(id3.clone()),
                                limit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 2); // id1 and id2 should be deleted
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_nonexistent_key() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &XtrimInput {
                                key: RedisKey::String("xtrim_nonexistent".into()),
                                strategy: TrimStrategy::MaxLen,
                                operator: None,
                                threshold: RedisJsonValue::Integer(5),
                                limit: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XtrimOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            for i in 0..5 {
                xadd_entry(&mut ctx, "xtrim_r2", &format!("f{}", i), &format!("v{}", i)).await;
            }

            let result = ctx
                .raw(
                    &XtrimInput {
                        key: RedisKey::String("xtrim_r2".into()),
                        strategy: TrimStrategy::MaxLen,
                        operator: None,
                        threshold: RedisJsonValue::Integer(2),
                        limit: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = XtrimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 3);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            for i in 0..5 {
                xadd_entry(&mut ctx, "xtrim_r3", &format!("f{}", i), &format!("v{}", i)).await;
            }

            let result = ctx
                .raw(
                    &XtrimInput {
                        key: RedisKey::String("xtrim_r3".into()),
                        strategy: TrimStrategy::MaxLen,
                        operator: None,
                        threshold: RedisJsonValue::Integer(2),
                        limit: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XtrimOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 3);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xtrim_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Add entries to two streams
                    for i in 0..5 {
                        xadd_entry(ctx, "xtrim_pipe1", &format!("f{}", i), &format!("v{}", i)).await;
                        xadd_entry(ctx, "xtrim_pipe2", &format!("f{}", i), &format!("v{}", i)).await;
                    }

                    // Pipeline two XTRIM commands
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &XtrimInput {
                            key: RedisKey::String("xtrim_pipe1".into()),
                            strategy: TrimStrategy::MaxLen,
                            operator: None,
                            threshold: RedisJsonValue::Integer(2),
                            limit: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &XtrimInput {
                            key: RedisKey::String("xtrim_pipe2".into()),
                            strategy: TrimStrategy::MaxLen,
                            operator: None,
                            threshold: RedisJsonValue::Integer(3),
                            limit: None,
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XtrimOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.deleted(), 3);

                    let out2 = XtrimOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.deleted(), 2);
                })
            })
            .await;
        }
    }
}
