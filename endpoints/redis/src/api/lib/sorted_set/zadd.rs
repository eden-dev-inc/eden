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

pub(crate) const API_INFO: ApiInfo<RedisApi, ZaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zadd,
    "Adds one or more members to a sorted set, or updates their scores. Creates the key if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZADD`
/// https://redis.io/docs/latest/commands/zadd/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZaddInput {
    pub key: RedisKey,
    #[builder(default)]
    exist: Option<Exist>,
    #[builder(default)]
    size: Option<Size>,
    #[builder(default)]
    ch: Option<bool>,
    #[builder(default)]
    incr: Option<bool>,
    pub scores: Vec<Scores>,
}

impl ZaddInput {
    pub fn new(key: impl Into<RedisKey>, scores: Vec<Scores>) -> Self {
        Self {
            key: key.into(),
            exist: None,
            size: None,
            ch: None,
            incr: None,
            scores,
        }
    }

    pub fn single(key: impl Into<RedisKey>, score: impl Into<RedisJsonValue>, member: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            exist: None,
            size: None,
            ch: None,
            incr: None,
            scores: vec![Scores { score: score.into(), member: member.into() }],
        }
    }

    pub fn with_nx(mut self) -> Self {
        self.exist = Some(Exist::NX);
        self
    }

    pub fn with_xx(mut self) -> Self {
        self.exist = Some(Exist::XX);
        self
    }

    pub fn with_gt(mut self) -> Self {
        self.size = Some(Size::GT);
        self
    }

    pub fn with_lt(mut self) -> Self {
        self.size = Some(Size::LT);
        self
    }

    pub fn with_ch(mut self) -> Self {
        self.ch = Some(true);
        self
    }

    pub fn with_incr(mut self) -> Self {
        self.incr = Some(true);
        self
    }
}

impl Serialize for ZaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, scores
        if self.exist.is_some() {
            fields += 1;
        }
        if self.size.is_some() {
            fields += 1;
        }
        if self.ch.is_some() {
            fields += 1;
        }
        if self.incr.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZaddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("scores", &self.scores)?;

        if let Some(exist) = &self.exist {
            state.serialize_field("exist", exist)?;
        }
        if let Some(size) = &self.size {
            state.serialize_field("size", size)?;
        }
        if let Some(ch) = &self.ch {
            state.serialize_field("ch", ch)?;
        }
        if let Some(incr) = &self.incr {
            state.serialize_field("incr", incr)?;
        }
        state.end()
    }
}

#[derive(
    Debug, Serialize, Deserialize, PartialEq, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema,
)]
pub struct Scores {
    pub score: RedisJsonValue,
    pub member: RedisJsonValue,
}

impl Scores {
    pub fn new(score: impl Into<RedisJsonValue>, member: impl Into<RedisJsonValue>) -> Self {
        Self { score: score.into(), member: member.into() }
    }
}

#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, ToSchema, JsonSchema,
)]
pub enum Size {
    #[default]
    GT,
    LT,
}

#[derive(
    Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, PartialEq, ToSchema, JsonSchema,
)]
pub enum Exist {
    #[default]
    NX,
    XX,
}

impl_redis_operation!(ZaddInput, API_INFO, { key, exist, size, ch, incr, scores });

impl RedisCommandInput for ZaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(exist) = &self.exist {
            match exist {
                Exist::NX => command.arg("NX"),
                Exist::XX => command.arg("XX"),
            };
        }

        if let Some(size) = &self.size {
            match size {
                Size::GT => command.arg("GT"),
                Size::LT => command.arg("LT"),
            };
        }

        if let Some(ch) = &self.ch
            && *ch
        {
            command.arg("CH");
        }

        if let Some(incr) = &self.incr
            && *incr
        {
            command.arg("INCR");
        }

        for score in &self.scores {
            command.arg(&score.score).arg(&score.member);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("ZADD requires at least a key argument"));
        }

        let key = args[0].clone().try_into()?;
        let mut exist = None;
        let mut size = None;
        let mut ch = None;
        let mut incr = None;
        let mut i = 1;

        // Parse optional flags
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "NX" => {
                        exist = Some(Exist::NX);
                        i += 1;
                    }
                    "XX" => {
                        exist = Some(Exist::XX);
                        i += 1;
                    }
                    "GT" => {
                        size = Some(Size::GT);
                        i += 1;
                    }
                    "LT" => {
                        size = Some(Size::LT);
                        i += 1;
                    }
                    "CH" => {
                        ch = Some(true);
                        i += 1;
                    }
                    "INCR" => {
                        incr = Some(true);
                        i += 1;
                    }
                    _ => break,
                }
            } else {
                break;
            }
        }

        // Parse score/member pairs
        if !(args.len() - i).is_multiple_of(2) {
            return Err(EpError::request("ZADD requires score/member pairs"));
        }

        let mut scores = Vec::new();
        while i + 1 < args.len() {
            scores.push(Scores { score: args[i].clone(), member: args[i + 1].clone() });
            i += 2;
        }

        if scores.is_empty() {
            return Err(EpError::request("ZADD requires at least one score/member pair"));
        }

        Ok(Self { key, exist, size, ch, incr, scores })
    }
}

/// Output for Redis ZADD command
///
/// Returns the number of elements added (or changed if CH is specified),
/// or the new score if INCR option is used.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum ZaddOutput {
    /// Number of elements added/changed (default behavior)
    Count(i64),
    /// New score when INCR option is used
    Score(f64),
    /// Nil response (INCR with NX/XX conditions not met)
    Nil,
}

impl ZaddOutput {
    pub fn count(n: i64) -> Self {
        ZaddOutput::Count(n)
    }

    pub fn score(s: f64) -> Self {
        ZaddOutput::Score(s)
    }

    /// Get the count if this is a Count variant
    pub fn as_count(&self) -> Option<i64> {
        match self {
            ZaddOutput::Count(n) => Some(*n),
            _ => None,
        }
    }

    /// Get the score if this is a Score variant
    pub fn as_score(&self) -> Option<f64> {
        match self {
            ZaddOutput::Score(s) => Some(*s),
            _ => None,
        }
    }

    /// Check if this is a Nil response
    pub fn is_nil(&self) -> bool {
        matches!(self, ZaddOutput::Nil)
    }

    /// Decode response - pass `incr=true` if INCR option was used
    pub fn decode(bytes: &[u8], incr: bool) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            // Integer response (count of elements added/changed)
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => Ok(ZaddOutput::Count(n)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => Ok(ZaddOutput::Count(data)),

            // Nil response (INCR with conditions not met)
            DecoderRespFrame::Resp2(Resp2Frame::Null) => Ok(ZaddOutput::Nil),
            DecoderRespFrame::Resp3(Resp3Frame::Null) => Ok(ZaddOutput::Nil),

            // Bulk string response (new score when INCR is used)
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(data)) if incr => {
                let score =
                    String::from_utf8(data).map_err(EpError::parse)?.parse::<f64>().map_err(|_| EpError::parse("Invalid score format"))?;
                Ok(ZaddOutput::Score(score))
            }

            // RESP3 Double response (new score when INCR is used)
            DecoderRespFrame::Resp3(Resp3Frame::Double { data, .. }) => Ok(ZaddOutput::Score(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) if incr => {
                let score =
                    String::from_utf8(data).map_err(EpError::parse)?.parse::<f64>().map_err(|_| EpError::parse("Invalid score format"))?;
                Ok(ZaddOutput::Score(score))
            }

            // Error responses
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }

            _ => Err(EpError::parse("ZADD unexpected response format")),
        }
    }
}

impl Serialize for ZaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            ZaddOutput::Count(n) => {
                let mut state = serializer.serialize_struct("ZaddOutput", 1)?;
                state.serialize_field("count", n)?;
                state.end()
            }
            ZaddOutput::Score(s) => {
                let mut state = serializer.serialize_struct("ZaddOutput", 1)?;
                state.serialize_field("score", s)?;
                state.end()
            }
            ZaddOutput::Nil => {
                let mut state = serializer.serialize_struct("ZaddOutput", 1)?;
                state.serialize_field("nil", &true)?;
                state.end()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = ZaddInput::single(
                RedisKey::String("myzset".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("member1".into()),
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZADD"));
            assert!(cmd_str.contains("myzset"));
            assert!(cmd_str.contains("member1"));
        }

        #[test]
        fn test_encode_command_with_nx() {
            let input =
                ZaddInput::single(RedisKey::String("myzset".into()), RedisJsonValue::Integer(1), RedisJsonValue::String("m".into()))
                    .with_nx();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NX"));
        }

        #[test]
        fn test_encode_command_with_xx_gt_ch() {
            let input =
                ZaddInput::single(RedisKey::String("myzset".into()), RedisJsonValue::Integer(1), RedisJsonValue::String("m".into()))
                    .with_xx()
                    .with_gt()
                    .with_ch();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("XX"));
            assert!(cmd_str.contains("GT"));
            assert!(cmd_str.contains("CH"));
        }

        #[test]
        fn test_encode_command_with_incr() {
            let input =
                ZaddInput::single(RedisKey::String("myzset".into()), RedisJsonValue::Integer(5), RedisJsonValue::String("m".into()))
                    .with_incr();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("INCR"));
        }

        #[test]
        fn test_encode_command_multiple_members() {
            let input = ZaddInput::new(
                RedisKey::String("myzset".into()),
                vec![
                    Scores::new(RedisJsonValue::Integer(1), RedisJsonValue::String("a".into())),
                    Scores::new(RedisJsonValue::Integer(2), RedisJsonValue::String("b".into())),
                    Scores::new(RedisJsonValue::Integer(3), RedisJsonValue::String("c".into())),
                ],
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZADD"));
        }

        #[test]
        fn test_decode_output_count() {
            let output = ZaddOutput::decode(b":3\r\n", false).unwrap();
            assert_eq!(output.as_count(), Some(3));
        }

        #[test]
        fn test_decode_output_score_with_incr() {
            let output = ZaddOutput::decode(b"$3\r\n5.5\r\n", true).unwrap();
            assert_eq!(output.as_score(), Some(5.5));
        }

        #[test]
        fn test_decode_output_nil() {
            let output = ZaddOutput::decode(b"$-1\r\n", true).unwrap();
            assert!(output.is_nil());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ZaddOutput::decode(b":0\r\n", false).unwrap();
            assert_eq!(output.as_count(), Some(0));
        }

        #[test]
        fn test_decode_error() {
            let err = ZaddOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("member".into()),
            ];
            let input = ZaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert_eq!(input.scores.len(), 1);
        }

        #[test]
        fn test_decode_input_with_flags() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("NX".into()),
                RedisJsonValue::String("CH".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("member".into()),
            ];
            let input = ZaddInput::decode(args).unwrap();
            assert_eq!(input.exist, Some(Exist::NX));
            assert_eq!(input.ch, Some(true));
        }

        #[test]
        fn test_decode_input_multiple_members() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("a".into()),
                RedisJsonValue::Integer(2),
                RedisJsonValue::String("b".into()),
            ];
            let input = ZaddInput::decode(args).unwrap();
            assert_eq!(input.scores.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(1)];
            let err = ZaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("score/member"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input =
                ZaddInput::single(RedisKey::String("myzset".into()), RedisJsonValue::Integer(1), RedisJsonValue::String("m".into()));
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_scores_new() {
            let s = Scores::new(RedisJsonValue::Float(1.5), RedisJsonValue::String("member".into()));
            assert_eq!(s.score, RedisJsonValue::Float(1.5));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_single_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzadd_single\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &ZaddInput::single(
                                RedisKey::String("zadd_single".into()),
                                RedisJsonValue::Integer(1),
                                RedisJsonValue::String("member1".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.as_count(), Some(1));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_multiple_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzadd_multi\r\n").await.expect("del");

                    let result = ctx
                        .raw(
                            &ZaddInput::new(
                                RedisKey::String("zadd_multi".into()),
                                vec![
                                    Scores::new(RedisJsonValue::Integer(1), RedisJsonValue::String("a".into())),
                                    Scores::new(RedisJsonValue::Integer(2), RedisJsonValue::String("b".into())),
                                    Scores::new(RedisJsonValue::Integer(3), RedisJsonValue::String("c".into())),
                                ],
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.as_count(), Some(3));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_update_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nzadd_update\r\n").await.expect("del");

                    // Add initial member
                    ctx.raw(
                        &ZaddInput::single(
                            RedisKey::String("zadd_update".into()),
                            RedisJsonValue::Integer(1),
                            RedisJsonValue::String("member".into()),
                        )
                        .command(),
                    )
                    .await
                    .expect("zadd");

                    // Update score (should return 0 - no new members)
                    let result = ctx
                        .raw(
                            &ZaddInput::single(
                                RedisKey::String("zadd_update".into()),
                                RedisJsonValue::Integer(10),
                                RedisJsonValue::String("member".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.as_count(), Some(0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_with_nx() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzadd_nx\r\n").await.expect("del");

                    // Add initial member
                    ctx.raw(
                        &ZaddInput::single(
                            RedisKey::String("zadd_nx".into()),
                            RedisJsonValue::Integer(1),
                            RedisJsonValue::String("existing".into()),
                        )
                        .command(),
                    )
                    .await
                    .expect("zadd");

                    // Try to add with NX (existing should not be updated, new should be added)
                    let result = ctx
                        .raw(
                            &ZaddInput::new(
                                RedisKey::String("zadd_nx".into()),
                                vec![
                                    Scores::new(RedisJsonValue::Integer(10), RedisJsonValue::String("existing".into())),
                                    Scores::new(RedisJsonValue::Integer(2), RedisJsonValue::String("new".into())),
                                ],
                            )
                            .with_nx()
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.as_count(), Some(1)); // Only "new" was added
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_with_ch() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzadd_ch\r\n").await.expect("del");

                    // Add initial member
                    ctx.raw(
                        &ZaddInput::single(
                            RedisKey::String("zadd_ch".into()),
                            RedisJsonValue::Integer(1),
                            RedisJsonValue::String("member".into()),
                        )
                        .command(),
                    )
                    .await
                    .expect("zadd");

                    // Update with CH - should count changed members
                    let result = ctx
                        .raw(
                            &ZaddInput::single(
                                RedisKey::String("zadd_ch".into()),
                                RedisJsonValue::Integer(10),
                                RedisJsonValue::String("member".into()),
                            )
                            .with_ch()
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, false).expect("decode");
                    assert_eq!(output.as_count(), Some(1)); // Member was changed
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_with_incr() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nzadd_incr\r\n").await.expect("del");

                    // Add initial member with score 10
                    ctx.raw(
                        &ZaddInput::single(
                            RedisKey::String("zadd_incr".into()),
                            RedisJsonValue::Integer(10),
                            RedisJsonValue::String("member".into()),
                        )
                        .command(),
                    )
                    .await
                    .expect("zadd");

                    // Increment by 5
                    let result = ctx
                        .raw(
                            &ZaddInput::single(
                                RedisKey::String("zadd_incr".into()),
                                RedisJsonValue::Integer(5),
                                RedisJsonValue::String("member".into()),
                            )
                            .with_incr()
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZaddOutput::decode(&result, true).expect("decode");
                    assert_eq!(output.as_score(), Some(15.0)); // 10 + 5 = 15
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$10\r\nzadd_wrong\r\n$5\r\nvalue\r\n").await.expect("set");

                    let result = ctx
                        .raw(
                            &ZaddInput::single(
                                RedisKey::String("zadd_wrong".into()),
                                RedisJsonValue::Integer(1),
                                RedisJsonValue::String("member".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = ZaddOutput::decode(&result, false).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzadd_r2\r\n").await.expect("del");

            let result = ctx
                .raw(
                    &ZaddInput::single(
                        RedisKey::String("zadd_r2".into()),
                        RedisJsonValue::Integer(1),
                        RedisJsonValue::String("member".into()),
                    )
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ZaddOutput::decode(&result, false).expect("decode");
            assert_eq!(output.as_count(), Some(1));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zadd_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nzadd_r3\r\n").await.expect("del");

            let result = ctx
                .raw(
                    &ZaddInput::new(
                        RedisKey::String("zadd_r3".into()),
                        vec![
                            Scores::new(RedisJsonValue::Integer(1), RedisJsonValue::String("a".into())),
                            Scores::new(RedisJsonValue::Integer(2), RedisJsonValue::String("b".into())),
                        ],
                    )
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = ZaddOutput::decode(&result, false).expect("decode");
            assert_eq!(output.as_count(), Some(2));

            ctx.stop().await;
        }
    }
}
