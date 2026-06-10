#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Scores, ScoresBuilder, key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use redis_protocol::resp3::types::OwnedFrame as Resp3OwnedFrame;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ZrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrange,
    "Returns members in a sorted set within a range of indexes",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZRANGE`
/// https://redis.io/docs/latest/commands/zrange/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrangeInput {
    key: RedisKey,
    start: RedisJsonValue,
    stop: RedisJsonValue,
    #[builder(default)]
    by: Option<By>,
    #[builder(default)]
    rev: Option<bool>,
    #[builder(default)]
    limit: Option<Limit>,
    #[builder(default)]
    with_scores: Option<bool>,
}

impl ZrangeInput {
    pub fn new(key: impl Into<RedisKey>, start: impl Into<RedisJsonValue>, stop: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            start: start.into(),
            stop: stop.into(),
            by: None,
            rev: None,
            limit: None,
            with_scores: None,
        }
    }

    pub fn by_score(mut self) -> Self {
        self.by = Some(By::BYSCORE);
        self
    }

    pub fn by_lex(mut self) -> Self {
        self.by = Some(By::BYLEX);
        self
    }

    pub fn rev(mut self) -> Self {
        self.rev = Some(true);
        self
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit { offset: offset.into(), count: count.into() });
        self
    }

    pub fn with_scores(mut self) -> Self {
        self.with_scores = Some(true);
        self
    }
}

impl Serialize for ZrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.by.is_some() {
            fields += 1;
        }
        if self.rev.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        if self.with_scores.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ZrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;

        if let Some(by) = &self.by {
            state.serialize_field("by", by)?;
        }
        if let Some(rev) = &self.rev {
            state.serialize_field("rev", rev)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        if let Some(with_scores) = &self.with_scores {
            state.serialize_field("with_scores", with_scores)?;
        }
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Limit {
    offset: RedisJsonValue,
    count: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
enum By {
    #[default]
    BYSCORE,
    BYLEX,
}

impl_redis_operation!(ZrangeInput, API_INFO, {key, start, stop, by, rev, limit, with_scores});

impl RedisCommandInput for ZrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.start).arg(&self.stop);

        if let Some(by) = &self.by {
            match by {
                By::BYSCORE => {
                    command.arg("BYSCORE");
                }
                By::BYLEX => {
                    command.arg("BYLEX");
                }
            }
        }
        if self.rev == Some(true) {
            command.arg("REV");
        }
        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }
        if self.with_scores == Some(true) {
            command.arg("WITHSCORES");
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request("ZRANGE requires at least 3 arguments"));
        }

        let key = args[0].clone().try_into()?;
        let start = args[1].clone();
        let stop = args[2].clone();
        let mut by = None;
        let mut rev = None;
        let mut limit = None;
        let mut with_scores = None;

        let mut i = 3;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "BYSCORE" => {
                        by = Some(By::BYSCORE);
                        i += 1;
                    }
                    "BYLEX" => {
                        by = Some(By::BYLEX);
                        i += 1;
                    }
                    "REV" => {
                        rev = Some(true);
                        i += 1;
                    }
                    "LIMIT" if i + 2 < args.len() => {
                        limit = Some(Limit { offset: args[i + 1].clone(), count: args[i + 2].clone() });
                        i += 3;
                    }
                    "WITHSCORES" => {
                        with_scores = Some(true);
                        i += 1;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }
        Ok(Self { key, start, stop, by, rev, limit, with_scores })
    }
}

#[derive(Debug, Clone)]
pub struct ZrangeOutput(Vec<RedisJsonValue>);

impl ZrangeOutput {
    pub fn new(elements: Vec<RedisJsonValue>) -> Self {
        Self(elements)
    }
    pub fn elements(&self) -> &Vec<RedisJsonValue> {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame for ZrangeOutput"))?;

        let elements = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                arr.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                data.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("ZRANGE must return array")),
        };
        Ok(Self(elements))
    }
}

#[derive(Debug, Clone)]
pub struct ZrangeWithScoresOutput(Vec<Scores>);

impl ZrangeWithScoresOutput {
    pub fn new(elements: Vec<Scores>) -> Self {
        Self(elements)
    }
    pub fn elements(&self) -> &Vec<Scores> {
        &self.0
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) =
            RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame for ZrangeWithScoresOutput"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(frames)) => {
                let mut elements = Vec::new();
                let mut iter = frames.into_iter();
                while let Some(member_frame) = iter.next() {
                    let score_frame = iter.next().ok_or_else(|| EpError::parse("ZRANGE WITHSCORES missing score"))?;
                    let member: RedisJsonValue = member_frame.try_into()?;
                    let score: RedisJsonValue = score_frame.try_into()?;
                    elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                }
                Ok(ZrangeWithScoresOutput(elements))
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut elements = Vec::new();

                // Check if this is a flat array (RESP2 parsed as RESP3) or nested array (true RESP3)
                if !data.is_empty() && !matches!(data[0], Resp3Frame::Array { .. }) {
                    // Flat array: [member1, score1, member2, score2, ...]
                    let mut iter = data.into_iter();
                    while let Some(member_frame) = iter.next() {
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("ZRANGE WITHSCORES missing score"))?;
                        let member: RedisJsonValue = member_frame.try_into()?;
                        let score: RedisJsonValue = score_frame.try_into()?;
                        elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                    }
                } else {
                    // Nested array: [[member1, score1], [member2, score2], ...]
                    for frame in data {
                        match frame {
                            Resp3OwnedFrame::Array { data, .. } if data.len() == 2 => {
                                let mut it = data.into_iter();
                                let member: RedisJsonValue = it.next().unwrap().try_into()?;
                                let score: RedisJsonValue = it.next().unwrap().try_into()?;
                                elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                            }
                            _ => {
                                return Err(EpError::parse("ZRANGE WITHSCORES response frame must be an array"));
                            }
                        }
                    }
                }
                Ok(ZrangeWithScoresOutput(elements))
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("ZRANGE WITHSCORES must return an array")),
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
            let input = ZrangeInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*4\r\n$6\r\nZRANGE\r\n$6\r\nmyzset\r\n$1\r\n0\r\n$2\r\n-1\r\n");
        }

        #[test]
        fn test_encode_command_with_byscore() {
            let input = ZrangeInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("0".into()),
                RedisJsonValue::String("+inf".into()),
            )
            .by_score();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("BYSCORE"));
        }

        #[test]
        fn test_encode_command_with_rev() {
            let input = ZrangeInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1)).rev();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("REV"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input =
                ZrangeInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1)).with_scores();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("WITHSCORES"));
        }

        #[test]
        fn test_decode_output_array() {
            let output = ZrangeOutput::decode(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").unwrap();
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrangeOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_with_scores() {
            let output = ZrangeWithScoresOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_error() {
            let err = ZrangeOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            ];
            let input = ZrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let err = ZrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrangeInput::new(RedisKey::String("myzset".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1));
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrange_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzrange_basic\r\n").await.expect("raw failed");
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$12\r\nzrange_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZrangeInput::new(RedisKey::String("zrange_basic".into()), RedisJsonValue::Integer(0), RedisJsonValue::Integer(-1)).command()).await.expect("raw failed");
                    let output = ZrangeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                })
            }).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrange_with_scores() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzrange_scores\r\n").await.expect("raw failed");
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$13\r\nzrange_scores\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrangeInput::new(
                                RedisKey::String("zrange_scores".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(-1),
                            )
                            .with_scores()
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangeWithScoresOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrange_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nzrange_empty\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangeInput::new(
                                RedisKey::String("zrange_empty".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(-1),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangeOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrange_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$12\r\nzrange_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangeInput::new(
                                RedisKey::String("zrange_wrong".into()),
                                RedisJsonValue::Integer(0),
                                RedisJsonValue::Integer(-1),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let err = ZrangeOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
