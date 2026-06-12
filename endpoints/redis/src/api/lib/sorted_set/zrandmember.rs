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
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ZrandmemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrandmember,
    "Returns one or more random members from a sorted set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZRANDMEMBER`
/// https://redis.io/docs/latest/commands/zrandmember/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrandmemberInput {
    key: RedisKey,
    count: Option<Count>,
}

impl ZrandmemberInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into(), count: None }
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(Count { count: count.into(), with_scores: None });
        self
    }

    pub fn with_count_and_scores(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(Count { count: count.into(), with_scores: Some(true) });
        self
    }
}

impl Serialize for ZrandmemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ZrandmemberInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Count {
    count: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_scores: Option<bool>,
}

impl_redis_operation!(
    ZrandmemberInput,
    API_INFO,
    { key, count }
);

impl RedisCommandInput for ZrandmemberInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(count) = &self.count {
            command.arg(&count.count);

            if count.with_scores == Some(true) {
                command.arg("WITHSCORES");
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request(format!("ZRANDMEMBER requires at least 1 argument, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let mut count = None;

        if args.len() >= 2 {
            let count_value = args[1].clone();
            let mut with_scores = None;

            // Check for WITHSCORES
            if args.len() >= 3
                && let RedisJsonValue::String(s) = &args[2]
                && s.to_uppercase() == "WITHSCORES"
            {
                with_scores = Some(true);
            }

            count = Some(Count { count: count_value, with_scores });
        }

        Ok(Self { key, count })
    }
}

/// Output for Redis ZRANDMEMBER command (single member, no count)
///
/// Returns a single random member, or None if the set is empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrandmemberOutput {
    member: Option<RedisJsonValue>,
}

impl ZrandmemberOutput {
    pub fn new(member: Option<RedisJsonValue>) -> Self {
        Self { member }
    }

    pub fn member(&self) -> Option<&RedisJsonValue> {
        self.member.as_ref()
    }

    pub fn exists(&self) -> bool {
        self.member.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let member = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => None,
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(b)) => {
                Some(RedisJsonValue::String(String::from_utf8(b).map_err(EpError::parse)?))
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => None,
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                Some(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?))
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { member })
    }
}

impl Serialize for ZrandmemberOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrandmemberOutput", 1)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

/// Output for Redis ZRANDMEMBER command with COUNT (multiple members)
///
/// Returns an array of random members.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrandmemberArrayOutput {
    members: Vec<RedisJsonValue>,
}

impl ZrandmemberArrayOutput {
    pub fn new(members: Vec<RedisJsonValue>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &Vec<RedisJsonValue> {
        &self.members
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let members = arr.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let members = data.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { members })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("ZRANDMEMBER with count must return array")),
        }
    }
}

impl Serialize for ZrandmemberArrayOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrandmemberArrayOutput", 1)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

/// Output for Redis ZRANDMEMBER command with COUNT and WITHSCORES
///
/// Returns an array of members with their scores.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrandmemberWithScoresOutput {
    elements: Vec<Scores>,
}

impl ZrandmemberWithScoresOutput {
    pub fn new(elements: Vec<Scores>) -> Self {
        Self { elements }
    }

    pub fn elements(&self) -> &Vec<Scores> {
        &self.elements
    }

    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    pub fn len(&self) -> usize {
        self.elements.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut elements = Vec::new();
                // RESP2 returns flat array: [member1, score1, member2, score2, ...]
                let mut iter = arr.into_iter();
                while let Some(member_frame) = iter.next() {
                    let score_frame = iter.next().ok_or_else(|| EpError::parse("ZRANDMEMBER missing score for member"))?;

                    let member: RedisJsonValue = member_frame.try_into()?;
                    let score: RedisJsonValue = score_frame.try_into()?;

                    elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                }
                Ok(Self { elements })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut elements = Vec::new();

                // Check if this is a flat array (RESP2 parsed as RESP3) or nested array (true RESP3)
                if !data.is_empty() && !matches!(data[0], Resp3Frame::Array { .. }) {
                    // Flat array: [member1, score1, member2, score2, ...]
                    let mut iter = data.into_iter();
                    while let Some(member_frame) = iter.next() {
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("ZRANDMEMBER missing score for member"))?;

                        let member: RedisJsonValue = member_frame.try_into()?;
                        let score: RedisJsonValue = score_frame.try_into()?;

                        elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                    }
                } else {
                    // Nested array: [[member1, score1], [member2, score2], ...]
                    for frame in data {
                        match frame {
                            Resp3Frame::Array { data, .. } if data.len() == 2 => {
                                let mut it = data.into_iter();
                                let member: RedisJsonValue = it.next().unwrap().try_into()?;
                                let score: RedisJsonValue = it.next().unwrap().try_into()?;

                                elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                            }
                            _ => {
                                return Err(EpError::parse("ZRANDMEMBER element must be [member, score] array"));
                            }
                        }
                    }
                }
                Ok(Self { elements })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("ZRANDMEMBER WITHSCORES must return array")),
        }
    }
}

impl Serialize for ZrandmemberWithScoresOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrandmemberWithScoresOutput", 1)?;
        state.serialize_field("elements", &self.elements)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_count() {
            let input = ZrandmemberInput::new(RedisKey::String("myzset".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$11\r\nZRANDMEMBER\r\n$6\r\nmyzset\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = ZrandmemberInput::new(RedisKey::String("myzset".into())).with_count(RedisJsonValue::Integer(3));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$11\r\nZRANDMEMBER\r\n$6\r\nmyzset\r\n$1\r\n3\r\n");
        }

        #[test]
        fn test_encode_command_with_count_and_scores() {
            let input = ZrandmemberInput::new(RedisKey::String("myzset".into())).with_count_and_scores(RedisJsonValue::Integer(2));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*4\r\n$11\r\nZRANDMEMBER\r\n$6\r\nmyzset\r\n$1\r\n2\r\n$10\r\nWITHSCORES\r\n");
        }

        #[test]
        fn test_decode_output_single() {
            let output = ZrandmemberOutput::decode(b"$6\r\nmember\r\n").unwrap();
            assert!(output.exists());
            assert!(output.member().is_some());
        }

        #[test]
        fn test_decode_output_null() {
            let output = ZrandmemberOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_output_array() {
            let output = ZrandmemberArrayOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_output_array_empty() {
            let output = ZrandmemberArrayOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_with_scores() {
            // RESP2 flat array: [member1, score1, member2, score2]
            let output = ZrandmemberWithScoresOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_error() {
            let err = ZrandmemberOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let input = ZrandmemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(5)];
            let input = ZrandmemberInput::decode(args).unwrap();
            assert!(input.count.is_some());
            assert_eq!(input.count.as_ref().unwrap().count, RedisJsonValue::Integer(5));
        }

        #[test]
        fn test_decode_input_with_withscores() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(5),
                RedisJsonValue::String("WITHSCORES".into()),
            ];
            let input = ZrandmemberInput::decode(args).unwrap();
            assert!(input.count.is_some());
            assert_eq!(input.count.as_ref().unwrap().with_scores, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ZrandmemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrandmemberInput::new(RedisKey::String("myzset".into()));
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
        async fn test_zrandmember_single() {
            // ZRANDMEMBER requires Redis 6.2+
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzrandmember_single\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$18\r\nzrandmember_single\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n")
                        .await
                        .expect("raw failed");

                    let result =
                        ctx.raw(&ZrandmemberInput::new(RedisKey::String("zrandmember_single".into())).command()).await.expect("raw failed");

                    let output = ZrandmemberOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrandmember_with_count() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrandmember_count\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$17\r\nzrandmember_count\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrandmemberInput::new(RedisKey::String("zrandmember_count".into()))
                                .with_count(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZrandmemberArrayOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrandmember_with_scores() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nzrandmember_scores\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$18\r\nzrandmember_scores\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrandmemberInput::new(RedisKey::String("zrandmember_scores".into()))
                                .with_count_and_scores(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZrandmemberWithScoresOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrandmember_empty_set() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrandmember_empty\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZrandmemberInput::new(RedisKey::String("zrandmember_empty".into())).command()).await.expect("raw failed");

                    let output = ZrandmemberOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrandmember_negative_count() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrandmember_neg\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nZADD\r\n$15\r\nzrandmember_neg\r\n$1\r\n1\r\n$3\r\none\r\n").await.expect("raw failed");

                    // Negative count allows duplicates
                    let result = ctx
                        .raw(
                            &ZrandmemberInput::new(RedisKey::String("zrandmember_neg".into()))
                                .with_count(RedisJsonValue::Integer(-5))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZrandmemberArrayOutput::decode(&result).expect("decode failed");
                    // With negative count, can return more elements than set size (with duplicates)
                    assert_eq!(output.len(), 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrandmember_wrongtype() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nzrandmember_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&ZrandmemberInput::new(RedisKey::String("zrandmember_wrong".into())).command()).await.expect("raw failed");

                    let err = ZrandmemberOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
