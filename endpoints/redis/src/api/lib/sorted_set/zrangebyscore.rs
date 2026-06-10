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

const API_INFO: ApiInfo<RedisApi, ZrangebyscoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrangebyscore,
    "Returns members in a sorted set within a range of scores",
    ReqType::Read,
    true,
);

#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrangebyscoreInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    with_scores: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<Limit>,
}

impl ZrangebyscoreInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
            with_scores: None,
            limit: None,
        }
    }

    pub fn with_scores(mut self) -> Self {
        self.with_scores = Some(true);
        self
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit { offset: offset.into(), count: count.into() });
        self
    }
}

impl Serialize for ZrangebyscoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.with_scores.is_some() {
            fields += 1;
        }
        if self.limit.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZrangebyscoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
        if let Some(with_scores) = &self.with_scores {
            state.serialize_field("with_scores", with_scores)?;
        }
        if let Some(limit) = &self.limit {
            state.serialize_field("limit", limit)?;
        }
        state.end()
    }
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Limit {
    offset: RedisJsonValue,
    count: RedisJsonValue,
}

impl_redis_operation!(ZrangebyscoreInput, API_INFO, {key, min, max, with_scores, limit});

impl RedisCommandInput for ZrangebyscoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.min).arg(&self.max);

        // WITHSCORES must come before LIMIT per Redis documentation
        if self.with_scores == Some(true) {
            command.arg("WITHSCORES");
        }
        if let Some(limit) = &self.limit {
            command.arg("LIMIT").arg(&limit.offset).arg(&limit.count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request("ZRANGEBYSCORE requires at least 3 arguments"));
        }
        let key = args[0].clone().try_into()?;
        let min = args[1].clone();
        let max = args[2].clone();
        let mut with_scores = None;
        let mut limit = None;

        let mut i = 3;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "WITHSCORES" => {
                        with_scores = Some(true);
                        i += 1;
                    }
                    "LIMIT" if i + 2 < args.len() => {
                        limit = Some(Limit { offset: args[i + 1].clone(), count: args[i + 2].clone() });
                        i += 3;
                    }
                    _ => i += 1,
                }
            } else {
                i += 1;
            }
        }
        Ok(Self { key, min, max, with_scores, limit })
    }
}

#[derive(Debug, Clone)]
pub struct ZrangebyscoreOutput(Vec<RedisJsonValue>);

impl ZrangebyscoreOutput {
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
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
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
            _ => return Err(EpError::parse("ZRANGEBYSCORE must return array")),
        };
        Ok(Self(elements))
    }
}

#[derive(Debug, Clone)]
pub struct ZrangebyscoreWithScoresOutput(Vec<Scores>);

impl ZrangebyscoreWithScoresOutput {
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
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut elements = Vec::new();
                let mut iter = arr.into_iter();
                while let Some(member_frame) = iter.next() {
                    let score_frame = iter.next().ok_or_else(|| EpError::parse("missing score"))?;
                    let member: RedisJsonValue = member_frame.try_into()?;
                    let score: RedisJsonValue = score_frame.try_into()?;
                    elements.push(ScoresBuilder::default().member(member).score(score).build().map_err(EpError::parse)?);
                }
                Ok(Self(elements))
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut elements = Vec::new();

                // Check if this is a flat array (RESP2 parsed as RESP3) or nested array (true RESP3)
                if !data.is_empty() && !matches!(data[0], Resp3Frame::Array { .. }) {
                    // Flat array: [member1, score1, member2, score2, ...]
                    let mut iter = data.into_iter();
                    while let Some(member_frame) = iter.next() {
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("missing score"))?;
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
                            _ => return Err(EpError::parse("invalid response format")),
                        }
                    }
                }
                Ok(Self(elements))
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            _ => Err(EpError::parse("ZRANGEBYSCORE must return array")),
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
            let input = ZrangebyscoreInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            );
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("ZRANGEBYSCORE"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZrangebyscoreInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("0".into()),
                RedisJsonValue::String("100".into()),
            )
            .with_scores();
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("WITHSCORES"));
        }

        #[test]
        fn test_encode_command_order() {
            // WITHSCORES should come before LIMIT
            let input = ZrangebyscoreInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("0".into()),
                RedisJsonValue::String("100".into()),
            )
            .with_scores()
            .with_limit(RedisJsonValue::Integer(0), RedisJsonValue::Integer(10));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            let withscores_pos = cmd_str.find("WITHSCORES").unwrap();
            let limit_pos = cmd_str.find("LIMIT").unwrap();
            assert!(withscores_pos < limit_pos, "WITHSCORES must come before LIMIT");
        }

        #[test]
        fn test_decode_output() {
            let output = ZrangebyscoreOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrangebyscoreOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_with_scores() {
            let output = ZrangebyscoreWithScoresOutput::decode(b"*4\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_error() {
            let err = ZrangebyscoreOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            ];
            let input = ZrangebyscoreInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("0".into())];
            let err = ZrangebyscoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrangebyscoreInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-inf".into()),
                RedisJsonValue::String("+inf".into()),
            );
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
        async fn test_zrangebyscore_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nzrangebyscore_basic\r\n").await.expect("raw failed");
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$19\r\nzrangebyscore_basic\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZrangebyscoreInput::new(RedisKey::String("zrangebyscore_basic".into()), RedisJsonValue::String("1".into()), RedisJsonValue::String("2".into())).command()).await.expect("raw failed");
                    let output = ZrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            }).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangebyscore_with_scores() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$20\r\nzrangebyscore_scores\r\n").await.expect("raw failed");
                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$20\r\nzrangebyscore_scores\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZrangebyscoreInput::new(
                                RedisKey::String("zrangebyscore_scores".into()),
                                RedisJsonValue::String("-inf".into()),
                                RedisJsonValue::String("+inf".into()),
                            )
                            .with_scores()
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangebyscoreWithScoresOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangebyscore_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nzrangebyscore_empty\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangebyscoreInput::new(
                                RedisKey::String("zrangebyscore_empty".into()),
                                RedisJsonValue::String("-inf".into()),
                                RedisJsonValue::String("+inf".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangebyscoreOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangebyscore_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$19\r\nzrangebyscore_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangebyscoreInput::new(
                                RedisKey::String("zrangebyscore_wrong".into()),
                                RedisJsonValue::String("-inf".into()),
                                RedisJsonValue::String("+inf".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let err = ZrangebyscoreOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
