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

const API_INFO: ApiInfo<RedisApi, ZrevrangeInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrevrange,
    "Returns members in a sorted set within a range of indexes in reverse order",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ZREVRANGE`
/// https://redis.io/docs/latest/commands/zrevrange/
///
/// Note: ZREVRANGE is deprecated as of Redis 6.2.0, use ZRANGE with REV instead.
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrevrangeInput {
    key: RedisKey,
    start: RedisJsonValue,
    stop: RedisJsonValue,
    withscores: Option<bool>,
}

impl ZrevrangeInput {
    pub fn new(key: impl Into<RedisKey>, start: impl Into<RedisJsonValue>, stop: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            start: start.into(),
            stop: stop.into(),
            withscores: None,
        }
    }

    pub fn with_scores(mut self) -> Self {
        self.withscores = Some(true);
        self
    }
}

impl Serialize for ZrevrangeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.withscores.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZrevrangeInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("start", &self.start)?;
        state.serialize_field("stop", &self.stop)?;
        if let Some(withscores) = self.withscores {
            state.serialize_field("withscores", &withscores)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZrevrangeInput,
    API_INFO,
    {key, start, stop, withscores }
);

impl RedisCommandInput for ZrevrangeInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.start).arg(&self.stop);

        if self.withscores == Some(true) {
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
            return Err(EpError::request(format!("ZREVRANGE requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let start = args[1].clone();
        let stop = args[2].clone();
        let mut withscores = None;

        if args.len() >= 4
            && let RedisJsonValue::String(s) = &args[3]
            && s.to_uppercase() == "WITHSCORES"
        {
            withscores = Some(true);
        }

        Ok(Self { key, start, stop, withscores })
    }
}

/// Entry in ZREVRANGE result with member and optional score
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, ToSchema, JsonSchema)]
pub struct ZrevrangeEntry {
    pub member: String,
    pub score: Option<f64>,
}

/// Output for Redis ZREVRANGE command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZrevrangeOutput {
    entries: Vec<ZrevrangeEntry>,
}

impl ZrevrangeOutput {
    pub fn new(entries: Vec<ZrevrangeEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[ZrevrangeEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn members(&self) -> Vec<&str> {
        self.entries.iter().map(|e| e.member.as_str()).collect()
    }

    pub fn decode(bytes: &[u8], with_scores: bool) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let entries = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_resp2_array(&arr, with_scores)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_resp3_array(&data, with_scores)?,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected array response")),
        };

        Ok(Self { entries })
    }

    fn parse_resp2_array(arr: &[Resp2Frame], with_scores: bool) -> Result<Vec<ZrevrangeEntry>, EpError> {
        let mut entries = Vec::new();

        if with_scores {
            let mut i = 0;
            while i + 1 < arr.len() {
                let member = match &arr[i] {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                let score = match &arr[i + 1] {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec())
                        .map_err(EpError::parse)?
                        .parse::<f64>()
                        .map_err(|_| EpError::parse("score must be numeric"))?,
                    Resp2Frame::Integer(n) => *n as f64,
                    _ => return Err(EpError::parse("expected numeric score")),
                };
                entries.push(ZrevrangeEntry { member, score: Some(score) });
                i += 2;
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZrevrangeEntry { member, score: None });
            }
        }

        Ok(entries)
    }

    fn parse_resp3_array(arr: &[Resp3Frame], with_scores: bool) -> Result<Vec<ZrevrangeEntry>, EpError> {
        let mut entries = Vec::new();
        if with_scores {
            // Check if it's nested arrays (true RESP3) or flat array (RESP2 fallback)
            let is_nested = arr.first().is_some_and(|f| matches!(f, Resp3Frame::Array { .. }));

            if is_nested {
                // RESP3 nested: [[member1, score1], [member2, score2], ...]
                for entry_frame in arr {
                    match entry_frame {
                        Resp3Frame::Array { data, .. } => {
                            if data.len() != 2 {
                                return Err(EpError::parse("ZREVRANGE entry must have 2 elements"));
                            }

                            let member = match &data[0] {
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                                _ => return Err(EpError::parse("expected string member")),
                            };

                            let score = match &data[1] {
                                Resp3Frame::Double { data, .. } => *data,
                                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                                    .map_err(EpError::parse)?
                                    .parse::<f64>()
                                    .map_err(|_| EpError::parse("score must be numeric"))?,
                                Resp3Frame::Number { data, .. } => *data as f64,
                                _ => return Err(EpError::parse("expected numeric score")),
                            };

                            entries.push(ZrevrangeEntry { member, score: Some(score) });
                        }
                        _ => {
                            return Err(EpError::parse("expected nested array for ZREVRANGE entry"));
                        }
                    }
                }
            } else {
                // Flat array (RESP2 compatibility): [member1, score1, member2, score2, ...]
                let mut i = 0;
                while i + 1 < arr.len() {
                    let member = match &arr[i] {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected string member")),
                    };
                    let score = match &arr[i + 1] {
                        Resp3Frame::Double { data, .. } => *data,
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec())
                            .map_err(EpError::parse)?
                            .parse::<f64>()
                            .map_err(|_| EpError::parse("score must be numeric"))?,
                        Resp3Frame::Number { data, .. } => *data as f64,
                        _ => return Err(EpError::parse("expected numeric score")),
                    };
                    entries.push(ZrevrangeEntry { member, score: Some(score) });
                    i += 2;
                }
            }
        } else {
            for frame in arr {
                let member = match frame {
                    Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                    _ => return Err(EpError::parse("expected string member")),
                };
                entries.push(ZrevrangeEntry { member, score: None });
            }
        }
        Ok(entries)
    }
}

impl Serialize for ZrevrangeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZrevrangeOutput", 1)?;
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
        fn test_encode_command() {
            let input = ZrevrangeInput::new(RedisKey::String("myzset".into()), 0, -1);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ZREVRANGE"));
            assert!(cmd_str.contains("myzset"));
        }

        #[test]
        fn test_encode_command_with_scores() {
            let input = ZrevrangeInput::new(RedisKey::String("myzset".into()), 0, 2).with_scores();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("WITHSCORES"));
        }

        #[test]
        fn test_decode_output_without_scores() {
            let output = ZrevrangeOutput::decode(b"*3\r\n$1\r\nc\r\n$1\r\nb\r\n$1\r\na\r\n", false).unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.members(), vec!["c", "b", "a"]);
        }

        #[test]
        fn test_decode_output_with_scores() {
            let output = ZrevrangeOutput::decode(b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n", true).unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.entries()[0].score, Some(2.0));
            assert_eq!(output.entries()[1].score, Some(1.0));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrevrangeOutput::decode(b"*0\r\n", false).unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZrevrangeOutput::decode(b"-WRONGTYPE Operation\r\n", false).unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
            ];
            let input = ZrevrangeInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_with_withscores() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(-1),
                RedisJsonValue::String("WITHSCORES".into()),
            ];
            let input = ZrevrangeInput::decode(args).unwrap();
            assert_eq!(input.withscores, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(0)];
            let err = ZrevrangeInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrevrangeInput::new(RedisKey::String("myzset".into()), 0, -1);
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
        async fn test_zrevrange_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrevrange_basic\r\n").await.expect("raw failed");

                    // ZADD: a=1, b=2, c=3
                    ctx.raw(
                        b"*8\r\n$4\r\nZADD\r\n$15\r\nzrevrange_basic\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n",
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangeInput::new(RedisKey::String("zrevrange_basic".into()), 0, -1).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangeOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    // Reverse order: c, b, a
                    assert_eq!(output.members(), vec!["c", "b", "a"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrange_with_scores() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzrevrange_scores\r\n").await.expect("raw failed");

                    ctx.raw(b"*6\r\n$4\r\nZADD\r\n$16\r\nzrevrange_scores\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangeInput::new(RedisKey::String("zrevrange_scores".into()), 0, -1).with_scores().command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangeOutput::decode(&result, true).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.entries()[0].member, "b");
                    assert_eq!(output.entries()[0].score, Some(2.0));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrange_subset() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nzrevrange_subset\r\n")
                        .await
                        .expect("raw failed");

                    // Add 5 elements
                    ctx.raw(b"*12\r\n$4\r\nZADD\r\n$16\r\nzrevrange_subset\r\n$1\r\n1\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\nb\r\n$1\r\n3\r\n$1\r\nc\r\n$1\r\n4\r\n$1\r\nd\r\n$1\r\n5\r\n$1\r\ne\r\n")
                        .await
                        .expect("raw failed");

                    // Get top 2 (indices 0-1 in reverse)
                    let result = ctx
                        .raw(&ZrevrangeInput::new(
                            RedisKey::String("zrevrange_subset".into()),
                            0,
                            1,
                        ).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangeOutput::decode(&result, false).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.members(), vec!["e", "d"]);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrange_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nzrevrange_empty\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangeInput::new(RedisKey::String("zrevrange_empty".into()), 0, -1).command())
                        .await
                        .expect("raw failed");

                    let output = ZrevrangeOutput::decode(&result, false).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrevrange_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\nzrevrange_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&ZrevrangeInput::new(RedisKey::String("zrevrange_wrong".into()), 0, -1).command())
                        .await
                        .expect("raw failed");

                    let err = ZrevrangeOutput::decode(&result, false).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
