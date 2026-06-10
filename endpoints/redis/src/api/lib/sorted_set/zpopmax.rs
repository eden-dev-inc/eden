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

const API_INFO: ApiInfo<RedisApi, ZpopmaxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zpopmax,
    "Returns the highest-scoring members from a sorted set after removing them. Deletes the sorted set if the last member was popped",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `ZPOPMAX`
/// https://redis.io/docs/latest/commands/zpopmax/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZpopmaxInput {
    key: RedisKey,
    count: Option<RedisJsonValue>,
}

impl ZpopmaxInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into(), count: None }
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for ZpopmaxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.count.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZpopmaxInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    ZpopmaxInput,
    API_INFO,
    { key, count }
);

impl RedisCommandInput for ZpopmaxInput {
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
            command.arg(count);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request(format!("ZPOPMAX requires at least 1 argument, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            count: args.get(1).cloned(),
        })
    }
}

/// Output for Redis ZPOPMAX command
///
/// Returns an array of members with their scores (highest score first),
/// or an empty array if the sorted set is empty or doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ZpopmaxOutput {
    elements: Vec<Scores>,
}

impl ZpopmaxOutput {
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
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame for ZpopmaxOutput"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut elements = Vec::new();
                // RESP2 returns flat array: [member1, score1, member2, score2, ...]
                let mut iter = arr.into_iter();
                while let Some(member_frame) = iter.next() {
                    let score_frame = iter.next().ok_or_else(|| EpError::parse("ZPOPMAX missing score for member"))?;

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
                        let score_frame = iter.next().ok_or_else(|| EpError::parse("ZPOPMAX missing score for member"))?;

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
                                return Err(EpError::parse("ZPOPMAX element must be [member, score] array"));
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
            _ => Err(EpError::parse("ZPOPMAX must return an array")),
        }
    }
}

impl Serialize for ZpopmaxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ZpopmaxOutput", 1)?;
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
            let input = ZpopmaxInput::new(RedisKey::String("myzset".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$7\r\nZPOPMAX\r\n$6\r\nmyzset\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = ZpopmaxInput::new(RedisKey::String("myzset".into())).with_count(RedisJsonValue::Integer(3));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$7\r\nZPOPMAX\r\n$6\r\nmyzset\r\n$1\r\n3\r\n");
        }

        #[test]
        fn test_decode_output_single_element() {
            // RESP2 flat array: [member, score]
            let output = ZpopmaxOutput::decode(b"*2\r\n$6\r\nmember\r\n$3\r\n1.5\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_multiple_elements() {
            // RESP2 flat array: [member1, score1, member2, score2]
            let output = ZpopmaxOutput::decode(b"*4\r\n$1\r\nb\r\n$1\r\n2\r\n$1\r\na\r\n$1\r\n1\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZpopmaxOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = ZpopmaxOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myzset".into())];
            let input = ZpopmaxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::Integer(5)];
            let input = ZpopmaxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ZpopmaxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZpopmaxInput::new(RedisKey::String("myzset".into()));
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myzset".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_single() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nzpopmax_single\r\n")
                        .await
                        .expect("raw failed");

                    // ZADD zpopmax_single 1 one 2 two 3 three
                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$14\r\nzpopmax_single\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&ZpopmaxInput::new(RedisKey::String("zpopmax_single".into())).command())
                        .await
                        .expect("raw failed");

                    let output = ZpopmaxOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    // Should pop "three" with score 3 (highest)
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzpopmax_count\r\n")
                        .await
                        .expect("raw failed");

                    ctx.raw(b"*8\r\n$4\r\nZADD\r\n$13\r\nzpopmax_count\r\n$1\r\n1\r\n$3\r\none\r\n$1\r\n2\r\n$3\r\ntwo\r\n$1\r\n3\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &ZpopmaxInput::new(RedisKey::String("zpopmax_count".into()))
                                .with_count(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ZpopmaxOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nzpopmax_empty\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZpopmaxInput::new(RedisKey::String("zpopmax_empty".into())).command()).await.expect("raw failed");

                    let output = ZpopmaxOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$13\r\nzpopmax_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZpopmaxInput::new(RedisKey::String("zpopmax_wrong".into())).command()).await.expect("raw failed");

                    let err = ZpopmaxOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzpopmax_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzpopmax_r2\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZpopmaxInput::new(RedisKey::String("zpopmax_r2".into())).command()).await.expect("raw failed");

            // RESP2 returns flat array
            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ZpopmaxOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zpopmax_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nzpopmax_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nZADD\r\n$10\r\nzpopmax_r3\r\n$1\r\n5\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ZpopmaxInput::new(RedisKey::String("zpopmax_r3".into())).command()).await.expect("raw failed");

            let output = ZpopmaxOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }
    }
}
