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

const API_INFO: ApiInfo<RedisApi, LposInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Lpos,
    "Returns the index of matching elements in a list",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `LPOS`
/// https://redis.io/docs/latest/commands/lpos/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LposInput {
    pub(crate) key: RedisKey,
    pub(crate) element: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) rank: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) max_len: Option<RedisJsonValue>,
}

impl Serialize for LposInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;
        if self.rank.is_some() {
            fields += 1;
        }
        if self.count.is_some() {
            fields += 1;
        }
        if self.max_len.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("LposInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        if let Some(rank) = &self.rank {
            state.serialize_field("rank", &rank)?;
        }
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        if let Some(max_len) = &self.max_len {
            state.serialize_field("max_len", max_len)?;
        }
        state.end()
    }
}

impl_redis_operation!(LposInput, API_INFO, { key, element, rank, count, max_len });

impl RedisCommandInput for LposInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.element);
        if let Some(rank) = &self.rank {
            command.arg("RANK").arg(rank);
        }
        if let Some(count) = &self.count {
            command.arg("COUNT").arg(count);
        }
        if let Some(max_len) = &self.max_len {
            command.arg("MAXLEN").arg(max_len);
        }
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("LPOS requires at least 2 arguments, given {}", args.len())));
        }
        let key = args[0].clone().try_into()?;
        let element = args[1].clone();
        let mut rank = None;
        let mut count = None;
        let mut max_len = None;
        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(cmd) = &args[i] {
                match cmd.to_uppercase().as_str() {
                    "RANK" if i + 1 < args.len() => {
                        rank = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "COUNT" if i + 1 < args.len() => {
                        count = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "MAXLEN" if i + 1 < args.len() => {
                        max_len = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => return Err(EpError::parse(format!("Unknown option: {}", cmd))),
                }
            } else {
                return Err(EpError::parse("Expected string option"));
            }
        }
        Ok(Self { key, element, rank, count, max_len })
    }
}

/// Output for Redis LPOS command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LposOutput {
    /// Single index when count not specified
    index: Option<i64>,
    /// Array of indices when count is specified
    indices: Vec<i64>,
}

impl LposOutput {
    pub fn new_single(index: Option<i64>) -> Self {
        Self { index, indices: vec![] }
    }
    pub fn new_multi(indices: Vec<i64>) -> Self {
        Self { index: None, indices }
    }
    pub fn index(&self) -> Option<i64> {
        self.index
    }
    pub fn indices(&self) -> &[i64] {
        &self.indices
    }
    pub fn not_found(&self) -> bool {
        self.index.is_none() && self.indices.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { index: None, indices: vec![] }),
                Resp2Frame::Integer(n) => Ok(Self { index: Some(n), indices: vec![] }),
                Resp2Frame::Array(arr) => {
                    let indices: Result<Vec<i64>, _> = arr
                        .into_iter()
                        .map(|f| {
                            if let Resp2Frame::Integer(n) = f {
                                Ok(n)
                            } else {
                                Err(EpError::parse("expected integer in array"))
                            }
                        })
                        .collect();
                    Ok(Self { index: None, indices: indices? })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LPOS response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { index: None, indices: vec![] }),
                Resp3Frame::Number { data, .. } => Ok(Self { index: Some(data), indices: vec![] }),
                Resp3Frame::Array { data, .. } => {
                    let indices: Result<Vec<i64>, _> = data
                        .into_iter()
                        .map(|f| {
                            if let Resp3Frame::Number { data: n, .. } = f {
                                Ok(n)
                            } else {
                                Err(EpError::parse("expected integer in array"))
                            }
                        })
                        .collect();
                    Ok(Self { index: None, indices: indices? })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LPOS response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LposOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LposOutput", 2)?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("indices", &self.indices)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_simple() {
            let input = LposInput {
                key: RedisKey::String("mylist".into()),
                element: RedisJsonValue::String("value".into()),
                rank: None,
                count: None,
                max_len: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$4\r\nLPOS\r\n"));
        }

        #[test]
        fn test_encode_command_with_options() {
            let input = LposInput {
                key: RedisKey::String("mylist".into()),
                element: RedisJsonValue::String("value".into()),
                rank: Some(RedisJsonValue::Integer(1)),
                count: Some(RedisJsonValue::Integer(2)),
                max_len: Some(RedisJsonValue::Integer(100)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RANK"));
            assert!(cmd_str.contains("COUNT"));
            assert!(cmd_str.contains("MAXLEN"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::String("elem".into())];
            let input = LposInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_output_single_index() {
            let output = LposOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.index(), Some(5));
            assert!(!output.not_found());
        }

        #[test]
        fn test_decode_output_null() {
            let output = LposOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.not_found());
        }

        #[test]
        fn test_decode_output_array() {
            let output = LposOutput::decode(b"*3\r\n:0\r\n:2\r\n:4\r\n").unwrap();
            assert_eq!(output.indices(), &[0, 2, 4]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpos_found() {
            test_all_protocols_min_version("6.0.6", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lpos_list".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LposInput {
                                key: RedisKey::String("lpos_list".into()),
                                element: RedisJsonValue::String("b".into()),
                                rank: None,
                                count: None,
                                max_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("lpos failed");

                    let output = LposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.index(), Some(1));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpos_not_found() {
            test_all_protocols_min_version("6.0.6", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lpos_notfound".into()),
                            elements: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LposInput {
                                key: RedisKey::String("lpos_notfound".into()),
                                element: RedisJsonValue::String("z".into()),
                                rank: None,
                                count: None,
                                max_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("lpos failed");

                    let output = LposOutput::decode(&result).expect("decode failed");
                    assert!(output.not_found());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_lpos_with_count() {
            test_all_protocols_min_version("6.0.6", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("lpos_count".into()),
                            elements: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("c".into()),
                                RedisJsonValue::String("a".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LposInput {
                                key: RedisKey::String("lpos_count".into()),
                                element: RedisJsonValue::String("a".into()),
                                rank: None,
                                count: Some(RedisJsonValue::Integer(0)), // 0 means all matches
                                max_len: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("lpos failed");

                    let output = LposOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.indices(), &[0, 2, 4]);
                })
            })
            .await;
        }
    }
}
