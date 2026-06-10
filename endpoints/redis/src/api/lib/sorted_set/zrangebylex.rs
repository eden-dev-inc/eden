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

const API_INFO: ApiInfo<RedisApi, ZrangebylexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Zrangebylex,
    "Returns members in a sorted set within a lexicographical range",
    ReqType::Read,
    true,
);

#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ZrangebylexInput {
    key: RedisKey,
    min: RedisJsonValue,
    max: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    limit: Option<Limit>,
}

impl ZrangebylexInput {
    pub fn new(key: impl Into<RedisKey>, min: impl Into<RedisJsonValue>, max: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            min: min.into(),
            max: max.into(),
            limit: None,
        }
    }

    pub fn with_limit(mut self, offset: impl Into<RedisJsonValue>, count: impl Into<RedisJsonValue>) -> Self {
        self.limit = Some(Limit { offset: offset.into(), count: count.into() });
        self
    }
}

impl Serialize for ZrangebylexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.limit.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("ZrangebylexInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("min", &self.min)?;
        state.serialize_field("max", &self.max)?;
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

impl_redis_operation!(ZrangebylexInput, API_INFO, {key, min, max, limit});

impl RedisCommandInput for ZrangebylexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.min).arg(&self.max);
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
            return Err(EpError::request(format!("ZRANGEBYLEX requires at least 3 arguments, given {}", args.len())));
        }
        let key = args[0].clone().try_into()?;
        let min = args[1].clone();
        let max = args[2].clone();
        let mut limit = None;
        if args.len() >= 6
            && let RedisJsonValue::String(s) = &args[3]
            && s.to_uppercase() == "LIMIT"
        {
            limit = Some(Limit { offset: args[4].clone(), count: args[5].clone() });
        }
        Ok(Self { key, min, max, limit })
    }
}

#[derive(Debug, Clone)]
pub struct ZrangebylexOutput(Vec<RedisJsonValue>);

impl ZrangebylexOutput {
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
            _ => return Err(EpError::parse("ZRANGEBYLEX must return array")),
        };
        Ok(Self(elements))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = ZrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("[a".into()),
                RedisJsonValue::String("[z".into()),
            );
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("ZRANGEBYLEX"));
        }

        #[test]
        fn test_encode_command_with_limit() {
            let input = ZrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
            )
            .with_limit(RedisJsonValue::Integer(0), RedisJsonValue::Integer(10));
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("LIMIT"));
        }

        #[test]
        fn test_decode_output() {
            let output = ZrangebylexOutput::decode(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").unwrap();
            assert_eq!(output.len(), 3);
        }

        #[test]
        fn test_decode_output_empty() {
            let output = ZrangebylexOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ZrangebylexOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myzset".into()),
                RedisJsonValue::String("[a".into()),
                RedisJsonValue::String("[z".into()),
            ];
            let input = ZrangebylexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myzset".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myzset".into()), RedisJsonValue::String("[a".into())];
            let err = ZrangebylexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ZrangebylexInput::new(
                RedisKey::String("myzset".into()),
                RedisJsonValue::String("-".into()),
                RedisJsonValue::String("+".into()),
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
        async fn test_zrangebylex_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangebylex_basic\r\n").await.expect("raw failed");
                    ctx.raw(b"*10\r\n$4\r\nZADD\r\n$17\r\nzrangebylex_basic\r\n$1\r\n0\r\n$1\r\na\r\n$1\r\n0\r\n$1\r\nb\r\n$1\r\n0\r\n$1\r\nc\r\n$1\r\n0\r\n$1\r\nd\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ZrangebylexInput::new(RedisKey::String("zrangebylex_basic".into()), RedisJsonValue::String("[a".into()), RedisJsonValue::String("[c".into())).command()).await.expect("raw failed");
                    let output = ZrangebylexOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                })
            }).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangebylex_empty() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nzrangebylex_empty\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangebylexInput::new(
                                RedisKey::String("zrangebylex_empty".into()),
                                RedisJsonValue::String("-".into()),
                                RedisJsonValue::String("+".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output = ZrangebylexOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_zrangebylex_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nzrangebylex_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");
                    let result = ctx
                        .raw(
                            &ZrangebylexInput::new(
                                RedisKey::String("zrangebylex_wrong".into()),
                                RedisJsonValue::String("-".into()),
                                RedisJsonValue::String("+".into()),
                            )
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let err = ZrangebylexOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }
    }
}
