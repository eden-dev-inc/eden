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

const API_INFO: ApiInfo<RedisApi, SpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Spop,
    "Returns one or more random members from a set after removing them. Deletes the set if the last member was popped",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SPOP`
/// https://redis.io/docs/latest/commands/spop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SpopInput {
    pub(crate) key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
}

impl Serialize for SpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.count.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("SpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(SpopInput, API_INFO, { key, count });

impl RedisCommandInput for SpopInput {
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
            return Err(EpError::request("SPOP requires at least 1 argument"));
        }
        let key = args[0].clone().try_into()?;
        let count = args.get(1).cloned();
        Ok(Self { key, count })
    }
}

/// Output for Redis SPOP command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SpopOutput {
    /// Single value when count not specified, or array of values when count specified
    value: Option<RedisJsonValue>,
    elements: Vec<RedisJsonValue>,
}

impl SpopOutput {
    pub fn new_single(value: Option<RedisJsonValue>) -> Self {
        Self { value, elements: vec![] }
    }
    pub fn new_multi(elements: Vec<RedisJsonValue>) -> Self {
        Self { value: None, elements }
    }
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }
    pub fn elements(&self) -> &[RedisJsonValue] {
        &self.elements
    }
    pub fn is_empty(&self) -> bool {
        self.value.is_none() && self.elements.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { value: None, elements: vec![] }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)),
                    elements: vec![],
                }),
                Resp2Frame::Array(arr) => {
                    let elements: Result<Vec<_>, _> = arr.into_iter().map(RedisJsonValue::try_from).collect();
                    Ok(Self { value: None, elements: elements? })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SPOP response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { value: None, elements: vec![] }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    value: Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                    elements: vec![],
                }),
                Resp3Frame::Array { data, .. } => {
                    let elements: Result<Vec<_>, _> = data.into_iter().map(RedisJsonValue::try_from).collect();
                    Ok(Self { value: None, elements: elements? })
                }
                Resp3Frame::Set { data, .. } => {
                    let elements: Result<Vec<_>, _> = data.into_iter().map(RedisJsonValue::try_from).collect();
                    Ok(Self { value: None, elements: elements? })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SPOP response: {:?}", other))),
            },
        }
    }
}

impl Serialize for SpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SpopOutput", 2)?;
        state.serialize_field("value", &self.value)?;
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
        fn test_encode_command_without_count() {
            let input = SpopInput { key: RedisKey::String("myset".into()), count: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nSPOP\r\n$5\r\nmyset\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = SpopInput {
                key: RedisKey::String("myset".into()),
                count: Some(RedisJsonValue::Integer(3)),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$4\r\nSPOP\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myset".into())];
            let input = SpopInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("myset".into()), RedisJsonValue::Integer(5)];
            let input = SpopInput::decode(args).unwrap();
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_decode_output_single() {
            let output = SpopOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null() {
            let output = SpopOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_array() {
            let output = SpopOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.elements().len(), 2);
        }

        #[test]
        fn test_keys_accessor() {
            let input = SpopInput { key: RedisKey::String("myset".into()), count: None };
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::set::sadd::SaddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spop_single() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("spop_single".into()),
                            members: vec![
                                RedisJsonValue::String("first".into()),
                                RedisJsonValue::String("second".into()),
                                RedisJsonValue::String("third".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd failed");

                    let result = ctx
                        .raw(&SpopInput { key: RedisKey::String("spop_single".into()), count: None }.command())
                        .await
                        .expect("spop failed");

                    let output = SpopOutput::decode(&result).expect("decode failed");
                    assert!(output.value().is_some());
                    let value = output.value().unwrap();
                    assert!(matches!(
                        value,
                        RedisJsonValue::String(s) if s == "first" || s == "second" || s == "third"
                    ));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spop_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SaddInput {
                            key: RedisKey::String("spop_count".into()),
                            members: vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("sadd failed");

                    let result = ctx
                        .raw(
                            &SpopInput {
                                key: RedisKey::String("spop_count".into()),
                                count: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("spop failed");

                    let output = SpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.elements().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_spop_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SpopInput {
                                key: RedisKey::String("nonexistent_spop".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("spop failed");

                    let output = SpopOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
