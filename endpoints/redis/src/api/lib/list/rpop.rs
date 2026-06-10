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

const API_INFO: ApiInfo<RedisApi, RpopInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Rpop,
    "Returns and removes the last elements of a list. Deletes the list if the last element was popped",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RPOP`
/// https://redis.io/docs/latest/commands/rpop/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RpopInput {
    pub(crate) key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) count: Option<RedisJsonValue>,
}

impl Serialize for RpopInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.count.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("RpopInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(RpopInput, API_INFO, { key, count });

impl RedisCommandInput for RpopInput {
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
            return Err(EpError::request("RPOP requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let count = args.get(1).cloned();

        Ok(Self { key, count })
    }
}

/// Output for Redis RPOP command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RpopOutput {
    /// Single value when count is not specified, or array when count is specified
    values: Vec<RedisJsonValue>,
    /// True if the list was empty
    empty: bool,
}

impl RpopOutput {
    pub fn new(values: Vec<RedisJsonValue>) -> Self {
        let empty = values.is_empty();
        Self { values, empty }
    }

    pub fn values(&self) -> &[RedisJsonValue] {
        &self.values
    }

    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.values.first()
    }

    pub fn is_empty(&self) -> bool {
        self.empty
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Null => Ok(Self { values: vec![], empty: true }),
                Resp2Frame::BulkString(b) => Ok(Self {
                    values: vec![RedisJsonValue::from(String::from_utf8(b).map_err(EpError::parse)?)],
                    empty: false,
                }),
                Resp2Frame::Array(arr) => {
                    let values = arr.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                    let empty = values.is_empty();
                    Ok(Self { values, empty })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected RPOP response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Null => Ok(Self { values: vec![], empty: true }),
                Resp3Frame::BlobString { data, .. } => Ok(Self {
                    values: vec![RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)],
                    empty: false,
                }),
                Resp3Frame::Array { data, .. } => {
                    let values = data.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                    let empty = values.is_empty();
                    Ok(Self { values, empty })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected RPOP response: {:?}", other))),
            },
        }
    }
}

impl Serialize for RpopOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RpopOutput", 2)?;
        state.serialize_field("values", &self.values)?;
        state.serialize_field("empty", &self.empty)?;
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
            let input = RpopInput { key: RedisKey::String("mylist".into()), count: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nRPOP\r\n$6\r\nmylist\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = RpopInput {
                key: RedisKey::String("mylist".into()),
                count: Some(RedisJsonValue::Integer(3)),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$4\r\nRPOP\r\n"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let input = RpopInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::Integer(2)];
            let input = RpopInput::decode(args).unwrap();
            assert!(input.count.is_some());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = RpopInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1"));
        }

        #[test]
        fn test_decode_output_single_value() {
            let output = RpopOutput::decode(b"$5\r\nvalue\r\n").unwrap();
            assert!(!output.is_empty());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("value")));
        }

        #[test]
        fn test_decode_output_null() {
            let output = RpopOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_array() {
            let output = RpopOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.values().len(), 2);
        }

        #[test]
        fn test_decode_output_error() {
            let err = RpopOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
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
        async fn test_rpop_single() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rpop_list".into()),
                            elements: vec![RedisJsonValue::String("first".into()), RedisJsonValue::String("last".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result =
                        ctx.raw(&RpopInput { key: RedisKey::String("rpop_list".into()), count: None }.command()).await.expect("raw failed");

                    let output = RpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("last")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpop_with_count() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("rpop_count".into()),
                            elements: vec![
                                RedisJsonValue::String("1".into()),
                                RedisJsonValue::String("2".into()),
                                RedisJsonValue::String("3".into()),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &RpopInput {
                                key: RedisKey::String("rpop_count".into()),
                                count: Some(RedisJsonValue::Integer(2)),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpopOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rpop_empty_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &RpopInput {
                                key: RedisKey::String("nonexistent_rpop".into()),
                                count: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RpopOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }
    }
}
