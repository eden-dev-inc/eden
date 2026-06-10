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

const API_INFO: ApiInfo<RedisApi, LlenInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Llen, "Returns the length of a list", ReqType::Read, true);

/// See official Redis documentation for `LLEN`
/// https://redis.io/docs/latest/commands/llen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LlenInput {
    pub(crate) key: RedisKey,
}

impl Serialize for LlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LlenInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(LlenInput, API_INFO, { key });

impl RedisCommandInput for LlenInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("LLEN requires 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis LLEN command
///
/// Returns the length of the list, or 0 if key doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LlenOutput {
    /// The length of the list
    length: i64,
}

impl LlenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the length of the list
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the list is empty or doesn't exist
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into a LlenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(Self { length: n }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LLEN response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { length: data }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LLEN response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LlenOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = LlenInput { key: RedisKey::String("mylist".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nLLEN\r\n$6\r\nmylist\r\n");
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mylist".into())];
            let input = LlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = LlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = LlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = LlenOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.length(), 5);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = LlenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LlenOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LlenInput { key: RedisKey::String("mylist".into()) };
            assert_eq!(input.keys().len(), 1);
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
        async fn test_llen_empty_list() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&LlenInput { key: RedisKey::String("nonexistent_llen".into()) }.command()).await.expect("raw failed");

                    let output = LlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_llen_with_elements() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("llen_list".into()),
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

                    let result = ctx.raw(&LlenInput { key: RedisKey::String("llen_list".into()) }.command()).await.expect("raw failed");

                    let output = LlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_llen_after_operations() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Push elements
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("llen_ops".into()),
                            elements: vec![RedisJsonValue::String("1".into()), RedisJsonValue::String("2".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result1 = ctx.raw(&LlenInput { key: RedisKey::String("llen_ops".into()) }.command()).await.expect("raw failed");

                    let output1 = LlenOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.length(), 2);

                    // Push more
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("llen_ops".into()),
                            elements: vec![RedisJsonValue::String("3".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result2 = ctx.raw(&LlenInput { key: RedisKey::String("llen_ops".into()) }.command()).await.expect("raw failed");

                    let output2 = LlenOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.length(), 3);
                })
            })
            .await;
        }
    }
}
