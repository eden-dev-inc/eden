use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSuglenInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSuglen,
    "Gets the size of an auto-complete suggestion dictionary",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.SUGLEN`
/// https://redis.io/docs/latest/commands/ft.suglen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, PartialEq)]
pub struct FtSuglenInput {
    pub(crate) key: RedisKey,
}

impl Serialize for FtSuglenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSuglenInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(FtSuglenInput, API_INFO, { key });

impl RedisCommandInput for FtSuglenInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("FT.SUGLEN requires 1 argument, given none"));
        }

        Ok(FtSuglenInput { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis FT.SUGLEN command
///
/// Returns the number of entries in the suggestion dictionary.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSuglenOutput {
    /// The number of entries in the suggestion dictionary
    length: i64,
}

impl FtSuglenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the length (number of entries)
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Decode the Redis protocol response into a FtSuglenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGLEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for FtSuglenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FtSuglenOutput", 1)?;
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
            let input = FtSuglenInput { key: RedisKey::String("mydict".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$9\r\nFT.SUGLEN\r\n$6\r\nmydict\r\n");
        }

        #[test]
        fn test_decode_integer_resp2() {
            let output = FtSuglenOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.length(), 42);
        }

        #[test]
        fn test_decode_zero() {
            let output = FtSuglenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FtSuglenOutput::decode(b"-ERR unknown key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mydict".into())];
            let input = FtSuglenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mydict".into()));
        }

        #[test]
        fn test_decode_input_empty_args_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtSuglenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = FtSuglenInput { key: RedisKey::String("testkey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::{api::FtSugaddInput, test_utils::*};
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_suglen_empty_dict() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtSuglenInput { key: RedisKey::String("empty_sug".into()) }.command()).await.expect("raw failed");

                    let output = FtSuglenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_suglen_after_sugadd() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add some suggestions
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("sug".into()),
                            string: RedisJsonValue::String("world".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&FtSuglenInput { key: RedisKey::String("sug".into()) }.command()).await.expect("raw failed");

                    let output = FtSuglenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_suglen_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp2_sug".into()),
                    string: RedisJsonValue::String("test".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&FtSuglenInput { key: RedisKey::String("resp2_sug".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 integer format");
            let output = FtSuglenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_suglen_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            ctx.raw(
                &FtSugaddInput {
                    key: RedisKey::String("resp3_sug".into()),
                    string: RedisJsonValue::String("test".into()),
                    score: RedisJsonValue::Float(1.0),
                    incr: None,
                    payload: None,
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&FtSuglenInput { key: RedisKey::String("resp3_sug".into()) }.command()).await.expect("raw failed");

            let output = FtSuglenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);
            ctx.stop().await;
        }
    }
}
