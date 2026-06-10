use crate::api::RedisCommandOutput;
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

const API_INFO: ApiInfo<RedisApi, HexistsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hexists, "Determines whether a field exists in a hash", ReqType::Read, true);

/// See official Redis documentation for `HEXISTS`
/// https://redis.io/docs/latest/commands/hexists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HexistsInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
}

impl Serialize for HexistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HexistsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.end()
    }
}

impl_redis_operation!(
    HexistsInput,
    API_INFO,
    {key, field}
);

impl RedisCommandInput for HexistsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.field);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("HEXISTS requires exactly 2 arguments, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()?, field: args[1].clone() })
    }
}

/// Output for Redis HEXISTS command
///
/// Returns whether the field exists in the hash.
/// See official Redis documentation for `HEXISTS`
/// https://redis.io/docs/latest/commands/hexists/
#[derive(Debug, Clone)]
pub struct HexistsOutput {
    exists: bool,
}

impl HexistsOutput {
    pub fn new(exists: bool) -> Self {
        Self { exists }
    }

    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Decode the Redis protocol response into a HexistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let exists = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i == 1,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HEXISTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data == 1,
                Resp3Frame::Boolean { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HEXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { exists })
    }
}

impl RedisCommandOutput for HexistsOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Hexists
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        Self::decode(bytes)
    }
}

impl Serialize for HexistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HexistsOutput", 1)?;
        state.serialize_field("exists", &self.exists)?;
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
            let input = HexistsInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$7\r\nHEXISTS\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n");
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = HexistsOutput::decode(b":0\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_integer_one() {
            let output = HexistsOutput::decode(b":1\r\n").unwrap();
            assert!(output.exists());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HexistsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HexistsInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("f".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("field1".into())];
            let input = HexistsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = HexistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("f1".into()),
                RedisJsonValue::String("f2".into()),
            ];
            let err = HexistsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::HsetInput;
        use crate::api::lib::hash::Field;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexists_field_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhexists_test1\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexists_test1".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HexistsInput {
                                key: RedisKey::String("hexists_test1".into()),
                                field: RedisJsonValue::String("field1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexistsOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexists_field_not_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhexists_test2\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hexists_test2".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("field1".into()),
                                RedisJsonValue::String("value1".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &HexistsInput {
                                key: RedisKey::String("hexists_test2".into()),
                                field: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexistsOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexists_key_not_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhexists_test3\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HexistsInput {
                                key: RedisKey::String("hexists_test3".into()),
                                field: RedisJsonValue::String("anyfield".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HexistsOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexists_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhexists_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hexists_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HexistsInput {
                        key: RedisKey::String("hexists_r2".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HexistsOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hexists_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhexists_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hexists_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HexistsInput {
                        key: RedisKey::String("hexists_r3".into()),
                        field: RedisJsonValue::String("f".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HexistsOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }
    }
}
