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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, HgetallInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hgetall, "Returns all fields and values in a hash", ReqType::Read, true);

/// See official Redis documentation for `HGETALL`
/// https://redis.io/docs/latest/commands/hgetall/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HgetallInput {
    pub(crate) key: RedisKey,
}

impl Serialize for HgetallInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HgetallInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(HgetallInput, API_INFO, { key });

impl RedisCommandInput for HgetallInput {
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
            return Err(EpError::request(format!("HGETALL requires exactly 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis HGETALL command
///
/// Returns all field-value pairs in the hash.
/// See official Redis documentation for `HGETALL`
/// https://redis.io/docs/latest/commands/hgetall/
#[derive(Debug, Clone)]
pub struct HgetallOutput {
    fields: HashMap<String, RedisJsonValue>,
}

impl HgetallOutput {
    pub fn new(fields: HashMap<String, RedisJsonValue>) -> Self {
        Self { fields }
    }

    pub fn fields(&self) -> &HashMap<String, RedisJsonValue> {
        &self.fields
    }

    /// Get value for a specific field
    pub fn get(&self, field: &str) -> Option<&RedisJsonValue> {
        self.fields.get(field)
    }

    /// Get the number of fields
    pub fn len(&self) -> usize {
        self.fields.len()
    }

    /// Check if the hash is empty
    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    /// Decode the Redis protocol response into a HgetallOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let fields = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                if arr.len() % 2 != 0 {
                    return Err(EpError::parse("HGETALL must return even number of elements"));
                }

                let mut map = HashMap::new();

                for chunk in arr.chunks(2) {
                    let field = RedisJsonValue::try_from(&chunk[0])?.to_string();
                    let value = RedisJsonValue::try_from(&chunk[1])?;

                    map.insert(field, value);
                }

                map
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut map = HashMap::new();
                for (key, value) in data.into_iter() {
                    let field = RedisJsonValue::try_from(key)?.to_string();
                    let value = RedisJsonValue::try_from(value)?;
                    map.insert(field, value);
                }
                map
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                if data.len() % 2 != 0 {
                    return Err(EpError::parse("HGETALL must return even number of elements"));
                }

                let mut map = HashMap::new();
                for chunk in data.chunks(2) {
                    let field = RedisJsonValue::try_from(&chunk[0])?.to_string();
                    let value = RedisJsonValue::try_from(&chunk[1])?;

                    map.insert(field, value);
                }
                map
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => {
                return Err(EpError::parse(format!("unexpected HGETALL response: {:?}", other)));
            }
        };

        Ok(Self { fields })
    }
}

impl RedisCommandOutput for HgetallOutput {
    fn kind(&self) -> RedisApi {
        RedisApi::Hgetall
    }

    fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        Self::decode(bytes)
    }
}

impl Serialize for HgetallOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HgetallOutput", 1)?;
        state.serialize_field("fields", &self.fields)?;
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
            let input = HgetallInput { key: RedisKey::String("myhash".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nHGETALL\r\n$6\r\nmyhash\r\n");
        }

        #[test]
        fn test_decode_output_with_fields() {
            // *4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n
            let output = HgetallOutput::decode(b"*4\r\n$2\r\nf1\r\n$2\r\nv1\r\n$2\r\nf2\r\n$2\r\nv2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.get("f1"), Some(&RedisJsonValue::String("v1".into())));
            assert_eq!(output.get("f2"), Some(&RedisJsonValue::String("v2".into())));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = HgetallOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HgetallOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HgetallInput { key: RedisKey::String("myhash".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = HgetallInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("extra".into())];
            let err = HgetallInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
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
        async fn test_hgetall_with_fields() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhgetall_test1\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetall_test1".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&HgetallInput { key: RedisKey::String("hgetall_test1".into()) }.command()).await.expect("raw failed");

                    let output = HgetallOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                    assert_eq!(output.get("f1"), Some(&RedisJsonValue::String("v1".into())));
                    assert_eq!(output.get("f2"), Some(&RedisJsonValue::String("v2".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetall_empty_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhgetall_test2\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&HgetallInput { key: RedisKey::String("hgetall_test2".into()) }.command()).await.expect("raw failed");

                    let output = HgetallOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetall_single_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhgetall_test3\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hgetall_test3".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("only".into()),
                                RedisJsonValue::String("one".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&HgetallInput { key: RedisKey::String("hgetall_test3".into()) }.command()).await.expect("raw failed");

                    let output = HgetallOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.get("only"), Some(&RedisJsonValue::String("one".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetall_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhgetall_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hgetall_r2".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HgetallInput { key: RedisKey::String("hgetall_r2".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HgetallOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hgetall_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nhgetall_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hgetall_r3".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                        Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HgetallInput { key: RedisKey::String("hgetall_r3".into()) }.command()).await.expect("raw failed");

            let output = HgetallOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 2);

            ctx.stop().await;
        }
    }
}
