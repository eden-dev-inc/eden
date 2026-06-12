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

const API_INFO: ApiInfo<RedisApi, ScardInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Scard, "Returns the number of members in a set", ReqType::Read, true);

/// See official Redis documentation for `SCARD`
/// https://redis.io/docs/latest/commands/scard/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ScardInput {
    key: RedisKey,
}

impl ScardInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for ScardInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ScardInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(ScardInput, API_INFO, { key });

impl RedisCommandInput for ScardInput {
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
            return Err(EpError::request(format!("SCARD requires exactly 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis SCARD command
///
/// Returns the cardinality (number of elements) of the set,
/// or 0 if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ScardOutput {
    count: i64,
}

impl ScardOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of members in the set
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if the set is empty or doesn't exist
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("SCARD must return integer")),
        };

        Ok(Self { count })
    }
}

impl Serialize for ScardOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ScardOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = ScardInput::new(RedisKey::String("myset".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$5\r\nSCARD\r\n$5\r\nmyset\r\n");
        }

        #[test]
        fn test_decode_output_positive() {
            let output = ScardOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.count(), 5);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = ScardOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = ScardOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myset".into())];
            let input = ScardInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = ScardInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 1"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let err = ScardInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 1"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = ScardInput::new(RedisKey::String("myset".into()));
            assert_eq!(input.keys().len(), 1);
            assert_eq!(input.keys()[0], RedisKey::String("myset".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scard_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nscard_basic\r\n").await.expect("raw failed");

                    // SADD scard_basic one two three
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$11\r\nscard_basic\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx.raw(&ScardInput::new(RedisKey::String("scard_basic".into())).command()).await.expect("raw failed");

                    let output = ScardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scard_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nscard_noexist\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ScardInput::new(RedisKey::String("scard_noexist".into())).command()).await.expect("raw failed");

                    let output = ScardOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scard_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$11\r\nscard_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result = ctx.raw(&ScardInput::new(RedisKey::String("scard_wrong".into())).command()).await.expect("raw failed");

                    let err = ScardOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scard_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nscard_r2\r\n").await.expect("raw failed");

            ctx.raw(b"*3\r\n$4\r\nSADD\r\n$8\r\nscard_r2\r\n$1\r\na\r\n").await.expect("raw failed");

            let result = ctx.raw(&ScardInput::new(RedisKey::String("scard_r2".into())).command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = ScardOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_scard_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\nscard_r3\r\n").await.expect("raw failed");

            ctx.raw(b"*4\r\n$4\r\nSADD\r\n$8\r\nscard_r3\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("raw failed");

            let result = ctx.raw(&ScardInput::new(RedisKey::String("scard_r3".into())).command()).await.expect("raw failed");

            let output = ScardOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 2);

            ctx.stop().await;
        }
    }
}
