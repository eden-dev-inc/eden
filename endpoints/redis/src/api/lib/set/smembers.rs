use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SmembersInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Smembers, "Returns all members of a set", ReqType::Read, true);

/// See official Redis documentation for `SMEMBERS`
/// https://redis.io/docs/latest/commands/smembers/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SmembersInput {
    pub(crate) key: RedisKey,
}

impl SmembersInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for SmembersInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SmembersInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(SmembersInput, API_INFO, { key });

impl RedisCommandInput for SmembersInput {
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
        if args.is_empty() {
            return Err(EpError::request("SMEMBERS requires 1 argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SMEMBERS takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis SMEMBERS command
///
/// Returns all members of a set.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SmembersOutput {
    values: Vec<RedisJsonValue>,
}

impl SmembersOutput {
    pub fn new(values: Vec<RedisJsonValue>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &Vec<RedisJsonValue> {
        &self.values
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut values = Vec::new();
                for val in arr {
                    values.push(RedisJsonValue::try_from(val)?);
                }
                values
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut values = Vec::new();
                for val in data {
                    values.push(RedisJsonValue::try_from(val)?);
                }
                values
            }
            DecoderRespFrame::Resp3(Resp3Frame::Set { data, .. }) => {
                let mut values = Vec::new();
                for val in data {
                    values.push(RedisJsonValue::try_from(val)?);
                }
                values
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("SMEMBERS must return array or set")),
        };

        Ok(Self { values })
    }
}

impl Serialize for SmembersOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SmembersOutput", 1)?;
        state.serialize_field("values", &self.values)?;
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
            let input = SmembersInput::new(RedisKey::String("myset".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$8\r\nSMEMBERS\r\n$5\r\nmyset\r\n");
        }

        #[test]
        fn test_decode_output_with_members() {
            let output = SmembersOutput::decode(b"*3\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n").unwrap();
            assert_eq!(output.values().len(), 3);
        }

        #[test]
        fn test_decode_output_empty_set() {
            let output = SmembersOutput::decode(b"*0\r\n").unwrap();
            assert_eq!(output.values().len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = SmembersOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myset".into())];
            let input = SmembersInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SmembersInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SmembersInput::new(RedisKey::String("myset".into()));
            assert_eq!(input.keys().len(), 1);
        }

        #[test]
        fn test_new_helper() {
            let input = SmembersInput::new("myset");
            assert_eq!(input.key, RedisKey::String("myset".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_with_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsmembers_test1\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$14\r\nsmembers_test1\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result =
                        ctx.raw(&SmembersInput::new(RedisKey::String("smembers_test1".into())).command()).await.expect("raw failed");

                    let output = SmembersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsmembers_test2\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SmembersInput::new(RedisKey::String("smembers_test2".into())).command()).await.expect("raw failed");

                    let output = SmembersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$14\r\nsmembers_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SmembersInput::new(RedisKey::String("smembers_wrong".into())).command()).await.expect("raw failed");

                    let err = SmembersOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_large_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsmembers_test3\r\n")
                        .await
                        .expect("raw failed");

                    // Add 10 members
                    ctx.raw(b"*12\r\n$4\r\nSADD\r\n$14\r\nsmembers_test3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n$1\r\ne\r\n$1\r\nf\r\n$1\r\ng\r\n$1\r\nh\r\n$1\r\ni\r\n$1\r\nj\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(&SmembersInput::new(RedisKey::String("smembers_test3".into())).command())
                        .await
                        .expect("raw failed");

                    let output = SmembersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 10);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_integer_members() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsmembers_test4\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$14\r\nsmembers_test4\r\n$1\r\n1\r\n$1\r\n2\r\n$1\r\n3\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SmembersInput::new(RedisKey::String("smembers_test4".into())).command()).await.expect("raw failed");

                    let output = SmembersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_smembers_single_member() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nsmembers_test5\r\n").await.expect("raw failed");

                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$14\r\nsmembers_test5\r\n$6\r\nmember\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SmembersInput::new(RedisKey::String("smembers_test5".into())).command()).await.expect("raw failed");

                    let output = SmembersOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.values().len(), 1);
                    assert_eq!(output.values()[0], RedisJsonValue::String("member".into()));
                })
            })
            .await;
        }
    }
}
