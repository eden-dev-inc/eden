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

const API_INFO: ApiInfo<RedisApi, HlenInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hlen, "Returns the number of fields in a hash", ReqType::Read, true);

/// See official Redis documentation for `HLEN`
/// https://redis.io/docs/latest/commands/hlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HlenInput {
    pub(crate) key: RedisKey,
}

impl Serialize for HlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HlenInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(HlenInput, API_INFO, { key });

impl RedisCommandInput for HlenInput {
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
            return Err(EpError::request("HLEN requires 1 argument, given 0"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "HLEN expects 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis HLEN command
///
/// Returns the number of fields in the hash.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HlenOutput {
    /// Number of fields in the hash
    length: i64,
}

impl HlenOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the number of fields
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the hash is empty
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into a HlenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HLEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for HlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HlenOutput", 1)?;
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
            let input = HlenInput { key: RedisKey::String("myhash".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HLEN"));
            assert!(cmd_str.contains("myhash"));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = HlenOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.length(), 5);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = HlenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_large() {
            let output = HlenOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.length(), 1000000);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HlenOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = HlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = HlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            // Extra args should warn but not fail
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("extra".into())];
            let input = HlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key1".into()));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HlenInput { key: RedisKey::String("myhash".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
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
        async fn test_hlen_existing_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhlen_test\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hlen_test".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                                Field::new(RedisJsonValue::String("f3".into()), RedisJsonValue::String("v3".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HlenInput { key: RedisKey::String("hlen_test".into()) }.command()).await.expect("raw failed");

                    let output = HlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hlen_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhlen_nonexistent\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&HlenInput { key: RedisKey::String("hlen_nonexistent".into()) }.command()).await.expect("raw failed");

                    let output = HlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hlen_single_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhlen_single\r\n").await.expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hlen_single".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("only".into()),
                                RedisJsonValue::String("one".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HlenInput { key: RedisKey::String("hlen_single".into()) }.command()).await.expect("raw failed");

                    let output = HlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hlen_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhlen_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hlen_r2".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                        Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HlenInput { key: RedisKey::String("hlen_r2".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = HlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 2);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hlen_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$7\r\nhlen_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hlen_r3".into()),
                    fields: vec![Field::new(RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HlenInput { key: RedisKey::String("hlen_r3".into()) }.command()).await.expect("raw failed");

            let output = HlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);

            ctx.stop().await;
        }
    }
}
