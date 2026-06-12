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

const API_INFO: ApiInfo<RedisApi, DecrInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Decr,
    "Decrements the number stored at key by one. If the key does not exist, it is set to 0 before performing the operation. An error is returned if the key contains a value of the wrong type or contains a string that can not be represented as integer. This operation is limited to 64 bit signed integers",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `DECR`
/// https://redis.io/docs/latest/commands/decr/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DecrInput {
    pub(crate) key: RedisKey,
}

impl Serialize for DecrInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DecrInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(DecrInput, API_INFO, { key });

impl RedisCommandInput for DecrInput {
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
            return Err(EpError::request("DECR requires one argument, given none"));
        }

        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "DECR takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis DECR command
///
/// Returns the value of key after the decrement.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DecrOutput {
    /// The value after decrementing
    value: i64,
}

impl DecrOutput {
    pub fn new(value: i64) -> Self {
        Self { value }
    }

    /// Get the decremented value
    pub fn value(&self) -> i64 {
        self.value
    }

    /// Decode the Redis protocol response into a DecrOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected DECR response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected DECR response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for DecrOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("DecrOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = DecrInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nDECR\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_positive_value() {
            let output = DecrOutput::decode(b":9\r\n").unwrap();
            assert_eq!(output.value(), 9);
        }

        #[test]
        fn test_decode_zero_value() {
            let output = DecrOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.value(), 0);
        }

        #[test]
        fn test_decode_negative_value() {
            let output = DecrOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.value(), -1);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = DecrOutput::decode(b"-ERR value is not an integer or out of range\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = DecrInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = DecrInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = DecrInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decr_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // DECR on non-existent key initializes to 0, then decrements
                    let result = ctx.raw(&DecrInput { key: RedisKey::String("decr_new".into()) }.command()).await.expect("raw failed");

                    let output = DecrOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), -1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decr_existing_value() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decr_exist".into()),
                            value: RedisJsonValue::String("10".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&DecrInput { key: RedisKey::String("decr_exist".into()) }.command()).await.expect("raw failed");

                    let output = DecrOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.value(), 9);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decr_multiple_times() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decr_multi".into()),
                            value: RedisJsonValue::String("5".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    for expected in [4, 3, 2, 1, 0, -1] {
                        let result =
                            ctx.raw(&DecrInput { key: RedisKey::String("decr_multi".into()) }.command()).await.expect("raw failed");

                        let output = DecrOutput::decode(&result).expect("decode failed");
                        assert_eq!(output.value(), expected);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decr_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("decr_pipe".into()),
                            value: RedisJsonValue::String("100".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    for _ in 0..5 {
                        pipeline.extend_from_slice(&DecrInput { key: RedisKey::String("decr_pipe".into()) }.command());
                    }

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 5);

                    for (i, resp) in responses.iter().enumerate() {
                        let output = DecrOutput::decode(resp).expect("decode failed");
                        assert_eq!(output.value(), 99 - i as i64);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_decr_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("10".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&DecrInput { key: RedisKey::String("r2key".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":9\r\n", "RESP2 integer format");
            let output = DecrOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value(), 9);

            ctx.stop().await;
        }
    }
}
