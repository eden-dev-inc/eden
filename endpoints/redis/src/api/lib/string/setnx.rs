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

const API_INFO: ApiInfo<RedisApi, SetnxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Setnx,
    "Set the string value of a key only when the key doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `SETNX`
/// https://redis.io/docs/latest/commands/setnx/
///
/// Note: As of Redis 2.6.12, this command is considered deprecated.
/// The recommended alternative is `SET` with the `NX` option.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SetnxInput {
    pub(crate) key: RedisKey,
    pub(crate) value: RedisJsonValue,
}

impl Serialize for SetnxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SetnxInput", 3)?;

        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    SetnxInput,
    API_INFO,
    {key, value}
);

impl RedisCommandInput for SetnxInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.value);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("SETNX requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "SETNX expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, value: args[1].clone() })
    }
}

/// Output for Redis SETNX command
///
/// Returns 1 if the key was set, 0 if the key was not set (already existed).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SetnxOutput {
    /// 1 if key was set, 0 if key already existed
    result: i64,
}

impl SetnxOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Get the raw result (1 or 0)
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Check if the key was successfully set (didn't exist before)
    pub fn was_set(&self) -> bool {
        self.result == 1
    }

    /// Check if the key already existed (wasn't set)
    pub fn already_existed(&self) -> bool {
        self.result == 0
    }

    /// Decode the Redis protocol response into a SetnxOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SETNX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SETNX response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for SetnxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SetnxOutput", 1)?;
        state.serialize_field("result", &self.result)?;
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
            let input = SetnxInput {
                key: RedisKey::String("mykey".into()),
                value: RedisJsonValue::String("myvalue".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$5\r\nSETNX\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n");
        }

        #[test]
        fn test_encode_command_integer_value() {
            let input = SetnxInput {
                key: RedisKey::String("counter".into()),
                value: RedisJsonValue::Integer(42),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n$5\r\nSETNX\r\n"));
        }

        #[test]
        fn test_decode_success() {
            let output = SetnxOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.result(), 1);
            assert!(output.was_set());
            assert!(!output.already_existed());
        }

        #[test]
        fn test_decode_already_exists() {
            let output = SetnxOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.result(), 0);
            assert!(!output.was_set());
            assert!(output.already_existed());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = SetnxOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("value".into())];
            let input = SetnxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = SetnxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SetnxInput {
                key: RedisKey::String("mykey".into()),
                value: RedisJsonValue::String("val".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::GetInput;
        use crate::api::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: SETNX is deprecated as of Redis 2.6.12, replaced by SET with NX option.
        // The command still works in all versions for backward compatibility.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setnx_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &SetnxInput {
                                key: RedisKey::String("setnx_new".into()),
                                value: RedisJsonValue::String("value".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set(), "SETNX on new key should return 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setnx_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First SETNX
                    ctx.raw(
                        &SetnxInput {
                            key: RedisKey::String("setnx_existing".into()),
                            value: RedisJsonValue::String("first".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Second SETNX on same key
                    let result = ctx
                        .raw(
                            &SetnxInput {
                                key: RedisKey::String("setnx_existing".into()),
                                value: RedisJsonValue::String("second".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.already_existed(), "SETNX on existing key should return 0");

                    // Verify original value preserved
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("setnx_existing".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("first")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setnx_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetnxInput {
                            key: RedisKey::String("pipe_key".into()),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetnxInput {
                            key: RedisKey::String("pipe_key".into()),
                            value: RedisJsonValue::String("v2".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &SetnxInput {
                            key: RedisKey::String("pipe_key2".into()),
                            value: RedisJsonValue::String("v3".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = SetnxOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.was_set());

                    let out2 = SetnxOutput::decode(responses[1]).expect("decode second");
                    assert!(out2.already_existed());

                    let out3 = SetnxOutput::decode(responses[2]).expect("decode third");
                    assert!(out3.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setnx_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &SetnxInput {
                        key: RedisKey::String("r2key".into()),
                        value: RedisJsonValue::String("val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = SetnxOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_setnx_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &SetnxInput {
                        key: RedisKey::String("r3key".into()),
                        value: RedisJsonValue::String("val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = SetnxOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());

            ctx.stop().await;
        }
    }
}
