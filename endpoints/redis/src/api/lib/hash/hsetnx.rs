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

const API_INFO: ApiInfo<RedisApi, HsetnxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hsetnx,
    "Set the value of a field in a hash only when the field doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HSETNX`
/// https://redis.io/docs/latest/commands/hsetnx/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HsetnxInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
    pub(crate) value: RedisJsonValue,
}

impl Serialize for HsetnxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HsetnxInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    HsetnxInput,
    API_INFO,
    {key, field, value}
);

impl RedisCommandInput for HsetnxInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.field).arg(&self.value);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("HSETNX requires 3 arguments, given {}", args.len())));
        } else if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "HSETNX takes 3 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            field: args[1].clone(),
            value: args[2].clone(),
        })
    }
}

/// Output for Redis HSETNX command
///
/// Returns 1 if the field was set (didn't exist before).
/// Returns 0 if the field already existed and no operation was performed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HsetnxOutput {
    /// Whether the field was set (true = new field, false = already existed)
    was_set: bool,
}

impl HsetnxOutput {
    pub fn new(was_set: bool) -> Self {
        Self { was_set }
    }

    /// Returns true if the field was newly set
    pub fn was_set(&self) -> bool {
        self.was_set
    }

    /// Returns true if the field already existed (no operation performed)
    pub fn already_existed(&self) -> bool {
        !self.was_set
    }

    /// Decode the Redis protocol response into a HsetnxOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HSETNX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HSETNX response: {:?}", other)));
                }
            },
        };

        Ok(Self { was_set: result == 1 })
    }
}

impl Serialize for HsetnxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HsetnxOutput", 1)?;
        state.serialize_field("was_set", &self.was_set)?;
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
            let input = HsetnxInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field1".into()),
                value: RedisJsonValue::String("value1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nHSETNX\r\n$6\r\nmyhash\r\n$6\r\nfield1\r\n$6\r\nvalue1\r\n");
        }

        #[test]
        fn test_decode_field_set() {
            let output = HsetnxOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_set());
            assert!(!output.already_existed());
        }

        #[test]
        fn test_decode_field_existed() {
            let output = HsetnxOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_set());
            assert!(output.already_existed());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HsetnxOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("value".into()),
            ];
            let input = HsetnxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.field, RedisJsonValue::String("field".into()));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let err = HsetnxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HsetnxInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field".into()),
                value: RedisJsonValue::String("value".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{Field, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_new_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Ensure clean state
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhsetnx_hash\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HsetnxInput {
                                key: RedisKey::String("hsetnx_hash".into()),
                                field: RedisJsonValue::String("newfield".into()),
                                value: RedisJsonValue::String("newvalue".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                    assert!(!output.already_existed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_existing_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a hash with a field
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hsetnx_exist".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("existing".into()),
                                RedisJsonValue::String("original".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try to set the same field
                    let result = ctx
                        .raw(
                            &HsetnxInput {
                                key: RedisKey::String("hsetnx_exist".into()),
                                field: RedisJsonValue::String("existing".into()),
                                value: RedisJsonValue::String("new".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetnxOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set());
                    assert!(output.already_existed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Ensure key doesn't exist
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nhsetnx_newkey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &HsetnxInput {
                                key: RedisKey::String("hsetnx_newkey".into()),
                                field: RedisJsonValue::String("field".into()),
                                value: RedisJsonValue::String("value".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HsetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nhsetnx_pipe\r\n").await.expect("raw failed");

                    // First set should succeed
                    ctx.raw(
                        &HsetnxInput {
                            key: RedisKey::String("hsetnx_pipe".into()),
                            field: RedisJsonValue::String("f1".into()),
                            value: RedisJsonValue::String("v1".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    // Try to set f1 again (should fail)
                    pipeline.extend_from_slice(
                        &HsetnxInput {
                            key: RedisKey::String("hsetnx_pipe".into()),
                            field: RedisJsonValue::String("f1".into()),
                            value: RedisJsonValue::String("v1_new".into()),
                        }
                        .command(),
                    );
                    // Set f2 (should succeed)
                    pipeline.extend_from_slice(
                        &HsetnxInput {
                            key: RedisKey::String("hsetnx_pipe".into()),
                            field: RedisJsonValue::String("f2".into()),
                            value: RedisJsonValue::String("v2".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = HsetnxOutput::decode(responses[0]).expect("decode first");
                    assert!(!out1.was_set(), "f1 already exists");

                    let out2 = HsetnxOutput::decode(responses[1]).expect("decode second");
                    assert!(out2.was_set(), "f2 is new");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$9\r\nhsetnx_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HsetnxInput {
                        key: RedisKey::String("hsetnx_r2".into()),
                        field: RedisJsonValue::String("f".into()),
                        value: RedisJsonValue::String("v".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format for success");

            let output = HsetnxOutput::decode(&result).expect("decode failed");
            assert!(output.was_set());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hsetnx_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            // First create the field
            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("hsetnx_r3".into()),
                    fields: vec![Field::new(
                        RedisJsonValue::String("existing".into()),
                        RedisJsonValue::String("val".into()),
                    )],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HsetnxInput {
                        key: RedisKey::String("hsetnx_r3".into()),
                        field: RedisJsonValue::String("existing".into()),
                        value: RedisJsonValue::String("new".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":0\r\n", "RESP3 integer format for failure");

            let output = HsetnxOutput::decode(&result).expect("decode failed");
            assert!(!output.was_set());

            ctx.stop().await;
        }
    }
}
