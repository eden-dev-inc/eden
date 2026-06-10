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

const API_INFO: ApiInfo<RedisApi, GetsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Getset,
    "Atomically sets key to value and returns the old value stored at key. Returns an error when key exists but does not hold a string value. Any previous time to live associated with the key is discarded on successful SET operation",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `GETSET`
/// https://redis.io/docs/latest/commands/getset/
///
/// Note: As of Redis 6.2.0, this command is considered deprecated.
/// The recommended alternative is `SET` with the `GET` option.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetsetInput {
    pub(crate) key: RedisKey,
    pub(crate) value: RedisJsonValue,
}

impl Serialize for GetsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GetsetInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    GetsetInput,
    API_INFO,
    {key, value}
);

impl RedisCommandInput for GetsetInput {
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
            return Err(EpError::request(format!("GETSET requires 2 arguments, given {}", args.len())));
        }

        if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GETSET expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, value: args[1].clone() })
    }
}

/// Output for Redis GETSET command
///
/// Returns the old value stored at key, or None if the key did not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetsetOutput {
    /// The old value stored at the key, or None if key didn't exist
    old_value: Option<RedisJsonValue>,
}

impl GetsetOutput {
    pub fn new(old_value: Option<RedisJsonValue>) -> Self {
        Self { old_value }
    }

    /// Get the old value from the output
    pub fn old_value(&self) -> Option<&RedisJsonValue> {
        self.old_value.as_ref()
    }

    /// Check if the key existed before (had an old value)
    pub fn had_value(&self) -> bool {
        self.old_value.is_some()
    }

    /// Decode the Redis protocol response into a GetsetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let old_value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?)),
                Resp2Frame::SimpleString(s) => Some(RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?)),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GETSET response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::SimpleString { data, .. } => Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected GETSET response: {:?}", other)));
                }
            },
        };

        Ok(Self { old_value })
    }
}

impl Serialize for GetsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GetsetOutput", 1)?;
        state.serialize_field("old_value", &self.old_value)?;
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
            let input = GetsetInput {
                key: RedisKey::String("mykey".into()),
                value: RedisJsonValue::String("newvalue".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nGETSET\r\n$5\r\nmykey\r\n$8\r\nnewvalue\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = GetsetOutput::decode(b"$8\r\noldvalue\r\n").unwrap();
            assert!(output.had_value());
            assert_eq!(output.old_value(), Some(&RedisJsonValue::from("oldvalue")));
        }

        #[test]
        fn test_decode_empty_string() {
            let output = GetsetOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(output.had_value());
            assert_eq!(output.old_value(), Some(&RedisJsonValue::from("")));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = GetsetOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.had_value());
            assert_eq!(output.old_value(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = GetsetOutput::decode(b"_\r\n").unwrap();
            assert!(!output.had_value());
            assert_eq!(output.old_value(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetsetOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("value".into())];
            let input = GetsetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.value, RedisJsonValue::String("value".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = GetsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GetsetInput {
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
        use crate::api::SetInput;
        use crate::api::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: GETSET is deprecated as of Redis 6.2.0, but still works in all versions.
        // We test on all protocols since the command remains functional.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &GetsetInput {
                                key: RedisKey::String("getset_new".into()),
                                value: RedisJsonValue::String("newvalue".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetsetOutput::decode(&result).expect("decode failed");
                    assert!(!output.had_value(), "New key should return nil");

                    // Verify the new value was set
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("getset_new".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("newvalue")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getset_exist".into()),
                            value: RedisJsonValue::String("oldvalue".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(
                            &GetsetInput {
                                key: RedisKey::String("getset_exist".into()),
                                value: RedisJsonValue::String("newvalue".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetsetOutput::decode(&result).expect("decode failed");
                    assert!(output.had_value());
                    assert_eq!(output.old_value(), Some(&RedisJsonValue::from("oldvalue")));

                    // Verify the new value was set
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("getset_exist".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("newvalue")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_atomic_swap() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial value
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getset_swap".into()),
                            value: RedisJsonValue::String("A".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Swap A -> B
                    let result1 = ctx
                        .raw(
                            &GetsetInput {
                                key: RedisKey::String("getset_swap".into()),
                                value: RedisJsonValue::String("B".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output1 = GetsetOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.old_value(), Some(&RedisJsonValue::from("A")));

                    // Swap B -> C
                    let result2 = ctx
                        .raw(
                            &GetsetInput {
                                key: RedisKey::String("getset_swap".into()),
                                value: RedisJsonValue::String("C".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");
                    let output2 = GetsetOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.old_value(), Some(&RedisJsonValue::from("B")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("gs1".into()),
                            value: RedisJsonValue::String("old1".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &GetsetInput {
                            key: RedisKey::String("gs1".into()),
                            value: RedisJsonValue::String("new1".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetsetInput {
                            key: RedisKey::String("gs_new".into()),
                            value: RedisJsonValue::String("val".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &GetsetInput {
                            key: RedisKey::String("gs1".into()),
                            value: RedisJsonValue::String("newest1".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = GetsetOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.had_value());
                    assert_eq!(out1.old_value(), Some(&RedisJsonValue::from("old1")));

                    let out2 = GetsetOutput::decode(responses[1]).expect("decode second");
                    assert!(!out2.had_value());

                    let out3 = GetsetOutput::decode(responses[2]).expect("decode third");
                    assert!(out3.had_value());
                    assert_eq!(out3.old_value(), Some(&RedisJsonValue::from("new1")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("old".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &GetsetInput {
                        key: RedisKey::String("r2key".into()),
                        value: RedisJsonValue::String("new".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = GetsetOutput::decode(&result).expect("decode failed");
            assert!(output.had_value());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &GetsetInput {
                        key: RedisKey::String("r2_missing".into()),
                        value: RedisJsonValue::String("val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = GetsetOutput::decode(&result).expect("decode failed");
            assert!(!output.had_value());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getset_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &GetsetInput {
                        key: RedisKey::String("r3_missing".into()),
                        value: RedisJsonValue::String("val".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = GetsetOutput::decode(&result).expect("decode failed");
            assert!(!output.had_value());

            ctx.stop().await;
        }
    }
}
