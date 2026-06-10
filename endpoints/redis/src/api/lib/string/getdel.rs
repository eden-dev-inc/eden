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

const API_INFO: ApiInfo<RedisApi, GetdelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Getdel,
    "Get the value of key and delete the key. This command is similar to GET, except for the fact that it also deletes the key on success (if and only if the key's value type is a string)",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `GETDEL`
/// https://redis.io/docs/latest/commands/getdel/
///
/// Available since Redis 6.2.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetdelInput {
    pub(crate) key: RedisKey,
}

impl Serialize for GetdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("GetdelInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(GetdelInput, API_INFO, { key });

impl RedisCommandInput for GetdelInput {
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
            return Err(EpError::parse("GETDEL requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GETDEL takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis GETDEL command
///
/// Returns the value of the key before deletion, or None if the key did not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetdelOutput {
    /// The value stored at the key before deletion, or None if key didn't exist
    value: Option<RedisJsonValue>,
}

impl GetdelOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the value from the output
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if the key existed (and was deleted)
    pub fn existed(&self) -> bool {
        self.value.is_some()
    }

    /// Decode the Redis protocol response into a GetdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?)),
                Resp2Frame::SimpleString(s) => Some(RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?)),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GETDEL response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected GETDEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for GetdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GetdelOutput", 1)?;
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
            let input = GetdelInput { key: RedisKey::String("mykey".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nGETDEL\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = GetdelOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(output.existed());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("hello")));
        }

        #[test]
        fn test_decode_empty_string() {
            let output = GetdelOutput::decode(b"$0\r\n\r\n").unwrap();
            assert!(output.existed());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("")));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = GetdelOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.existed());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = GetdelOutput::decode(b"_\r\n").unwrap();
            assert!(!output.existed());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetdelOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = GetdelInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = GetdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires one argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GetdelInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::{ExistsInput, ExistsOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getdel_existing_key() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getdel_exist".into()),
                            value: RedisJsonValue::String("myvalue".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&GetdelInput { key: RedisKey::String("getdel_exist".into()) }.command()).await.expect("raw failed");

                    let output = GetdelOutput::decode(&result).expect("decode failed");
                    assert!(output.existed());
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("myvalue")));

                    // Verify key is deleted
                    let exists_result =
                        ctx.raw(&ExistsInput { keys: vec![RedisKey::String("getdel_exist".into())] }.command()).await.expect("raw failed");

                    let exists_output = ExistsOutput::decode(&exists_result).expect("decode failed");
                    assert_eq!(exists_output.count(), 0, "Key should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getdel_nonexistent_key() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&GetdelInput { key: RedisKey::String("getdel_missing".into()) }.command()).await.expect("raw failed");

                    let output = GetdelOutput::decode(&result).expect("decode failed");
                    assert!(!output.existed());
                    assert_eq!(output.value(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getdel_pipeline() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("gd1".into()),
                            value: RedisJsonValue::String("v1".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("gd2".into()),
                            value: RedisJsonValue::String("v2".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&GetdelInput { key: RedisKey::String("gd1".into()) }.command());
                    pipeline.extend_from_slice(&GetdelInput { key: RedisKey::String("gd_missing".into()) }.command());
                    pipeline.extend_from_slice(&GetdelInput { key: RedisKey::String("gd2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = GetdelOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.existed());
                    assert_eq!(out1.value(), Some(&RedisJsonValue::from("v1")));

                    let out2 = GetdelOutput::decode(responses[1]).expect("decode second");
                    assert!(!out2.existed());

                    let out3 = GetdelOutput::decode(responses[2]).expect("decode third");
                    assert!(out3.existed());
                    assert_eq!(out3.value(), Some(&RedisJsonValue::from("v2")));

                    // Verify both keys are deleted
                    let exists_result = ctx
                        .raw(
                            &ExistsInput {
                                keys: vec![RedisKey::String("gd1".into()), RedisKey::String("gd2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let exists_output = ExistsOutput::decode(&exists_result).expect("decode failed");
                    assert_eq!(exists_output.count(), 0, "Both keys should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getdel_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.raw(
                &SetInput {
                    key: RedisKey::String("r2key".into()),
                    value: RedisJsonValue::String("val".into()),
                    ..Default::default()
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&GetdelInput { key: RedisKey::String("r2key".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = GetdelOutput::decode(&result).expect("decode failed");
            assert!(output.existed());

            ctx.stop().await;
        }
    }
}
