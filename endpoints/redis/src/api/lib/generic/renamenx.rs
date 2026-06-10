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

const API_INFO: ApiInfo<RedisApi, RenamenxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Renamenx,
    "Renames key to newkey if newkey does not yet exist. It returns an error when key does not exist.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RENAMENX`
/// https://redis.io/docs/latest/commands/renamenx/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RenamenxInput {
    /// The source key to rename
    pub(crate) key: RedisKey,
    /// The new key name (must not already exist)
    pub(crate) newkey: RedisKey,
}

impl Serialize for RenamenxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RenamenxInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("newkey", &self.newkey)?;
        state.end()
    }
}

impl_redis_operation!(RenamenxInput, API_INFO, { key, newkey });

impl RedisCommandInput for RenamenxInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone(), self.newkey.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.newkey);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("RENAMENX requires 2 arguments (key, newkey), given none"));
        } else if args.len() == 1 {
            return Err(EpError::parse("RENAMENX requires 2 arguments (key, newkey), given 1"));
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "RENAMENX expects 2 arguments, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            newkey: args[1].clone().try_into()?,
        })
    }
}

/// Output for Redis RENAMENX command
///
/// Returns 1 if key was renamed to newkey.
/// Returns 0 if newkey already exists.
/// Returns an error if the source key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RenamenxOutput {
    /// 1 if renamed successfully, 0 if newkey already exists
    result: i64,
}

impl RenamenxOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Get the raw result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Check if the rename was successful
    pub fn was_renamed(&self) -> bool {
        self.result == 1
    }

    /// Check if newkey already existed (rename did not occur)
    pub fn newkey_exists(&self) -> bool {
        self.result == 0
    }

    /// Decode the Redis protocol response into a RenamenxOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected RENAMENX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected RENAMENX response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for RenamenxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RenamenxOutput", 1)?;
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
            let input = RenamenxInput {
                key: RedisKey::String("oldkey".into()),
                newkey: RedisKey::String("newkey".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nRENAMENX\r\n$6\r\noldkey\r\n$6\r\nnewkey\r\n");
        }

        #[test]
        fn test_keys_returns_both_keys() {
            let input = RenamenxInput {
                key: RedisKey::String("src".into()),
                newkey: RedisKey::String("dst".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("src".into()));
            assert_eq!(keys[1], RedisKey::String("dst".into()));
        }

        #[test]
        fn test_decode_success_response() {
            let output = RenamenxOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_renamed());
            assert!(!output.newkey_exists());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_newkey_exists_response() {
            let output = RenamenxOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_renamed());
            assert!(output.newkey_exists());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_no_such_key() {
            let err = RenamenxOutput::decode(b"-ERR no such key\r\n").unwrap_err();
            assert!(err.to_string().contains("no such key"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("oldkey".into()), RedisJsonValue::String("newkey".into())];
            let input = RenamenxInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("oldkey".into()));
            assert_eq!(input.newkey, RedisKey::String("newkey".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = RenamenxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_one_arg() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = RenamenxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_serialize_input() {
            let input = RenamenxInput {
                key: RedisKey::String("src".into()),
                newkey: RedisKey::String("dst".into()),
            };
            let json = serde_json::to_value(&input).unwrap();
            assert_eq!(json["type"], "RENAMENX");
            assert_eq!(json["key"], "src");
            assert_eq!(json["newkey"], "dst");
        }

        #[test]
        fn test_serialize_output() {
            let output = RenamenxOutput::new(1);
            let json = serde_json::to_value(&output).unwrap();
            assert_eq!(json["result"], 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_success() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up source key
                    ctx.write(SetInput {
                        key: RedisKey::String("rename_src".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Rename to nonexistent key
                    let result = ctx
                        .raw(
                            &RenamenxInput {
                                key: RedisKey::String("rename_src".into()),
                                newkey: RedisKey::String("rename_dst".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RenamenxOutput::decode(&result).expect("decode failed");
                    assert!(output.was_renamed(), "rename should succeed");

                    // Verify old key is gone
                    let get_old = ctx.raw(&GetInput { key: RedisKey::String("rename_src".into()) }.command()).await.expect("raw failed");
                    let old_output = GetOutput::decode(&get_old).expect("decode get failed");
                    assert!(!old_output.exists(), "old key should not exist");

                    // Verify new key has value
                    let get_new = ctx.raw(&GetInput { key: RedisKey::String("rename_dst".into()) }.command()).await.expect("raw failed");
                    let new_output = GetOutput::decode(&get_new).expect("decode get failed");
                    assert!(new_output.exists());
                    assert_eq!(new_output.value(), Some(&RedisJsonValue::from("value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_target_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up both keys
                    ctx.write(SetInput {
                        key: RedisKey::String("nx_src".into()),
                        value: RedisJsonValue::String("src_val".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("nx_dst".into()),
                        value: RedisJsonValue::String("dst_val".into()),
                        ..Default::default()
                    })
                    .await;

                    // Attempt rename (should fail)
                    let result = ctx
                        .raw(
                            &RenamenxInput {
                                key: RedisKey::String("nx_src".into()),
                                newkey: RedisKey::String("nx_dst".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RenamenxOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_renamed(), "rename should not occur");
                    assert!(output.newkey_exists(), "should indicate newkey exists");

                    // Verify both keys unchanged
                    let get_src = ctx.raw(&GetInput { key: RedisKey::String("nx_src".into()) }.command()).await.expect("raw failed");
                    let src_output = GetOutput::decode(&get_src).expect("decode failed");
                    assert_eq!(src_output.value(), Some(&RedisJsonValue::from("src_val")));

                    let get_dst = ctx.raw(&GetInput { key: RedisKey::String("nx_dst".into()) }.command()).await.expect("raw failed");
                    let dst_output = GetOutput::decode(&get_dst).expect("decode failed");
                    assert_eq!(dst_output.value(), Some(&RedisJsonValue::from("dst_val")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_nonexistent_source() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &RenamenxInput {
                                key: RedisKey::String("nonexistent_key".into()),
                                newkey: RedisKey::String("some_dst".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = RenamenxOutput::decode(&result);
                    assert!(err.is_err(), "should error when source key doesn't exist");
                    assert!(err.unwrap_err().to_string().contains("no such key"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_same_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("same_key".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // Rename to itself
                    let result = ctx
                        .raw(
                            &RenamenxInput {
                                key: RedisKey::String("same_key".into()),
                                newkey: RedisKey::String("same_key".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    // Redis returns 0 when renaming to itself (key already exists)
                    let output = RenamenxOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_renamed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up keys
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_a".into()),
                        value: RedisJsonValue::String("a_val".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_c".into()),
                        value: RedisJsonValue::String("c_val".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    // This should succeed (pipe_b doesn't exist)
                    pipeline.extend_from_slice(
                        &RenamenxInput {
                            key: RedisKey::String("pipe_a".into()),
                            newkey: RedisKey::String("pipe_b".into()),
                        }
                        .command(),
                    );
                    // This should fail (pipe_c exists)
                    pipeline.extend_from_slice(
                        &RenamenxInput {
                            key: RedisKey::String("pipe_b".into()),
                            newkey: RedisKey::String("pipe_c".into()),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = RenamenxOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.was_renamed(), "first rename should succeed");

                    let out2 = RenamenxOutput::decode(responses[1]).expect("decode second");
                    assert!(!out2.was_renamed(), "second rename should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2_src".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &RenamenxInput {
                        key: RedisKey::String("r2_src".into()),
                        newkey: RedisKey::String("r2_dst".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = RenamenxOutput::decode(&result).expect("decode failed");
            assert!(output.was_renamed());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_renamenx_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3_src".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &RenamenxInput {
                        key: RedisKey::String("r3_src".into()),
                        newkey: RedisKey::String("r3_dst".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = RenamenxOutput::decode(&result).expect("decode failed");
            assert!(output.was_renamed());

            ctx.stop().await;
        }
    }
}
