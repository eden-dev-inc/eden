use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, RenameInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Rename,
    "Renames key to newkey. It returns an error when key does not exist. If newkey already exists it is overwritten.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RENAME`
/// https://redis.io/docs/latest/commands/rename/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RenameInput {
    /// The key to rename
    pub(crate) key: RedisKey,
    /// The new key name
    pub(crate) newkey: RedisKey,
}

impl Serialize for RenameInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RenameInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("newkey", &self.newkey)?;
        state.end()
    }
}

impl_redis_operation!(RenameInput, API_INFO, { key, newkey });

impl RedisCommandInput for RenameInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("RENAME requires exactly 2 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            newkey: args[1].clone().try_into()?,
        })
    }
}

/// Output for Redis RENAME command
///
/// Returns OK on success. An error is returned if the source key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RenameOutput {
    /// Always "OK" on success
    status: String,
}

impl RenameOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    /// Decode the Redis protocol response into a RenameOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected RENAME response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected RENAME response: {:?}", other))),
            },
        }
    }
}

impl Default for RenameOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for RenameOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RenameOutput", 1)?;
        state.serialize_field("status", &self.status)?;
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
            let input = RenameInput {
                key: RedisKey::String("oldkey".into()),
                newkey: RedisKey::String("newkey".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nRENAME\r\n$6\r\noldkey\r\n$6\r\nnewkey\r\n");
        }

        #[test]
        fn test_keys_returns_both_keys() {
            let input = RenameInput {
                key: RedisKey::String("src".into()),
                newkey: RedisKey::String("dst".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("src".into()));
            assert_eq!(keys[1], RedisKey::String("dst".into()));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = RenameOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_no_such_key() {
            let err = RenameOutput::decode(b"-ERR no such key\r\n").unwrap_err();
            assert!(err.to_string().contains("no such key"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("oldkey".into()), RedisJsonValue::String("newkey".into())];
            let input = RenameInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("oldkey".into()));
            assert_eq!(input.newkey, RedisKey::String("newkey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("onlykey".into())];
            let err = RenameInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = RenameInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 2 arguments"));
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
        async fn test_rename_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set initial key
                    ctx.write(SetInput {
                        key: RedisKey::String("rename_src".into()),
                        value: RedisJsonValue::String("myvalue".into()),
                        ..Default::default()
                    })
                    .await;

                    // Rename it
                    let result = ctx
                        .raw(
                            &RenameInput {
                                key: RedisKey::String("rename_src".into()),
                                newkey: RedisKey::String("rename_dst".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RenameOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify old key is gone
                    let get_old = ctx.raw(&GetInput { key: RedisKey::String("rename_src".into()) }.command()).await.expect("raw failed");
                    let old_output = GetOutput::decode(&get_old).expect("decode get failed");
                    assert!(!old_output.exists(), "old key should not exist");

                    // Verify new key has value
                    let get_new = ctx.raw(&GetInput { key: RedisKey::String("rename_dst".into()) }.command()).await.expect("raw failed");
                    let new_output = GetOutput::decode(&get_new).expect("decode get failed");
                    assert!(new_output.exists());
                    assert_eq!(new_output.value(), Some(&RedisJsonValue::from("myvalue")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &RenameInput {
                                key: RedisKey::String("nonexistent".into()),
                                newkey: RedisKey::String("newname".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = RenameOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().to_lowercase().contains("no such key"), "expected 'no such key' error, got: {}", err);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_overwrites_destination() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set source and destination
                    ctx.write(SetInput {
                        key: RedisKey::String("src_key".into()),
                        value: RedisJsonValue::String("source_value".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.write(SetInput {
                        key: RedisKey::String("dst_key".into()),
                        value: RedisJsonValue::String("destination_value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Rename should overwrite destination
                    let result = ctx
                        .raw(
                            &RenameInput {
                                key: RedisKey::String("src_key".into()),
                                newkey: RedisKey::String("dst_key".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RenameOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify destination has source's value
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("dst_key".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("source_value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_same_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("same_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Rename to itself should succeed
                    let result = ctx
                        .raw(
                            &RenameInput {
                                key: RedisKey::String("same_key".into()),
                                newkey: RedisKey::String("same_key".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = RenameOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Key should still exist with same value
                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("same_key".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("value")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_src".into()),
                        value: RedisJsonValue::String("pipe_val".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &RenameInput {
                            key: RedisKey::String("pipe_src".into()),
                            newkey: RedisKey::String("pipe_dst".into()),
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe_dst".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let rename_out = RenameOutput::decode(responses[0]).expect("decode rename");
                    assert!(rename_out.is_ok());

                    let get_out = GetOutput::decode(responses[1]).expect("decode get");
                    assert_eq!(get_out.value(), Some(&RedisJsonValue::from("pipe_val")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_resp2_ok_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2_src".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &RenameInput {
                        key: RedisKey::String("r2_src".into()),
                        newkey: RedisKey::String("r2_dst".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
            let output = RenameOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_rename_resp3_ok_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3_src".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx
                .raw(
                    &RenameInput {
                        key: RedisKey::String("r3_src".into()),
                        newkey: RedisKey::String("r3_dst".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 simple string OK format");
            let output = RenameOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());
            ctx.stop().await;
        }
    }
}
