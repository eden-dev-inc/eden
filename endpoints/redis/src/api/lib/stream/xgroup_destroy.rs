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

const API_INFO: ApiInfo<RedisApi, XgroupDestroyInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::XgroupDestroy, "Destroys a consumer group entirely", ReqType::Write, true);

/// Input for Redis `XGROUP DESTROY` command.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XgroupDestroyInput {
    key: RedisKey,
    group: RedisJsonValue,
}

impl Serialize for XgroupDestroyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupDestroyInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.end()
    }
}

impl_redis_operation!(XgroupDestroyInput, API_INFO, {key, group});

impl RedisCommandInput for XgroupDestroyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd("XGROUP");
        command.arg("DESTROY");
        command.arg(&self.key).arg(&self.group);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::parse(format!("XGROUP DESTROY requires exactly 2 arguments, given {}", args.len())));
        }
        Ok(Self { key: args[0].clone().try_into()?, group: args[1].clone() })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XgroupDestroyOutput {
    destroyed: i64,
}

impl XgroupDestroyOutput {
    pub fn new(destroyed: i64) -> Self {
        Self { destroyed }
    }
    pub fn destroyed(&self) -> i64 {
        self.destroyed
    }
    pub fn was_destroyed(&self) -> bool {
        self.destroyed == 1
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let destroyed = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Integer(n)) => n,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => return Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::Number { data, .. }) => data,
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
            }
            other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
        };
        Ok(Self { destroyed })
    }
}

impl Serialize for XgroupDestroyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupDestroyOutput", 1)?;
        state.serialize_field("destroyed", &self.destroyed)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = XgroupDestroyInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
            };
            let cmd = input.command();
            let expected = b"*4\r\n$6\r\nXGROUP\r\n$7\r\nDESTROY\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_decode_output_one() {
            let output = XgroupDestroyOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.destroyed(), 1);
            assert!(output.was_destroyed());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XgroupDestroyOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.destroyed(), 0);
            assert!(!output.was_destroyed());
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("mygroup".into())];
            let input = XgroupDestroyInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XgroupDestroyInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        async fn xadd_entry(ctx: &mut TestContext, key: &str) {
            let cmd = format!("*5\r\n$4\r\nXADD\r\n${}\r\n{}\r\n$1\r\n*\r\n$1\r\nf\r\n$1\r\nv\r\n", key.len(), key);
            ctx.raw(cmd.as_bytes()).await.expect("XADD failed");
        }

        async fn create_group(ctx: &mut TestContext, key: &str, group: &str) {
            let cmd = format!(
                "*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n$\r\n",
                key.len(),
                key,
                group.len(),
                group
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_destroy_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgd_basic").await;
                    create_group(ctx, "xgd_basic", "testgroup").await;

                    let result = ctx
                        .raw(
                            &XgroupDestroyInput {
                                key: RedisKey::String("xgd_basic".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupDestroyOutput::decode(&result).expect("decode failed");
                    assert!(output.was_destroyed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_destroy_nonexistent() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgd_noexist").await;

                    let result = ctx
                        .raw(
                            &XgroupDestroyInput {
                                key: RedisKey::String("xgd_noexist".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupDestroyOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_destroyed());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_destroy_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            xadd_entry(&mut ctx, "xgd_r2").await;
            create_group(&mut ctx, "xgd_r2", "testgroup").await;

            let result = ctx
                .raw(
                    &XgroupDestroyInput {
                        key: RedisKey::String("xgd_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"));
            ctx.stop().await;
        }
    }
}
