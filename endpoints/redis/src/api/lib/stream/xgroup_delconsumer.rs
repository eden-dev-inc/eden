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

const API_INFO: ApiInfo<RedisApi, XgroupDelconsumerInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XgroupDelconsumer,
    "Deletes a consumer from a consumer group",
    ReqType::Write,
    true,
);

/// Input for Redis `XGROUP DELCONSUMER` command.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XgroupDelconsumerInput {
    key: RedisKey,
    group: RedisJsonValue,
    consumer: RedisJsonValue,
}

impl Serialize for XgroupDelconsumerInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupDelconsumerInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("consumer", &self.consumer)?;
        state.end()
    }
}

impl_redis_operation!(XgroupDelconsumerInput, API_INFO, {key, group, consumer});

impl RedisCommandInput for XgroupDelconsumerInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd("XGROUP");
        command.arg("DELCONSUMER");
        command.arg(&self.key).arg(&self.group).arg(&self.consumer);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::parse(format!("XGROUP DELCONSUMER requires exactly 3 arguments, given {}", args.len())));
        }
        Ok(Self {
            key: args[0].clone().try_into()?,
            group: args[1].clone(),
            consumer: args[2].clone(),
        })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XgroupDelconsumerOutput {
    pending_count: i64,
}

impl XgroupDelconsumerOutput {
    pub fn new(pending_count: i64) -> Self {
        Self { pending_count }
    }
    pub fn pending_count(&self) -> i64 {
        self.pending_count
    }
    pub fn had_pending(&self) -> bool {
        self.pending_count > 0
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let pending_count = match frame {
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
        Ok(Self { pending_count })
    }
}

impl Serialize for XgroupDelconsumerOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupDelconsumerOutput", 1)?;
        state.serialize_field("pending_count", &self.pending_count)?;
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
            let input = XgroupDelconsumerInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                consumer: RedisJsonValue::String("myconsumer".into()),
            };
            let cmd = input.command();
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$11\r\nDELCONSUMER\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$10\r\nmyconsumer\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XgroupDelconsumerOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.pending_count(), 0);
            assert!(!output.had_pending());
        }

        #[test]
        fn test_decode_output_with_pending() {
            let output = XgroupDelconsumerOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.pending_count(), 5);
            assert!(output.had_pending());
        }

        #[test]
        fn test_decode_output_error_nogroup() {
            let err = XgroupDelconsumerOutput::decode(b"-NOGROUP No such group\r\n").unwrap_err();
            assert!(err.to_string().contains("NOGROUP"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("myconsumer".into()),
            ];
            let input = XgroupDelconsumerInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XgroupDelconsumerInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
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

        // Helper to create a consumer group starting from ID 0 (includes all existing entries)
        async fn create_group(ctx: &mut TestContext, key: &str, group: &str) {
            let cmd = format!(
                "*5\r\n$6\r\nXGROUP\r\n$6\r\nCREATE\r\n${}\r\n{}\r\n${}\r\n{}\r\n$1\r\n0\r\n",
                key.len(),
                key,
                group.len(),
                group
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        async fn xreadgroup(ctx: &mut TestContext, group: &str, consumer: &str, key: &str) {
            let cmd = format!(
                "*7\r\n$10\r\nXREADGROUP\r\n$5\r\nGROUP\r\n${}\r\n{}\r\n${}\r\n{}\r\n$7\r\nSTREAMS\r\n${}\r\n{}\r\n$1\r\n>\r\n",
                group.len(),
                group,
                consumer.len(),
                consumer,
                key.len(),
                key
            );
            let _ = ctx.raw(cmd.as_bytes()).await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_delconsumer_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgdc_basic").await;
                    create_group(ctx, "xgdc_basic", "testgroup").await;
                    xreadgroup(ctx, "testgroup", "consumer1", "xgdc_basic").await;

                    let result = ctx
                        .raw(
                            &XgroupDelconsumerInput {
                                key: RedisKey::String("xgdc_basic".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                consumer: RedisJsonValue::String("consumer1".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupDelconsumerOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.pending_count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_delconsumer_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            xadd_entry(&mut ctx, "xgdc_r2").await;
            create_group(&mut ctx, "xgdc_r2", "testgroup").await;
            xreadgroup(&mut ctx, "testgroup", "consumer1", "xgdc_r2").await;

            let result = ctx
                .raw(
                    &XgroupDelconsumerInput {
                        key: RedisKey::String("xgdc_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        consumer: RedisJsonValue::String("consumer1".into()),
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
