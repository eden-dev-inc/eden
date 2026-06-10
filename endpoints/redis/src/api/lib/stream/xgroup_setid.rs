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

// Import StreamId from xgroup_create (or define locally if needed)
use super::xgroup_create::StreamId;

const API_INFO: ApiInfo<RedisApi, XgroupSetidInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::XgroupSetid,
    "Sets the last-delivered ID of a consumer group",
    ReqType::Write,
    true,
);

/// Input for Redis `XGROUP SETID` command.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XgroupSetidInput {
    key: RedisKey,
    group: RedisJsonValue,
    id: StreamId,
    entries_read: Option<RedisJsonValue>,
}

impl Serialize for XgroupSetidInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.entries_read.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XgroupSetidInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("group", &self.group)?;
        state.serialize_field("id", &self.id)?;
        if let Some(entries_read) = &self.entries_read {
            state.serialize_field("entries_read", entries_read)?;
        }
        state.end()
    }
}

impl_redis_operation!(XgroupSetidInput, API_INFO, {key, group, id, entries_read});

impl RedisCommandInput for XgroupSetidInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd("XGROUP");
        command.arg("SETID");
        command.arg(&self.key).arg(&self.group);
        self.id.cmd(&mut command);

        if let Some(entries_read) = &self.entries_read {
            command.arg("ENTRIESREAD").arg(entries_read);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("XGROUP SETID requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let group = args[1].clone();
        let id = if let RedisJsonValue::String(s) = &args[2] {
            if s == "$" {
                StreamId::New
            } else {
                StreamId::Explicit(args[2].clone())
            }
        } else {
            StreamId::Explicit(args[2].clone())
        };

        let mut entries_read = None;
        let mut i = 3;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                if s.to_uppercase() == "ENTRIESREAD" && i + 1 < args.len() {
                    entries_read = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, group, id, entries_read })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XgroupSetidOutput {
    success: bool,
}

impl XgroupSetidOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }
    pub fn success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) if s == b"OK" => Ok(Self { success: true }),
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) if data == b"OK" => Ok(Self { success: true }),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected response: {:?}", other))),
        }
    }
}

impl Serialize for XgroupSetidOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XgroupSetidOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = XgroupSetidInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::New,
                entries_read: None,
            };
            let cmd = input.command();
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$5\r\nSETID\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n$\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_encode_command_explicit_id() {
            let input = XgroupSetidInput {
                key: RedisKey::String("mystream".into()),
                group: RedisJsonValue::String("mygroup".into()),
                id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                entries_read: None,
            };
            let cmd = input.command();
            let expected = b"*5\r\n$6\r\nXGROUP\r\n$5\r\nSETID\r\n$8\r\nmystream\r\n$7\r\nmygroup\r\n$1\r\n0\r\n";
            assert_eq!(cmd.to_vec(), expected);
        }

        #[test]
        fn test_decode_output_ok() {
            let output = XgroupSetidOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_output_error_nogroup() {
            let err = XgroupSetidOutput::decode(b"-NOGROUP No such group\r\n").unwrap_err();
            assert!(err.to_string().contains("NOGROUP"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("mygroup".into()),
                RedisJsonValue::String("$".into()),
            ];
            let input = XgroupSetidInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(matches!(input.id, StreamId::New));
        }

        #[test]
        fn test_decode_input_too_few() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XgroupSetidInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 3"));
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
        async fn test_xgroup_setid_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgs_basic").await;
                    create_group(ctx, "xgs_basic", "testgroup").await;

                    let result = ctx
                        .raw(
                            &XgroupSetidInput {
                                key: RedisKey::String("xgs_basic".into()),
                                group: RedisJsonValue::String("testgroup".into()),
                                id: StreamId::Explicit(RedisJsonValue::String("0".into())),
                                entries_read: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XgroupSetidOutput::decode(&result).expect("decode failed");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_setid_no_group() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xgs_nogroup").await;

                    let result = ctx
                        .raw(
                            &XgroupSetidInput {
                                key: RedisKey::String("xgs_nogroup".into()),
                                group: RedisJsonValue::String("nonexistent".into()),
                                id: StreamId::New,
                                entries_read: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = XgroupSetidOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("NOGROUP"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xgroup_setid_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            xadd_entry(&mut ctx, "xgs_r2").await;
            create_group(&mut ctx, "xgs_r2", "testgroup").await;

            let result = ctx
                .raw(
                    &XgroupSetidInput {
                        key: RedisKey::String("xgs_r2".into()),
                        group: RedisJsonValue::String("testgroup".into()),
                        id: StreamId::New,
                        entries_read: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"));
            ctx.stop().await;
        }
    }
}
