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

const API_INFO: ApiInfo<RedisApi, XsetidInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Xsetid,
    "An internal command for replicating stream values",
    ReqType::Write,
    true,
);

/// Input for Redis `XSETID` command.
///
/// An internal command used for replication to set the last delivered ID
/// of a stream without adding any entries.
///
/// See official Redis documentation for `XSETID`:
/// https://redis.io/docs/latest/commands/xsetid/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XsetidInput {
    /// The key of the stream
    key: RedisKey,
    /// The last ID to set
    last_id: RedisJsonValue,
    /// Optional ENTRIESADDED value (Redis 7.0+)
    #[serde(skip_serializing_if = "Option::is_none")]
    entries_added: Option<RedisJsonValue>,
    /// Optional MAXDELETEDID value (Redis 7.0+)
    #[serde(skip_serializing_if = "Option::is_none")]
    max_deleted_id: Option<RedisJsonValue>,
}

impl Serialize for XsetidInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, last_id
        if self.entries_added.is_some() {
            fields += 1;
        }
        if self.max_deleted_id.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("XsetidInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("last_id", &self.last_id)?;
        if let Some(entries_added) = &self.entries_added {
            state.serialize_field("entries_added", entries_added)?;
        }
        if let Some(max_deleted_id) = &self.max_deleted_id {
            state.serialize_field("max_deleted_id", max_deleted_id)?;
        }
        state.end()
    }
}

impl_redis_operation!(XsetidInput, API_INFO, { key, last_id, entries_added, max_deleted_id });

impl RedisCommandInput for XsetidInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.last_id);

        if let Some(entries_added) = &self.entries_added {
            command.arg("ENTRIESADDED").arg(entries_added);
        }

        if let Some(max_deleted_id) = &self.max_deleted_id {
            command.arg("MAXDELETEDID").arg(max_deleted_id);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("XSETID requires at least 2 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let last_id = args[1].clone();
        let mut entries_added = None;
        let mut max_deleted_id = None;
        let mut i = 2;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                let upper = s.to_uppercase();
                if upper == "ENTRIESADDED" && i + 1 < args.len() {
                    entries_added = Some(args[i + 1].clone());
                    i += 2;
                } else if upper == "MAXDELETEDID" && i + 1 < args.len() {
                    max_deleted_id = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    i += 1;
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, last_id, entries_added, max_deleted_id })
    }
}

/// Output for Redis `XSETID` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XsetidOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl XsetidOutput {
    /// Create a new XsetidOutput
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation succeeded
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into an XsetidOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected XSETID response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected XSETID response: {:?}", other))),
            },
        }
    }
}

impl Serialize for XsetidOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XsetidOutput", 1)?;
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
            let input = XsetidInput {
                key: RedisKey::String("mystream".into()),
                last_id: RedisJsonValue::String("1234567890123-0".into()),
                entries_added: None,
                max_deleted_id: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*3\r\n"));
            assert!(cmd.windows(6).any(|w| w == b"XSETID"));
        }

        #[test]
        fn test_encode_command_with_entries_added() {
            let input = XsetidInput {
                key: RedisKey::String("mystream".into()),
                last_id: RedisJsonValue::String("1234567890123-0".into()),
                entries_added: Some(RedisJsonValue::Integer(100)),
                max_deleted_id: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(12).any(|w| w == b"ENTRIESADDED"));
        }

        #[test]
        fn test_encode_command_with_max_deleted_id() {
            let input = XsetidInput {
                key: RedisKey::String("mystream".into()),
                last_id: RedisJsonValue::String("1234567890123-0".into()),
                entries_added: None,
                max_deleted_id: Some(RedisJsonValue::String("1234567890122-0".into())),
            };
            let cmd = input.command();
            assert!(cmd.windows(12).any(|w| w == b"MAXDELETEDID"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XsetidInput {
                key: RedisKey::String("mystream".into()),
                last_id: RedisJsonValue::String("1234-0".into()),
                entries_added: None,
                max_deleted_id: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mystream".into()), RedisJsonValue::String("1234-0".into())];
            let input = XsetidInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
            assert!(input.entries_added.is_none());
            assert!(input.max_deleted_id.is_none());
        }

        #[test]
        fn test_decode_input_with_options() {
            let args = vec![
                RedisJsonValue::String("mystream".into()),
                RedisJsonValue::String("1234-0".into()),
                RedisJsonValue::String("ENTRIESADDED".into()),
                RedisJsonValue::Integer(100),
            ];
            let input = XsetidInput::decode(args).unwrap();
            assert!(input.entries_added.is_some());
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let err = XsetidInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = XsetidOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XsetidOutput::decode(b"-ERR invalid stream ID\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = XsetidOutput::new(true);
            assert!(output.is_success());
        }

        #[test]
        fn test_output_serialize() {
            let output = XsetidOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("success"));
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::stream::xadd::{Entry, Id, XaddInput, XaddOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        async fn xadd_entry(ctx: &mut TestContext, key: &str, field: &str, value: &str) -> String {
            let result = ctx
                .raw(
                    &XaddInput {
                        key: RedisKey::String(key.into()),
                        no_mk_stream: None,
                        trim: None,
                        id: Id::Auto,
                        entries: vec![Entry {
                            field: RedisJsonValue::String(field.into()),
                            value: RedisJsonValue::String(value.into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("XADD failed");

            XaddOutput::decode(&result).expect("decode XADD failed").id().unwrap().to_string()
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xsetid_basic() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create stream first
                    let _id = xadd_entry(ctx, "xsetid_basic", "f", "v").await;

                    // Set a higher ID
                    let result = ctx
                        .raw(
                            &XsetidInput {
                                key: RedisKey::String("xsetid_basic".into()),
                                last_id: RedisJsonValue::String("9999999999999-0".into()),
                                entries_added: None,
                                max_deleted_id: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = XsetidOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xsetid_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xsetid_r2", "f", "v").await;

            let result = ctx
                .raw(
                    &XsetidInput {
                        key: RedisKey::String("xsetid_r2".into()),
                        last_id: RedisJsonValue::String("9999999999999-0".into()),
                        entries_added: None,
                        max_deleted_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"+OK"), "RESP2 should return +OK");
            let output = XsetidOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xsetid_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xsetid_r3", "f", "v").await;

            let result = ctx
                .raw(
                    &XsetidInput {
                        key: RedisKey::String("xsetid_r3".into()),
                        last_id: RedisJsonValue::String("9999999999999-0".into()),
                        entries_added: None,
                        max_deleted_id: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = XsetidOutput::decode(&result).expect("decode failed");
            assert!(output.is_success());

            ctx.stop().await;
        }
    }
}
