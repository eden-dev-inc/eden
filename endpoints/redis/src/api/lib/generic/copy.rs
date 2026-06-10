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

const API_INFO: ApiInfo<RedisApi, CopyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Copy,
    "Copies the value of a key to a new key. Returns 1 if key was copied, 0 if key was not copied.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `COPY`
/// https://redis.io/docs/latest/commands/copy/
///
/// Available since Redis 6.2.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, Default)]
#[builder(default)]
pub struct CopyInput {
    pub(crate) source: RedisKey,
    pub(crate) destination: RedisKey,
    /// Target DB index for the destination key
    pub(crate) db: Option<u64>,
    /// If true, removes the destination key before copying
    pub(crate) replace: Option<bool>,
}

impl Serialize for CopyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 3;
        if self.db.is_some() {
            field_count += 1;
        }
        if self.replace.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("CopyInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source", &self.source)?;
        state.serialize_field("destination", &self.destination)?;
        if let Some(db) = &self.db {
            state.serialize_field("db", db)?;
        }
        if let Some(replace) = self.replace {
            state.serialize_field("replace", &replace)?;
        }
        state.end()
    }
}

impl_redis_operation!(CopyInput, API_INFO, { source, destination, db, replace });

impl RedisCommandInput for CopyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source.clone(), self.destination.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.source).arg(&self.destination);

        if let Some(db) = self.db {
            command.arg("DB").arg(db);
        }

        if self.replace == Some(true) {
            command.arg("REPLACE");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!("COPY requires at least 2 arguments, given {}", args.len())));
        }

        if args.len() > 5 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "COPY expects at most 5 arguments (source, dest, DB, db_index, REPLACE), given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let source = args[0].clone().try_into()?;
        let destination = args[1].clone().try_into()?;
        let mut db = None;
        let mut replace = None;

        let mut i = 2;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "DB" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("DB requires a value"));
                        }
                        db = Some(parse_db_index(&args[i + 1])?);
                        i += 2;
                    }
                    "REPLACE" => {
                        replace = Some(true);
                        i += 1;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown COPY option: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::parse("COPY options must be strings"));
                }
            }
        }

        Ok(Self { source, destination, db, replace })
    }
}

/// Parse a RedisJsonValue as a database index
fn parse_db_index(value: &RedisJsonValue) -> Result<u64, EpError> {
    match value {
        RedisJsonValue::Integer(n) => Ok(*n as u64),
        RedisJsonValue::String(s) => s.parse::<u64>().map_err(|_| EpError::parse("DB index must be a valid integer")),
        _ => Err(EpError::parse("DB index must be a number or numeric string")),
    }
}

/// Output for Redis COPY command
///
/// Returns whether the key was successfully copied.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CopyOutput {
    /// True if the key was copied, false if not (e.g., destination exists without REPLACE)
    copied: bool,
}

impl CopyOutput {
    pub fn new(copied: bool) -> Self {
        Self { copied }
    }

    /// Returns true if the key was successfully copied
    pub fn copied(&self) -> bool {
        self.copied
    }

    /// Decode the Redis protocol response into a CopyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let copied = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i == 1,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected COPY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data == 1,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected COPY response: {:?}", other)));
                }
            },
        };

        Ok(Self { copied })
    }
}

impl Serialize for CopyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CopyOutput", 1)?;
        state.serialize_field("copied", &self.copied)?;
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
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                ..Default::default()
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n");
        }

        #[test]
        fn test_encode_command_with_replace() {
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                replace: Some(true),
                ..Default::default()
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$7\r\nREPLACE\r\n");
        }

        #[test]
        fn test_encode_command_with_db() {
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                db: Some(5),
                ..Default::default()
            };
            assert_eq!(input.command().to_vec(), b"*5\r\n$4\r\nCOPY\r\n$3\r\nsrc\r\n$3\r\ndst\r\n$2\r\nDB\r\n$1\r\n5\r\n");
        }

        #[test]
        fn test_encode_command_with_all_options() {
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                db: Some(2),
                replace: Some(true),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("COPY"));
            assert!(cmd_str.contains("src"));
            assert!(cmd_str.contains("dst"));
            assert!(cmd_str.contains("DB"));
            assert!(cmd_str.contains("REPLACE"));
        }

        #[test]
        fn test_encode_command_replace_false_not_included() {
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                replace: Some(false),
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("REPLACE"));
        }

        #[test]
        fn test_decode_success() {
            // RESP2 integer :1\r\n
            let output = CopyOutput::decode(b":1\r\n").unwrap();
            assert!(output.copied());
        }

        #[test]
        fn test_decode_not_copied() {
            // RESP2 integer :0\r\n
            let output = CopyOutput::decode(b":0\r\n").unwrap();
            assert!(!output.copied());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = CopyOutput::decode(b"-ERR no such key\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_both() {
            let input = CopyInput {
                source: RedisKey::String("src".into()),
                destination: RedisKey::String("dst".into()),
                ..Default::default()
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_existing_key() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set up source key
                    ctx.write(SetInput {
                        key: RedisKey::String("copy_src".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &CopyInput {
                                source: RedisKey::String("copy_src".into()),
                                destination: RedisKey::String("copy_dst".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = CopyOutput::decode(&result).expect("decode failed");
                    assert!(output.copied(), "should copy existing key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_nonexistent_source() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &CopyInput {
                                source: RedisKey::String("nonexistent".into()),
                                destination: RedisKey::String("dst".into()),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = CopyOutput::decode(&result).expect("decode failed");
                    assert!(!output.copied(), "should not copy nonexistent key");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_existing_destination_without_replace() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set up both keys
                    ctx.write(SetInput {
                        key: RedisKey::String("src".into()),
                        value: RedisJsonValue::String("src_val".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("dst".into()),
                        value: RedisJsonValue::String("dst_val".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &CopyInput {
                                source: RedisKey::String("src".into()),
                                destination: RedisKey::String("dst".into()),
                                replace: None,
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = CopyOutput::decode(&result).expect("decode failed");
                    assert!(!output.copied(), "should not overwrite without REPLACE");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_with_replace() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set up both keys
                    ctx.write(SetInput {
                        key: RedisKey::String("src_r".into()),
                        value: RedisJsonValue::String("new_val".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("dst_r".into()),
                        value: RedisJsonValue::String("old_val".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx
                        .raw(
                            &CopyInput {
                                source: RedisKey::String("src_r".into()),
                                destination: RedisKey::String("dst_r".into()),
                                replace: Some(true),
                                ..Default::default()
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = CopyOutput::decode(&result).expect("decode failed");
                    assert!(output.copied(), "should overwrite with REPLACE");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_resp2_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("6.2", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp2, Some(version)).await;

                ctx.write(SetInput {
                    key: RedisKey::String("r2src".into()),
                    value: RedisJsonValue::String("val".into()),
                    ..Default::default()
                })
                .await;

                let result = ctx
                    .raw(
                        &CopyInput {
                            source: RedisKey::String("r2src".into()),
                            destination: RedisKey::String("r2dst".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
                let output = CopyOutput::decode(&result).expect("decode failed");
                assert!(output.copied());

                ctx.stop().await;
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_copy_resp3_format() {
            for version in REDIS_VERSIONS {
                if version_is_earlier("6.2", version) {
                    continue;
                }

                let mut ctx = setup(RespVersion::Resp3, Some(version)).await;

                ctx.write(SetInput {
                    key: RedisKey::String("r3src".into()),
                    value: RedisJsonValue::String("val".into()),
                    ..Default::default()
                })
                .await;

                let result = ctx
                    .raw(
                        &CopyInput {
                            source: RedisKey::String("r3src".into()),
                            destination: RedisKey::String("r3dst".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                // RESP3 also uses :1\r\n for integers
                let output = CopyOutput::decode(&result).expect("decode failed");
                assert!(output.copied());

                ctx.stop().await;
            }
        }
    }
}
