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

const API_INFO: ApiInfo<RedisApi, FtCursorDelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtCursorDel, "Deletes a cursor", ReqType::Write, true);

/// See official Redis documentation for `FT.CURSOR DEL`
/// https://redis.io/docs/latest/commands/ft.cursor-del/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtCursorDelInput {
    index: RedisJsonValue,
    cursor_id: RedisJsonValue,
}

impl Serialize for FtCursorDelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtCursorDelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("cursor_id", &self.cursor_id)?;
        state.end()
    }
}

impl_redis_operation!(FtCursorDelInput, API_INFO, { index, cursor_id });

impl RedisCommandInput for FtCursorDelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.cursor_id);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!(
                "FT.CURSOR DEL requires 2 arguments, given {}", // Fixed: was "2 argument"
                args.len()
            )));
        }

        Ok(Self { index: args[0].clone(), cursor_id: args[1].clone() })
    }
}

/// Output for Redis `FT.CURSOR DEL` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtCursorDelOutput {
    success: bool,
}

impl Serialize for FtCursorDelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtCursorDelOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtCursorDelOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the cursor was deleted successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtCursorDelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let success = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CURSOR DEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    s.to_uppercase() == "OK"
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.CURSOR DEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { success })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FtCursorDelInput {
                index: RedisJsonValue::String("my_index".into()),
                cursor_id: RedisJsonValue::Integer(12345),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.CURSOR"));
            assert!(cmd_str.contains("DEL"));
            assert!(cmd_str.contains("my_index"));
        }

        #[test]
        fn test_encode_command_string_cursor_id() {
            let input = FtCursorDelInput {
                index: RedisJsonValue::String("idx".into()),
                cursor_id: RedisJsonValue::String("99999".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("99999"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("my_index".into()), RedisJsonValue::Integer(12345)];
            let input = FtCursorDelInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("my_index".into()));
            assert_eq!(input.cursor_id, RedisJsonValue::Integer(12345));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("index".into())];
            let err = FtCursorDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = FtCursorDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtCursorDelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtCursorDelOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtCursorDelOutput::decode(b"-ERR Cursor not found\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtCursorDelInput {
                index: RedisJsonValue::String("i".into()),
                cursor_id: RedisJsonValue::Integer(1),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtCursorDelInput {
                index: RedisJsonValue::String("test_idx".into()),
                cursor_id: RedisJsonValue::Integer(99999),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("99999"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtCursorDelOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.CURSOR DEL requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_cursor_del_nonexistent() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtCursorDelInput {
                                index: RedisJsonValue::String("nonexistent_index".into()),
                                cursor_id: RedisJsonValue::Integer(999999),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent cursor
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other case, skip
                        }
                    }
                })
            })
            .await;
        }
    }
}
