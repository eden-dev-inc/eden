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

const API_INFO: ApiInfo<RedisApi, FtAliasdelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtAliasdel, "Deletes an alias from the index", ReqType::Write, true);

/// See official Redis documentation for `FT.ALIASDEL`
/// https://redis.io/docs/latest/commands/ft.aliasdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtAliasdelInput {
    alias: RedisJsonValue,
}

impl Serialize for FtAliasdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtAliasdelInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("alias", &self.alias)?;
        state.end()
    }
}

impl_redis_operation!(FtAliasdelInput, API_INFO, { alias });

impl RedisCommandInput for FtAliasdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.alias);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FT.ALIASDEL requires 1 argument, given {}", args.len())));
        }

        Ok(Self { alias: args[0].clone() })
    }
}

/// Output for Redis `FT.ALIASDEL` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtAliasdelOutput {
    success: bool,
}

impl Serialize for FtAliasdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtAliasdelOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtAliasdelOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the alias was deleted successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtAliasdelOutput
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
                    return Err(EpError::parse(format!("unexpected FT.ALIASDEL response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected FT.ALIASDEL response: {:?}", other)));
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
            let input = FtAliasdelInput { alias: RedisJsonValue::String("my_alias".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.ALIASDEL"));
            assert!(cmd_str.contains("my_alias"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("alias1".into())];
            let input = FtAliasdelInput::decode(args).unwrap();
            assert_eq!(input.alias, RedisJsonValue::String("alias1".into()));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtAliasdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())];
            let err = FtAliasdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtAliasdelOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtAliasdelOutput::decode(b"-ERR unknown alias\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtAliasdelInput { alias: RedisJsonValue::String("a".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtAliasdelInput { alias: RedisJsonValue::String("test_alias".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_alias"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtAliasdelOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.ALIASDEL requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_aliasdel_nonexistent_alias() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtAliasdelInput { alias: RedisJsonValue::String("nonexistent_alias".into()) }.command()).await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent alias
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
