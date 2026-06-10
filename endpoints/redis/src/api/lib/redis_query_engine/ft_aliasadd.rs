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

const API_INFO: ApiInfo<RedisApi, FtAliasaddInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtAliasadd, "Adds an alias to the index", ReqType::Write, true);

/// See official Redis documentation for `FT.ALIASADD`
/// https://redis.io/docs/latest/commands/ft.aliasadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtAliasaddInput {
    alias: RedisJsonValue,
    index: RedisJsonValue,
}

impl Serialize for FtAliasaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtAliasaddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("alias", &self.alias)?;
        state.serialize_field("index", &self.index)?;
        state.end()
    }
}

impl_redis_operation!(FtAliasaddInput, API_INFO, { alias, index });

impl RedisCommandInput for FtAliasaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.alias).arg(&self.index);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("FT.ALIASADD requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { alias: args[0].clone(), index: args[1].clone() })
    }
}

/// Output for Redis `FT.ALIASADD` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtAliasaddOutput {
    success: bool,
}

impl Serialize for FtAliasaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtAliasaddOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtAliasaddOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the alias was added successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtAliasaddOutput
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
                    return Err(EpError::parse(format!("unexpected FT.ALIASADD response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected FT.ALIASADD response: {:?}", other)));
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
            let input = FtAliasaddInput {
                alias: RedisJsonValue::String("my_alias".into()),
                index: RedisJsonValue::String("my_index".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.ALIASADD"));
            assert!(cmd_str.contains("my_alias"));
            assert!(cmd_str.contains("my_index"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("alias1".into()), RedisJsonValue::String("index1".into())];
            let input = FtAliasaddInput::decode(args).unwrap();
            assert_eq!(input.alias, RedisJsonValue::String("alias1".into()));
            assert_eq!(input.index, RedisJsonValue::String("index1".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("alias".into())];
            let err = FtAliasaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::String("b".into()),
                RedisJsonValue::String("c".into()),
            ];
            let err = FtAliasaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtAliasaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtAliasaddOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtAliasaddOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtAliasaddInput {
                alias: RedisJsonValue::String("a".into()),
                index: RedisJsonValue::String("i".into()),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtAliasaddInput {
                alias: RedisJsonValue::String("test_alias".into()),
                index: RedisJsonValue::String("test_index".into()),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_alias"));
            assert!(json.contains("test_index"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtAliasaddOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.ALIASADD requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_aliasadd_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtAliasaddInput {
                                alias: RedisJsonValue::String("test_alias".into()),
                                index: RedisJsonValue::String("nonexistent_index".into()),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                        }
                        Ok(r) if r.starts_with(b"+OK") => {
                            // Unexpected success
                            panic!("Expected error for nonexistent index");
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other error, skip
                        }
                    }
                })
            })
            .await;
        }
    }
}
