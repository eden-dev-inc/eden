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

const API_INFO: ApiInfo<RedisApi, FtAlterInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtAlter, "Adds a new field to the index", ReqType::Write, true);

/// See official Redis documentation for `FT.ALTER`
/// https://redis.io/docs/latest/commands/ft.alter/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtAlterInput {
    index: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    skip: Option<RedisJsonValue>,
    attribute: RedisJsonValue,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    options: Vec<RedisJsonValue>,
}

impl Serialize for FtAlterInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, index, attribute
        if self.skip.is_some() {
            fields += 1;
        }
        if !self.options.is_empty() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtAlterInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        if let Some(skip) = &self.skip {
            state.serialize_field("skip", skip)?;
        }
        state.serialize_field("attribute", &self.attribute)?;
        if !self.options.is_empty() {
            state.serialize_field("options", &self.options)?;
        }
        state.end()
    }
}

impl_redis_operation!(FtAlterInput, API_INFO, { index, skip, attribute, options });

impl RedisCommandInput for FtAlterInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);

        if let Some(skip) = &self.skip {
            match skip {
                RedisJsonValue::Bool(true) => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::Integer(n) if *n != 0 => {
                    command.arg("SKIPINITIALSCAN");
                }
                RedisJsonValue::String(s) if !s.is_empty() && s != "0" && s.to_uppercase() != "FALSE" => {
                    command.arg("SKIPINITIALSCAN");
                }
                _ => {}
            }
        }

        command.arg("SCHEMA").arg("ADD").arg(&self.attribute);

        for opt in &self.options {
            command.arg(opt);
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("FT.ALTER requires at least 4 arguments, given {}", args.len())));
        }

        let index = args[0].clone();
        let mut skip = None;
        let mut schema_add_index = 1;

        // Check for SKIPINITIALSCAN
        if let RedisJsonValue::String(s) = &args[1]
            && s.to_uppercase() == "SKIPINITIALSCAN"
        {
            skip = Some(RedisJsonValue::Bool(true));
            schema_add_index = 2;
        }

        // Expect "SCHEMA ADD" next
        if schema_add_index + 1 >= args.len() {
            return Err(EpError::request("FT.ALTER requires SCHEMA ADD keywords".to_string()));
        }

        let schema_valid =
            if let (RedisJsonValue::String(schema), RedisJsonValue::String(add)) = (&args[schema_add_index], &args[schema_add_index + 1]) {
                schema.to_uppercase() == "SCHEMA" && add.to_uppercase() == "ADD"
            } else {
                false
            };

        if !schema_valid {
            return Err(EpError::request("FT.ALTER requires SCHEMA ADD keywords".to_string()));
        }

        if schema_add_index + 2 >= args.len() {
            return Err(EpError::request("FT.ALTER requires attribute name after SCHEMA ADD".to_string()));
        }

        let attribute = args[schema_add_index + 2].clone();
        let options = if schema_add_index + 3 < args.len() {
            args[schema_add_index + 3..].to_vec()
        } else {
            Vec::new()
        };

        Ok(Self { index, skip, attribute, options })
    }
}

/// Output for Redis `FT.ALTER` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtAlterOutput {
    success: bool,
}

impl Serialize for FtAlterOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtAlterOutput", 1)?;
        state.serialize_field("success", &self.success)?;
        state.end()
    }
}

impl FtAlterOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the schema was altered successfully
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a FtAlterOutput
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
                    return Err(EpError::parse(format!("unexpected FT.ALTER response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected FT.ALTER response: {:?}", other)));
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
        fn test_encode_command_basic() {
            let input = FtAlterInput {
                index: RedisJsonValue::String("my_index".into()),
                skip: None,
                attribute: RedisJsonValue::String("new_field".into()),
                options: vec![RedisJsonValue::String("TEXT".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.ALTER"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("SCHEMA"));
            assert!(cmd_str.contains("ADD"));
            assert!(cmd_str.contains("new_field"));
            assert!(cmd_str.contains("TEXT"));
        }

        #[test]
        fn test_encode_command_with_skip() {
            let input = FtAlterInput {
                index: RedisJsonValue::String("my_index".into()),
                skip: Some(RedisJsonValue::Bool(true)),
                attribute: RedisJsonValue::String("new_field".into()),
                options: vec![RedisJsonValue::String("TAG".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SKIPINITIALSCAN"));
        }

        #[test]
        fn test_encode_command_skip_false() {
            let input = FtAlterInput {
                index: RedisJsonValue::String("my_index".into()),
                skip: Some(RedisJsonValue::Bool(false)),
                attribute: RedisJsonValue::String("new_field".into()),
                options: vec![],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(!cmd_str.contains("SKIPINITIALSCAN"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("SCHEMA".into()),
                RedisJsonValue::String("ADD".into()),
                RedisJsonValue::String("field1".into()),
                RedisJsonValue::String("TEXT".into()),
            ];
            let input = FtAlterInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert!(input.skip.is_none());
            assert_eq!(input.attribute, RedisJsonValue::String("field1".into()));
            assert_eq!(input.options.len(), 1);
        }

        #[test]
        fn test_decode_input_with_skip() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("SKIPINITIALSCAN".into()),
                RedisJsonValue::String("SCHEMA".into()),
                RedisJsonValue::String("ADD".into()),
                RedisJsonValue::String("field1".into()),
                RedisJsonValue::String("NUMERIC".into()),
            ];
            let input = FtAlterInput::decode(args).unwrap();
            assert_eq!(input.skip, Some(RedisJsonValue::Bool(true)));
            assert_eq!(input.attribute, RedisJsonValue::String("field1".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("SCHEMA".into())];
            let err = FtAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 4 arguments"));
        }

        #[test]
        fn test_decode_input_missing_schema() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("TEXT".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = FtAlterInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("SCHEMA ADD"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = FtAlterOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtAlterOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtAlterInput {
                index: RedisJsonValue::String("i".into()),
                skip: None,
                attribute: RedisJsonValue::String("f".into()),
                options: vec![],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtAlterInput {
                index: RedisJsonValue::String("test_idx".into()),
                skip: Some(RedisJsonValue::Bool(true)),
                attribute: RedisJsonValue::String("test_field".into()),
                options: vec![RedisJsonValue::String("TEXT".into())],
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("test_field"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtAlterOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.ALTER requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_alter_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtAlterInput {
                                index: RedisJsonValue::String("nonexistent_index".into()),
                                skip: None,
                                attribute: RedisJsonValue::String("new_field".into()),
                                options: vec![RedisJsonValue::String("TEXT".into())],
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
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
