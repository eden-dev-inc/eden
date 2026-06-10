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

const API_INFO: ApiInfo<RedisApi, SelectInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Select, "Changes the selected database", ReqType::Read, false);

/// See official Redis documentation for `SELECT`
/// https://redis.io/docs/latest/commands/select/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SelectInput {
    /// The database index to select (0-15 by default)
    index: RedisJsonValue,
}

impl Serialize for SelectInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SelectInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.end()
    }
}

impl_redis_operation!(SelectInput, API_INFO, { index });

impl RedisCommandInput for SelectInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.index);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("SELECT requires 1 argument, given {}", args.len())));
        }

        Ok(Self { index: args[0].clone() })
    }
}

/// Output for Redis SELECT command
///
/// Returns OK if the database was successfully selected.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SelectOutput {
    /// The response, typically "OK"
    result: String,
}

impl SelectOutput {
    pub fn new(result: String) -> Self {
        Self { result }
    }

    /// Get the result string
    pub fn result(&self) -> &str {
        &self.result
    }

    /// Check if the result is OK
    pub fn is_ok(&self) -> bool {
        self.result == "OK"
    }

    /// Decode the Redis protocol response into a SelectOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected SELECT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected SELECT response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for SelectOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SelectOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_integer() {
            let input = SelectInput { index: RedisJsonValue::Integer(0) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n");
        }

        #[test]
        fn test_encode_command_string() {
            let input = SelectInput { index: RedisJsonValue::String("5".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nSELECT\r\n$1\r\n5\r\n");
        }

        #[test]
        fn test_encode_command_double_digit() {
            let input = SelectInput { index: RedisJsonValue::Integer(15) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nSELECT\r\n$2\r\n15\r\n");
        }

        #[test]
        fn test_decode_simple_ok() {
            let output = SelectOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.result(), "OK");
        }

        #[test]
        fn test_decode_bulk_string_ok() {
            let output = SelectOutput::decode(b"$2\r\nOK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_invalid_db() {
            let err = SelectOutput::decode(b"-ERR invalid DB index\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid DB index"));
        }

        #[test]
        fn test_decode_error_out_of_range() {
            let err = SelectOutput::decode(b"-ERR DB index is out of range\r\n").unwrap_err();
            assert!(err.to_string().contains("out of range"));
        }

        #[test]
        fn test_decode_input_valid_integer() {
            let args = vec![RedisJsonValue::Integer(5)];
            let input = SelectInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::Integer(5));
        }

        #[test]
        fn test_decode_input_valid_string() {
            let args = vec![RedisJsonValue::String("10".into())];
            let input = SelectInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("10".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SelectInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)];
            let err = SelectInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SelectInput { index: RedisJsonValue::Integer(0) };
            assert!(input.keys().is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::GetInput;
        use crate::api::lib::string::set::SetInput;
        use crate::api::{key::RedisKey, value::RedisJsonValue};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_db_0() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&SelectInput { index: RedisJsonValue::Integer(0) }.command()).await.expect("raw failed");

                    let output = SelectOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_different_dbs() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Test selecting multiple valid databases
                    for db in [0, 1, 5, 15] {
                        let result = ctx.raw(&SelectInput { index: RedisJsonValue::Integer(db) }.command()).await.expect("raw failed");

                        let output = SelectOutput::decode(&result).expect("decode failed");
                        assert!(output.is_ok(), "SELECT {} should succeed", db);
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_db_isolation() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Select DB 0 and set a key
                    ctx.raw(&SelectInput { index: RedisJsonValue::Integer(0) }.command()).await.expect("SELECT 0 failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("test_key".into()),
                            value: RedisJsonValue::String("db0_value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("SET failed");

                    // Select DB 1 - the key should not exist there
                    ctx.raw(&SelectInput { index: RedisJsonValue::Integer(1) }.command()).await.expect("SELECT 1 failed");

                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("test_key".into()) }.command()).await.expect("GET failed");

                    // Should be null in DB 1
                    use crate::api::lib::string::get::GetOutput;
                    let output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(!output.exists(), "Key should not exist in different database");

                    // Switch back to DB 0 - key should exist
                    ctx.raw(&SelectInput { index: RedisJsonValue::Integer(0) }.command()).await.expect("SELECT 0 failed");

                    let get_result = ctx.raw(&GetInput { key: RedisKey::String("test_key".into()) }.command()).await.expect("GET failed");

                    let output = GetOutput::decode(&get_result).expect("decode failed");
                    assert!(output.exists(), "Key should exist in original database");
                    assert_eq!(output.value(), Some(&RedisJsonValue::String("db0_value".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_invalid_db() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Try to select an invalid database (negative)
                    let result = ctx.raw(&SelectInput { index: RedisJsonValue::Integer(-1) }.command()).await.expect("raw failed");

                    let err = SelectOutput::decode(&result);
                    assert!(err.is_err(), "SELECT -1 should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&SelectInput { index: RedisJsonValue::Integer(0) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK");
            let output = SelectOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_select_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&SelectInput { index: RedisJsonValue::Integer(0) }.command()).await.expect("raw failed");

            let output = SelectOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }
    }
}
