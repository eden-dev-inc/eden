use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FunctionLoadInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FunctionLoad, "Creates a library", ReqType::Write, true);

/// See official Redis documentation for `FUNCTION LOAD`
/// https://redis.io/docs/latest/commands/function-load/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionLoadInput {
    replace: Option<bool>,
    function_code: RedisJsonValue,
}

impl FunctionLoadInput {
    pub fn new(function_code: impl Into<String>) -> Self {
        Self {
            replace: None,
            function_code: RedisJsonValue::String(function_code.into()),
        }
    }

    pub fn with_replace(function_code: impl Into<String>) -> Self {
        Self {
            replace: Some(true),
            function_code: RedisJsonValue::String(function_code.into()),
        }
    }

    pub fn function_code(&self) -> &RedisJsonValue {
        &self.function_code
    }

    pub fn is_replace(&self) -> bool {
        self.replace.unwrap_or(false)
    }
}

impl Serialize for FunctionLoadInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.replace.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("FunctionLoadInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(replace) = &self.replace {
            state.serialize_field("replace", replace)?;
        }
        state.serialize_field("function_code", &self.function_code)?;
        state.end()
    }
}

impl_redis_operation!(FunctionLoadInput, API_INFO, {replace, function_code});

impl RedisCommandInput for FunctionLoadInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if self.replace.unwrap_or(false) {
            command.arg("REPLACE");
        }
        command.arg(&self.function_code);
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("FUNCTION LOAD requires at least 1 argument".to_string()));
        }
        let mut replace = None;
        let mut idx = 0;
        if let RedisJsonValue::String(s) = &args[0]
            && s.to_uppercase() == "REPLACE"
        {
            replace = Some(true);
            idx = 1;
        }
        if idx >= args.len() {
            return Err(EpError::request("FUNCTION LOAD requires function code".to_string()));
        }
        Ok(Self { replace, function_code: args[idx].clone() })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FunctionLoadOutput {
    library_name: String,
}

impl FunctionLoadOutput {
    pub fn new(library_name: String) -> Self {
        Self { library_name }
    }

    pub fn library_name(&self) -> &str {
        &self.library_name
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let library_name = match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::BulkString(d) => String::from_utf8(d).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(d) => String::from_utf8(d).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };
        Ok(Self { library_name })
    }
}

impl Serialize for FunctionLoadOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionLoadOutput", 1)?;
        state.serialize_field("library_name", &self.library_name)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_replace() {
            let input = FunctionLoadInput::new("#!lua name=mylib\nreturn 1");
            let cmd = input.command();
            assert!(!cmd.windows(7).any(|w| w == b"REPLACE"));
        }

        #[test]
        fn test_encode_command_with_replace() {
            let input = FunctionLoadInput::with_replace("#!lua name=mylib\nreturn 1");
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"REPLACE"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = FunctionLoadOutput::decode(b"$5\r\nmylib\r\n").unwrap();
            assert_eq!(output.library_name(), "mylib");
        }

        #[test]
        fn test_decode_error() {
            let err = FunctionLoadOutput::decode(b"-ERR Library already exists\r\n").unwrap_err();
            assert!(err.to_string().contains("already exists"));
        }

        #[test]
        fn test_decode_input_code_only() {
            let args = vec![RedisJsonValue::String("#!lua name=t\nreturn 1".into())];
            let input = FunctionLoadInput::decode(args).unwrap();
            assert!(!input.is_replace());
        }

        #[test]
        fn test_decode_input_with_replace() {
            let args = vec![
                RedisJsonValue::String("REPLACE".into()),
                RedisJsonValue::String("#!lua name=t\nreturn 1".into()),
            ];
            let input = FunctionLoadInput::decode(args).unwrap();
            assert!(input.is_replace());
        }

        #[test]
        fn test_decode_input_no_args() {
            let err = FunctionLoadInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires"));
        }

        #[test]
        fn test_keys_returns_empty() {
            assert!(FunctionLoadInput::new("code").keys().is_empty());
        }

        #[test]
        fn test_kind() {
            assert_eq!(RedisCommandInput::kind(&FunctionLoadInput::new("c")), RedisApi::FunctionLoad);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        const MIN_VERSION: &str = "7";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_load_simple() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
                    let lua = "#!lua name=loadtest\nredis.register_function('f', function(k,a) return 1 end)";
                    let result = ctx.raw(&FunctionLoadInput::new(lua).command()).await.expect("raw");
                    let output = FunctionLoadOutput::decode(&result).expect("decode");
                    assert_eq!(output.library_name(), "loadtest");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_load_with_replace() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
                    let lua1 = "#!lua name=rlib\nredis.register_function('f', function(k,a) return 1 end)";
                    ctx.raw(&FunctionLoadInput::new(lua1).command()).await.expect("load1");
                    let lua2 = "#!lua name=rlib\nredis.register_function('f', function(k,a) return 2 end)";
                    let result = ctx.raw(&FunctionLoadInput::with_replace(lua2).command()).await.expect("raw");
                    let output = FunctionLoadOutput::decode(&result).expect("decode");
                    assert_eq!(output.library_name(), "rlib");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_load_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
            let lua = "#!lua name=r2lib\nredis.register_function('f', function(k,a) return 1 end)";
            let result = ctx.raw(&FunctionLoadInput::new(lua).command()).await.expect("raw");
            assert!(result.starts_with(b"$"));
            let output = FunctionLoadOutput::decode(&result).expect("decode");
            assert_eq!(output.library_name(), "r2lib");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_load_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
            let lua = "#!lua name=r3lib\nredis.register_function('f', function(k,a) return 1 end)";
            let result = ctx.raw(&FunctionLoadInput::new(lua).command()).await.expect("raw");
            let output = FunctionLoadOutput::decode(&result).expect("decode");
            assert_eq!(output.library_name(), "r3lib");
            ctx.stop().await;
        }
    }
}
