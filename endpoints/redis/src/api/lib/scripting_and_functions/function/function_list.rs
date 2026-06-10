use crate::api::lib::scripting_and_functions::function::{FunctionInfo, LibraryInfo};
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

const API_INFO: ApiInfo<RedisApi, FunctionListInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FunctionList,
    "Returns information about all libraries",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FUNCTION LIST`
/// https://redis.io/docs/latest/commands/function-list/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionListInput {
    /// Optional library name pattern filter
    library_name_pattern: Option<RedisJsonValue>,
    /// Whether to include library code
    with_code: Option<bool>,
}

impl FunctionListInput {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_pattern(pattern: impl Into<String>) -> Self {
        Self {
            library_name_pattern: Some(RedisJsonValue::String(pattern.into())),
            with_code: None,
        }
    }

    pub fn with_code(mut self) -> Self {
        self.with_code = Some(true);
        self
    }
}

impl Serialize for FunctionListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.library_name_pattern.is_some() {
            fields += 1;
        }
        if self.with_code.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("FunctionListInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(p) = &self.library_name_pattern {
            state.serialize_field("library_name_pattern", p)?;
        }
        if let Some(w) = &self.with_code {
            state.serialize_field("with_code", w)?;
        }
        state.end()
    }
}

impl_redis_operation!(FunctionListInput, API_INFO, {library_name_pattern, with_code});

impl RedisCommandInput for FunctionListInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        if let Some(pattern) = &self.library_name_pattern {
            command.arg("LIBRARYNAME").arg(pattern);
        }
        if self.with_code.unwrap_or(false) {
            command.arg("WITHCODE");
        }
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut library_name_pattern = None;
        let mut with_code = None;
        let mut i = 0;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "LIBRARYNAME" => {
                        if i + 1 < args.len() {
                            library_name_pattern = Some(args[i + 1].clone());
                            i += 2;
                            continue;
                        }
                    }
                    "WITHCODE" => {
                        with_code = Some(true);
                    }
                    _ => {}
                }
            }
            i += 1;
        }
        Ok(Self { library_name_pattern, with_code })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, JsonSchema)]
pub struct FunctionListOutput {
    libraries: Vec<LibraryInfo>,
}

impl FunctionListOutput {
    pub fn new(libraries: Vec<LibraryInfo>) -> Self {
        Self { libraries }
    }

    pub fn libraries(&self) -> &[LibraryInfo] {
        &self.libraries
    }

    pub fn is_empty(&self) -> bool {
        self.libraries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.libraries.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        Self::decode_frame(frame)
    }

    fn decode_frame(frame: DecoderRespFrame) -> Result<Self, EpError> {
        let items = match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::Array(items) => items.into_iter().map(DecoderRespFrame::Resp2).collect::<Vec<DecoderRespFrame>>(),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::Array { data, .. } => data.into_iter().map(DecoderRespFrame::Resp3).collect::<Vec<DecoderRespFrame>>(),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        let mut libraries = Vec::new();
        for item in items {
            libraries.push(Self::parse_library(item)?);
        }
        Ok(Self { libraries })
    }

    fn parse_library(frame: DecoderRespFrame) -> Result<LibraryInfo, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(a)) => a.into_iter().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut v = Vec::new();
                for (k, val) in data {
                    v.push(DecoderRespFrame::Resp3(k));
                    v.push(DecoderRespFrame::Resp3(val));
                }
                v
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.into_iter().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array or map for library")),
        };

        let mut library_name = String::new();
        let mut engine = String::new();
        let mut functions = Vec::new();
        let mut library_code = None;

        let mut i = 0;
        while i + 1 < items.len() {
            let key = Self::extract_string(&items[i])?;
            match key.to_lowercase().as_str() {
                "library_name" => library_name = Self::extract_string(&items[i + 1])?,
                "engine" => engine = Self::extract_string(&items[i + 1])?,
                "library_code" => library_code = Some(Self::extract_string(&items[i + 1])?),
                "functions" => functions = Self::parse_functions(&items[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok(LibraryInfo { library_name, engine, functions, library_code })
    }

    fn parse_functions(frame: &DecoderRespFrame) -> Result<Vec<FunctionInfo>, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(a)) => a.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array for functions")),
        };

        let mut funcs = Vec::new();
        for item in items {
            funcs.push(Self::parse_function(item)?);
        }
        Ok(funcs)
    }

    fn parse_function(frame: DecoderRespFrame) -> Result<FunctionInfo, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(a)) => a.into_iter().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Map { data, .. }) => {
                let mut v = Vec::new();
                for (k, val) in data {
                    v.push(DecoderRespFrame::Resp3(k));
                    v.push(DecoderRespFrame::Resp3(val));
                }
                v
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.into_iter().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array or map for function")),
        };

        let mut name = String::new();
        let mut description = None;
        let mut flags = Vec::new();

        let mut i = 0;
        while i + 1 < items.len() {
            let key = Self::extract_string(&items[i])?;
            match key.to_lowercase().as_str() {
                "name" => name = Self::extract_string(&items[i + 1])?,
                "description" => {
                    if let Ok(s) = Self::extract_string(&items[i + 1])
                        && !s.is_empty()
                    {
                        description = Some(s);
                    }
                }
                "flags" => flags = Self::parse_string_array(&items[i + 1])?,
                _ => {}
            }
            i += 2;
        }

        Ok(FunctionInfo { name, description, flags })
    }

    fn extract_string(frame: &DecoderRespFrame) -> Result<String, EpError> {
        match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::BulkString(d) => String::from_utf8(d.clone()).map_err(EpError::parse),
                Resp2Frame::SimpleString(d) => String::from_utf8(d.clone()).map_err(EpError::parse),
                Resp2Frame::Null => Ok(String::new()),
                _ => Err(EpError::parse("expected string")),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse),
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).map_err(EpError::parse),
                Resp3Frame::Null => Ok(String::new()),
                _ => Err(EpError::parse("expected string")),
            },
        }
    }

    fn parse_string_array(frame: &DecoderRespFrame) -> Result<Vec<String>, EpError> {
        let items: Vec<DecoderRespFrame> = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(a)) => a.iter().cloned().map(DecoderRespFrame::Resp2).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            DecoderRespFrame::Resp3(Resp3Frame::Set { data, .. }) => data.iter().cloned().map(DecoderRespFrame::Resp3).collect(),
            _ => return Err(EpError::parse("expected array")),
        };
        items.iter().map(Self::extract_string).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_args() {
            let input = FunctionListInput::new();
            assert_eq!(input.command().to_vec(), b"*2\r\n$8\r\nFUNCTION\r\n$4\r\nLIST\r\n");
        }

        #[test]
        fn test_encode_command_with_pattern() {
            let input = FunctionListInput::with_pattern("mylib*");
            let cmd = input.command();
            assert!(cmd.windows(11).any(|w| w == b"LIBRARYNAME"));
        }

        #[test]
        fn test_encode_command_with_code() {
            let input = FunctionListInput::new().with_code();
            let cmd = input.command();
            assert!(cmd.windows(8).any(|w| w == b"WITHCODE"));
        }

        #[test]
        fn test_decode_empty_array() {
            let output = FunctionListOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = FunctionListOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = FunctionListInput::decode(vec![]).unwrap();
            assert!(input.library_name_pattern.is_none());
            assert!(input.with_code.is_none());
        }

        #[test]
        fn test_decode_input_with_pattern() {
            let args = vec![RedisJsonValue::String("LIBRARYNAME".into()), RedisJsonValue::String("test*".into())];
            let input = FunctionListInput::decode(args).unwrap();
            assert_eq!(input.library_name_pattern, Some(RedisJsonValue::String("test*".into())));
        }

        #[test]
        fn test_decode_input_with_code() {
            let args = vec![RedisJsonValue::String("WITHCODE".into())];
            let input = FunctionListInput::decode(args).unwrap();
            assert_eq!(input.with_code, Some(true));
        }

        #[test]
        fn test_keys_returns_empty() {
            assert!(FunctionListInput::new().keys().is_empty());
        }

        #[test]
        fn test_kind() {
            assert_eq!(RedisCommandInput::kind(&FunctionListInput::new()), RedisApi::FunctionList);
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
        async fn test_function_list_empty() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
                    let result = ctx.raw(&FunctionListInput::new().command()).await.expect("raw");
                    let output = FunctionListOutput::decode(&result).expect("decode");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_list_with_library() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
                    let lua = "#!lua name=listlib\nredis.register_function('listfunc', function(k,a) return 1 end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua.len(), lua);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load");

                    let result = ctx.raw(&FunctionListInput::new().command()).await.expect("raw");
                    let output = FunctionListOutput::decode(&result).expect("decode");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.libraries()[0].library_name, "listlib");
                    assert_eq!(output.libraries()[0].functions.len(), 1);
                    assert_eq!(output.libraries()[0].functions[0].name, "listfunc");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_list_with_pattern() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");

                    let lua1 = "#!lua name=matchlib\nredis.register_function('f1', function(k,a) return 1 end)";
                    let load1 = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua1.len(), lua1);
                    ctx.raw(load1.as_bytes()).await.expect("load1");

                    let lua2 = "#!lua name=otherlib\nredis.register_function('f2', function(k,a) return 2 end)";
                    let load2 = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua2.len(), lua2);
                    ctx.raw(load2.as_bytes()).await.expect("load2");

                    let result = ctx.raw(&FunctionListInput::with_pattern("match*").command()).await.expect("raw");
                    let output = FunctionListOutput::decode(&result).expect("decode");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.libraries()[0].library_name, "matchlib");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
            let result = ctx.raw(&FunctionListInput::new().command()).await.expect("raw");
            assert!(result.starts_with(b"*"));
            let output = FunctionListOutput::decode(&result).expect("decode");
            assert!(output.is_empty());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_list_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");
            let result = ctx.raw(&FunctionListInput::new().command()).await.expect("raw");
            let output = FunctionListOutput::decode(&result).expect("decode");
            assert!(output.is_empty());
            ctx.stop().await;
        }
    }
}
