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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, ModuleListInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::ModuleList, "Returns all loaded modules", ReqType::Read, true);

/// See official Redis documentation for `MODULE LIST`
/// https://redis.io/docs/latest/commands/module-list/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ModuleListInput {}

impl Serialize for ModuleListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleListInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(ModuleListInput, API_INFO);

impl RedisCommandInput for ModuleListInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "MODULE LIST expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Information about a loaded module
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema, Serialize)]
pub struct ModuleInfo {
    /// Module name
    pub name: String,
    /// Module version
    pub ver: i64,
    /// Module path (if available)
    pub path: Option<String>,
    /// Module arguments (if available)
    pub args: Option<Vec<String>>,
}

/// Output for Redis MODULE LIST command
///
/// Returns a list of loaded modules with their details.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ModuleListOutput {
    /// List of loaded modules
    modules: Vec<ModuleInfo>,
}

impl ModuleListOutput {
    pub fn new(modules: Vec<ModuleInfo>) -> Self {
        Self { modules }
    }

    /// Get the list of modules
    pub fn modules(&self) -> &[ModuleInfo] {
        &self.modules
    }

    /// Get the count of loaded modules
    pub fn count(&self) -> usize {
        self.modules.len()
    }

    /// Check if any modules are loaded
    pub fn has_modules(&self) -> bool {
        !self.modules.is_empty()
    }

    /// Find a module by name
    pub fn find(&self, name: &str) -> Option<&ModuleInfo> {
        self.modules.iter().find(|m| m.name == name)
    }

    /// Decode the Redis protocol response into a ModuleListOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let modules = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::parse_resp2(resp2_frame)?,
            DecoderRespFrame::Resp3(resp3_frame) => Self::parse_resp3(resp3_frame)?,
        };

        Ok(Self { modules })
    }

    fn parse_resp2(frame: Resp2Frame) -> Result<Vec<ModuleInfo>, EpError> {
        match frame {
            Resp2Frame::Array(items) => {
                let mut modules = Vec::new();
                for item in items {
                    if let Resp2Frame::Array(module_data) = item
                        && let Some(info) = Self::parse_resp2_module(module_data)?
                    {
                        modules.push(info);
                    }
                }
                Ok(modules)
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected MODULE LIST response: {:?}", other))),
        }
    }

    fn parse_resp2_module(data: Vec<Resp2Frame>) -> Result<Option<ModuleInfo>, EpError> {
        let mut name = String::new();
        let mut ver: i64 = 0;
        let mut path = None;
        let mut args = None;

        let mut iter = data.into_iter();
        while let Some(key_frame) = iter.next() {
            let key = match key_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                _ => continue,
            };

            if let Some(value_frame) = iter.next() {
                match key.as_str() {
                    "name" => {
                        if let Resp2Frame::BulkString(bytes) = value_frame {
                            name = String::from_utf8(bytes).map_err(EpError::parse)?;
                        }
                    }
                    "ver" => {
                        if let Resp2Frame::Integer(v) = value_frame {
                            ver = v;
                        }
                    }
                    "path" => {
                        if let Resp2Frame::BulkString(bytes) = value_frame {
                            path = Some(String::from_utf8(bytes).map_err(EpError::parse)?);
                        }
                    }
                    "args" => {
                        if let Resp2Frame::Array(arr) = value_frame {
                            let mut arg_list = Vec::new();
                            for a in arr {
                                if let Resp2Frame::BulkString(bytes) = a {
                                    arg_list.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                                }
                            }
                            if !arg_list.is_empty() {
                                args = Some(arg_list);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }

        if name.is_empty() {
            return Ok(None);
        }

        Ok(Some(ModuleInfo { name, ver, path, args }))
    }

    fn parse_resp3(frame: Resp3Frame) -> Result<Vec<ModuleInfo>, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut modules = Vec::new();
                for item in data {
                    if let Some(info) = Self::parse_resp3_module(item)? {
                        modules.push(info);
                    }
                }
                Ok(modules)
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected MODULE LIST response: {:?}", other))),
        }
    }

    fn parse_resp3_module(frame: Resp3Frame) -> Result<Option<ModuleInfo>, EpError> {
        let map = match frame {
            Resp3Frame::Map { data, .. } => data.into_iter().collect::<Vec<(_, _)>>(),
            Resp3Frame::Array { data, .. } => convert_array_to_map(data),
            _ => return Ok(None),
        };

        fn convert_array_to_map(data: Vec<Resp3Frame>) -> Vec<(Resp3Frame, Resp3Frame)> {
            let mut pairs = Vec::new();
            let mut iter = data.into_iter();
            while let Some(k) = iter.next() {
                if let Some(v) = iter.next() {
                    pairs.push((k, v));
                }
            }
            pairs
        }

        let mut name = String::new();
        let mut ver: i64 = 0;
        let mut path = None;
        let mut args = None;

        for (key_frame, value_frame) in map {
            let key = match key_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                _ => continue,
            };

            match key.as_str() {
                "name" => {
                    if let Resp3Frame::BlobString { data, .. } = value_frame {
                        name = String::from_utf8(data).map_err(EpError::parse)?;
                    }
                }
                "ver" => {
                    if let Resp3Frame::Number { data, .. } = value_frame {
                        ver = data;
                    }
                }
                "path" => {
                    if let Resp3Frame::BlobString { data, .. } = value_frame {
                        path = Some(String::from_utf8(data).map_err(EpError::parse)?);
                    }
                }
                "args" => {
                    if let Resp3Frame::Array { data: arr, .. } = value_frame {
                        let mut arg_list = Vec::new();
                        for a in arr {
                            if let Resp3Frame::BlobString { data, .. } = a {
                                arg_list.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                        }
                        if !arg_list.is_empty() {
                            args = Some(arg_list);
                        }
                    }
                }
                _ => {}
            }
        }

        if name.is_empty() {
            return Ok(None);
        }

        Ok(Some(ModuleInfo { name, ver, path, args }))
    }
}

impl Serialize for ModuleListOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ModuleListOutput", 1)?;
        state.serialize_field("modules", &self.modules)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = ModuleListInput {};
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nMODULE\r\n$4\r\nLIST\r\n");
        }

        #[test]
        fn test_decode_empty_array() {
            let output = ModuleListOutput::decode(b"*0\r\n").unwrap();
            assert!(!output.has_modules());
            assert_eq!(output.count(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ModuleListOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = ModuleListInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_extra_args_warns() {
            let input = ModuleListInput::decode(vec![RedisJsonValue::String("extra".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ModuleListInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ModuleListInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::ModuleList);
        }

        #[test]
        fn test_find_module() {
            let modules = vec![
                ModuleInfo {
                    name: "ReJSON".to_string(),
                    ver: 20000,
                    path: None,
                    args: None,
                },
                ModuleInfo {
                    name: "search".to_string(),
                    ver: 20600,
                    path: None,
                    args: None,
                },
            ];
            let output = ModuleListOutput::new(modules);
            assert!(output.find("ReJSON").is_some());
            assert!(output.find("nonexistent").is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_module_list_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&ModuleListInput {}.command()).await.expect("raw failed");

                    let output = ModuleListOutput::decode(&result).expect("decode failed");
                    // Standard Redis may have no modules loaded
                    let _ = output.count();
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_module_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx.raw(&ModuleListInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = ModuleListOutput::decode(&result).expect("decode failed");
            let _ = output.modules();

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_module_list_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            let result = ctx.raw(&ModuleListInput {}.command()).await.expect("raw failed");

            let output = ModuleListOutput::decode(&result).expect("decode failed");
            let _ = output.modules();

            ctx.stop().await;
        }
    }
}
