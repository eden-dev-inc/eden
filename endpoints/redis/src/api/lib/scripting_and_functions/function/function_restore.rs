use crate::api::lib::redis_query_engine::RestorePolicy;
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

const API_INFO: ApiInfo<RedisApi, FunctionRestoreInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FunctionRestore,
    "Restores all libraries from a payload",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `FUNCTION RESTORE`
/// https://redis.io/docs/latest/commands/function-restore/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FunctionRestoreInput {
    serialized_value: RedisJsonValue,
    policy: Option<RestorePolicy>,
}

impl FunctionRestoreInput {
    pub fn new(serialized_value: Vec<u8>) -> Self {
        Self {
            serialized_value: RedisJsonValue::Bytes(serialized_value),
            policy: None,
        }
    }

    pub fn with_policy(serialized_value: Vec<u8>, policy: RestorePolicy) -> Self {
        Self {
            serialized_value: RedisJsonValue::Bytes(serialized_value),
            policy: Some(policy),
        }
    }

    pub fn serialized_value(&self) -> &RedisJsonValue {
        &self.serialized_value
    }

    pub fn policy(&self) -> Option<&RestorePolicy> {
        self.policy.as_ref()
    }
}

impl Serialize for FunctionRestoreInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.policy.is_some() {
            fields += 1;
        }
        let mut state = serializer.serialize_struct("FunctionRestoreInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("serialized_value", &self.serialized_value)?;
        if let Some(policy) = &self.policy {
            state.serialize_field("policy", policy)?;
        }
        state.end()
    }
}

impl_redis_operation!(FunctionRestoreInput, API_INFO, {serialized_value, policy});

impl RedisCommandInput for FunctionRestoreInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.serialized_value);
        if let Some(policy) = &self.policy {
            match policy {
                RestorePolicy::APPEND => command.arg("APPEND"),
                RestorePolicy::FLUSH => command.arg("FLUSH"),
                RestorePolicy::REPLACE => command.arg("REPLACE"),
            };
        }
        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("FUNCTION RESTORE requires at least 1 argument".to_string()));
        }
        let serialized_value = args[0].clone();
        let mut policy = None;
        if args.len() >= 2
            && let RedisJsonValue::String(s) = &args[1]
        {
            policy = match s.to_uppercase().as_str() {
                "APPEND" => Some(RestorePolicy::APPEND),
                "FLUSH" => Some(RestorePolicy::FLUSH),
                "REPLACE" => Some(RestorePolicy::REPLACE),
                other => {
                    return Err(EpError::request(format!(
                        "FUNCTION RESTORE invalid policy '{}', expected APPEND, FLUSH, or REPLACE",
                        other
                    )));
                }
            };
        }
        Ok(Self { serialized_value, policy })
    }
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FunctionRestoreOutput {
    success: bool,
}

impl FunctionRestoreOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    pub fn success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;
        let success = match frame {
            DecoderRespFrame::Resp2(f) => match f {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(f) => match f {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map(|s| s.to_uppercase() == "OK").unwrap_or(false),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };
        Ok(Self { success })
    }
}

impl Serialize for FunctionRestoreOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FunctionRestoreOutput", 1)?;
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
        fn test_encode_command_no_policy() {
            let input = FunctionRestoreInput::new(vec![1, 2, 3]);
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"RESTORE"));
            assert!(!cmd.windows(6).any(|w| w == b"APPEND"));
        }

        #[test]
        fn test_encode_command_with_flush() {
            let input = FunctionRestoreInput::with_policy(vec![1, 2, 3], RestorePolicy::FLUSH);
            let cmd = input.command();
            assert!(cmd.windows(5).any(|w| w == b"FLUSH"));
        }

        #[test]
        fn test_encode_command_with_replace() {
            let input = FunctionRestoreInput::with_policy(vec![1, 2, 3], RestorePolicy::REPLACE);
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"REPLACE"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = FunctionRestoreOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error() {
            let err = FunctionRestoreOutput::decode(b"-ERR invalid payload\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid"));
        }

        #[test]
        fn test_decode_input_no_policy() {
            let args = vec![RedisJsonValue::Bytes(vec![1, 2, 3])];
            let input = FunctionRestoreInput::decode(args).unwrap();
            assert!(input.policy.is_none());
        }

        #[test]
        fn test_decode_input_with_policy() {
            let args = vec![RedisJsonValue::Bytes(vec![1, 2, 3]), RedisJsonValue::String("REPLACE".into())];
            let input = FunctionRestoreInput::decode(args).unwrap();
            assert_eq!(input.policy, Some(RestorePolicy::REPLACE));
        }

        #[test]
        fn test_decode_input_invalid_policy() {
            let args = vec![RedisJsonValue::Bytes(vec![1, 2, 3]), RedisJsonValue::String("INVALID".into())];
            let err = FunctionRestoreInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("invalid policy"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let err = FunctionRestoreInput::decode(vec![]).unwrap_err();
            assert!(err.to_string().contains("requires"));
        }

        #[test]
        fn test_keys_returns_empty() {
            assert!(FunctionRestoreInput::new(vec![]).keys().is_empty());
        }

        #[test]
        fn test_kind() {
            assert_eq!(RedisCommandInput::kind(&FunctionRestoreInput::new(vec![])), RedisApi::FunctionRestore);
        }

        #[test]
        fn test_policy_enum() {
            assert_eq!(RestorePolicy::default(), RestorePolicy::APPEND);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::RedisCommandOutput;
        use crate::api::lib::scripting_and_functions::function::function_dump::{FunctionDumpInput, FunctionDumpOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        const MIN_VERSION: &str = "7";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_restore_roundtrip() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");

                    // Load a library
                    let lua = "#!lua name=restorelib\nredis.register_function('f', function(k,a) return 42 end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua.len(), lua);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load");

                    // Dump
                    let dump_result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("dump");
                    let dump = FunctionDumpOutput::decode(&dump_result).expect("decode dump");
                    let payload = dump.payload().to_vec();

                    // Flush
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush2");

                    // Restore
                    let result = ctx.raw(&FunctionRestoreInput::new(payload).command()).await.expect("restore");
                    let output = FunctionRestoreOutput::decode(&result).expect("decode");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_restore_with_flush_policy() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");

                    let lua = "#!lua name=flushlib\nredis.register_function('f', function(k,a) return 1 end)";
                    let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua.len(), lua);
                    ctx.raw(load_cmd.as_bytes()).await.expect("load");

                    let dump_result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("dump");
                    let dump = FunctionDumpOutput::decode(&dump_result).expect("decode dump");
                    let payload = dump.payload().to_vec();

                    // Load different library
                    let lua2 = "#!lua name=otherlib\nredis.register_function('g', function(k,a) return 2 end)";
                    let load_cmd2 = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua2.len(), lua2);
                    ctx.raw(load_cmd2.as_bytes()).await.expect("load2");

                    // Restore with FLUSH should replace all
                    let result =
                        ctx.raw(&FunctionRestoreInput::with_policy(payload, RestorePolicy::FLUSH).command()).await.expect("restore");
                    let output = FunctionRestoreOutput::decode(&result).expect("decode");
                    assert!(output.success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_restore_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");

            let lua = "#!lua name=r2lib\nredis.register_function('f', function(k,a) return 1 end)";
            let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua.len(), lua);
            ctx.raw(load_cmd.as_bytes()).await.expect("load");

            let dump_result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("dump");
            let dump = FunctionDumpOutput::decode(&dump_result).expect("decode dump");
            let payload = dump.payload().to_vec();

            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush2");

            let result = ctx.raw(&FunctionRestoreInput::new(payload).command()).await.expect("restore");
            assert_eq!(&result[..], b"+OK\r\n");
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_function_restore_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;
            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush");

            let lua = "#!lua name=r3lib\nredis.register_function('f', function(k,a) return 1 end)";
            let load_cmd = format!("*3\r\n$8\r\nFUNCTION\r\n$4\r\nLOAD\r\n${}\r\n{}\r\n", lua.len(), lua);
            ctx.raw(load_cmd.as_bytes()).await.expect("load");

            let dump_result = ctx.raw(&FunctionDumpInput::new().command()).await.expect("dump");
            let dump = FunctionDumpOutput::decode(&dump_result).expect("decode dump");
            let payload = dump.payload().to_vec();

            ctx.raw(b"*2\r\n$8\r\nFUNCTION\r\n$5\r\nFLUSH\r\n").await.expect("flush2");

            let result = ctx.raw(&FunctionRestoreInput::new(payload).command()).await.expect("restore");
            assert_eq!(&result[..], b"+OK\r\n");
            ctx.stop().await;
        }
    }
}
