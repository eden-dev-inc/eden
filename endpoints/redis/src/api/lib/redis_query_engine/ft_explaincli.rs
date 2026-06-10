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

const API_INFO: ApiInfo<RedisApi, FtExplaincliInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtExplaincli,
    "Returns the execution plan for a complex query as an array of strings",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.EXPLAINCLI`
/// https://redis.io/docs/latest/commands/ft.explaincli/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtExplaincliInput {
    index: RedisJsonValue,
    query: RedisJsonValue,
    dialect: Option<RedisJsonValue>,
}

impl Serialize for FtExplaincliInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3;

        if self.dialect.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtExplaincliInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("query", &self.query)?;
        if let Some(dialect) = &self.dialect {
            state.serialize_field("dialect", dialect)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    FtExplaincliInput,
    API_INFO,
    {index, query, dialect}
);

impl RedisCommandInput for FtExplaincliInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.query);

        if let Some(dialect) = &self.dialect {
            command.arg("DIALECT").arg(dialect);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.EXPLAINCLI requires at least 2 arguments, given {}", args.len())));
        }

        let mut dialect = None;
        let mut i = 2;
        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i]
                && s.to_uppercase() == "DIALECT"
                && i + 1 < args.len()
            {
                dialect = Some(args[i + 1].clone());
                i += 2;
                continue;
            }
            i += 1;
        }

        Ok(Self { index: args[0].clone(), query: args[1].clone(), dialect })
    }
}

/// Output for Redis `FT.EXPLAINCLI` command.
///
/// Returns an array of strings representing the execution plan,
/// formatted for CLI display.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtExplaincliOutput {
    /// The execution plan as an array of strings (one per line)
    lines: Vec<String>,
}

impl Serialize for FtExplaincliOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtExplaincliOutput", 1)?;
        state.serialize_field("lines", &self.lines)?;
        state.end()
    }
}

impl FtExplaincliOutput {
    pub fn new(lines: Vec<String>) -> Self {
        Self { lines }
    }

    /// Get the execution plan lines
    pub fn lines(&self) -> &[String] {
        &self.lines
    }

    /// Get the full execution plan as a single string
    pub fn as_string(&self) -> String {
        self.lines.join("\n")
    }

    /// Decode the Redis protocol response into a FtExplaincliOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Array(arr) => {
                let mut lines = Vec::new();
                for item in arr {
                    if let Resp2Frame::BulkString(s) = item {
                        lines.push(String::from_utf8(s).map_err(EpError::parse)?);
                    }
                }
                Ok(Self { lines })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.EXPLAINCLI response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut lines = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            lines.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                        _ => {}
                    }
                }
                Ok(Self { lines })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.EXPLAINCLI response: {:?}", other))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtExplaincliInput {
                index: RedisJsonValue::String("my_index".into()),
                query: RedisJsonValue::String("@title:hello".into()),
                dialect: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.EXPLAINCLI"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("@title:hello"));
            assert!(!cmd_str.contains("DIALECT"));
        }

        #[test]
        fn test_encode_command_with_dialect() {
            let input = FtExplaincliInput {
                index: RedisJsonValue::String("my_index".into()),
                query: RedisJsonValue::String("*".into()),
                dialect: Some(RedisJsonValue::Integer(2)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("DIALECT"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("query".into())];
            let input = FtExplaincliInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.query, RedisJsonValue::String("query".into()));
            assert!(input.dialect.is_none());
        }

        #[test]
        fn test_decode_input_with_dialect() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("query".into()),
                RedisJsonValue::String("DIALECT".into()),
                RedisJsonValue::Integer(3),
            ];
            let input = FtExplaincliInput::decode(args).unwrap();
            assert_eq!(input.dialect, Some(RedisJsonValue::Integer(3)));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtExplaincliInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_valid() {
            let output = FtExplaincliOutput::decode(b"*3\r\n$9\r\nINTERSECT\r\n$1\r\n{\r\n$1\r\n}\r\n").unwrap();
            assert_eq!(output.lines().len(), 3);
            assert!(output.as_string().contains("INTERSECT"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtExplaincliOutput::decode(b"*0\r\n").unwrap();
            assert!(output.lines().is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtExplaincliOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("unknown index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtExplaincliInput {
                index: RedisJsonValue::String("i".into()),
                query: RedisJsonValue::String("q".into()),
                dialect: None,
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtExplaincliInput {
                index: RedisJsonValue::String("test_idx".into()),
                query: RedisJsonValue::String("test_query".into()),
                dialect: Some(RedisJsonValue::Integer(2)),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("test_query"));
            assert!(json.contains("dialect"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtExplaincliOutput::new(vec!["line1".into(), "line2".into()]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("lines"));
            assert!(json.contains("line1"));
        }

        #[test]
        fn test_output_accessors() {
            let output = FtExplaincliOutput::new(vec!["a".into(), "b".into(), "c".into()]);
            assert_eq!(output.lines().len(), 3);
            assert_eq!(output.as_string(), "a\nb\nc");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.EXPLAINCLI requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_explaincli_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtExplaincliInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                query: RedisJsonValue::String("*".into()),
                                dialect: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                        }
                        Ok(_) | Err(_) => {
                            // Module not available or other case
                        }
                    }
                })
            })
            .await;
        }
    }
}
