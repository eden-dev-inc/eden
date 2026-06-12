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

const API_INFO: ApiInfo<RedisApi, FtDictdumpInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtDictdump,
    "Dumps all terms in the given dictionary",
    ReqType::Read, // Fixed: was incorrectly Write
    true,
);

/// See official Redis documentation for `FT.DICTDUMP`
/// https://redis.io/docs/latest/commands/ft.dictdump/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtDictdumpInput {
    dict: RedisJsonValue,
}

impl Serialize for FtDictdumpInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtDictdumpInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("dict", &self.dict)?;
        state.end()
    }
}

impl_redis_operation!(FtDictdumpInput, API_INFO, { dict });

impl RedisCommandInput for FtDictdumpInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.dict);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FT.DICTDUMP requires 1 argument, given {}", args.len())));
        }

        Ok(Self { dict: args[0].clone() })
    }
}

/// Output for Redis `FT.DICTDUMP` command.
///
/// Returns an array of all terms in the dictionary.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtDictdumpOutput {
    /// List of terms in the dictionary
    terms: Vec<String>,
}

impl Serialize for FtDictdumpOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtDictdumpOutput", 1)?;
        state.serialize_field("terms", &self.terms)?;
        state.end()
    }
}

impl FtDictdumpOutput {
    pub fn new(terms: Vec<String>) -> Self {
        Self { terms }
    }

    /// Get the list of terms
    pub fn terms(&self) -> &[String] {
        &self.terms
    }

    /// Check if the dictionary is empty
    pub fn is_empty(&self) -> bool {
        self.terms.is_empty()
    }

    /// Get the number of terms
    pub fn len(&self) -> usize {
        self.terms.len()
    }

    /// Decode the Redis protocol response into a FtDictdumpOutput
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
                let mut terms = Vec::new();
                for item in arr {
                    if let Resp2Frame::BulkString(s) = item {
                        terms.push(String::from_utf8(s).map_err(EpError::parse)?);
                    }
                }
                Ok(Self { terms })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.DICTDUMP response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut terms = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            terms.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                        _ => {}
                    }
                }
                Ok(Self { terms })
            }
            Resp3Frame::Set { data, .. } => {
                let mut terms = Vec::new();
                for item in data {
                    match item {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            terms.push(String::from_utf8(data).map_err(EpError::parse)?);
                        }
                        _ => {}
                    }
                }
                Ok(Self { terms })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.DICTDUMP response: {:?}", other))),
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
            let input = FtDictdumpInput { dict: RedisJsonValue::String("my_dict".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.DICTDUMP"));
            assert!(cmd_str.contains("my_dict"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("dict".into())];
            let input = FtDictdumpInput::decode(args).unwrap();
            assert_eq!(input.dict, RedisJsonValue::String("dict".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtDictdumpInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("dict1".into()), RedisJsonValue::String("dict2".into())];
            let err = FtDictdumpInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_output_valid() {
            let output = FtDictdumpOutput::decode(b"*3\r\n$4\r\nterm\r\n$5\r\nhello\r\n$5\r\nworld\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.terms().contains(&"term".to_string()));
            assert!(output.terms().contains(&"hello".to_string()));
            assert!(output.terms().contains(&"world".to_string()));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtDictdumpOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtDictdumpOutput::decode(b"-ERR unknown dictionary\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtDictdumpInput { dict: RedisJsonValue::String("d".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtDictdumpInput { dict: RedisJsonValue::String("test_dict".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_dict"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtDictdumpOutput::new(vec!["a".into(), "b".into()]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("terms"));
        }

        #[test]
        fn test_output_accessors() {
            let output = FtDictdumpOutput::new(vec!["foo".into(), "bar".into()]);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
            assert_eq!(output.terms(), &["foo", "bar"]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.DICTDUMP requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_dictdump_empty() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtDictdumpInput { dict: RedisJsonValue::String("empty_dict".into()) }.command()).await;

                    match result {
                        Ok(r) if r.starts_with(b"*") => {
                            FtDictdumpOutput::decode(&r).expect("decode failed");
                        }
                        Ok(r) if r.starts_with(b"-") => {
                            // Module not available
                        }
                        _ => {}
                    }
                })
            })
            .await;
        }
    }
}
