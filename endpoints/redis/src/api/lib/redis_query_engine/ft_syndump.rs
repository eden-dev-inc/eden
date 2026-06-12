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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtSyndumpInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSyndump,
    "Dumps the contents of a synonym group",
    ReqType::Read, // Fixed: was incorrectly Write
    true,
);

/// See official Redis documentation for `FT.SYNDUMP`
/// https://redis.io/docs/latest/commands/ft.syndump/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtSyndumpInput {
    index: RedisJsonValue,
}

impl Serialize for FtSyndumpInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSyndumpInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.end()
    }
}

impl_redis_operation!(FtSyndumpInput, API_INFO, { index });

impl RedisCommandInput for FtSyndumpInput {
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
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 1 {
            return Err(EpError::request(format!("FT.SYNDUMP requires 1 argument, given {}", args.len())));
        }

        Ok(Self { index: args[0].clone() })
    }
}

/// Output for Redis `FT.SYNDUMP` command.
///
/// Returns a map of synonym terms to their synonym group IDs.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSyndumpOutput {
    /// Map of term to synonym group IDs
    synonyms: HashMap<String, Vec<String>>,
}

impl Serialize for FtSyndumpOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtSyndumpOutput", 1)?;
        state.serialize_field("synonyms", &self.synonyms)?;
        state.end()
    }
}

impl FtSyndumpOutput {
    pub fn new(synonyms: HashMap<String, Vec<String>>) -> Self {
        Self { synonyms }
    }

    /// Get the synonyms map
    pub fn synonyms(&self) -> &HashMap<String, Vec<String>> {
        &self.synonyms
    }

    /// Check if there are any synonyms
    pub fn is_empty(&self) -> bool {
        self.synonyms.is_empty()
    }

    /// Get synonym group IDs for a term
    pub fn get(&self, term: &str) -> Option<&Vec<String>> {
        self.synonyms.get(term)
    }

    /// Decode the Redis protocol response into a FtSyndumpOutput
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
                let mut synonyms = HashMap::new();
                let mut i = 0;
                while i + 1 < arr.len() {
                    let term = if let Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) = &arr[i] {
                        s.clone()
                    } else {
                        i += 2;
                        continue;
                    };

                    let mut group_ids = Vec::new();
                    if let Resp2Frame::Array(ids) = &arr[i + 1] {
                        for id in ids {
                            if let Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) = id {
                                group_ids.push(String::from_utf8(s.to_vec()).map_err(EpError::parse)?)
                            }
                        }
                    }

                    synonyms.insert(String::from_utf8(term).map_err(EpError::parse)?, group_ids);
                    i += 2;
                }
                Ok(Self { synonyms })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.SYNDUMP response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let mut synonyms = HashMap::new();
                let mut i = 0;
                while i + 1 < data.len() {
                    let term = match &data[i] {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            String::from_utf8(data.to_vec()).map_err(EpError::parse)?
                        }
                        _ => {
                            i += 2;
                            continue;
                        }
                    };
                    let mut group_ids = Vec::new();
                    if let Resp3Frame::Array { data, .. } = &data[i + 1] {
                        for id in data {
                            match id {
                                Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                                    group_ids.push(String::from_utf8(data.to_vec()).map_err(EpError::parse)?)
                                }
                                _ => {}
                            }
                        }
                    }

                    synonyms.insert(term, group_ids);
                    i += 2;
                }
                Ok(Self { synonyms })
            }
            Resp3Frame::Map { data, .. } => {
                let mut synonyms = HashMap::new();
                for (k, v) in data {
                    let term = match k {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            String::from_utf8(data.to_vec()).map_err(EpError::parse)?
                        }
                        _ => continue,
                    };

                    let mut group_ids = Vec::new();
                    if let Resp3Frame::Array { data, .. } = v {
                        for id in data {
                            if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = id {
                                group_ids.push(String::from_utf8(data.to_vec()).map_err(EpError::parse)?);
                            }
                        }
                    }

                    synonyms.insert(term, group_ids);
                }
                Ok(Self { synonyms })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.SYNDUMP response: {:?}", other))),
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
            let input = FtSyndumpInput { index: RedisJsonValue::String("my_index".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.SYNDUMP"));
            assert!(cmd_str.contains("my_index"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let input = FtSyndumpInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = FtSyndumpInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("idx1".into()), RedisJsonValue::String("idx2".into())];
            let err = FtSyndumpInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("1 argument"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtSyndumpOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtSyndumpOutput::decode(b"-ERR Unknown Index name\r\n").unwrap_err();
            assert!(err.to_string().contains("Unknown Index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtSyndumpInput { index: RedisJsonValue::String("i".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtSyndumpInput { index: RedisJsonValue::String("test_idx".into()) };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
        }

        #[test]
        fn test_serialize_output() {
            let mut synonyms = HashMap::new();
            synonyms.insert("hello".into(), vec!["group1".into()]);
            let output = FtSyndumpOutput::new(synonyms);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("synonyms"));
            assert!(json.contains("hello"));
        }

        #[test]
        fn test_output_accessors() {
            let mut synonyms = HashMap::new();
            synonyms.insert("foo".into(), vec!["g1".into(), "g2".into()]);
            let output = FtSyndumpOutput::new(synonyms);

            assert!(!output.is_empty());
            assert!(output.get("foo").is_some());
            assert_eq!(output.get("foo").unwrap().len(), 2);
            assert!(output.get("bar").is_none());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.SYNDUMP requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_syndump_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtSyndumpInput { index: RedisJsonValue::String("nonexistent_index".into()) }.command()).await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                            let err = FtSyndumpOutput::decode(&r);
                            assert!(err.is_err());
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
