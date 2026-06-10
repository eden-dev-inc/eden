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

const API_INFO: ApiInfo<RedisApi, FtTagvalsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtTagvals,
    "Returns the distinct tags indexed in a Tag field",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `FT.TAGVALS`
/// https://redis.io/docs/latest/commands/ft.tagvals/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtTagvalsInput {
    index: RedisJsonValue,
    field_name: RedisJsonValue,
}

impl Serialize for FtTagvalsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtTagvalsInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("field_name", &self.field_name)?;
        state.end()
    }
}

impl_redis_operation!(
    FtTagvalsInput,
    API_INFO,
    {index, field_name}
);

impl RedisCommandInput for FtTagvalsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index).arg(&self.field_name);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("FT.TAGVALS requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { index: args[0].clone(), field_name: args[1].clone() })
    }
}

/// Output for Redis `FT.TAGVALS` command.
///
/// Returns an array of all distinct tag values in the specified field.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtTagvalsOutput {
    /// List of distinct tag values
    tags: Vec<String>,
}

impl Serialize for FtTagvalsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtTagvalsOutput", 1)?;
        state.serialize_field("tags", &self.tags)?;
        state.end()
    }
}

impl FtTagvalsOutput {
    pub fn new(tags: Vec<String>) -> Self {
        Self { tags }
    }

    /// Get the list of tags
    pub fn tags(&self) -> &[String] {
        &self.tags
    }

    /// Check if there are any tags
    pub fn is_empty(&self) -> bool {
        self.tags.is_empty()
    }

    /// Get the number of tags
    pub fn len(&self) -> usize {
        self.tags.len()
    }

    /// Check if a specific tag exists
    pub fn contains(&self, tag: &str) -> bool {
        self.tags.iter().any(|t| t == tag)
    }

    /// Decode the Redis protocol response into a FtTagvalsOutput
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
                let tags = arr
                    .into_iter()
                    .filter_map(|item| {
                        if let Resp2Frame::BulkString(s) = item {
                            Some(String::from_utf8_lossy(&s).to_string())
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<String>>();
                Ok(Self { tags })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.TAGVALS response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                let tags = data
                    .into_iter()
                    .filter_map(|item| match item {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            Some(String::from_utf8_lossy(&data).to_string())
                        }
                        _ => None,
                    })
                    .collect::<Vec<String>>();
                Ok(Self { tags })
            }
            Resp3Frame::Set { data, .. } => {
                let tags = data
                    .into_iter()
                    .filter_map(|item| match item {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            Some(String::from_utf8_lossy(&data).to_string())
                        }
                        _ => None,
                    })
                    .collect::<Vec<String>>();
                Ok(Self { tags })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.TAGVALS response: {:?}", other))),
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
            let input = FtTagvalsInput {
                index: RedisJsonValue::String("my_index".into()),
                field_name: RedisJsonValue::String("category".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.TAGVALS"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("category"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("field".into())];
            let input = FtTagvalsInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert_eq!(input.field_name, RedisJsonValue::String("field".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into())];
            let err = FtTagvalsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = FtTagvalsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("2 arguments"));
        }

        #[test]
        fn test_decode_output_valid() {
            let output = FtTagvalsOutput::decode(b"*3\r\n$3\r\nred\r\n$5\r\ngreen\r\n$4\r\nblue\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert!(output.contains("red"));
            assert!(output.contains("green"));
            assert!(output.contains("blue"));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = FtTagvalsOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtTagvalsOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("unknown index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtTagvalsInput {
                index: RedisJsonValue::String("i".into()),
                field_name: RedisJsonValue::String("f".into()),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtTagvalsInput {
                index: RedisJsonValue::String("test_idx".into()),
                field_name: RedisJsonValue::String("test_field".into()),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("test_field"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtTagvalsOutput::new(vec!["a".into(), "b".into()]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("tags"));
        }

        #[test]
        fn test_output_accessors() {
            let output = FtTagvalsOutput::new(vec!["foo".into(), "bar".into()]);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
            assert!(output.contains("foo"));
            assert!(output.contains("bar"));
            assert!(!output.contains("baz"));
            assert_eq!(output.tags(), &["foo", "bar"]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.TAGVALS requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_tagvals_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtTagvalsInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                field_name: RedisJsonValue::String("tags".into()),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"-") => {
                            // Expected error for nonexistent index
                            let err = FtTagvalsOutput::decode(&r);
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
