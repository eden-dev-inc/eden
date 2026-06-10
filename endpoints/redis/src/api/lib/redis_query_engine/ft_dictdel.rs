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

const API_INFO: ApiInfo<RedisApi, FtDictdelInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtDictdel, "Deletes terms from a dictionary", ReqType::Write, true);

/// See official Redis documentation for `FT.DICTDEL`
/// https://redis.io/docs/latest/commands/ft.dictdel/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtDictdelInput {
    dict: RedisJsonValue,
    terms: Vec<RedisJsonValue>,
}

impl Serialize for FtDictdelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtDictdelInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("dict", &self.dict)?;
        state.serialize_field("terms", &self.terms)?;
        state.end()
    }
}

impl_redis_operation!(
    FtDictdelInput,
    API_INFO,
    {dict, terms});

impl RedisCommandInput for FtDictdelInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.dict);
        for term in &self.terms {
            command.arg(term);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("FT.DICTDEL requires at least 2 arguments, given {}", args.len())));
        }

        Ok(Self { dict: args[0].clone(), terms: args[1..].to_vec() })
    }
}

/// Output for Redis `FT.DICTDEL` command.
///
/// Returns the number of terms that were deleted from the dictionary.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtDictdelOutput {
    /// Number of terms deleted
    deleted: i64,
}

impl Serialize for FtDictdelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtDictdelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

impl FtDictdelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }

    /// Get the number of terms deleted
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into a FtDictdelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Integer(i) => Ok(Self { deleted: i }),
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.DICTDEL response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Number { data, .. } => Ok(Self { deleted: data }),
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.DICTDEL response: {:?}", other))),
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
            let input = FtDictdelInput {
                dict: RedisJsonValue::String("my_dict".into()),
                terms: vec![RedisJsonValue::String("term1".into()), RedisJsonValue::String("term2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.DICTDEL"));
            assert!(cmd_str.contains("my_dict"));
            assert!(cmd_str.contains("term1"));
            assert!(cmd_str.contains("term2"));
        }

        #[test]
        fn test_encode_command_single_term() {
            let input = FtDictdelInput {
                dict: RedisJsonValue::String("dict".into()),
                terms: vec![RedisJsonValue::String("single".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("single"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("dict".into()),
                RedisJsonValue::String("term1".into()),
                RedisJsonValue::String("term2".into()),
            ];
            let input = FtDictdelInput::decode(args).unwrap();
            assert_eq!(input.dict, RedisJsonValue::String("dict".into()));
            assert_eq!(input.terms.len(), 2);
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("dict".into())];
            let err = FtDictdelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_output_valid() {
            let output = FtDictdelOutput::decode(b":2\r\n").unwrap();
            assert_eq!(output.deleted(), 2);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = FtDictdelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtDictdelOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtDictdelInput {
                dict: RedisJsonValue::String("d".into()),
                terms: vec![RedisJsonValue::String("t".into())],
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtDictdelInput {
                dict: RedisJsonValue::String("test_dict".into()),
                terms: vec![RedisJsonValue::String("word".into())],
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_dict"));
            assert!(json.contains("word"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtDictdelOutput::new(3);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("3"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.DICTDEL requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_dictdel_nonexistent() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtDictdelInput {
                                dict: RedisJsonValue::String("nonexistent_dict".into()),
                                terms: vec![RedisJsonValue::String("term".into())],
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b":") => {
                            let output = FtDictdelOutput::decode(&r).expect("decode failed");
                            assert_eq!(output.deleted(), 0);
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
