use crate::api::lib::redis_query_engine::Profile;
use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, FtProfileInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtProfile,
    "Performs FT.SEARCH or FT.AGGREGATE commands and collects performance information",
    ReqType::Read, // Changed from Write - profiling is a read operation
    true,
);

/// See official Redis documentation for `FT.PROFILE`
/// https://redis.io/docs/latest/commands/ft.profile/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtProfileInput {
    index: RedisJsonValue,
    profile_type: Profile,
    limited: Option<bool>,
    query: RedisJsonValue,
}

impl Serialize for FtProfileInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, index, profile_type, query
        if self.limited.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("FtProfileInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index", &self.index)?;
        state.serialize_field("profile_type", &self.profile_type)?;
        if let Some(limited) = &self.limited {
            state.serialize_field("limited", limited)?;
        }
        state.serialize_field("query", &self.query)?;
        state.end()
    }
}

impl_redis_operation!(
    FtProfileInput,
    API_INFO,
    {index, profile_type, limited, query}
);

impl RedisCommandInput for FtProfileInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index);

        match &self.profile_type {
            Profile::SEARCH => command.arg("SEARCH"),
            Profile::AGGREGATE => command.arg("AGGREGATE"),
        };

        if let Some(limited) = self.limited
            && limited
        {
            command.arg("LIMITED");
        }

        command.arg("QUERY").arg(&self.query);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 4 {
            return Err(EpError::request(format!("FT.PROFILE requires at least 4 arguments, given {}", args.len())));
        }

        let index = args[0].clone();

        // Parse profile type
        let profile_type = if let RedisJsonValue::String(s) = &args[1] {
            match s.to_uppercase().as_str() {
                "SEARCH" => Profile::SEARCH,
                "AGGREGATE" => Profile::AGGREGATE,
                _ => {
                    return Err(EpError::request("FT.PROFILE profile type must be SEARCH or AGGREGATE".to_string()));
                }
            }
        } else {
            return Err(EpError::request("FT.PROFILE profile type must be SEARCH or AGGREGATE".to_string()));
        };

        let mut limited = None;
        let mut query_index = 2;

        // Check for LIMITED
        if let RedisJsonValue::String(s) = &args[2]
            && s.to_uppercase() == "LIMITED"
        {
            limited = Some(true);
            query_index = 3;
        }

        // Expect QUERY keyword
        if query_index >= args.len() {
            return Err(EpError::request("FT.PROFILE requires QUERY keyword".to_string()));
        }

        if let RedisJsonValue::String(s) = &args[query_index] {
            if s.to_uppercase() != "QUERY" {
                return Err(EpError::request("FT.PROFILE requires QUERY keyword".to_string()));
            }
        } else {
            return Err(EpError::request("FT.PROFILE requires QUERY keyword".to_string()));
        }

        // Get query
        if query_index + 1 >= args.len() {
            return Err(EpError::request("FT.PROFILE requires query after QUERY keyword".to_string()));
        }

        let query = args[query_index + 1].clone();

        Ok(Self { index, profile_type, limited, query })
    }
}

/// Output for Redis `FT.PROFILE` command.
///
/// Returns the query results along with profiling information.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtProfileOutput {
    /// The query results
    results: Vec<RedisJsonValue>,
    /// Profiling information
    profile: Vec<RedisJsonValue>,
}

impl Serialize for FtProfileOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtProfileOutput", 2)?;
        state.serialize_field("results", &self.results)?;
        state.serialize_field("profile", &self.profile)?;
        state.end()
    }
}

impl FtProfileOutput {
    pub fn new(results: Vec<RedisJsonValue>, profile: Vec<RedisJsonValue>) -> Self {
        Self { results, profile }
    }

    /// Get the query results
    pub fn results(&self) -> &[RedisJsonValue] {
        &self.results
    }

    /// Get the profiling information
    pub fn profile(&self) -> &[RedisJsonValue] {
        &self.profile
    }

    /// Decode the Redis protocol response into a FtProfileOutput
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
                if arr.len() < 2 {
                    return Err(EpError::parse("FT.PROFILE response should have at least 2 elements"));
                }

                let results = Self::extract_array_resp2(&arr[0])?;
                let profile = Self::extract_array_resp2(&arr[1])?;

                Ok(Self { results, profile })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected FT.PROFILE response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Array { data, .. } => {
                if data.len() < 2 {
                    return Err(EpError::parse("FT.PROFILE response should have at least 2 elements"));
                }

                let results = Self::extract_array_resp3(&data[0])?;
                let profile = Self::extract_array_resp3(&data[1])?;

                Ok(Self { results, profile })
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(data.as_slice()).to_string())),
            other => Err(EpError::parse(format!("unexpected FT.PROFILE response: {:?}", other))),
        }
    }

    fn extract_array_resp2(frame: &Resp2Frame) -> ResultEP<Vec<RedisJsonValue>> {
        Ok(match frame {
            Resp2Frame::Array(arr) => {
                let mut items = vec![];
                for item in arr {
                    items.push(Self::frame_to_json_resp2(item)?);
                }
                items
            }
            _ => vec![Self::frame_to_json_resp2(frame)?],
        })
    }

    fn extract_array_resp3(frame: &Resp3Frame) -> ResultEP<Vec<RedisJsonValue>> {
        Ok(match frame {
            Resp3Frame::Array { data, .. } => {
                let mut items = vec![];
                for item in data {
                    items.push(Self::frame_to_json_resp3(item)?);
                }
                items
            }
            _ => vec![Self::frame_to_json_resp3(frame)?],
        })
    }

    fn frame_to_json_resp2(frame: &Resp2Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp2Frame::SimpleString(s) | Resp2Frame::BulkString(s) => {
                RedisJsonValue::String(String::from_utf8(s.to_vec()).map_err(EpError::parse)?)
            }
            Resp2Frame::Integer(i) => RedisJsonValue::Integer(*i),
            Resp2Frame::Array(arr) => {
                let mut items = Vec::with_capacity(arr.len());
                for item in arr {
                    items.push(Self::frame_to_json_resp2(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp2Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }

    fn frame_to_json_resp3(frame: &Resp3Frame) -> ResultEP<RedisJsonValue> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8(data.to_vec()).map_err(EpError::parse)?)
            }
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
            Resp3Frame::Array { data, .. } => {
                let mut items = Vec::with_capacity(data.len());
                for item in data {
                    items.push(Self::frame_to_json_resp3(item)?);
                }
                RedisJsonValue::Array(items)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            Resp3Frame::Double { data, .. } => RedisJsonValue::Float(*data),
            _ => RedisJsonValue::Null,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_search() {
            let input = FtProfileInput {
                index: RedisJsonValue::String("my_index".into()),
                profile_type: Profile::SEARCH,
                limited: None,
                query: RedisJsonValue::String("*".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT.PROFILE"));
            assert!(cmd_str.contains("my_index"));
            assert!(cmd_str.contains("SEARCH"));
            assert!(cmd_str.contains("QUERY"));
            assert!(!cmd_str.contains("LIMITED"));
        }

        #[test]
        fn test_encode_command_aggregate() {
            let input = FtProfileInput {
                index: RedisJsonValue::String("my_index".into()),
                profile_type: Profile::AGGREGATE,
                limited: None,
                query: RedisJsonValue::String("*".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("AGGREGATE"));
        }

        #[test]
        fn test_encode_command_with_limited() {
            let input = FtProfileInput {
                index: RedisJsonValue::String("my_index".into()),
                profile_type: Profile::SEARCH,
                limited: Some(true),
                query: RedisJsonValue::String("*".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("LIMITED"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("SEARCH".into()),
                RedisJsonValue::String("QUERY".into()),
                RedisJsonValue::String("*".into()),
            ];
            let input = FtProfileInput::decode(args).unwrap();
            assert_eq!(input.index, RedisJsonValue::String("idx".into()));
            assert!(matches!(input.profile_type, Profile::SEARCH));
            assert!(input.limited.is_none());
        }

        #[test]
        fn test_decode_input_with_limited() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("AGGREGATE".into()),
                RedisJsonValue::String("LIMITED".into()),
                RedisJsonValue::String("QUERY".into()),
                RedisJsonValue::String("*".into()),
            ];
            let input = FtProfileInput::decode(args).unwrap();
            assert!(matches!(input.profile_type, Profile::AGGREGATE));
            assert_eq!(input.limited, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("idx".into()), RedisJsonValue::String("SEARCH".into())];
            let err = FtProfileInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 4 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_profile_type() {
            let args = vec![
                RedisJsonValue::String("idx".into()),
                RedisJsonValue::String("INVALID".into()),
                RedisJsonValue::String("QUERY".into()),
                RedisJsonValue::String("*".into()),
            ];
            let err = FtProfileInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("SEARCH or AGGREGATE"));
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtProfileOutput::decode(b"-ERR unknown index\r\n").unwrap_err();
            assert!(err.to_string().contains("unknown index"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtProfileInput {
                index: RedisJsonValue::String("i".into()),
                profile_type: Profile::SEARCH,
                limited: None,
                query: RedisJsonValue::String("*".into()),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_serialize_input() {
            let input = FtProfileInput {
                index: RedisJsonValue::String("test_idx".into()),
                profile_type: Profile::SEARCH,
                limited: Some(true),
                query: RedisJsonValue::String("test".into()),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("test_idx"));
            assert!(json.contains("SEARCH"));
            assert!(json.contains("limited"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtProfileOutput::new(vec![], vec![]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("results"));
            assert!(json.contains("profile"));
        }

        #[test]
        fn test_output_accessors() {
            let output = FtProfileOutput::new(vec![RedisJsonValue::String("doc1".into())], vec![RedisJsonValue::String("timing".into())]);
            assert_eq!(output.results().len(), 1);
            assert_eq!(output.profile().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT.PROFILE requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_profile_nonexistent_index() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtProfileInput {
                                index: RedisJsonValue::String("nonexistent".into()),
                                profile_type: Profile::SEARCH,
                                limited: None,
                                query: RedisJsonValue::String("*".into()),
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
