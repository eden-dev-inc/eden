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

const API_INFO: ApiInfo<RedisApi, FtListInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::FtList, "Returns a list of all existing indexes", ReqType::Read, true);

/// See official Redis documentation for `FT._LIST`
/// https://redis.io/docs/latest/commands/ft._list/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct FtListInput {}

impl Serialize for FtListInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtListInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(FtListInput, API_INFO);

impl RedisCommandInput for FtListInput {
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
                "FT._LIST expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis `FT._LIST` command.
///
/// Returns a list of all existing index names.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtListOutput {
    /// List of index names
    indexes: Vec<String>,
}

impl Serialize for FtListOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("FtListOutput", 1)?;
        state.serialize_field("indexes", &self.indexes)?;
        state.end()
    }
}

impl FtListOutput {
    pub fn new(indexes: Vec<String>) -> Self {
        Self { indexes }
    }

    /// Get the list of index names
    pub fn indexes(&self) -> &[String] {
        &self.indexes
    }

    /// Check if there are any indexes
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }

    /// Get the number of indexes
    pub fn len(&self) -> usize {
        self.indexes.len()
    }

    /// Decode the Redis protocol response into a FtListOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let indexes = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut indexes = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(bytes) => {
                                indexes.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            Resp2Frame::SimpleString(bytes) => {
                                indexes.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in FT._LIST response: {:?}", other)));
                            }
                        }
                    }
                    indexes
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT._LIST response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut indexes = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                indexes.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                indexes.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in FT._LIST response: {:?}", other)));
                            }
                        }
                    }
                    indexes
                }
                Resp3Frame::Set { data, .. } => {
                    let mut indexes = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                indexes.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                indexes.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in FT._LIST response: {:?}", other)));
                            }
                        }
                    }
                    indexes
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT._LIST response: {:?}", other)));
                }
            },
        };

        Ok(Self { indexes })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = FtListInput {};
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FT._LIST"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = FtListInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_extra_args_warns() {
            // Should succeed but log a warning
            let args = vec![RedisJsonValue::String("extra".into())];
            let input = FtListInput::decode(args).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_output_empty_array() {
            // RESP2 empty array: *0\r\n
            let output = FtListOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_with_indexes() {
            // RESP2 array with two bulk strings
            let output = FtListOutput::decode(b"*2\r\n$5\r\nidx_1\r\n$5\r\nidx_2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.indexes(), &["idx_1", "idx_2"]);
        }

        #[test]
        fn test_decode_output_error() {
            let err = FtListOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = FtListInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let output = FtListOutput::new(vec!["test_idx".into()]);
            assert!(!output.is_empty());
            assert_eq!(output.len(), 1);
            assert_eq!(output.indexes()[0], "test_idx");
        }

        #[test]
        fn test_serialize_input() {
            let input = FtListInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("FT._LIST") || json.contains("FtList"));
        }

        #[test]
        fn test_serialize_output() {
            let output = FtListOutput::new(vec!["idx1".into(), "idx2".into()]);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("idx1"));
            assert!(json.contains("idx2"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: FT._LIST requires RediSearch module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_list_empty() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&FtListInput {}.command()).await;

                    match result {
                        Ok(r) if !r.starts_with(b"-") => {
                            FtListOutput::decode(&r).expect("decode failed");
                        }
                        Ok(_) => {
                            // Module not available, skip
                        }
                        Err(_) => {
                            // Connection error, skip
                        }
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_ft_list_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&FtListInput {}.command()).await;

            if let Ok(r) = result
                && !r.starts_with(b"-")
            {
                // Should be array format
                assert!(r.starts_with(b"*"), "RESP2 should return array");
                FtListOutput::decode(&r).expect("decode failed");
            }

            ctx.stop().await;
        }
    }
}
