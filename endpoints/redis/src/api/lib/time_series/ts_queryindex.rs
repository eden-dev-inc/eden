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

const API_INFO: ApiInfo<RedisApi, TsQueryindexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TsQueryindex,
    "Get all time series keys matching a filter list",
    ReqType::Read,
    true,
);

/// Input for Redis `TS.QUERYINDEX` command.
///
/// Returns all keys matching the specified filter expression(s).
///
/// See official Redis documentation for `TS.QUERYINDEX`:
/// https://redis.io/docs/latest/commands/ts.queryindex/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsQueryindexInput {
    /// Filter expression(s) to match time series keys.
    /// Examples: "sensor_id=1", "area_id!=2", "sensor_id=(1,2,3)"
    filter: RedisJsonValue,
}

impl Serialize for TsQueryindexInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TsQueryindexInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("filter", &self.filter)?;
        state.end()
    }
}

impl_redis_operation!(TsQueryindexInput, API_INFO, { filter });

impl RedisCommandInput for TsQueryindexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.filter);
        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("TS.QUERYINDEX requires at least 1 filter argument"));
        }

        // For multiple filters, they should be passed as an array or joined
        let filter = if args.len() == 1 {
            args[0].clone()
        } else {
            // Multiple filter expressions
            RedisJsonValue::Array(args)
        };

        Ok(TsQueryindexInput { filter })
    }
}

/// Output for Redis `TS.QUERYINDEX` command.
///
/// Contains the list of time series keys matching the filter(s).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsQueryindexOutput {
    /// Keys matching the filter expression(s)
    keys: Vec<String>,
}

impl TsQueryindexOutput {
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Get the matching keys
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Check if any keys matched
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Get the number of matching keys
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Decode the Redis protocol response into a TsQueryindexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => {
                    let mut keys = Vec::with_capacity(arr.len());
                    for item in arr {
                        match item {
                            Resp2Frame::BulkString(data) => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp2Frame::SimpleString(data) => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in TS.QUERYINDEX response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected TS.QUERYINDEX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut keys = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in TS.QUERYINDEX response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp3Frame::Set { data, .. } => {
                    let mut keys = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in TS.QUERYINDEX response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected TS.QUERYINDEX response: {:?}", other)));
                }
            },
        };

        Ok(Self { keys })
    }
}

impl Serialize for TsQueryindexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsQueryindexOutput", 1)?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_filter() {
            let input = TsQueryindexInput { filter: RedisJsonValue::String("sensor_id=1".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.QUERYINDEX"));
            assert!(cmd_str.contains("sensor_id=1"));
        }

        #[test]
        fn test_decode_input_single_filter() {
            let args = vec![RedisJsonValue::String("area=north".into())];
            let input = TsQueryindexInput::decode(args).unwrap();
            assert_eq!(input.filter, RedisJsonValue::String("area=north".into()));
        }

        #[test]
        fn test_decode_input_multiple_filters() {
            let args = vec![
                RedisJsonValue::String("sensor_id=1".into()),
                RedisJsonValue::String("area=north".into()),
            ];
            let input = TsQueryindexInput::decode(args).unwrap();
            match input.filter {
                RedisJsonValue::Array(arr) => assert_eq!(arr.len(), 2),
                _ => panic!("Expected array for multiple filters"),
            }
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TsQueryindexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_decode_output_empty_array() {
            // RESP2 empty array: *0\r\n
            let output = TsQueryindexOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_with_keys() {
            // RESP2 array with two bulk strings
            let output = TsQueryindexOutput::decode(b"*2\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.keys(), &["key1", "key2"]);
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsQueryindexOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = TsQueryindexInput { filter: RedisJsonValue::String("test=1".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_output_accessors() {
            let output = TsQueryindexOutput::new(vec!["ts:1".into(), "ts:2".into()]);
            assert!(!output.is_empty());
            assert_eq!(output.len(), 2);
            assert_eq!(output.keys()[0], "ts:1");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.QUERYINDEX requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_queryindex_no_matches() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsQueryindexInput {
                                filter: RedisJsonValue::String("nonexistent_label=value".into()),
                            }
                            .command(),
                        )
                        .await;

                    // May fail if RedisTimeSeries not installed
                    if let Ok(result) = result
                        && !result.starts_with(b"-")
                    {
                        let output = TsQueryindexOutput::decode(&result).expect("decode failed");
                        assert!(output.is_empty());
                    }
                })
            })
            .await;
        }
    }
}
