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

const API_INFO: ApiInfo<RedisApi, DbsizeInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Dbsize, "Returns the number of keys in the database", ReqType::Read, true);

/// See official Redis documentation for `DBSIZE`
/// https://redis.io/docs/latest/commands/dbsize/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DbsizeInput {}

impl Serialize for DbsizeInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DbsizeInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(DbsizeInput, API_INFO);

impl RedisCommandInput for DbsizeInput {
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
                "DBSIZE expects no arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }
        Ok(Self::default())
    }
}

/// Output for Redis DBSIZE command
///
/// Returns the number of keys in the currently-selected database.
///
/// See official Redis documentation for `DBSIZE`
/// https://redis.io/docs/latest/commands/dbsize/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DbsizeOutput {
    size: u64,
}

impl Serialize for DbsizeOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DbsizeOutput", 1)?;
        state.serialize_field("size", &self.size)?;
        state.end()
    }
}

impl DbsizeOutput {
    pub fn new(size: u64) -> Self {
        Self { size }
    }

    pub fn size(&self) -> u64 {
        self.size
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let size = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => {
                    if i < 0 {
                        return Err(EpError::parse("DBSIZE cannot return negative value"));
                    }
                    i as u64
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("DBSIZE must return integer, got: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => {
                    if data < 0 {
                        return Err(EpError::parse("DBSIZE cannot return negative value"));
                    }
                    data as u64
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("DBSIZE must return number, got: {:?}", other)));
                }
            },
        };

        Ok(Self::new(size))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = DbsizeInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$6\r\nDBSIZE\r\n");
        }

        #[test]
        fn test_decode_output_resp2_integer() {
            let output = DbsizeOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.size(), 42);
        }

        #[test]
        fn test_decode_output_zero() {
            let output = DbsizeOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.size(), 0);
        }

        #[test]
        fn test_decode_output_large_value() {
            let output = DbsizeOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.size(), 1000000);
        }

        #[test]
        fn test_decode_output_error() {
            let err = DbsizeOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = DbsizeInput::decode(vec![]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_decode_input_with_args_still_succeeds() {
            // Extra args are logged but don't cause failure
            let input = DbsizeInput::decode(vec![RedisJsonValue::String("unexpected".into())]).unwrap();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = DbsizeInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = DbsizeInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Dbsize);
        }

        #[test]
        fn test_serialize_input() {
            let input = DbsizeInput {};
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("DBSIZE") || json.contains("Dbsize"));
        }

        #[test]
        fn test_serialize_output() {
            let output = DbsizeOutput::new(100);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"size\":100"));
        }

        #[test]
        fn test_req_type_is_read() {
            assert_eq!(API_INFO.request_type, ReqType::Read);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dbsize_empty_db() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB to ensure empty database
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    let result = ctx.raw(&DbsizeInput {}.command()).await.expect("raw failed");

                    let output = DbsizeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 0, "empty db should have 0 keys");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dbsize_with_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB and set some keys
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    for i in 0..5 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("key{}", i)),
                            value: RedisJsonValue::String("value".into()),
                            ..Default::default()
                        })
                        .await;
                    }

                    let result = ctx.raw(&DbsizeInput {}.command()).await.expect("raw failed");

                    let output = DbsizeOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.size(), 5, "should have 5 keys");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dbsize_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    // Pipeline: SET + DBSIZE + SET + DBSIZE
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("k1".into()),
                            value: RedisJsonValue::String("v1".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&DbsizeInput {}.command());
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("k2".into()),
                            value: RedisJsonValue::String("v2".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&DbsizeInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 4);

                    let dbsize1 = DbsizeOutput::decode(responses[1]).expect("decode first dbsize");
                    assert_eq!(dbsize1.size(), 1);

                    let dbsize2 = DbsizeOutput::decode(responses[3]).expect("decode second dbsize");
                    assert_eq!(dbsize2.size(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dbsize_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&DbsizeInput {}.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = DbsizeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.size(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_dbsize_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&DbsizeInput {}.command()).await.expect("raw failed");

            let output = DbsizeOutput::decode(&result).expect("decode failed");
            assert_eq!(output.size(), 0);

            ctx.stop().await;
        }
    }
}
