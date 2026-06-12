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
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, HvalsInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hvals, "Returns all values in a hash", ReqType::Read, true);

/// See official Redis documentation for `HVALS`
/// https://redis.io/docs/latest/commands/hvals/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HvalsInput {
    pub(crate) key: RedisKey,
}

impl Serialize for HvalsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HvalsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(HvalsInput, API_INFO, { key });

impl RedisCommandInput for HvalsInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("HVALS requires 1 argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "HVALS takes 1 argument, but given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis HVALS command
///
/// Returns all values in the hash stored at key.
/// Returns an empty list if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HvalsOutput {
    /// All values in the hash
    values: Vec<RedisJsonValue>,
}

impl HvalsOutput {
    pub fn new(values: Vec<RedisJsonValue>) -> Self {
        Self { values }
    }

    /// Get the values
    pub fn values(&self) -> &[RedisJsonValue] {
        &self.values
    }

    /// Get the number of values
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Check if the hash is empty or doesn't exist
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Decode the Redis protocol response into a HvalsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let values = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(arr) => arr
                    .into_iter()
                    .map(|f| match f {
                        Resp2Frame::BulkString(b) | Resp2Frame::SimpleString(b) => {
                            Ok(RedisJsonValue::String(String::from_utf8_lossy(&b).into()))
                        }
                        Resp2Frame::Integer(i) => Ok(RedisJsonValue::Integer(i)),
                        Resp2Frame::Null => Ok(RedisJsonValue::Null),
                        other => Err(EpError::parse(format!("unexpected value in HVALS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HVALS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => data
                    .into_iter()
                    .map(|f| match f {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            Ok(RedisJsonValue::String(String::from_utf8_lossy(&data).into()))
                        }
                        Resp3Frame::Number { data, .. } => Ok(RedisJsonValue::Integer(data)),
                        Resp3Frame::Null => Ok(RedisJsonValue::Null),
                        other => Err(EpError::parse(format!("unexpected value in HVALS response: {:?}", other))),
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HVALS response: {:?}", other)));
                }
            },
        };

        Ok(Self { values })
    }
}

impl Serialize for HvalsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HvalsOutput", 1)?;
        state.serialize_field("values", &self.values)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = HvalsInput { key: RedisKey::String("myhash".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nHVALS\r\n$6\r\nmyhash\r\n");
        }

        #[test]
        fn test_decode_values() {
            // RESP2 array: *2\r\n$5\r\nvalue1\r\n$6\r\nvalue2\r\n
            let output = HvalsOutput::decode(b"*2\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert_eq!(output.values(), &[RedisJsonValue::String("value1".into()), RedisJsonValue::String("value2".into())]);
        }

        #[test]
        fn test_decode_empty_array() {
            let output = HvalsOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HvalsOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = HvalsInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = HvalsInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HvalsInput { key: RedisKey::String("myhash".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{Field, HsetInput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_existing_hash() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hvals_hash".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("f1".into()), RedisJsonValue::String("v1".into())),
                                Field::new(RedisJsonValue::String("f2".into()), RedisJsonValue::String("v2".into())),
                                Field::new(RedisJsonValue::String("f3".into()), RedisJsonValue::String("v3".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HvalsInput { key: RedisKey::String("hvals_hash".into()) }.command()).await.expect("raw failed");

                    let output = HvalsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    assert!(!output.is_empty());

                    // Values may be in any order
                    let values: Vec<String> = output
                        .values()
                        .iter()
                        .filter_map(|v| match v {
                            RedisJsonValue::String(s) => Some(s.clone()),
                            _ => None,
                        })
                        .collect();
                    assert!(values.contains(&"v1".to_string()));
                    assert!(values.contains(&"v2".to_string()));
                    assert!(values.contains(&"v3".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_missing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&HvalsInput { key: RedisKey::String("hvals_nonexistent".into()) }.command()).await.expect("raw failed");

                    let output = HvalsOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                    assert_eq!(output.len(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_single_field() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hvals_single".into()),
                            fields: vec![Field::new(
                                RedisJsonValue::String("only".into()),
                                RedisJsonValue::String("value".into()),
                            )],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx.raw(&HvalsInput { key: RedisKey::String("hvals_single".into()) }.command()).await.expect("raw failed");

                    let output = HvalsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.values()[0], RedisJsonValue::String("value".into()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hvals_p1".into()),
                            fields: vec![
                                Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                                Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                            ],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &HsetInput {
                            key: RedisKey::String("hvals_p2".into()),
                            fields: vec![Field::new(RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into()))],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&HvalsInput { key: RedisKey::String("hvals_p1".into()) }.command());
                    pipeline.extend_from_slice(&HvalsInput { key: RedisKey::String("hvals_p2".into()) }.command());
                    pipeline.extend_from_slice(&HvalsInput { key: RedisKey::String("hvals_missing".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = HvalsOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.len(), 2);

                    let out2 = HvalsOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.len(), 1);

                    let out3 = HvalsOutput::decode(responses[2]).expect("decode third");
                    assert!(out3.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("r2hash".into()),
                    fields: vec![Field::new(RedisJsonValue::String("f".into()), RedisJsonValue::String("v".into()))],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HvalsInput { key: RedisKey::String("r2hash".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HvalsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hvals_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(
                &HsetInput {
                    key: RedisKey::String("r3hash".into()),
                    fields: vec![
                        Field::new(RedisJsonValue::String("a".into()), RedisJsonValue::String("1".into())),
                        Field::new(RedisJsonValue::String("b".into()), RedisJsonValue::String("2".into())),
                    ],
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&HvalsInput { key: RedisKey::String("r3hash".into()) }.command()).await.expect("raw failed");

            let output = HvalsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 2);

            ctx.stop().await;
        }
    }
}
