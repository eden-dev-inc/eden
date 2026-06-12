use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, PfcountInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pfcount,
    "Returns the approximated cardinality of the set(s) observed by the HyperLogLog at key(s).",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `PFCOUNT`
/// https://redis.io/docs/latest/commands/pfcount/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PfcountInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for PfcountInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("PfcountInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(PfcountInput, API_INFO, { keys });

impl RedisCommandInput for PfcountInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for key in &self.keys {
            command.arg(key);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("PFCOUNT requires at least 1 key, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

/// Output for Redis PFCOUNT command
///
/// Returns the approximated cardinality computed by the HyperLogLog data structure
/// stored at the specified key(s).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PfcountOutput {
    /// The approximated number of unique elements observed
    cardinality: i64,
}

impl PfcountOutput {
    pub fn new(cardinality: i64) -> Self {
        Self { cardinality }
    }

    /// Get the approximated cardinality (number of unique elements)
    pub fn cardinality(&self) -> i64 {
        self.cardinality
    }

    /// Check if the HyperLogLog is empty (cardinality is 0)
    pub fn is_empty(&self) -> bool {
        self.cardinality == 0
    }

    /// Decode the Redis protocol response into a PfcountOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let cardinality = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PFCOUNT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PFCOUNT response: {:?}", other)));
                }
            },
        };

        Ok(Self { cardinality })
    }
}

impl Serialize for PfcountOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PfcountOutput", 1)?;
        state.serialize_field("cardinality", &self.cardinality)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_key() {
            let input = PfcountInput { keys: vec![RedisKey::String("hll".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$7\r\nPFCOUNT\r\n$3\r\nhll\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = PfcountInput {
                keys: vec![
                    RedisKey::String("hll1".into()),
                    RedisKey::String("hll2".into()),
                    RedisKey::String("hll3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$7\r\nPFCOUNT\r\n$4\r\nhll1\r\n$4\r\nhll2\r\n$4\r\nhll3\r\n");
        }

        #[test]
        fn test_decode_output_zero() {
            let output = PfcountOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.cardinality(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_positive() {
            let output = PfcountOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.cardinality(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_large_cardinality() {
            let output = PfcountOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.cardinality(), 1000000);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PfcountOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = PfcountInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("key3".into()),
            ];
            let input = PfcountInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 3);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PfcountInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 key"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = PfcountInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
            };
            assert_eq!(input.keys().len(), 2);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::PfaddInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_missing".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = PfcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 0, "nonexistent key should return 0");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_after_pfadd() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\npfcount_test\r\n").await.expect("raw failed");

                    // Add some elements
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfcount_test".into()),
                            elements: Some(vec![
                                RedisJsonValue::String("a".into()),
                                RedisJsonValue::String("b".into()),
                                RedisJsonValue::String("c".into()),
                            ]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_test".into())] }.command()).await.expect("raw failed");

                    let output = PfcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 3, "should count 3 unique elements");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_multiple_keys_union() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nDEL\r\n$14\r\npfcount_union1\r\n$14\r\npfcount_union2\r\n").await.expect("raw failed");

                    // Add elements to first HLL
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfcount_union1".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("b".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add elements to second HLL (c is unique, a overlaps)
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfcount_union2".into()),
                            elements: Some(vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("c".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Count union of both
                    let result = ctx
                        .raw(
                            &PfcountInput {
                                keys: vec![RedisKey::String("pfcount_union1".into()), RedisKey::String("pfcount_union2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 3, "union should have 3 unique elements: a, b, c");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_with_duplicates() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\npfcount_dup\r\n").await.expect("raw failed");

                    // Add duplicate elements
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfcount_dup".into()),
                            elements: Some(vec![
                                RedisJsonValue::String("x".into()),
                                RedisJsonValue::String("x".into()),
                                RedisJsonValue::String("x".into()),
                                RedisJsonValue::String("y".into()),
                            ]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result =
                        ctx.raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_dup".into())] }.command()).await.expect("raw failed");

                    let output = PfcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 2, "duplicates should be counted once");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_wrong_type_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\npfcount_wrongtype\r\n$5\r\nhello\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_wrongtype".into())] }.command())
                        .await
                        .expect("raw failed");

                    let err = PfcountOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"), "should fail with WRONGTYPE error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_empty_hll() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\npfcount_empty\r\n").await.expect("raw failed");

                    // Create empty HLL (PFADD with key only)
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfcount_empty".into()),
                            elements: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_empty".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = PfcountOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.cardinality(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfcount_r2\r\n").await.expect("raw failed");

            ctx.raw(
                &PfaddInput {
                    key: RedisKey::String("pfcount_r2".into()),
                    elements: Some(vec![RedisJsonValue::String("val".into())]),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_r2".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = PfcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.cardinality(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfcount_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfcount_r3\r\n").await.expect("raw failed");

            ctx.raw(
                &PfaddInput {
                    key: RedisKey::String("pfcount_r3".into()),
                    elements: Some(vec![RedisJsonValue::String("val".into())]),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx.raw(&PfcountInput { keys: vec![RedisKey::String("pfcount_r3".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = PfcountOutput::decode(&result).expect("decode failed");
            assert_eq!(output.cardinality(), 1);

            ctx.stop().await;
        }
    }
}
