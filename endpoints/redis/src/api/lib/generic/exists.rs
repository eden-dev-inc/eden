use crate::api::lib::{MultiCommand, RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use error::ResultEP;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, ExistsInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Exists,
    "Returns the number of keys that exist from those specified. Duplicate keys are counted multiple times.",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `EXISTS`
/// https://redis.io/docs/latest/commands/exists/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct ExistsInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for ExistsInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("ExistsInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(ExistsInput, API_INFO, { keys });

impl RedisCommandInput for ExistsInput {
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
            return Err(EpError::parse("EXISTS requires at least one argument, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

/// `MultiCommand` impl for `EXISTS`.
///
/// `EXISTS` counts duplicate keys in its multi-key form: if the same existing
/// key is specified twice, the response is `2`. The decomposed sum preserves
/// this semantic because each duplicate produces its own per-key `EXISTS 1`
/// call whose result is summed unchanged.
impl MultiCommand for ExistsInput {
    type Single = ExistsInput;
    type SingleOutput = ExistsOutput;
    type Output = ExistsOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|k| ExistsInput { keys: vec![k] }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let count = parts.into_iter().try_fold(0, |acc, part| part.map(|p| acc + p.count()))?;
        Ok(ExistsOutput::new(count))
    }
}

/// Output for Redis EXISTS command
///
/// Returns the number of keys that exist from those specified as arguments.
/// Duplicate keys are counted, so if a key exists and is specified twice, 2 is returned.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ExistsOutput {
    /// The count of keys that exist
    count: i64,
}

impl ExistsOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the count of existing keys
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if all queried keys exist
    pub fn all_exist(&self, expected: usize) -> bool {
        self.count == expected as i64
    }

    /// Check if at least one key exists
    pub fn any_exist(&self) -> bool {
        self.count > 0
    }

    /// Check if no keys exist
    pub fn none_exist(&self) -> bool {
        self.count == 0
    }

    /// Decode the Redis protocol response into an ExistsOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected EXISTS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected EXISTS response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for ExistsOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("ExistsOutput", 1)?;
        state.serialize_field("count", &self.count)?;
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
            let input = ExistsInput { keys: vec![RedisKey::String("mykey".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nEXISTS\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = ExistsInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nEXISTS\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = ExistsOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(output.none_exist());
            assert!(!output.any_exist());
        }

        #[test]
        fn test_decode_integer_one() {
            let output = ExistsOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.count(), 1);
            assert!(output.any_exist());
            assert!(!output.none_exist());
        }

        #[test]
        fn test_decode_integer_multiple() {
            let output = ExistsOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.count(), 3);
            assert!(output.all_exist(3));
            assert!(!output.all_exist(4));
        }

        #[test]
        fn test_decode_error_fails() {
            let err = ExistsOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_all_exist_helper() {
            let output = ExistsOutput::new(3);
            assert!(output.all_exist(3));
            assert!(!output.all_exist(2));
            assert!(!output.all_exist(4));
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = ExistsInput {
                keys: vec![
                    RedisKey::String("a".into()),
                    RedisKey::String("b".into()),
                    RedisKey::String("c".into()),
                ],
            };
            assert_eq!(input.deconstruct().len(), input.keys().len());
        }

        #[test]
        fn test_deconstruct_order_preserved() {
            let keys = vec![
                RedisKey::String("first".into()),
                RedisKey::String("second".into()),
                RedisKey::String("third".into()),
            ];
            let input = ExistsInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = ExistsInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
        }

        #[test]
        fn test_reconstruct_sums_counts() {
            let parts = vec![Ok(ExistsOutput::new(1)), Ok(ExistsOutput::new(1)), Ok(ExistsOutput::new(0))];
            let output = ExistsInput::reconstruct(parts).unwrap();
            assert_eq!(output.count(), 2);
        }

        #[test]
        fn test_reconstruct_preserves_duplicate_counting() {
            // Multi-key EXISTS with the same existing key twice returns 2.
            // Decomposed: two single-key EXISTS calls each return 1; sum = 2.
            let input = ExistsInput {
                keys: vec![RedisKey::String("dup".into()), RedisKey::String("dup".into())],
            };
            assert_eq!(input.deconstruct().len(), 2);
            let parts = vec![Ok(ExistsOutput::new(1)), Ok(ExistsOutput::new(1))];
            let output = ExistsInput::reconstruct(parts).unwrap();
            assert_eq!(output.count(), 2);
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
        async fn test_exists_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&ExistsInput { keys: vec![RedisKey::String("missing".into())] }.command()).await.expect("raw failed");

                    let output = ExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.none_exist());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_single_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("exists_key".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&ExistsInput { keys: vec![RedisKey::String("exists_key".into())] }.command()).await.expect("raw failed");

                    let output = ExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert!(output.any_exist());
                    assert!(output.all_exist(1));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_multiple_keys_mixed() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("e1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("e3".into()),
                        value: RedisJsonValue::String("v3".into()),
                        ..Default::default()
                    })
                    .await;

                    // Check e1 (exists), e2 (missing), e3 (exists)
                    let result = ctx
                        .raw(
                            &ExistsInput {
                                keys: vec![
                                    RedisKey::String("e1".into()),
                                    RedisKey::String("e2".into()),
                                    RedisKey::String("e3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2);
                    assert!(output.any_exist());
                    assert!(!output.all_exist(3));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_duplicate_keys_counted() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("dup".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // EXISTS dup dup dup should return 3 (key counted each time)
                    let result = ctx
                        .raw(
                            &ExistsInput {
                                keys: vec![
                                    RedisKey::String("dup".into()),
                                    RedisKey::String("dup".into()),
                                    RedisKey::String("dup".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3, "duplicate keys should be counted multiple times");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_all_missing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &ExistsInput {
                                keys: vec![
                                    RedisKey::String("nope1".into()),
                                    RedisKey::String("nope2".into()),
                                    RedisKey::String("nope3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = ExistsOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(output.none_exist());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("pk1".into()),
                        value: RedisJsonValue::String("pv1".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&ExistsInput { keys: vec![RedisKey::String("pk1".into())] }.command());
                    pipeline.extend_from_slice(&ExistsInput { keys: vec![RedisKey::String("missing".into())] }.command());
                    pipeline.extend_from_slice(
                        &ExistsInput {
                            keys: vec![RedisKey::String("pk1".into()), RedisKey::String("missing".into())],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = ExistsOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.count(), 1);

                    let out2 = ExistsOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.count(), 0);

                    let out3 = ExistsOutput::decode(responses[2]).expect("decode third");
                    assert_eq!(out3.count(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&ExistsInput { keys: vec![RedisKey::String("r2key".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = ExistsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_exists_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&ExistsInput { keys: vec![RedisKey::String("r3key".into())] }.command()).await.expect("raw failed");

            // RESP3 also uses :N\r\n for integers
            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = ExistsOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }
    }
}
