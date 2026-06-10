use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, MsetnxInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Msetnx,
    "Atomically modifies the string values of one or more keys only when all keys don't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `MSETNX`
/// https://redis.io/docs/latest/commands/msetnx/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MsetnxInput {
    pub(crate) sets: Vec<MsetnxPair>,
}

impl Serialize for MsetnxInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MsetnxInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("sets", &self.sets)?;
        state.end()
    }
}

/// A key-value pair for MSETNX command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub(crate) struct MsetnxPair {
    pub(crate) key: RedisKey,
    pub(crate) value: RedisJsonValue,
}

impl_redis_operation!(MsetnxInput, API_INFO, { sets });

impl RedisCommandInput for MsetnxInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.sets.iter().map(|s| s.key.clone()).collect()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        for set in &self.sets {
            command.arg(&set.key).arg(&set.value);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("MSETNX requires at least one key-value pair"));
        }

        if !args.len().is_multiple_of(2) {
            return Err(EpError::request(format!(
                "MSETNX requires an even number of arguments (key-value pairs), given {}",
                args.len()
            )));
        }

        let sets: Result<Vec<MsetnxPair>, EpError> = args
            .chunks_exact(2)
            .map(|chunk| Ok(MsetnxPair { key: chunk[0].clone().try_into()?, value: chunk[1].clone() }))
            .collect();

        Ok(Self { sets: sets? })
    }
}

/// Output for Redis MSETNX command
///
/// Returns 1 if all keys were set, 0 if no keys were set (at least one already existed).
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MsetnxOutput {
    /// 1 if all keys were set, 0 if none were set
    result: i64,
}

impl MsetnxOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Check if all keys were set successfully
    pub fn all_set(&self) -> bool {
        self.result == 1
    }

    /// Check if no keys were set (at least one key already existed)
    pub fn none_set(&self) -> bool {
        self.result == 0
    }

    /// Get the raw result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Decode the Redis protocol response into an MsetnxOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MSETNX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MSETNX response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for MsetnxOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MsetnxOutput", 1)?;
        state.serialize_field("result", &self.result)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_pair() {
            let input = MsetnxInput {
                sets: vec![MsetnxPair {
                    key: RedisKey::String("key1".into()),
                    value: RedisJsonValue::String("val1".into()),
                }],
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$6\r\nMSETNX\r\n$4\r\nkey1\r\n$4\r\nval1\r\n");
        }

        #[test]
        fn test_encode_command_multiple_pairs() {
            let input = MsetnxInput {
                sets: vec![
                    MsetnxPair {
                        key: RedisKey::String("k1".into()),
                        value: RedisJsonValue::String("v1".into()),
                    },
                    MsetnxPair {
                        key: RedisKey::String("k2".into()),
                        value: RedisJsonValue::String("v2".into()),
                    },
                ],
            };
            assert_eq!(input.command().to_vec(), b"*5\r\n$6\r\nMSETNX\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
        }

        #[test]
        fn test_decode_success() {
            let output = MsetnxOutput::decode(b":1\r\n").unwrap();
            assert!(output.all_set());
            assert!(!output.none_set());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_failure() {
            let output = MsetnxOutput::decode(b":0\r\n").unwrap();
            assert!(!output.all_set());
            assert!(output.none_set());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MsetnxOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("val1".into()),
                RedisJsonValue::String("key2".into()),
                RedisJsonValue::String("val2".into()),
            ];
            let input = MsetnxInput::decode(args).unwrap();
            assert_eq!(input.sets.len(), 2);
            assert_eq!(input.sets[0].key, RedisKey::String("key1".into()));
            assert_eq!(input.sets[1].key, RedisKey::String("key2".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = MsetnxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one key-value pair"));
        }

        #[test]
        fn test_decode_input_odd_args_fails() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("val1".into()),
                RedisJsonValue::String("key2".into()),
            ];
            let err = MsetnxInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("even number"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = MsetnxInput {
                sets: vec![
                    MsetnxPair {
                        key: RedisKey::String("a".into()),
                        value: RedisJsonValue::String("1".into()),
                    },
                    MsetnxPair {
                        key: RedisKey::String("b".into()),
                        value: RedisJsonValue::String("2".into()),
                    },
                ],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("a".into()));
            assert_eq!(keys[1], RedisKey::String("b".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::GetInput;
        use crate::api::SetInput;
        use crate::api::lib::string::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_msetnx_all_new_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MsetnxInput {
                                sets: vec![
                                    MsetnxPair {
                                        key: RedisKey::String("msetnx_new1".into()),
                                        value: RedisJsonValue::String("v1".into()),
                                    },
                                    MsetnxPair {
                                        key: RedisKey::String("msetnx_new2".into()),
                                        value: RedisJsonValue::String("v2".into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.all_set());

                    // Verify values were set
                    let get1 = ctx.raw(&GetInput { key: RedisKey::String("msetnx_new1".into()) }.command()).await.expect("raw failed");
                    let get1_output = GetOutput::decode(&get1).expect("decode failed");
                    assert_eq!(get1_output.value(), Some(&RedisJsonValue::from("v1")));

                    let get2 = ctx.raw(&GetInput { key: RedisKey::String("msetnx_new2".into()) }.command()).await.expect("raw failed");
                    let get2_output = GetOutput::decode(&get2).expect("decode failed");
                    assert_eq!(get2_output.value(), Some(&RedisJsonValue::from("v2")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_msetnx_one_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Pre-set one key
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("msetnx_exist".into()),
                            value: RedisJsonValue::String("original".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Try to set both existing and new key
                    let result = ctx
                        .raw(
                            &MsetnxInput {
                                sets: vec![
                                    MsetnxPair {
                                        key: RedisKey::String("msetnx_exist".into()),
                                        value: RedisJsonValue::String("updated".into()),
                                    },
                                    MsetnxPair {
                                        key: RedisKey::String("msetnx_new_fail".into()),
                                        value: RedisJsonValue::String("new".into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.none_set());

                    // Verify original key unchanged
                    let get1 = ctx.raw(&GetInput { key: RedisKey::String("msetnx_exist".into()) }.command()).await.expect("raw failed");
                    let get1_output = GetOutput::decode(&get1).expect("decode failed");
                    assert_eq!(get1_output.value(), Some(&RedisJsonValue::from("original")));

                    // Verify new key was NOT set
                    let get2 = ctx.raw(&GetInput { key: RedisKey::String("msetnx_new_fail".into()) }.command()).await.expect("raw failed");
                    let get2_output = GetOutput::decode(&get2).expect("decode failed");
                    assert!(!get2_output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_msetnx_single_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MsetnxInput {
                                sets: vec![MsetnxPair {
                                    key: RedisKey::String("msetnx_single".into()),
                                    value: RedisJsonValue::String("single_val".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetnxOutput::decode(&result).expect("decode failed");
                    assert!(output.all_set());

                    // Try again with same key - should fail
                    let result2 = ctx
                        .raw(
                            &MsetnxInput {
                                sets: vec![MsetnxPair {
                                    key: RedisKey::String("msetnx_single".into()),
                                    value: RedisJsonValue::String("updated".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output2 = MsetnxOutput::decode(&result2).expect("decode failed");
                    assert!(output2.none_set());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_msetnx_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &MsetnxInput {
                        sets: vec![MsetnxPair {
                            key: RedisKey::String("r2key_nx".into()),
                            value: RedisJsonValue::String("r2val".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = MsetnxOutput::decode(&result).expect("decode failed");
            assert!(output.all_set());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_msetnx_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &MsetnxInput {
                            sets: vec![
                                MsetnxPair {
                                    key: RedisKey::String("pipnx_k1".into()),
                                    value: RedisJsonValue::String("v1".into()),
                                },
                                MsetnxPair {
                                    key: RedisKey::String("pipnx_k2".into()),
                                    value: RedisJsonValue::String("v2".into()),
                                },
                            ],
                        }
                        .command(),
                    );
                    // Try to set again - should fail
                    pipeline.extend_from_slice(
                        &MsetnxInput {
                            sets: vec![MsetnxPair {
                                key: RedisKey::String("pipnx_k1".into()),
                                value: RedisJsonValue::String("updated".into()),
                            }],
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = MsetnxOutput::decode(responses[0]).expect("decode first MSETNX");
                    assert!(out1.all_set());

                    let out2 = MsetnxOutput::decode(responses[1]).expect("decode second MSETNX");
                    assert!(out2.none_set());
                })
            })
            .await;
        }
    }
}
