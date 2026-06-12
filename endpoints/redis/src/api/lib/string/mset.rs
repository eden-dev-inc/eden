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

const API_INFO: ApiInfo<RedisApi, MsetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Mset,
    "Sets the given keys to their respective values. MSET replaces existing values with new values, just as regular SET. See MSETNX if you don't want to overwrite existing values",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `MSET`
/// https://redis.io/docs/latest/commands/mset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct MsetInput {
    pub(crate) sets: Vec<MsetPair>,
}

impl Serialize for MsetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("MsetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("sets", &self.sets)?;
        state.end()
    }
}

/// A key-value pair for MSET command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub(crate) struct MsetPair {
    pub(crate) key: RedisKey,
    pub(crate) value: RedisJsonValue,
}

impl_redis_operation!(MsetInput, API_INFO, { sets });

impl RedisCommandInput for MsetInput {
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
            return Err(EpError::request("MSET requires at least one key-value pair"));
        }

        if !args.len().is_multiple_of(2) {
            return Err(EpError::request(format!(
                "MSET requires an even number of arguments (key-value pairs), given {}",
                args.len()
            )));
        }

        let sets: Result<Vec<MsetPair>, EpError> =
            args.chunks_exact(2).map(|chunk| Ok(MsetPair { key: chunk[0].clone().try_into()?, value: chunk[1].clone() })).collect();

        Ok(Self { sets: sets? })
    }
}

/// Output for Redis MSET command
///
/// Always returns OK since MSET never fails.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MsetOutput {
    /// Always "OK" on success
    status: String,
}

impl MsetOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    /// Decode the Redis protocol response into an MsetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected MSET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected MSET response: {:?}", other))),
            },
        }
    }
}

impl Default for MsetOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for MsetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("MsetOutput", 1)?;
        state.serialize_field("status", &self.status)?;
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
            let input = MsetInput {
                sets: vec![MsetPair {
                    key: RedisKey::String("key1".into()),
                    value: RedisJsonValue::String("val1".into()),
                }],
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nMSET\r\n$4\r\nkey1\r\n$4\r\nval1\r\n");
        }

        #[test]
        fn test_encode_command_multiple_pairs() {
            let input = MsetInput {
                sets: vec![
                    MsetPair {
                        key: RedisKey::String("k1".into()),
                        value: RedisJsonValue::String("v1".into()),
                    },
                    MsetPair {
                        key: RedisKey::String("k2".into()),
                        value: RedisJsonValue::String("v2".into()),
                    },
                ],
            };
            assert_eq!(input.command().to_vec(), b"*5\r\n$4\r\nMSET\r\n$2\r\nk1\r\n$2\r\nv1\r\n$2\r\nk2\r\n$2\r\nv2\r\n");
        }

        #[test]
        fn test_decode_ok_response() {
            let output = MsetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = MsetOutput::decode(b"-ERR unknown\r\n").unwrap_err();
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
            let input = MsetInput::decode(args).unwrap();
            assert_eq!(input.sets.len(), 2);
            assert_eq!(input.sets[0].key, RedisKey::String("key1".into()));
            assert_eq!(input.sets[1].key, RedisKey::String("key2".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = MsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one key-value pair"));
        }

        #[test]
        fn test_decode_input_odd_args_fails() {
            let args = vec![
                RedisJsonValue::String("key1".into()),
                RedisJsonValue::String("val1".into()),
                RedisJsonValue::String("key2".into()),
            ];
            let err = MsetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("even number"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = MsetInput {
                sets: vec![
                    MsetPair {
                        key: RedisKey::String("a".into()),
                        value: RedisJsonValue::String("1".into()),
                    },
                    MsetPair {
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
        use crate::api::lib::string::get::GetOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mset_single_pair() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MsetInput {
                                sets: vec![MsetPair {
                                    key: RedisKey::String("mset_single".into()),
                                    value: RedisJsonValue::String("val".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify the value was set
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("mset_single".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("val")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mset_multiple_pairs() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &MsetInput {
                                sets: vec![
                                    MsetPair {
                                        key: RedisKey::String("mset_m1".into()),
                                        value: RedisJsonValue::String("v1".into()),
                                    },
                                    MsetPair {
                                        key: RedisKey::String("mset_m2".into()),
                                        value: RedisJsonValue::String("v2".into()),
                                    },
                                    MsetPair {
                                        key: RedisKey::String("mset_m3".into()),
                                        value: RedisJsonValue::String("v3".into()),
                                    },
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify all values were set
                    for (key, expected) in [("mset_m1", "v1"), ("mset_m2", "v2"), ("mset_m3", "v3")] {
                        let get_result = ctx.raw(&GetInput { key: RedisKey::String(key.into()) }.command()).await.expect("raw failed");

                        let get_output = GetOutput::decode(&get_result).expect("decode failed");
                        assert_eq!(get_output.value(), Some(&RedisJsonValue::from(expected)));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mset_overwrites_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // First set
                    ctx.raw(
                        &MsetInput {
                            sets: vec![MsetPair {
                                key: RedisKey::String("mset_overwrite".into()),
                                value: RedisJsonValue::String("original".into()),
                            }],
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Overwrite
                    let result = ctx
                        .raw(
                            &MsetInput {
                                sets: vec![MsetPair {
                                    key: RedisKey::String("mset_overwrite".into()),
                                    value: RedisJsonValue::String("updated".into()),
                                }],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = MsetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());

                    // Verify overwrite
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("mset_overwrite".into()) }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode failed");
                    assert_eq!(get_output.value(), Some(&RedisJsonValue::from("updated")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mset_resp2_ok_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &MsetInput {
                        sets: vec![MsetPair {
                            key: RedisKey::String("r2key".into()),
                            value: RedisJsonValue::String("r2val".into()),
                        }],
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 simple string OK format");
            let output = MsetOutput::decode(&result).expect("decode failed");
            assert!(output.is_ok());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_mset_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &MsetInput {
                            sets: vec![
                                MsetPair {
                                    key: RedisKey::String("pipe_k1".into()),
                                    value: RedisJsonValue::String("pipe_v1".into()),
                                },
                                MsetPair {
                                    key: RedisKey::String("pipe_k2".into()),
                                    value: RedisJsonValue::String("pipe_v2".into()),
                                },
                            ],
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe_k1".into()) }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe_k2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let mset_output = MsetOutput::decode(responses[0]).expect("decode MSET");
                    assert!(mset_output.is_ok());

                    let get1 = GetOutput::decode(responses[1]).expect("decode GET 1");
                    assert_eq!(get1.value(), Some(&RedisJsonValue::from("pipe_v1")));

                    let get2 = GetOutput::decode(responses[2]).expect("decode GET 2");
                    assert_eq!(get2.value(), Some(&RedisJsonValue::from("pipe_v2")));
                })
            })
            .await;
        }
    }
}
