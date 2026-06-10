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

const API_INFO: ApiInfo<RedisApi, DelInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Del,
    "Removes the specified keys. A key is ignored if it does not exist. Returns the number of keys that were removed.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `DEL`
/// https://redis.io/docs/latest/commands/del/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct DelInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for DelInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("DelInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(DelInput, API_INFO, { keys });

impl RedisCommandInput for DelInput {
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
            return Err(EpError::parse("DEL requires at least one argument, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

impl MultiCommand for DelInput {
    type Single = DelInput;
    type SingleOutput = DelOutput;
    type Output = DelOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|k| DelInput { keys: vec![k] }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let deleted = parts.into_iter().try_fold(0, |acc, part| part.map(|p| acc + p.deleted()))?;
        Ok(DelOutput::new(deleted))
    }
}

/// Output for Redis DEL command
///
/// Returns the number of keys that were removed.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct DelOutput {
    /// The number of keys that were deleted
    deleted: i64,
}

impl DelOutput {
    pub fn new(deleted: i64) -> Self {
        Self { deleted }
    }
}

impl Serialize for DelOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("DelOutput", 1)?;
        state.serialize_field("deleted", &self.deleted)?;
        state.end()
    }
}

impl DelOutput {
    /// Get the number of deleted keys
    pub fn deleted(&self) -> i64 {
        self.deleted
    }

    /// Decode the Redis protocol response into a DelOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let deleted = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected DEL response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected DEL response: {:?}", other)));
                }
            },
        };

        Ok(Self { deleted })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_single_key() {
            let input = DelInput { keys: vec![RedisKey::String("mykey".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nDEL\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = DelInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$3\r\nDEL\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
        }

        #[test]
        fn test_decode_integer_zero() {
            let output = DelOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.deleted(), 0);
        }

        #[test]
        fn test_decode_integer_one() {
            let output = DelOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.deleted(), 1);
        }

        #[test]
        fn test_decode_integer_multiple() {
            let output = DelOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.deleted(), 5);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = DelOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = DelInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
            };
            assert_eq!(input.keys().len(), 2);
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = DelInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![RedisJsonValue::String("key1".into()), RedisJsonValue::String("key2".into())];
            let input = DelInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = DelInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least one argument"));
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = DelInput {
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
            let input = DelInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = DelInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
        }

        #[test]
        fn test_reconstruct_sums_counts() {
            let parts = vec![
                Ok(DelOutput::new(1)),
                Ok(DelOutput::new(0)),
                Ok(DelOutput::new(1)),
                Ok(DelOutput::new(1)),
            ];
            let output = DelInput::reconstruct(parts).unwrap();
            assert_eq!(output.deleted(), 3);
        }

        #[test]
        fn test_reconstruct_propagates_error() {
            let err = EpError::parse("ERR synthetic failure");
            let result = DelInput::reconstruct(vec![Ok(DelOutput::new(1)), Err(err.clone())]);
            assert_eq!(result.unwrap_err(), err);
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
        async fn test_del_nonexistent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&DelInput { keys: vec![RedisKey::String("nonexistent".into())] }.command()).await.expect("raw failed");

                    let output = DelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 0, "deleting nonexistent key should return 0");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("delkey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&DelInput { keys: vec![RedisKey::String("delkey".into())] }.command()).await.expect("raw failed");

                    let output = DelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 1);

                    // Verify key is gone
                    let get_result = ctx
                        .raw(&crate::api::lib::string::get::GetInput { key: RedisKey::String("delkey".into()) }.command())
                        .await
                        .expect("raw failed");

                    let get_output = crate::api::lib::string::get::GetOutput::decode(&get_result).unwrap();
                    assert!(!get_output.exists(), "key should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set up multiple keys
                    for i in 1..=3 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("multi{}", i)),
                            value: RedisJsonValue::String(format!("val{}", i)),
                            ..Default::default()
                        })
                        .await;
                    }

                    let result = ctx
                        .raw(
                            &DelInput {
                                keys: vec![
                                    RedisKey::String("multi1".into()),
                                    RedisKey::String("multi2".into()),
                                    RedisKey::String("multi3".into()),
                                    RedisKey::String("nonexistent".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = DelOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.deleted(), 3, "should delete 3 existing keys");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_pipeline_set_then_del() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &SetInput {
                            key: RedisKey::String("pipekey".into()),
                            value: RedisJsonValue::String("pipeval".into()),
                            ..Default::default()
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&DelInput { keys: vec![RedisKey::String("pipekey".into())] }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    // SET response
                    assert!(responses[0].starts_with(b"+OK") || responses[0].starts_with(b"$2\r\nOK"));

                    // DEL response
                    let del_output = DelOutput::decode(responses[1]).expect("decode DEL");
                    assert_eq!(del_output.deleted(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_idempotent() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("idemp".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // First delete
                    let result1 = ctx.raw(&DelInput { keys: vec![RedisKey::String("idemp".into())] }.command()).await.expect("raw failed");
                    let output1 = DelOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.deleted(), 1);

                    // Second delete (key already gone)
                    let result2 = ctx.raw(&DelInput { keys: vec![RedisKey::String("idemp".into())] }.command()).await.expect("raw failed");
                    let output2 = DelOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.deleted(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r2key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&DelInput { keys: vec![RedisKey::String("r2key".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = DelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_del_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("r3key".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&DelInput { keys: vec![RedisKey::String("r3key".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = DelOutput::decode(&result).expect("decode failed");
            assert_eq!(output.deleted(), 1);
            ctx.stop().await;
        }
    }
}
