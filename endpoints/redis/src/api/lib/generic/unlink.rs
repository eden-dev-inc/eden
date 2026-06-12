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
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, UnlinkInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Unlink,
    "Asynchronously deletes one or more keys. This command is very similar to DEL: it removes the specified keys. Just like DEL a key is ignored if it does not exist. However the command performs the actual memory reclaiming in a different thread, so it is not blocking.",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `UNLINK`
/// https://redis.io/docs/latest/commands/unlink/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct UnlinkInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for UnlinkInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("UnlinkInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(UnlinkInput, API_INFO, { keys });

impl RedisCommandInput for UnlinkInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("UNLINK requires at least 1 argument, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

impl MultiCommand for UnlinkInput {
    type Single = UnlinkInput;
    type SingleOutput = UnlinkOutput;
    type Output = UnlinkOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|k| UnlinkInput { keys: vec![k] }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let count = parts.into_iter().try_fold(0, |acc, part| part.map(|p| acc + p.count()))?;
        Ok(UnlinkOutput::new(count))
    }
}

/// Output for Redis UNLINK command
///
/// Returns the number of keys that were unlinked.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct UnlinkOutput {
    /// The number of keys that were unlinked
    count: i64,
}

impl UnlinkOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the number of keys that were unlinked
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if any keys were unlinked
    pub fn any_unlinked(&self) -> bool {
        self.count > 0
    }

    /// Decode the Redis protocol response into an UnlinkOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected UNLINK response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected UNLINK response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for UnlinkOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("UnlinkOutput", 1)?;
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
            let input = UnlinkInput { keys: vec![RedisKey::String("mykey".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$6\r\nUNLINK\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = UnlinkInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$6\r\nUNLINK\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = UnlinkInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
            assert_eq!(input.keys[0], RedisKey::String("testkey".into()));
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![RedisJsonValue::String("k1".into()), RedisJsonValue::String("k2".into())];
            let input = UnlinkInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = UnlinkInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = UnlinkInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = UnlinkInput {
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
            let input = UnlinkInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = UnlinkInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
        }

        #[test]
        fn test_reconstruct_sums_counts() {
            let parts = vec![
                Ok(UnlinkOutput::new(1)),
                Ok(UnlinkOutput::new(0)),
                Ok(UnlinkOutput::new(1)),
                Ok(UnlinkOutput::new(1)),
            ];
            let output = UnlinkInput::reconstruct(parts).unwrap();
            assert_eq!(output.count(), 3);
        }

        #[test]
        fn test_decode_output_integer() {
            let output = UnlinkOutput::decode(b":3\r\n").unwrap();
            assert_eq!(output.count(), 3);
            assert!(output.any_unlinked());
        }

        #[test]
        fn test_decode_output_zero() {
            let output = UnlinkOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(!output.any_unlinked());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = UnlinkOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::get::{GetInput, GetOutput};
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_nonexistent_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&UnlinkInput { keys: vec![RedisKey::String("nonexistent_key".into())] }.command())
                        .await
                        .expect("raw failed");

                    let output = UnlinkOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0, "nonexistent key returns 0");
                    assert!(!output.any_unlinked());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_single_existing_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set a key first
                    ctx.write(SetInput {
                        key: RedisKey::String("unlink_single".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    // Unlink it
                    let result =
                        ctx.raw(&UnlinkInput { keys: vec![RedisKey::String("unlink_single".into())] }.command()).await.expect("raw failed");

                    let output = UnlinkOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert!(output.any_unlinked());

                    // Verify key is gone
                    let get_result =
                        ctx.raw(&GetInput { key: RedisKey::String("unlink_single".into()) }.command()).await.expect("raw failed");
                    let get_output = GetOutput::decode(&get_result).expect("decode get failed");
                    assert!(!get_output.exists(), "key should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set multiple keys
                    for i in 1..=3 {
                        ctx.write(SetInput {
                            key: RedisKey::String(format!("unlink_multi_{}", i)),
                            value: RedisJsonValue::String(format!("val{}", i)),
                            ..Default::default()
                        })
                        .await;
                    }

                    // Unlink all of them
                    let result = ctx
                        .raw(
                            &UnlinkInput {
                                keys: vec![
                                    RedisKey::String("unlink_multi_1".into()),
                                    RedisKey::String("unlink_multi_2".into()),
                                    RedisKey::String("unlink_multi_3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = UnlinkOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 3);

                    // Verify all keys are gone
                    for i in 1..=3 {
                        let get_result = ctx
                            .raw(&GetInput { key: RedisKey::String(format!("unlink_multi_{}", i)) }.command())
                            .await
                            .expect("raw failed");
                        let get_output = GetOutput::decode(&get_result).expect("decode failed");
                        assert!(!get_output.exists());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_mixed_existing_nonexisting() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set only some keys
                    ctx.write(SetInput {
                        key: RedisKey::String("unlink_exists".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    // Unlink mix of existing and non-existing
                    let result = ctx
                        .raw(
                            &UnlinkInput {
                                keys: vec![
                                    RedisKey::String("unlink_exists".into()),
                                    RedisKey::String("unlink_missing_1".into()),
                                    RedisKey::String("unlink_missing_2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = UnlinkOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1, "only 1 key existed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Set keys
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_unlink_1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("pipe_unlink_2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&UnlinkInput { keys: vec![RedisKey::String("pipe_unlink_1".into())] }.command());
                    pipeline.extend_from_slice(&UnlinkInput { keys: vec![RedisKey::String("pipe_unlink_2".into())] }.command());
                    pipeline.extend_from_slice(&GetInput { key: RedisKey::String("pipe_unlink_1".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = UnlinkOutput::decode(responses[0]).expect("decode unlink1");
                    assert_eq!(out1.count(), 1);

                    let out2 = UnlinkOutput::decode(responses[1]).expect("decode unlink2");
                    assert_eq!(out2.count(), 1);

                    let get_out = GetOutput::decode(responses[2]).expect("decode get");
                    assert!(!get_out.exists(), "key should be deleted");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_resp2_integer_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp2_unlink".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&UnlinkInput { keys: vec![RedisKey::String("resp2_unlink".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = UnlinkOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_unlink_resp3_integer_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp3_unlink".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&UnlinkInput { keys: vec![RedisKey::String("resp3_unlink".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = UnlinkOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }
    }
}
