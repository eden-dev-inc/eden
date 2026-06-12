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

const API_INFO: ApiInfo<RedisApi, TouchInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Touch,
    "Returns the number of existing keys out of those specified after updating the time they were last accessed",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `TOUCH`
/// https://redis.io/docs/latest/commands/touch/
///
/// Available since Redis 3.2.1
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TouchInput {
    pub(crate) keys: Vec<RedisKey>,
}

impl Serialize for TouchInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TouchInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(TouchInput, API_INFO, { keys });

impl RedisCommandInput for TouchInput {
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
            return Err(EpError::parse("TOUCH requires at least 1 argument, given none"));
        }

        let keys: Result<Vec<RedisKey>, _> = args.into_iter().map(|k| k.try_into()).collect();

        Ok(Self { keys: keys? })
    }
}

impl MultiCommand for TouchInput {
    type Single = TouchInput;
    type SingleOutput = TouchOutput;
    type Output = TouchOutput;

    fn deconstruct(&self) -> Vec<Self::Single> {
        self.keys.iter().cloned().map(|k| TouchInput { keys: vec![k] }).collect()
    }

    fn reconstruct(parts: Vec<Result<Self::SingleOutput, ::error::EpError>>) -> ResultEP<Self::Output> {
        let count = parts.into_iter().try_fold(0, |acc, part| part.map(|p| acc + p.count()))?;
        Ok(TouchOutput::new(count))
    }
}

/// Output for Redis TOUCH command
///
/// Returns the number of keys that exist and had their last access time updated.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TouchOutput {
    /// The number of keys that were touched (existed)
    count: i64,
}

impl TouchOutput {
    pub fn new(count: i64) -> Self {
        Self { count }
    }

    /// Get the count of touched keys
    pub fn count(&self) -> i64 {
        self.count
    }

    /// Check if any keys were touched
    pub fn any_touched(&self) -> bool {
        self.count > 0
    }

    /// Decode the Redis protocol response into a TouchOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let count = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected TOUCH response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected TOUCH response: {:?}", other)));
                }
            },
        };

        Ok(Self { count })
    }
}

impl Serialize for TouchOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TouchOutput", 1)?;
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
            let input = TouchInput { keys: vec![RedisKey::String("mykey".into())] };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nTOUCH\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_multiple_keys() {
            let input = TouchInput {
                keys: vec![
                    RedisKey::String("key1".into()),
                    RedisKey::String("key2".into()),
                    RedisKey::String("key3".into()),
                ],
            };
            assert_eq!(input.command().to_vec(), b"*4\r\n$5\r\nTOUCH\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n");
        }

        #[test]
        fn test_decode_integer_response() {
            let output = TouchOutput::decode(b":2\r\n").unwrap();
            assert_eq!(output.count(), 2);
            assert!(output.any_touched());
        }

        #[test]
        fn test_decode_zero_response() {
            let output = TouchOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.count(), 0);
            assert!(!output.any_touched());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = TouchOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("testkey".into())];
            let input = TouchInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_multiple_keys() {
            let args = vec![RedisJsonValue::String("k1".into()), RedisJsonValue::String("k2".into())];
            let input = TouchInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TouchInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = TouchInput {
                keys: vec![RedisKey::String("a".into()), RedisKey::String("b".into())],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }

        #[test]
        fn test_deconstruct_length_matches_keys() {
            let input = TouchInput {
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
            let input = TouchInput { keys: keys.clone() };
            let parts = input.deconstruct();
            for (i, part) in parts.iter().enumerate() {
                assert_eq!(part.keys(), vec![keys[i].clone()]);
            }
        }

        #[test]
        fn test_deconstruct_single_key() {
            let input = TouchInput { keys: vec![RedisKey::String("only".into())] };
            let parts = input.deconstruct();
            assert_eq!(parts.len(), 1);
            assert_eq!(parts[0].keys(), vec![RedisKey::String("only".into())]);
        }

        #[test]
        fn test_reconstruct_sums_counts() {
            let parts = vec![
                Ok(TouchOutput::new(1)),
                Ok(TouchOutput::new(0)),
                Ok(TouchOutput::new(1)),
                Ok(TouchOutput::new(1)),
            ];
            let output = TouchInput::reconstruct(parts).unwrap();
            assert_eq!(output.count(), 3);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::string::set::SetInput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_nonexistent() {
            test_all_protocols_min_version("3.2.1", |ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&TouchInput { keys: vec![RedisKey::String("missing".into())] }.command()).await.expect("raw failed");

                    let output = TouchOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 0);
                    assert!(!output.any_touched());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_existing_key() {
            test_all_protocols_min_version("3.2.1", |ctx| {
                Box::pin(async move {
                    // Create a key first
                    ctx.write(SetInput {
                        key: RedisKey::String("touchme".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result =
                        ctx.raw(&TouchInput { keys: vec![RedisKey::String("touchme".into())] }.command()).await.expect("raw failed");

                    let output = TouchOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 1);
                    assert!(output.any_touched());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_multiple_keys_mixed() {
            test_all_protocols_min_version("3.2.1", |ctx| {
                Box::pin(async move {
                    // Create two keys
                    ctx.write(SetInput {
                        key: RedisKey::String("exists1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("exists2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    // Touch 2 existing + 1 missing
                    let result = ctx
                        .raw(
                            &TouchInput {
                                keys: vec![
                                    RedisKey::String("exists1".into()),
                                    RedisKey::String("missing".into()),
                                    RedisKey::String("exists2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TouchOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.count(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_pipeline() {
            test_all_protocols_min_version("3.2.1", |ctx| {
                Box::pin(async move {
                    // Setup keys
                    ctx.write(SetInput {
                        key: RedisKey::String("p1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&TouchInput { keys: vec![RedisKey::String("p1".into())] }.command());
                    pipeline.extend_from_slice(&TouchInput { keys: vec![RedisKey::String("nonexistent".into())] }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = TouchOutput::decode(responses[0]).expect("decode touch1");
                    assert_eq!(out1.count(), 1);

                    let out2 = TouchOutput::decode(responses[1]).expect("decode touch2");
                    assert_eq!(out2.count(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp2touch".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&TouchInput { keys: vec![RedisKey::String("resp2touch".into())] }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = TouchOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_touch_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            ctx.write(SetInput {
                key: RedisKey::String("resp3touch".into()),
                value: RedisJsonValue::String("val".into()),
                ..Default::default()
            })
            .await;

            let result = ctx.raw(&TouchInput { keys: vec![RedisKey::String("resp3touch".into())] }.command()).await.expect("raw failed");

            // RESP3 integer format is same as RESP2: :N\r\n
            let output = TouchOutput::decode(&result).expect("decode failed");
            assert_eq!(output.count(), 1);
            ctx.stop().await;
        }
    }
}
