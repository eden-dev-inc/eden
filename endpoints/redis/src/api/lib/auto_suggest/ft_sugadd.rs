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

const API_INFO: ApiInfo<RedisApi, FtSugaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::FtSugadd,
    "Add a suggestion string to an auto-complete suggestion dictionary",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `FT.SUGADD`
/// https://redis.io/docs/latest/commands/ft.sugadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, PartialEq)]
pub struct FtSugaddInput {
    pub(crate) key: RedisKey,
    pub(crate) string: RedisJsonValue,
    pub(crate) score: RedisJsonValue,
    pub(crate) incr: Option<bool>,
    pub(crate) payload: Option<RedisJsonValue>,
}

impl Serialize for FtSugaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut field_count = 4; // type, key, string, score

        if self.incr.is_some() {
            field_count += 1;
        }
        if self.payload.is_some() {
            field_count += 1;
        }

        let mut state = serializer.serialize_struct("FtSugaddInput", field_count)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("string", &self.string)?;
        state.serialize_field("score", &self.score)?;

        if let Some(incr) = &self.incr {
            state.serialize_field("incr", incr)?;
        }

        if let Some(payload) = &self.payload {
            state.serialize_field("payload", payload)?;
        }

        state.end()
    }
}

impl_redis_operation!(FtSugaddInput, API_INFO, { key, string, score, incr, payload });

impl RedisCommandInput for FtSugaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.string).arg(&self.score);

        if let Some(incr) = &self.incr
            && *incr
        {
            command.arg("INCR");
        }

        if let Some(payload) = &self.payload {
            command.arg("PAYLOAD").arg(payload);
        }

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("FT.SUGADD requires at least 3 arguments, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let string = args[1].clone();
        let score = args[2].clone();
        let mut incr = None;
        let mut payload = None;

        let mut i = 3;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "INCR" => {
                        incr = Some(true);
                        i += 1;
                    }
                    "PAYLOAD" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("PAYLOAD requires a value"));
                        }
                        payload = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown FT.SUGADD option: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::parse("FT.SUGADD options must be strings"));
                }
            }
        }

        Ok(FtSugaddInput { key, string, score, incr, payload })
    }
}

/// Output for Redis FT.SUGADD command
///
/// Returns the current size of the suggestion dictionary.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct FtSugaddOutput {
    /// The current size of the suggestion dictionary after the add
    length: i64,
}

impl FtSugaddOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the current dictionary size
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Decode the Redis protocol response into a FtSugaddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(i) => i,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected FT.SUGADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for FtSugaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("FtSugaddOutput", 1)?;
        state.serialize_field("length", &self.length)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_basic() {
            let input = FtSugaddInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
                score: RedisJsonValue::Float(1.0),
                incr: None,
                payload: None,
            };
            // Note: Rust's default f64 Display formats 1.0 as "1" (without decimal point)
            assert_eq!(input.command().to_vec(), b"*4\r\n$9\r\nFT.SUGADD\r\n$6\r\nmydict\r\n$5\r\nhello\r\n$1\r\n1\r\n");
        }

        #[test]
        fn test_encode_command_with_incr() {
            let input = FtSugaddInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
                score: RedisJsonValue::Float(1.0),
                incr: Some(true),
                payload: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"INCR"));
        }

        #[test]
        fn test_encode_command_incr_false_not_included() {
            let input = FtSugaddInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
                score: RedisJsonValue::Float(1.0),
                incr: Some(false),
                payload: None,
            };
            let cmd = input.command();
            assert!(!cmd.windows(4).any(|w| w == b"INCR"));
        }

        #[test]
        fn test_encode_command_with_payload() {
            let input = FtSugaddInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
                score: RedisJsonValue::Integer(1),
                incr: None,
                payload: Some(RedisJsonValue::String("mydata".into())),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"PAYLOAD"));
            assert!(cmd.windows(6).any(|w| w == b"mydata"));
        }

        #[test]
        fn test_encode_command_with_incr_and_payload() {
            let input = FtSugaddInput {
                key: RedisKey::String("mydict".into()),
                string: RedisJsonValue::String("hello".into()),
                score: RedisJsonValue::Integer(1),
                incr: Some(true),
                payload: Some(RedisJsonValue::String("mydata".into())),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"INCR"));
            assert!(cmd.windows(7).any(|w| w == b"PAYLOAD"));
        }

        #[test]
        fn test_decode_integer_response() {
            let output = FtSugaddOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.length(), 1);
        }

        #[test]
        fn test_decode_larger_integer() {
            let output = FtSugaddOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.length(), 42);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = FtSugaddOutput::decode(b"-ERR invalid score\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hello".into()),
                RedisJsonValue::Float(1.0),
            ];
            let input = FtSugaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mydict".into()));
            assert_eq!(input.string, RedisJsonValue::String("hello".into()));
            assert_eq!(input.score, RedisJsonValue::Float(1.0));
            assert_eq!(input.incr, None);
            assert_eq!(input.payload, None);
        }

        #[test]
        fn test_decode_input_with_incr() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hello".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("INCR".into()),
            ];
            let input = FtSugaddInput::decode(args).unwrap();
            assert_eq!(input.incr, Some(true));
        }

        #[test]
        fn test_decode_input_with_payload() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hello".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("PAYLOAD".into()),
                RedisJsonValue::String("mydata".into()),
            ];
            let input = FtSugaddInput::decode(args).unwrap();
            assert_eq!(input.payload, Some(RedisJsonValue::String("mydata".into())));
        }

        #[test]
        fn test_decode_input_insufficient_args() {
            let args = vec![RedisJsonValue::String("mydict".into()), RedisJsonValue::String("hello".into())];
            let err = FtSugaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_input_payload_missing_value() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hello".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("PAYLOAD".into()),
            ];
            let err = FtSugaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("PAYLOAD requires a value"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![
                RedisJsonValue::String("mydict".into()),
                RedisJsonValue::String("hello".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("UNKNOWN".into()),
            ];
            let err = FtSugaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown FT.SUGADD option"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = FtSugaddInput {
                key: RedisKey::String("testkey".into()),
                string: RedisJsonValue::String("test".into()),
                score: RedisJsonValue::Integer(1),
                incr: None,
                payload: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testkey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{FtSuglenInput, FtSuglenOutput};
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("add_sug".into()),
                                string: RedisJsonValue::String("hello".into()),
                                score: RedisJsonValue::Float(1.0),
                                incr: None,
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSugaddOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_multiple() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result1 = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("multi_sug".into()),
                                string: RedisJsonValue::String("hello".into()),
                                score: RedisJsonValue::Float(1.0),
                                incr: None,
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output1 = FtSugaddOutput::decode(&result1).expect("decode failed");
                    assert_eq!(output1.length(), 1);

                    let result2 = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("multi_sug".into()),
                                string: RedisJsonValue::String("world".into()),
                                score: RedisJsonValue::Float(2.0),
                                incr: None,
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output2 = FtSugaddOutput::decode(&result2).expect("decode failed");
                    assert_eq!(output2.length(), 2);

                    let result3 = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("multi_sug".into()),
                                string: RedisJsonValue::String("help".into()),
                                score: RedisJsonValue::Float(1.5),
                                incr: None,
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output3 = FtSugaddOutput::decode(&result3).expect("decode failed");
                    assert_eq!(output3.length(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_with_incr() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Add initial suggestion
                    ctx.raw(
                        &FtSugaddInput {
                            key: RedisKey::String("incr_sug".into()),
                            string: RedisJsonValue::String("hello".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add same suggestion with INCR - should increment score
                    let result = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("incr_sug".into()),
                                string: RedisJsonValue::String("hello".into()),
                                score: RedisJsonValue::Float(2.0),
                                incr: Some(true),
                                payload: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSugaddOutput::decode(&result).expect("decode failed");
                    // Length should still be 1 since it's the same string
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_with_payload() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &FtSugaddInput {
                                key: RedisKey::String("payload_sug".into()),
                                string: RedisJsonValue::String("hello".into()),
                                score: RedisJsonValue::Float(1.0),
                                incr: None,
                                payload: Some(RedisJsonValue::String("extra_data".into())),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = FtSugaddOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_pipeline() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(
                        &FtSugaddInput {
                            key: RedisKey::String("pipe_add".into()),
                            string: RedisJsonValue::String("one".into()),
                            score: RedisJsonValue::Float(1.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(
                        &FtSugaddInput {
                            key: RedisKey::String("pipe_add".into()),
                            string: RedisJsonValue::String("two".into()),
                            score: RedisJsonValue::Float(2.0),
                            incr: None,
                            payload: None,
                        }
                        .command(),
                    );
                    pipeline.extend_from_slice(&FtSuglenInput { key: RedisKey::String("pipe_add".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let add1 = FtSugaddOutput::decode(responses[0]).expect("decode add1");
                    assert_eq!(add1.length(), 1);

                    let add2 = FtSugaddOutput::decode(responses[1]).expect("decode add2");
                    assert_eq!(add2.length(), 2);

                    let len = FtSuglenOutput::decode(responses[2]).expect("decode len");
                    assert_eq!(len.length(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_resp2_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            let result = ctx
                .raw(
                    &FtSugaddInput {
                        key: RedisKey::String("resp2_add".into()),
                        string: RedisJsonValue::String("test".into()),
                        score: RedisJsonValue::Float(1.0),
                        incr: None,
                        payload: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 integer format");
            let output = FtSugaddOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sugadd_resp3_format() {
            let mut ctx = setup_with_stack(RespVersion::Resp3, None).await;

            let result = ctx
                .raw(
                    &FtSugaddInput {
                        key: RedisKey::String("resp3_add".into()),
                        string: RedisJsonValue::String("test".into()),
                        score: RedisJsonValue::Float(1.0),
                        incr: None,
                        payload: None,
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = FtSugaddOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);
            ctx.stop().await;
        }
    }
}
