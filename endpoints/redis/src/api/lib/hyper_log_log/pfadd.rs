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

const API_INFO: ApiInfo<RedisApi, PfaddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Pfadd,
    "Adds elements to a HyperLogLog key. Creates the key if it doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `PFADD`
/// https://redis.io/docs/latest/commands/pfadd/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct PfaddInput {
    pub(crate) key: RedisKey,
    pub(crate) elements: Option<Vec<RedisJsonValue>>,
}

impl Serialize for PfaddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.elements.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("PfaddInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(elements) = &self.elements {
            state.serialize_field("elements", elements)?;
        }
        state.end()
    }
}

impl_redis_operation!(PfaddInput, API_INFO, { key, elements });

impl RedisCommandInput for PfaddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(elements) = &self.elements {
            for e in elements {
                command.arg(e);
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("PFADD requires at least 1 argument, given none"));
        }

        let key = args[0].clone().try_into()?;
        let elements = if args.len() > 1 { Some(args[1..].to_vec()) } else { None };

        Ok(Self { key, elements })
    }
}

/// Output for Redis PFADD command
///
/// Returns 1 if at least one internal register was altered, 0 otherwise.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct PfaddOutput {
    /// 1 if the HyperLogLog internal registers were altered, 0 otherwise
    altered: i64,
}

impl PfaddOutput {
    pub fn new(altered: i64) -> Self {
        Self { altered }
    }

    /// Returns true if the HyperLogLog registers were altered (new elements added)
    pub fn was_altered(&self) -> bool {
        self.altered == 1
    }

    /// Get the raw return value (1 = altered, 0 = not altered)
    pub fn altered(&self) -> i64 {
        self.altered
    }

    /// Decode the Redis protocol response into a PfaddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let altered = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected PFADD response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected PFADD response: {:?}", other)));
                }
            },
        };

        Ok(Self { altered })
    }
}

impl Serialize for PfaddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("PfaddOutput", 1)?;
        state.serialize_field("altered", &self.altered)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_key_only() {
            let input = PfaddInput { key: RedisKey::String("hll".into()), elements: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nPFADD\r\n$3\r\nhll\r\n");
        }

        #[test]
        fn test_encode_command_with_elements() {
            let input = PfaddInput {
                key: RedisKey::String("hll".into()),
                elements: Some(vec![
                    RedisJsonValue::String("a".into()),
                    RedisJsonValue::String("b".into()),
                    RedisJsonValue::String("c".into()),
                ]),
            };
            assert_eq!(input.command().to_vec(), b"*5\r\n$5\r\nPFADD\r\n$3\r\nhll\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
        }

        #[test]
        fn test_decode_output_altered() {
            let output = PfaddOutput::decode(b":1\r\n").unwrap();
            assert_eq!(output.altered(), 1);
            assert!(output.was_altered());
        }

        #[test]
        fn test_decode_output_not_altered() {
            let output = PfaddOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.altered(), 0);
            assert!(!output.was_altered());
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = PfaddOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("elem1".into()),
                RedisJsonValue::String("elem2".into()),
            ];
            let input = PfaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.elements.as_ref().unwrap().len(), 2);
        }

        #[test]
        fn test_decode_input_key_only() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = PfaddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.elements.is_none());
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = PfaddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = PfaddInput { key: RedisKey::String("hll".into()), elements: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("hll".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_new_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up first
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\npfadd_newkey\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &PfaddInput {
                                key: RedisKey::String("pfadd_newkey".into()),
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

                    let output = PfaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_altered(), "new elements should alter HLL");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_duplicate_elements() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\npfadd_dups\r\n").await.expect("raw failed");

                    // Add initial elements
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfadd_dups".into()),
                            elements: Some(vec![RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add same elements again
                    let result = ctx
                        .raw(
                            &PfaddInput {
                                key: RedisKey::String("pfadd_dups".into()),
                                elements: Some(vec![RedisJsonValue::String("x".into()), RedisJsonValue::String("y".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfaddOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_altered(), "duplicate elements should not alter HLL");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_key_only_no_elements() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\npfadd_keyonly\r\n").await.expect("raw failed");

                    // PFADD with key only creates an empty HLL
                    let result = ctx
                        .raw(
                            &PfaddInput {
                                key: RedisKey::String("pfadd_keyonly".into()),
                                elements: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_altered(), "creating new HLL should return 1");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_mixed_new_and_existing() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\npfadd_mixed\r\n").await.expect("raw failed");

                    // Add initial element
                    ctx.raw(
                        &PfaddInput {
                            key: RedisKey::String("pfadd_mixed".into()),
                            elements: Some(vec![RedisJsonValue::String("existing".into())]),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Add mix of existing and new
                    let result = ctx
                        .raw(
                            &PfaddInput {
                                key: RedisKey::String("pfadd_mixed".into()),
                                elements: Some(vec![RedisJsonValue::String("existing".into()), RedisJsonValue::String("new".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = PfaddOutput::decode(&result).expect("decode failed");
                    assert!(output.was_altered(), "new element should alter HLL");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_wrong_type_error() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Create a string key
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$15\r\npfadd_wrongtype\r\n$5\r\nhello\r\n").await.expect("raw failed");

                    // Try PFADD on string key
                    let result = ctx
                        .raw(
                            &PfaddInput {
                                key: RedisKey::String("pfadd_wrongtype".into()),
                                elements: Some(vec![RedisJsonValue::String("elem".into())]),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = PfaddOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"), "should fail with WRONGTYPE error");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\npfadd_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &PfaddInput {
                        key: RedisKey::String("pfadd_r2".into()),
                        elements: Some(vec![RedisJsonValue::String("val".into())]),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP2 integer format");
            let output = PfaddOutput::decode(&result).expect("decode failed");
            assert!(output.was_altered());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_pfadd_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$8\r\npfadd_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &PfaddInput {
                        key: RedisKey::String("pfadd_r3".into()),
                        elements: Some(vec![RedisJsonValue::String("val".into())]),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b":1\r\n", "RESP3 integer format");
            let output = PfaddOutput::decode(&result).expect("decode failed");
            assert!(output.was_altered());

            ctx.stop().await;
        }
    }
}
