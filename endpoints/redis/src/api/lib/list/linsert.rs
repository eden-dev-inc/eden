use crate::api::lib::list::Traverse;
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

const API_INFO: ApiInfo<RedisApi, LinsertInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Linsert,
    "Inserts an element before or after another element in a list",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `LINSERT`
/// https://redis.io/docs/latest/commands/linsert/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct LinsertInput {
    pub(crate) key: RedisKey,
    pub(crate) traverse: Traverse,
    pub(crate) pivot: RedisJsonValue,
    pub(crate) element: RedisJsonValue,
}

impl Serialize for LinsertInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("LinsertInput", 5)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("traverse", &self.traverse)?;
        state.serialize_field("pivot", &self.pivot)?;
        state.serialize_field("element", &self.element)?;
        state.end()
    }
}

impl_redis_operation!(LinsertInput, API_INFO, { key, traverse, pivot, element });

impl RedisCommandInput for LinsertInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        match self.traverse {
            Traverse::Before => command.arg("BEFORE"),
            Traverse::After => command.arg("AFTER"),
        };

        command.arg(&self.pivot).arg(&self.element);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 4 {
            return Err(EpError::request(format!("LINSERT requires 4 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            traverse: Traverse::try_from(args[1].clone())?,
            pivot: args[2].clone(),
            element: args[3].clone(),
        })
    }
}

/// Output for Redis LINSERT command
///
/// Returns the length of the list after the insert, -1 if pivot not found, or 0 if key doesn't exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct LinsertOutput {
    /// The length of the list after insert, -1 if pivot not found, 0 if key doesn't exist
    length: i64,
}

impl LinsertOutput {
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the new length of the list
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the pivot was found
    pub fn pivot_found(&self) -> bool {
        self.length != -1
    }

    /// Check if the key exists
    pub fn key_exists(&self) -> bool {
        self.length != 0
    }

    /// Decode the Redis protocol response into a LinsertOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Ok(Self { length: n }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected LINSERT response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Ok(Self { length: data }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected LINSERT response: {:?}", other))),
            },
        }
    }
}

impl Serialize for LinsertOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("LinsertOutput", 1)?;
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
        fn test_encode_command_before() {
            let input = LinsertInput {
                key: RedisKey::String("mylist".into()),
                traverse: Traverse::Before,
                pivot: RedisJsonValue::String("pivot".into()),
                element: RedisJsonValue::String("new".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*5\r\n$7\r\nLINSERT\r\n"));
            assert!(String::from_utf8_lossy(&cmd).contains("BEFORE"));
        }

        #[test]
        fn test_encode_command_after() {
            let input = LinsertInput {
                key: RedisKey::String("mylist".into()),
                traverse: Traverse::After,
                pivot: RedisJsonValue::String("pivot".into()),
                element: RedisJsonValue::String("new".into()),
            };
            let cmd = input.command();
            assert!(String::from_utf8_lossy(&cmd).contains("AFTER"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("BEFORE".into()),
                RedisJsonValue::String("pivot".into()),
                RedisJsonValue::String("elem".into()),
            ];
            let input = LinsertInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mylist".into()));
            assert!(matches!(input.traverse, Traverse::Before));
        }

        #[test]
        fn test_decode_input_wrong_count() {
            let args = vec![RedisJsonValue::String("mylist".into()), RedisJsonValue::String("BEFORE".into())];
            let err = LinsertInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("4 arguments"));
        }

        #[test]
        fn test_decode_input_invalid_traverse() {
            let args = vec![
                RedisJsonValue::String("mylist".into()),
                RedisJsonValue::String("INVALID".into()),
                RedisJsonValue::String("pivot".into()),
                RedisJsonValue::String("elem".into()),
            ];
            let err = LinsertInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("BEFORE or AFTER"));
        }

        #[test]
        fn test_traverse_try_from() {
            assert!(matches!(Traverse::try_from(RedisJsonValue::String("before".into())), Ok(Traverse::Before)));
            assert!(matches!(Traverse::try_from(RedisJsonValue::String("AFTER".into())), Ok(Traverse::After)));
        }

        #[test]
        fn test_decode_output_positive() {
            let output = LinsertOutput::decode(b":5\r\n").unwrap();
            assert_eq!(output.length(), 5);
            assert!(output.pivot_found());
            assert!(output.key_exists());
        }

        #[test]
        fn test_decode_output_pivot_not_found() {
            let output = LinsertOutput::decode(b":-1\r\n").unwrap();
            assert_eq!(output.length(), -1);
            assert!(!output.pivot_found());
        }

        #[test]
        fn test_decode_output_key_not_exists() {
            let output = LinsertOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(!output.key_exists());
        }

        #[test]
        fn test_decode_output_error() {
            let err = LinsertOutput::decode(b"-ERR wrong type\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = LinsertInput {
                key: RedisKey::String("mylist".into()),
                traverse: Traverse::Before,
                pivot: RedisJsonValue::String("p".into()),
                element: RedisJsonValue::String("e".into()),
            };
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::list::lrange::{LrangeInput, LrangeOutput};
        use crate::api::lib::list::rpush::RpushInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_linsert_before() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("linsert_before".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("c".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LinsertInput {
                                key: RedisKey::String("linsert_before".into()),
                                traverse: Traverse::Before,
                                pivot: RedisJsonValue::String("c".into()),
                                element: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LinsertOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);

                    // Verify order
                    let range_result = ctx
                        .raw(
                            &LrangeInput {
                                key: RedisKey::String("linsert_before".into()),
                                start: RedisJsonValue::Integer(0),
                                stop: RedisJsonValue::Integer(-1),
                            }
                            .command(),
                        )
                        .await
                        .expect("lrange failed");

                    let range_output = LrangeOutput::decode(&range_result).expect("decode");
                    assert_eq!(range_output.elements().len(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_linsert_after() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("linsert_after".into()),
                            elements: vec![RedisJsonValue::String("a".into()), RedisJsonValue::String("c".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LinsertInput {
                                key: RedisKey::String("linsert_after".into()),
                                traverse: Traverse::After,
                                pivot: RedisJsonValue::String("a".into()),
                                element: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LinsertOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 3);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_linsert_pivot_not_found() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &RpushInput {
                            key: RedisKey::String("linsert_nopivot".into()),
                            elements: vec![RedisJsonValue::String("a".into())],
                        }
                        .command(),
                    )
                    .await
                    .expect("rpush failed");

                    let result = ctx
                        .raw(
                            &LinsertInput {
                                key: RedisKey::String("linsert_nopivot".into()),
                                traverse: Traverse::Before,
                                pivot: RedisJsonValue::String("nonexistent".into()),
                                element: RedisJsonValue::String("b".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LinsertOutput::decode(&result).expect("decode failed");
                    assert!(!output.pivot_found());
                    assert_eq!(output.length(), -1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_linsert_key_not_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &LinsertInput {
                                key: RedisKey::String("nonexistent_linsert".into()),
                                traverse: Traverse::Before,
                                pivot: RedisJsonValue::String("p".into()),
                                element: RedisJsonValue::String("e".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = LinsertOutput::decode(&result).expect("decode failed");
                    assert!(!output.key_exists());
                    assert_eq!(output.length(), 0);
                })
            })
            .await;
        }
    }
}
