use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, VremInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vrem, "Remove one or more elements from a vector set", ReqType::Write, true);

/// See official Redis documentation for `VREM`
/// https://redis.io/docs/latest/commands/vrem/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VremInput {
    key: RedisKey,
    element: RedisJsonValue,
}

impl VremInput {
    pub fn new(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), element: element.into() }
    }
}

impl Serialize for VremInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VremInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        state.end()
    }
}

impl_redis_operation!(VremInput, API_INFO, { key, element });

impl RedisCommandInput for VremInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.element);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request(format!("VREM requires 2 arguments, given {}", args.len())));
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "VREM expects 2 arguments, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()?, element: args[1].clone() })
    }
}

/// Output for Redis VREM command
///
/// Returns 1 if the element was removed, 0 if the element did not exist.
///
/// See official Redis documentation for `VREM`
/// https://redis.io/docs/latest/commands/vrem/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VremOutput {
    /// 1 if element was removed, 0 if it did not exist
    removed: i64,
}

impl VremOutput {
    pub fn new(removed: i64) -> Self {
        Self { removed }
    }

    /// Check if the element was actually removed
    pub fn was_removed(&self) -> bool {
        self.removed > 0
    }

    /// Get the number of elements removed
    pub fn removed(&self) -> i64 {
        self.removed
    }

    /// Decode the Redis protocol response into a VremOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let removed = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VREM response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::Boolean { data, .. } => {
                    if data {
                        1
                    } else {
                        0
                    }
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VREM response: {:?}", other)));
                }
            },
        };

        Ok(Self { removed })
    }
}

impl Serialize for VremOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VremOutput", 1)?;
        state.serialize_field("removed", &self.removed)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command() {
            let input = VremInput {
                key: RedisKey::String("myvset".into()),
                element: RedisJsonValue::String("elem1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$4\r\nVREM\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n");
        }

        #[test]
        fn test_decode_removed() {
            let output = VremOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_removed());
            assert_eq!(output.removed(), 1);
        }

        #[test]
        fn test_decode_not_removed() {
            let output = VremOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_removed());
            assert_eq!(output.removed(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VremOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("elem".into())];
            let input = VremInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.element, RedisJsonValue::String("elem".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = VremInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VremInput {
                key: RedisKey::String("test".into()),
                element: RedisJsonValue::String("elem".into()),
            };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VREM requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrem_nonexistent_element() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VremInput::new("nonexistent_vset", "elem").command()).await.expect("raw failed");

                    let output = VremOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_removed());
                    assert_eq!(output.removed(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrem_existing_element() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // First add an element
                    ctx.raw(
                        b"*8\r\n$4\r\nVADD\r\n$13\r\nvrem_testvset\r\n$6\r\nVALUES\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$3\r\n3.0\r\n$4\r\nelem\r\n"
                    )
                        .await
                        .expect("vadd failed");

                    // Then remove it
                    let result = ctx
                        .raw(&VremInput::new("vrem_testvset", "elem").command())
                        .await
                        .expect("raw failed");

                    let output = VremOutput::decode(&result).expect("decode failed");
                    assert!(output.was_removed());
                    assert_eq!(output.removed(), 1);

                    // Try to remove again
                    let result2 = ctx
                        .raw(&VremInput::new("vrem_testvset", "elem").command())
                        .await
                        .expect("raw failed");

                    let output2 = VremOutput::decode(&result2).expect("decode failed");
                    assert!(!output2.was_removed());
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrem_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VremInput::new("missing_vset", "elem").command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = VremOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrem_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VremInput::new("missing_vset", "elem").command()).await.expect("raw failed");

            let output = VremOutput::decode(&result).expect("decode failed");
            assert_eq!(output.removed(), 0);

            ctx.stop().await;
        }
    }
}
