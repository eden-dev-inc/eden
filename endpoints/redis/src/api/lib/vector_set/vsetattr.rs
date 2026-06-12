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

const API_INFO: ApiInfo<RedisApi, VsetattrInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Vsetattr,
    "Associate or remove the JSON attributes of elements",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `VSETATTR`
/// https://redis.io/docs/latest/commands/vsetattr/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VsetattrInput {
    key: RedisKey,
    element: RedisJsonValue,
    json_object: RedisJsonValue,
}

impl VsetattrInput {
    pub fn new(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>, json_object: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            element: element.into(),
            json_object: json_object.into(),
        }
    }
}

impl Serialize for VsetattrInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VsetattrInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        state.serialize_field("json_object", &self.json_object)?;
        state.end()
    }
}

impl_redis_operation!(VsetattrInput, API_INFO, { key, element, json_object });

impl RedisCommandInput for VsetattrInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.element).arg(&self.json_object);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::request(format!("VSETATTR requires 3 arguments, given {}", args.len())));
        } else if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "VSETATTR expects 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            element: args[1].clone(),
            json_object: args[2].clone(),
        })
    }
}

/// Output for Redis VSETATTR command
///
/// Returns 1 if attributes were set, 0 if the element does not exist.
///
/// See official Redis documentation for `VSETATTR`
/// https://redis.io/docs/latest/commands/vsetattr/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VsetattrOutput {
    /// 1 if attributes were set, 0 if element does not exist
    result: i64,
}

impl VsetattrOutput {
    pub fn new(result: i64) -> Self {
        Self { result }
    }

    /// Check if attributes were successfully set
    pub fn was_set(&self) -> bool {
        self.result > 0
    }

    /// Get the result value
    pub fn result(&self) -> i64 {
        self.result
    }

    /// Decode the Redis protocol response into a VsetattrOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let result = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VSETATTR response: {:?}", other)));
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
                    return Err(EpError::parse(format!("unexpected VSETATTR response: {:?}", other)));
                }
            },
        };

        Ok(Self { result })
    }
}

impl Serialize for VsetattrOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VsetattrOutput", 1)?;
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
        fn test_encode_command() {
            let input = VsetattrInput {
                key: RedisKey::String("myvset".into()),
                element: RedisJsonValue::String("elem1".into()),
                json_object: RedisJsonValue::String("{\"name\":\"test\"}".into()),
            };
            assert_eq!(
                input.command().to_vec(),
                b"*4\r\n$8\r\nVSETATTR\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n$15\r\n{\"name\":\"test\"}\r\n"
            );
        }

        #[test]
        fn test_decode_success() {
            let output = VsetattrOutput::decode(b":1\r\n").unwrap();
            assert!(output.was_set());
            assert_eq!(output.result(), 1);
        }

        #[test]
        fn test_decode_element_not_found() {
            let output = VsetattrOutput::decode(b":0\r\n").unwrap();
            assert!(!output.was_set());
            assert_eq!(output.result(), 0);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VsetattrOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("elem".into()),
                RedisJsonValue::String("{}".into()),
            ];
            let input = VsetattrInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.element, RedisJsonValue::String("elem".into()));
            assert_eq!(input.json_object, RedisJsonValue::String("{}".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("elem".into())];
            let err = VsetattrInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VsetattrInput {
                key: RedisKey::String("test".into()),
                element: RedisJsonValue::String("elem".into()),
                json_object: RedisJsonValue::String("{}".into()),
            };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VSETATTR requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsetattr_nonexistent_element() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VsetattrInput::new("nonexistent_vset", "elem", "{}").command()).await.expect("raw failed");

                    let output = VsetattrOutput::decode(&result).expect("decode failed");
                    assert!(!output.was_set());
                    assert_eq!(output.result(), 0);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsetattr_existing_element() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // First add an element
                    ctx.raw(
                        b"*8\r\n$4\r\nVADD\r\n$17\r\nvsetattr_testvset\r\n$6\r\nVALUES\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$3\r\n3.0\r\n$4\r\nelem\r\n"
                    )
                        .await
                        .expect("vadd failed");

                    // Then set attributes
                    let result = ctx
                        .raw(&VsetattrInput::new("vsetattr_testvset", "elem", "{\"key\":\"value\"}").command())
                        .await
                        .expect("raw failed");

                    let output = VsetattrOutput::decode(&result).expect("decode failed");
                    assert!(output.was_set());
                    assert_eq!(output.result(), 1);
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsetattr_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VsetattrInput::new("missing_vset", "elem", "{}").command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = VsetattrOutput::decode(&result).expect("decode failed");
            assert_eq!(output.result(), 0);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vsetattr_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VsetattrInput::new("missing_vset", "elem", "{}").command()).await.expect("raw failed");

            let output = VsetattrOutput::decode(&result).expect("decode failed");
            assert_eq!(output.result(), 0);

            ctx.stop().await;
        }
    }
}
