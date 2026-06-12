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

const API_INFO: ApiInfo<RedisApi, VgetattrInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Vgetattr, "Retrieve the JSON attributes of elements", ReqType::Read, true);

/// See official Redis documentation for `VGETATTR`
/// https://redis.io/docs/latest/commands/vgetattr/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VgetattrInput {
    key: RedisKey,
    element: RedisJsonValue,
}

impl VgetattrInput {
    pub fn new(key: impl Into<RedisKey>, element: impl Into<RedisJsonValue>) -> Self {
        Self { key: key.into(), element: element.into() }
    }
}

impl Serialize for VgetattrInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VgetattrInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("element", &self.element)?;
        state.end()
    }
}

impl_redis_operation!(VgetattrInput, API_INFO, { key, element });

impl RedisCommandInput for VgetattrInput {
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
            return Err(EpError::request(format!("VGETATTR requires 2 arguments, given {}", args.len())));
        } else if args.len() > 2 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "VGETATTR expects 2 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self { key: args[0].clone().try_into()?, element: args[1].clone() })
    }
}

/// Output for Redis VGETATTR command
///
/// Returns the JSON attributes of an element, or None if not set.
///
/// See official Redis documentation for `VGETATTR`
/// https://redis.io/docs/latest/commands/vgetattr/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VgetattrOutput {
    /// JSON attributes string, or None if element has no attributes
    attributes: Option<String>,
}

impl VgetattrOutput {
    pub fn new(attributes: Option<String>) -> Self {
        Self { attributes }
    }

    /// Get the attributes JSON string
    pub fn attributes(&self) -> Option<&str> {
        self.attributes.as_deref()
    }

    /// Check if attributes exist
    pub fn has_attributes(&self) -> bool {
        self.attributes.is_some()
    }

    /// Parse attributes as JSON value
    pub fn as_json(&self) -> Option<Result<serde_json::Value, serde_json::Error>> {
        self.attributes.as_ref().map(|s| serde_json::from_str(s))
    }

    /// Decode the Redis protocol response into a VgetattrOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let attributes = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(String::from_utf8(bytes).map_err(EpError::parse)?),
                Resp2Frame::SimpleString(s) => Some(String::from_utf8(s).map_err(EpError::parse)?),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VGETATTR response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::SimpleString { data, .. } => Some(String::from_utf8(data).map_err(EpError::parse)?),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VGETATTR response: {:?}", other)));
                }
            },
        };

        Ok(Self { attributes })
    }
}

impl Serialize for VgetattrOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VgetattrOutput", 1)?;
        state.serialize_field("attributes", &self.attributes)?;
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
            let input = VgetattrInput {
                key: RedisKey::String("myvset".into()),
                element: RedisJsonValue::String("elem1".into()),
            };
            assert_eq!(input.command().to_vec(), b"*3\r\n$8\r\nVGETATTR\r\n$6\r\nmyvset\r\n$5\r\nelem1\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = VgetattrOutput::decode(b"$15\r\n{\"name\":\"test\"}\r\n").unwrap();
            assert!(output.has_attributes());
            assert_eq!(output.attributes(), Some("{\"name\":\"test\"}"));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VgetattrOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.has_attributes());
            assert_eq!(output.attributes(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VgetattrOutput::decode(b"_\r\n").unwrap();
            assert!(!output.has_attributes());
            assert_eq!(output.attributes(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VgetattrOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("elem".into())];
            let input = VgetattrInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.element, RedisJsonValue::String("elem".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = VgetattrInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VgetattrInput {
                key: RedisKey::String("test".into()),
                element: RedisJsonValue::String("elem".into()),
            };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }

        #[test]
        fn test_as_json() {
            let output = VgetattrOutput::new(Some("{\"key\":\"value\"}".into()));
            let json = output.as_json().unwrap().unwrap();
            assert_eq!(json["key"], "value");
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VGETATTR requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vgetattr_no_attributes() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // Add element without attributes
                    ctx.raw(b"*5\r\n$4\r\nVADD\r\n$16\r\nvgetattr_testvset\r\n$4\r\nFP32\r\n$13\r\n[1.0,2.0,3.0]\r\n$4\r\nelem\r\n")
                        .await
                        .expect("vadd failed");

                    let result = ctx.raw(&VgetattrInput::new("vgetattr_testvset", "elem").command()).await.expect("raw failed");

                    let output = VgetattrOutput::decode(&result).expect("decode failed");
                    assert!(!output.has_attributes());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vgetattr_with_attributes() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // Add element with attributes using SETATTR
                    ctx.raw(
                        b"*8\r\n$4\r\nVADD\r\n$18\r\nvgetattr_testvset2\r\n$6\r\nVALUES\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$3\r\n3.0\r\n$4\r\nelem\r\n"
                    )
                        .await
                        .expect("vadd failed");

                    ctx.raw(
                        b"*4\r\n$8\r\nVSETATTR\r\n$18\r\nvgetattr_testvset2\r\n$4\r\nelem\r\n$15\r\n{\"name\":\"test\"}\r\n",
                    )
                        .await
                        .expect("vsetattr failed");

                    let result = ctx
                        .raw(&VgetattrInput::new("vgetattr_testvset2", "elem").command())
                        .await
                        .expect("raw failed");

                    let output = VgetattrOutput::decode(&result).expect("decode failed");
                    assert!(output.has_attributes());
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vgetattr_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VgetattrInput::new("missing_vset", "elem").command()).await.expect("raw failed");

            let output = VgetattrOutput::decode(&result).expect("decode failed");
            assert!(!output.has_attributes());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vgetattr_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VgetattrInput::new("missing_vset", "elem").command()).await.expect("raw failed");

            let output = VgetattrOutput::decode(&result).expect("decode failed");
            assert!(!output.has_attributes());

            ctx.stop().await;
        }
    }
}
