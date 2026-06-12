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

const API_INFO: ApiInfo<RedisApi, AclGenpassInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::AclGenpass,
    "Generates a pseudorandom, secure password that can be used to identify ACL users",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `ACL GENPASS`
/// https://redis.io/docs/latest/commands/acl-genpass/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclGenpassInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    bits: Option<RedisJsonValue>,
}

impl Serialize for AclGenpassInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1;
        if self.bits.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("AclGenpassInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(bits) = &self.bits {
            state.serialize_field("bits", &bits)?;
        }
        state.end()
    }
}

impl_redis_operation!(AclGenpassInput, API_INFO, { bits });

impl RedisCommandInput for AclGenpassInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(bits) = &self.bits {
            command.arg(bits);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "ACL GENPASS expected 0 or 1 argument, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self { bits: args.first().cloned() })
    }
}

/// Output for Redis ACL GENPASS command
///
/// Returns a randomly generated password.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclGenpassOutput {
    /// The generated password
    password: String,
}

impl AclGenpassOutput {
    pub fn new(password: String) -> Self {
        Self { password }
    }

    /// Get the generated password
    pub fn password(&self) -> &str {
        &self.password
    }

    /// Get the length of the password in characters
    pub fn len(&self) -> usize {
        self.password.len()
    }

    /// Check if the password is empty (should never happen)
    pub fn is_empty(&self) -> bool {
        self.password.is_empty()
    }

    /// Decode the Redis protocol response into an AclGenpassOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let password = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::SimpleString(bytes) => String::from_utf8(bytes).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected ACL GENPASS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected ACL GENPASS response: {:?}", other)));
                }
            },
        };

        Ok(Self { password })
    }
}

impl Serialize for AclGenpassOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclGenpassOutput", 1)?;
        state.serialize_field("password", &self.password)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_default() {
            let input = AclGenpassInput { bits: None };
            // ACL GENPASS splits into: ACL, GENPASS
            assert_eq!(input.command().to_vec(), b"*2\r\n$3\r\nACL\r\n$7\r\nGENPASS\r\n");
        }

        #[test]
        fn test_encode_command_with_bits() {
            let input = AclGenpassInput { bits: Some(RedisJsonValue::Integer(128)) };
            // ACL GENPASS splits into: ACL, GENPASS, 128
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$7\r\nGENPASS\r\n$3\r\n128\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = AclGenpassOutput::decode(b"$64\r\na1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2c3d4e5f6a1b2\r\n").unwrap();
            assert_eq!(output.len(), 64);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_simple_string() {
            let output = AclGenpassOutput::decode(b"+abcd1234\r\n").unwrap();
            assert_eq!(output.password(), "abcd1234");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclGenpassOutput::decode(b"-ERR invalid bits\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let input = AclGenpassInput::decode(vec![]).unwrap();
            assert!(input.bits.is_none());
        }

        #[test]
        fn test_decode_input_with_bits() {
            let args = vec![RedisJsonValue::Integer(256)];
            let input = AclGenpassInput::decode(args).unwrap();
            assert_eq!(input.bits, Some(RedisJsonValue::Integer(256)));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclGenpassInput { bits: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclGenpassInput { bits: None };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclGenpass);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&AclGenpassInput { bits: None }.command()).await.expect("raw failed");

                    let output = AclGenpassOutput::decode(&result).expect("decode failed");
                    // Default is 256 bits = 64 hex characters
                    assert_eq!(output.len(), 64, "default should be 64 hex characters");
                    assert!(output.password().chars().all(|c| c.is_ascii_hexdigit()), "password should be hex");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_custom_bits() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&AclGenpassInput { bits: Some(RedisJsonValue::Integer(128)) }.command()).await.expect("raw failed");

                    let output = AclGenpassOutput::decode(&result).expect("decode failed");
                    // 128 bits = 32 hex characters
                    assert_eq!(output.len(), 32, "128 bits should be 32 hex characters");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_randomness() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Generate two passwords and ensure they're different
                    let result1 = ctx.raw(&AclGenpassInput { bits: None }.command()).await.expect("raw failed");
                    let output1 = AclGenpassOutput::decode(&result1).expect("decode failed");

                    let result2 = ctx.raw(&AclGenpassInput { bits: None }.command()).await.expect("raw failed");
                    let output2 = AclGenpassOutput::decode(&result2).expect("decode failed");

                    assert_ne!(output1.password(), output2.password(), "consecutive passwords should be different");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_min_bits() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Minimum is 1 bit (rounds up to 4 = 1 hex char)
                    let result = ctx.raw(&AclGenpassInput { bits: Some(RedisJsonValue::Integer(1)) }.command()).await.expect("raw failed");

                    let output = AclGenpassOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty(), "should return at least 1 character");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_max_bits() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Maximum is 1024 bits = 256 hex characters
                    let result =
                        ctx.raw(&AclGenpassInput { bits: Some(RedisJsonValue::Integer(1024)) }.command()).await.expect("raw failed");

                    let output = AclGenpassOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 256, "1024 bits should be 256 hex characters");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx.raw(&AclGenpassInput { bits: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = AclGenpassOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 64);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_genpass_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx.raw(&AclGenpassInput { bits: None }.command()).await.expect("raw failed");

            let output = AclGenpassOutput::decode(&result).expect("decode failed");
            assert_eq!(output.len(), 64);

            ctx.stop().await;
        }
    }
}
