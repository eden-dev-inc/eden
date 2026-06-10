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

const API_INFO: ApiInfo<RedisApi, VdimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Vdim,
    "Return the dimension of vectors in the vector set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `VDIM`
/// https://redis.io/docs/latest/commands/vdim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VdimInput {
    key: RedisKey,
}

impl VdimInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into() }
    }
}

impl Serialize for VdimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("VdimInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(VdimInput, API_INFO, { key });

impl RedisCommandInput for VdimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("VDIM requires 1 argument, given None"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "VDIM expects 1 argument, given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis VDIM command
///
/// Returns the dimension of vectors stored in the vector set.
/// Returns null if the key does not exist.
///
/// See official Redis documentation for `VDIM`
/// https://redis.io/docs/latest/commands/vdim/
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VdimOutput {
    /// Dimension of vectors in the set, or None if key doesn't exist
    dimension: Option<i64>,
}

impl VdimOutput {
    pub fn new(dimension: Option<i64>) -> Self {
        Self { dimension }
    }

    /// Get the vector dimension
    pub fn dimension(&self) -> Option<i64> {
        self.dimension
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.dimension.is_some()
    }

    /// Decode the Redis protocol response into a VdimOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let dimension = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => Some(n),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected VDIM response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => Some(data),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected VDIM response: {:?}", other)));
                }
            },
        };

        Ok(Self { dimension })
    }
}

impl Serialize for VdimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VdimOutput", 1)?;
        state.serialize_field("dimension", &self.dimension)?;
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
            let input = VdimInput { key: RedisKey::String("myvset".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nVDIM\r\n$6\r\nmyvset\r\n");
        }

        #[test]
        fn test_decode_integer() {
            let output = VdimOutput::decode(b":128\r\n").unwrap();
            assert_eq!(output.dimension(), Some(128));
            assert!(output.exists());
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VdimOutput::decode(b"$-1\r\n").unwrap();
            assert_eq!(output.dimension(), None);
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VdimOutput::decode(b"_\r\n").unwrap();
            assert_eq!(output.dimension(), None);
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VdimOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = VdimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = VdimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = VdimInput { key: RedisKey::String("test".into()) };
            assert_eq!(input.keys(), vec![RedisKey::String("test".into())]);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // VDIM requires Redis 8.0+
        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vdim_nonexistent_key() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VdimInput::new("nonexistent_vset").command()).await.expect("raw failed");

                    let err = VdimOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("key does not exist"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vdim_after_vadd() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    // Add element with 3-dimensional vector
                    ctx.raw(
                        b"*8\r\n$4\r\nVADD\r\n$13\r\nvdim_testvset\r\n$6\r\nVALUES\r\n$1\r\n3\r\n$3\r\n1.0\r\n$3\r\n2.0\r\n$3\r\n3.0\r\n$4\r\nelem\r\n"
                    )
                        .await
                        .expect("vadd failed");

                    let result = ctx
                        .raw(&VdimInput::new("vdim_testvset").command())
                        .await
                        .expect("raw failed");

                    let output = VdimOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.dimension(), Some(3));
                })
            })
                .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vdim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;

            let result = ctx.raw(&VdimInput::new("missing_vset").command()).await.expect("raw failed");

            let err = VdimOutput::decode(&result).unwrap_err();
            assert!(err.to_string().contains("key does not exist"));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vdim_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("8")).await;

            let result = ctx.raw(&VdimInput::new("missing_vset").command()).await.expect("raw failed");

            let err = VdimOutput::decode(&result).unwrap_err();
            assert!(err.to_string().contains("key does not exist"));

            ctx.stop().await;
        }
    }
}
