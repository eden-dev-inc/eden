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

const API_INFO: ApiInfo<RedisApi, HincrbyfloatInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Hincrbyfloat,
    "Increments the floating point value of a field by a number. Uses 0 as initial value if the field doesn't exist",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `HINCRBYFLOAT`
/// https://redis.io/docs/latest/commands/hincrbyfloat/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HincrbyfloatInput {
    pub(crate) key: RedisKey,
    pub(crate) field: RedisJsonValue,
    pub(crate) increment: RedisJsonValue,
}

impl Serialize for HincrbyfloatInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("HincrbyfloatInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("field", &self.field)?;
        state.serialize_field("increment", &self.increment)?;
        state.end()
    }
}

impl_redis_operation!(
    HincrbyfloatInput,
    API_INFO,
    {key, field, increment}
);

impl RedisCommandInput for HincrbyfloatInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.field).arg(&self.increment);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("HINCRBYFLOAT requires exactly 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            field: args[1].clone(),
            increment: args[2].clone(),
        })
    }
}

/// Output for Redis HINCRBYFLOAT command
///
/// Returns the string representation of the field value after the increment.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HincrbyfloatOutput {
    /// The string representation of the value after the increment
    value: String,
}

impl HincrbyfloatOutput {
    pub fn new(value: String) -> Self {
        Self { value }
    }

    /// Get the value as a string
    pub fn value(&self) -> &str {
        &self.value
    }

    /// Get the value as f64
    pub fn value_f64(&self) -> Option<f64> {
        self.value.parse().ok()
    }

    /// Decode the Redis protocol response into a HincrbyfloatOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(|e| EpError::parse(e.to_string()))?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HINCRBYFLOAT response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(|e| EpError::parse(e.to_string()))?,
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HINCRBYFLOAT response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for HincrbyfloatOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("HincrbyfloatOutput", 1)?;
        state.serialize_field("value", &self.value)?;
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
            let input = HincrbyfloatInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("field1".into()),
                increment: RedisJsonValue::String("0.5".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HINCRBYFLOAT"));
            assert!(cmd_str.contains("myhash"));
            assert!(cmd_str.contains("field1"));
            assert!(cmd_str.contains("0.5"));
        }

        #[test]
        fn test_encode_command_negative_increment() {
            let input = HincrbyfloatInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("counter".into()),
                increment: RedisJsonValue::String("-2.5".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("HINCRBYFLOAT"));
            assert!(cmd_str.contains("-2.5"));
        }

        #[test]
        fn test_decode_output() {
            let output = HincrbyfloatOutput::decode(b"$4\r\n10.5\r\n").unwrap();
            assert_eq!(output.value(), "10.5");
            assert_eq!(output.value_f64(), Some(10.5));
        }

        #[test]
        fn test_decode_output_negative() {
            let output = HincrbyfloatOutput::decode(b"$6\r\n-3.125\r\n").unwrap();
            assert_eq!(output.value(), "-3.125");
            assert_eq!(output.value_f64(), Some(-3.125));
        }

        #[test]
        fn test_decode_output_scientific() {
            let output = HincrbyfloatOutput::decode(b"$7\r\n1.5e-10\r\n").unwrap();
            assert_eq!(output.value(), "1.5e-10");
        }

        #[test]
        fn test_decode_error_fails() {
            let err = HincrbyfloatOutput::decode(b"-ERR hash value is not a float\r\n").unwrap_err();
            assert!(err.to_string().contains("hash value is not a float"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("field".into()),
                RedisJsonValue::String("1.5".into()),
            ];
            let input = HincrbyfloatInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert_eq!(input.field, RedisJsonValue::String("field".into()));
            assert_eq!(input.increment, RedisJsonValue::String("1.5".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("field".into())];
            let err = HincrbyfloatInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires exactly 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = HincrbyfloatInput {
                key: RedisKey::String("myhash".into()),
                field: RedisJsonValue::String("f".into()),
                increment: RedisJsonValue::String("1.0".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("myhash".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrbyfloat_new_field() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nhincrbyfloat_test\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyfloatInput {
                        key: RedisKey::String("hincrbyfloat_test".into()),
                        field: RedisJsonValue::String("price".into()),
                        increment: RedisJsonValue::String("10.5".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HincrbyfloatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value_f64(), Some(10.5));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrbyfloat_existing_field() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$19\r\nhincrbyfloat_exists\r\n").await.expect("raw failed");

            // Set initial value
            ctx.raw(
                &HincrbyfloatInput {
                    key: RedisKey::String("hincrbyfloat_exists".into()),
                    field: RedisJsonValue::String("price".into()),
                    increment: RedisJsonValue::String("10.0".into()),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            // Increment again
            let result = ctx
                .raw(
                    &HincrbyfloatInput {
                        key: RedisKey::String("hincrbyfloat_exists".into()),
                        field: RedisJsonValue::String("price".into()),
                        increment: RedisJsonValue::String("0.75".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HincrbyfloatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value_f64(), Some(10.75));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrbyfloat_negative_increment() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nhincrbyfloat_neg\r\n").await.expect("raw failed");

            ctx.raw(
                &HincrbyfloatInput {
                    key: RedisKey::String("hincrbyfloat_neg".into()),
                    field: RedisJsonValue::String("balance".into()),
                    increment: RedisJsonValue::String("100.0".into()),
                }
                .command(),
            )
            .await
            .expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyfloatInput {
                        key: RedisKey::String("hincrbyfloat_neg".into()),
                        field: RedisJsonValue::String("balance".into()),
                        increment: RedisJsonValue::String("-25.50".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HincrbyfloatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value_f64(), Some(74.5));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrbyfloat_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhincrbyfloat_r2\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyfloatInput {
                        key: RedisKey::String("hincrbyfloat_r2".into()),
                        field: RedisJsonValue::String("f".into()),
                        increment: RedisJsonValue::String("3.125".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"$"), "RESP2 should return bulk string");
            let output = HincrbyfloatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value_f64(), Some(3.125));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hincrbyfloat_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nhincrbyfloat_r3\r\n").await.expect("raw failed");

            let result = ctx
                .raw(
                    &HincrbyfloatInput {
                        key: RedisKey::String("hincrbyfloat_r3".into()),
                        field: RedisJsonValue::String("f".into()),
                        increment: RedisJsonValue::String("2.701".into()),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            let output = HincrbyfloatOutput::decode(&result).expect("decode failed");
            assert_eq!(output.value_f64(), Some(2.701));

            ctx.stop().await;
        }
    }
}
