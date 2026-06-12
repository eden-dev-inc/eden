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

const API_INFO: ApiInfo<RedisApi, BfReserveInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::BfReserve, "Creates a new Bloom Filter", ReqType::Write, true);

/// See official Redis documentation for `BF.RESERVE`
/// https://redis.io/docs/latest/commands/bf.reserve/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfReserveInput {
    pub(crate) key: RedisKey,
    pub(crate) error_rate: RedisJsonValue,
    pub(crate) capacity: RedisJsonValue,
    pub(crate) expansion: Option<RedisJsonValue>,
    pub(crate) non_scaling: Option<bool>,
}

impl Serialize for BfReserveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4;
        if self.expansion.is_some() {
            fields += 1;
        }
        if self.non_scaling.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("BfReserveInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("error_rate", &self.error_rate)?;
        state.serialize_field("capacity", &self.capacity)?;
        if let Some(expansion) = &self.expansion {
            state.serialize_field("expansion", expansion)?;
        }
        if let Some(non_scaling) = &self.non_scaling {
            state.serialize_field("non_scaling", non_scaling)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    BfReserveInput,
    API_INFO,
    { key, error_rate, capacity, expansion, non_scaling }
);

impl RedisCommandInput for BfReserveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.error_rate).arg(&self.capacity);

        if let Some(expansion) = &self.expansion {
            command.arg("EXPANSION").arg(expansion);
        }

        if let Some(true) = self.non_scaling {
            command.arg("NONSCALING");
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("BF.RESERVE requires at least 3 arguments, given {}", args.len())));
        }

        if args.len() > 6 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.RESERVE expects at most 6 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let mut expansion = None;
        let mut non_scaling = None;

        let mut i = 3;
        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "EXPANSION" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("EXPANSION requires a value"));
                        }
                        expansion = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "NONSCALING" => {
                        non_scaling = Some(true);
                        i += 1;
                    }
                    _ => {
                        return Err(EpError::parse(format!("Unknown parameter: {}", s)));
                    }
                },
                _ => {
                    return Err(EpError::parse("Optional parameters must be strings"));
                }
            }
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            error_rate: args[1].clone(),
            capacity: args[2].clone(),
            expansion,
            non_scaling,
        })
    }
}

/// Output for Redis BF.RESERVE command
///
/// Returns OK on success, or an error if the filter already exists.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfReserveOutput {
    status: String,
}

impl BfReserveOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    /// Decode the Redis protocol response into a BfReserveOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BF.RESERVE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BF.RESERVE response: {:?}", other))),
            },
        }
    }
}

impl Default for BfReserveOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for BfReserveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfReserveOutput", 1)?;
        state.serialize_field("status", &self.status)?;
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
            let input = BfReserveInput {
                key: RedisKey::String("myfilter".into()),
                error_rate: RedisJsonValue::String("0.01".into()),
                capacity: RedisJsonValue::Integer(1000),
                expansion: None,
                non_scaling: None,
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$10\r\nBF.RESERVE\r\n"));
        }

        #[test]
        fn test_encode_command_with_expansion() {
            let input = BfReserveInput {
                key: RedisKey::String("myfilter".into()),
                error_rate: RedisJsonValue::String("0.01".into()),
                capacity: RedisJsonValue::Integer(1000),
                expansion: Some(RedisJsonValue::Integer(2)),
                non_scaling: None,
            };
            let cmd = input.command();
            assert!(cmd.windows(9).any(|w| w == b"EXPANSION"));
        }

        #[test]
        fn test_encode_command_with_nonscaling() {
            let input = BfReserveInput {
                key: RedisKey::String("myfilter".into()),
                error_rate: RedisJsonValue::String("0.01".into()),
                capacity: RedisJsonValue::Integer(1000),
                expansion: None,
                non_scaling: Some(true),
            };
            let cmd = input.command();
            assert!(cmd.windows(10).any(|w| w == b"NONSCALING"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = BfReserveOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = BfReserveOutput::decode(b"-ERR item exists\r\n").unwrap_err();
            assert!(err.to_string().contains("item exists"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("0.01".into()),
                RedisJsonValue::Integer(1000),
            ];
            let input = BfReserveInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_with_expansion() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("0.01".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("EXPANSION".into()),
                RedisJsonValue::Integer(2),
            ];
            let input = BfReserveInput::decode(args).unwrap();
            assert!(input.expansion.is_some());
        }

        #[test]
        fn test_decode_input_with_nonscaling() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::String("0.01".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("NONSCALING".into()),
            ];
            let input = BfReserveInput::decode(args).unwrap();
            assert_eq!(input.non_scaling, Some(true));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::String("0.01".into())];
            let err = BfReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfReserveInput {
                key: RedisKey::String("testkey".into()),
                error_rate: RedisJsonValue::String("0.01".into()),
                capacity: RedisJsonValue::Integer(100),
                expansion: None,
                non_scaling: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_reserve_basic() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfReserveInput {
                                key: RedisKey::String("bf_reserve_test".into()),
                                error_rate: RedisJsonValue::String("0.01".into()),
                                capacity: RedisJsonValue::Integer(1000),
                                expansion: None,
                                non_scaling: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfReserveOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_reserve_with_expansion() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &BfReserveInput {
                                key: RedisKey::String("bf_reserve_exp".into()),
                                error_rate: RedisJsonValue::String("0.01".into()),
                                capacity: RedisJsonValue::Integer(1000),
                                expansion: Some(RedisJsonValue::Integer(2)),
                                non_scaling: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfReserveOutput::decode(&result).expect("decode failed");
                    assert!(output.is_ok());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_reserve_duplicate_fails() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // First reserve should succeed
                    ctx.raw(
                        &BfReserveInput {
                            key: RedisKey::String("bf_reserve_dup".into()),
                            error_rate: RedisJsonValue::String("0.01".into()),
                            capacity: RedisJsonValue::Integer(100),
                            expansion: None,
                            non_scaling: None,
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Second reserve should fail
                    let result = ctx
                        .raw(
                            &BfReserveInput {
                                key: RedisKey::String("bf_reserve_dup".into()),
                                error_rate: RedisJsonValue::String("0.01".into()),
                                capacity: RedisJsonValue::Integer(100),
                                expansion: None,
                                non_scaling: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = BfReserveOutput::decode(&result);
                    assert!(err.is_err());
                })
            })
            .await;
        }
    }
}
