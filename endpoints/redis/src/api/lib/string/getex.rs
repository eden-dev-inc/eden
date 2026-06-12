use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use function_name::named;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, GetexInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Getex,
    "Get the value of key and optionally set its expiration. GETEX is similar to GET, but is a write command with additional options",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `GETEX`
/// https://redis.io/docs/latest/commands/getex/
///
/// Available since Redis 6.2.0
#[derive(Debug, Serialize, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct GetexInput {
    pub(crate) key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) options: Option<GetexOptions>,
}

/// Options for GETEX command
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum GetexOptions {
    /// Set expiration in seconds
    EX(GetexEX),
    /// Set expiration in milliseconds
    PX(GetexPX),
    /// Set expiration at Unix timestamp (seconds)
    EXAT(GetexEXAT),
    /// Set expiration at Unix timestamp (milliseconds)
    PXAT(GetexPXAT),
    /// Remove the existing expiration
    PERSIST,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct GetexEX {
    pub(crate) seconds: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct GetexPX {
    pub(crate) milliseconds: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct GetexEXAT {
    pub(crate) unix_time_seconds: RedisJsonValue,
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct GetexPXAT {
    pub(crate) unix_time_milliseconds: RedisJsonValue,
}

impl_redis_operation!(
    GetexInput,
    API_INFO,
    {key, options}
);

impl RedisCommandInput for GetexInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(options) = &self.options {
            match options {
                GetexOptions::EX(e) => command.arg("EX").arg(&e.seconds),
                GetexOptions::PX(m) => command.arg("PX").arg(&m.milliseconds),
                GetexOptions::EXAT(e) => command.arg("EXAT").arg(&e.unix_time_seconds),
                GetexOptions::PXAT(p) => command.arg("PXAT").arg(&p.unix_time_milliseconds),
                GetexOptions::PERSIST => command.arg("PERSIST"),
            };
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("GETEX requires at least 1 argument, given none"));
        } else if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "GETEX takes at most 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        let key = args[0].clone().try_into()?;
        let options = match args.len() {
            1 => None, // GETEX key
            2 => {
                // GETEX key PERSIST
                match &args[1] {
                    RedisJsonValue::String(s) if s.to_uppercase() == "PERSIST" => Some(GetexOptions::PERSIST),
                    _ => return Err(EpError::request("Single option must be PERSIST")),
                }
            }
            _ => {
                // GETEX key OPTION value
                let option = match &args[1] {
                    RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                        "EX" => GetexOptions::EX(GetexEX { seconds: args[2].clone() }),
                        "PX" => GetexOptions::PX(GetexPX { milliseconds: args[2].clone() }),
                        "EXAT" => GetexOptions::EXAT(GetexEXAT { unix_time_seconds: args[2].clone() }),
                        "PXAT" => GetexOptions::PXAT(GetexPXAT { unix_time_milliseconds: args[2].clone() }),
                        _ => return Err(EpError::request(format!("Unknown GETEX option: {}", s))),
                    },
                    _ => return Err(EpError::request("GETEX option must be a string")),
                };
                Some(option)
            }
        };

        Ok(GetexInput { key, options })
    }
}

/// Output for Redis GETEX command
///
/// Returns the value of the key, or None if the key did not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct GetexOutput {
    /// The value stored at the key, or None if key doesn't exist
    value: Option<RedisJsonValue>,
}

impl GetexOutput {
    pub fn new(value: Option<RedisJsonValue>) -> Self {
        Self { value }
    }

    /// Get the value from the output
    pub fn value(&self) -> Option<&RedisJsonValue> {
        self.value.as_ref()
    }

    /// Check if the key exists
    pub fn exists(&self) -> bool {
        self.value.is_some()
    }

    /// Decode the Redis protocol response into a GetexOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let value = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(RedisJsonValue::from(String::from_utf8(bytes).map_err(EpError::parse)?)),
                Resp2Frame::SimpleString(s) => Some(RedisJsonValue::from(String::from_utf8(s).map_err(EpError::parse)?)),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected GETEX response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::SimpleString { data, .. } => Some(RedisJsonValue::from(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected GETEX response: {:?}", other)));
                }
            },
        };

        Ok(Self { value })
    }
}

impl Serialize for GetexOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("GetexOutput", 1)?;
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
        fn test_encode_command_basic() {
            let input = GetexInput { key: RedisKey::String("mykey".into()), options: None };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nGETEX\r\n$5\r\nmykey\r\n");
        }

        #[test]
        fn test_encode_command_with_ex() {
            let input = GetexInput {
                key: RedisKey::String("mykey".into()),
                options: Some(GetexOptions::EX(GetexEX { seconds: RedisJsonValue::Integer(60) })),
            };
            let cmd = input.command();
            assert!(cmd.windows(2).any(|w| w == b"EX"));
        }

        #[test]
        fn test_encode_command_with_persist() {
            let input = GetexInput {
                key: RedisKey::String("mykey".into()),
                options: Some(GetexOptions::PERSIST),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"PERSIST"));
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = GetexOutput::decode(b"$5\r\nhello\r\n").unwrap();
            assert!(output.exists());
            assert_eq!(output.value(), Some(&RedisJsonValue::from("hello")));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = GetexOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = GetexOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
            assert_eq!(output.value(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = GetexOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("key".into())];
            let input = GetexInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("key".into()));
            assert!(input.options.is_none());
        }

        #[test]
        fn test_decode_input_with_persist() {
            let args = vec![RedisJsonValue::String("key".into()), RedisJsonValue::String("PERSIST".into())];
            let input = GetexInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(GetexOptions::PERSIST)));
        }

        #[test]
        fn test_decode_input_with_ex() {
            let args = vec![
                RedisJsonValue::String("key".into()),
                RedisJsonValue::String("EX".into()),
                RedisJsonValue::Integer(60),
            ];
            let input = GetexInput::decode(args).unwrap();
            assert!(matches!(input.options, Some(GetexOptions::EX(_))));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = GetexInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = GetexInput { key: RedisKey::String("mykey".into()), options: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::TtlInput;
        use crate::api::{SetInput, Ttl, TtlOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getex_basic() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getex_key".into()),
                            value: RedisJsonValue::String("myvalue".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let result = ctx
                        .raw(&GetexInput { key: RedisKey::String("getex_key".into()), options: None }.command())
                        .await
                        .expect("raw failed");

                    let output = GetexOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                    assert_eq!(output.value(), Some(&RedisJsonValue::from("myvalue")));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getex_nonexistent() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&GetexInput { key: RedisKey::String("getex_missing".into()), options: None }.command())
                        .await
                        .expect("raw failed");

                    let output = GetexOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getex_with_ex() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getex_ex".into()),
                            value: RedisJsonValue::String("value".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GETEX with EX to set TTL
                    let result = ctx
                        .raw(
                            &GetexInput {
                                key: RedisKey::String("getex_ex".into()),
                                options: Some(GetexOptions::EX(GetexEX { seconds: RedisJsonValue::Integer(300) })),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetexOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());

                    // Check TTL was set
                    let ttl_result = ctx.raw(&TtlInput { key: RedisKey::String("getex_ex".into()) }.command()).await.expect("raw failed");

                    let ttl_output = TtlOutput::decode(&ttl_result).expect("decode failed");
                    assert!(ttl_output.has_expiration());
                    let seconds = ttl_output.seconds().expect("should have TTL");
                    assert!(seconds > 0 && seconds <= 300);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getex_persist() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    // Set key with TTL
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("getex_persist".into()),
                            value: RedisJsonValue::String("value".into()),
                            options: Some(crate::api::lib::string::set::args::Options::EX(crate::api::lib::string::set::args::EX {
                                seconds: RedisJsonValue::Integer(300),
                            })),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // GETEX with PERSIST to remove TTL
                    let result = ctx
                        .raw(
                            &GetexInput {
                                key: RedisKey::String("getex_persist".into()),
                                options: Some(GetexOptions::PERSIST),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = GetexOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());

                    // Check TTL was removed
                    let ttl_result =
                        ctx.raw(&TtlInput { key: RedisKey::String("getex_persist".into()) }.command()).await.expect("raw failed");

                    let ttl_output = TtlOutput::decode(&ttl_result).expect("decode failed");
                    assert_eq!(ttl_output.ttl(), &Ttl::NoExpiration);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_getex_pipeline() {
            test_all_protocols_min_version("6.2", |ctx| {
                Box::pin(async move {
                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("ge1".into()),
                            value: RedisJsonValue::String("v1".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    ctx.raw(
                        &SetInput {
                            key: RedisKey::String("ge2".into()),
                            value: RedisJsonValue::String("v2".into()),
                            ..Default::default()
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&GetexInput { key: RedisKey::String("ge1".into()), options: None }.command());
                    pipeline.extend_from_slice(&GetexInput { key: RedisKey::String("ge_missing".into()), options: None }.command());
                    pipeline.extend_from_slice(
                        &GetexInput {
                            key: RedisKey::String("ge2".into()),
                            options: Some(GetexOptions::EX(GetexEX { seconds: RedisJsonValue::Integer(60) })),
                        }
                        .command(),
                    );

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    let out1 = GetexOutput::decode(responses[0]).expect("decode first");
                    assert!(out1.exists());
                    assert_eq!(out1.value(), Some(&RedisJsonValue::from("v1")));

                    let out2 = GetexOutput::decode(responses[1]).expect("decode second");
                    assert!(!out2.exists());

                    let out3 = GetexOutput::decode(responses[2]).expect("decode third");
                    assert!(out3.exists());
                    assert_eq!(out3.value(), Some(&RedisJsonValue::from("v2")));
                })
            })
            .await;
        }
    }
}
