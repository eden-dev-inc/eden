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
use utoipa::ToSchema;

const API_INFO: ApiInfo<RedisApi, RandomkeyInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Randomkey, "Returns a random key from the database", ReqType::Read, true);

/// See official Redis documentation for `RANDOMKEY`
/// https://redis.io/docs/latest/commands/randomkey/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RandomkeyInput {}

impl Serialize for RandomkeyInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RandomkeyInput", 1)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.end()
    }
}

impl_redis_operation!(RandomkeyInput, API_INFO);

impl RedisCommandInput for RandomkeyInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }

    fn command(&self) -> bytes::Bytes {
        crate::command::cmd(&API_INFO.api.to_string()).get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if !args.is_empty() {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "RANDOMKEY expects no arguments, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        Ok(Self {})
    }
}

/// Output for Redis RANDOMKEY command
///
/// Returns a random key from the database, or None if the database is empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RandomkeyOutput {
    /// The random key, or None if database is empty
    key: Option<RedisKey>,
}

impl RandomkeyOutput {
    pub fn new(key: Option<RedisKey>) -> Self {
        Self { key }
    }

    /// Get the random key from the output
    pub fn key(&self) -> Option<&RedisKey> {
        self.key.as_ref()
    }

    /// Check if the database is empty (no key returned)
    pub fn is_empty(&self) -> bool {
        self.key.is_none()
    }

    /// Decode the Redis protocol response into a RandomkeyOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let key = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(bytes) => Some(RedisKey::String(String::from_utf8(bytes).map_err(EpError::parse)?)),
                Resp2Frame::SimpleString(s) => Some(RedisKey::String(String::from_utf8(s).map_err(EpError::parse)?)),
                Resp2Frame::Null => None,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected RANDOMKEY response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => Some(RedisKey::String(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::SimpleString { data, .. } => Some(RedisKey::String(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::Null => None,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected RANDOMKEY response: {:?}", other)));
                }
            },
        };

        Ok(Self { key })
    }
}

impl Serialize for RandomkeyOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RandomkeyOutput", 1)?;
        state.serialize_field("key", &self.key)?;
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
            let input = RandomkeyInput {};
            assert_eq!(input.command().to_vec(), b"*1\r\n$9\r\nRANDOMKEY\r\n");
        }

        #[test]
        fn test_decode_bulk_string() {
            let output = RandomkeyOutput::decode(b"$5\r\nmykey\r\n").unwrap();
            assert!(!output.is_empty());
            assert_eq!(output.key(), Some(&RedisKey::String("mykey".into())));
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = RandomkeyOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.key(), None);
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = RandomkeyOutput::decode(b"_\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.key(), None);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = RandomkeyOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_empty_args() {
            let input = RandomkeyInput::decode(vec![]).unwrap();
            assert_eq!(input.keys().len(), 0);
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = RandomkeyInput {};
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = RandomkeyInput {};
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Randomkey);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::api::lib::string::get::GetInput;
        use crate::api::lib::string::get::GetOutput;
        use crate::protocol::RedisProtocol;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_empty_db() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB to ensure empty database
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

                    let output = RandomkeyOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty(), "empty db should return null");
                    assert_eq!(output.key(), None);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_single_key() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB and set one key
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("onlykey".into()),
                        value: RedisJsonValue::String("value".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

                    let output = RandomkeyOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());
                    assert_eq!(output.key(), Some(&RedisKey::String("onlykey".into())));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_multiple_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB and set multiple keys
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    let keys = ["key1", "key2", "key3"];
                    for key in &keys {
                        ctx.write(SetInput {
                            key: RedisKey::String((*key).into()),
                            value: RedisJsonValue::String("value".into()),
                            ..Default::default()
                        })
                        .await;
                    }

                    let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

                    let output = RandomkeyOutput::decode(&result).expect("decode failed");
                    assert!(!output.is_empty());

                    // Verify returned key is one of the keys we set
                    let returned_key = output.key().expect("should have key");
                    match returned_key {
                        RedisKey::String(k) => {
                            assert!(keys.contains(&k.as_str()), "returned key '{}' not in expected set", k);
                        }
                        _ => panic!("expected string key"),
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_returned_key_exists() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB and set a key
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("testkey".into()),
                        value: RedisJsonValue::String("testvalue".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

                    let output = RandomkeyOutput::decode(&result).expect("decode failed");
                    let key = output.key().expect("should have key").clone();

                    // Verify we can GET the returned key
                    let get_result = ctx.raw(&GetInput { key: key.clone() }.command()).await.expect("raw failed");

                    let get_output = GetOutput::decode(&get_result).expect("decode get failed");
                    assert!(get_output.exists(), "returned key should exist");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_pipeline() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // FLUSHDB and set a key
                    ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

                    ctx.write(SetInput {
                        key: RedisKey::String("pipekey".into()),
                        value: RedisJsonValue::String("pipeval".into()),
                        ..Default::default()
                    })
                    .await;

                    // Pipeline multiple RANDOMKEY calls
                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&RandomkeyInput {}.command());
                    pipeline.extend_from_slice(&RandomkeyInput {}.command());
                    pipeline.extend_from_slice(&RandomkeyInput {}.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 3);

                    // All should return the same key (only one in db)
                    for resp in responses {
                        let output = RandomkeyOutput::decode(resp).expect("decode failed");
                        assert!(!output.is_empty());
                        assert_eq!(output.key(), Some(&RedisKey::String("pipekey".into())));
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_resp2_null_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;

            // FLUSHDB to ensure empty
            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"$-1\r\n", "RESP2 null bulk string format");
            let output = RandomkeyOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_randomkey_resp3_null_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;

            // FLUSHDB to ensure empty
            ctx.raw(b"*1\r\n$7\r\nFLUSHDB\r\n").await.expect("raw failed");

            let result = ctx.raw(&RandomkeyInput {}.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"_\r\n", "RESP3 null format");
            let output = RandomkeyOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }
    }
}
