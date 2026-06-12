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

const API_INFO: ApiInfo<RedisApi, KeysInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Keys,
    "Returns all keys matching pattern. Warning: KEYS should only be used in production with extreme care as it may block the server",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `KEYS`
/// https://redis.io/docs/latest/commands/keys/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct KeysInput {
    pub pattern: RedisKey,
}

impl Serialize for KeysInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("KeysInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("pattern", &self.pattern)?;
        state.end()
    }
}

impl_redis_operation!(KeysInput, API_INFO, { pattern });

impl RedisCommandInput for KeysInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.pattern);
        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("KEYS requires one argument, given none"));
        } else if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(_ctx, "KEYS takes 1 argument, but given {}", audience = LogAudience::Client, args_given = args.len());
        }

        Ok(Self { pattern: args[0].clone().try_into()? })
    }
}

/// Output for Redis KEYS command
///
/// Returns a list of keys matching the specified pattern.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct KeysOutput {
    /// The list of keys matching the pattern
    keys: Vec<String>,
}

impl KeysOutput {
    pub fn new(keys: Vec<String>) -> Self {
        Self { keys }
    }

    /// Get the keys from the output
    pub fn keys(&self) -> &[String] {
        &self.keys
    }

    /// Check if any keys matched
    pub fn is_empty(&self) -> bool {
        self.keys.is_empty()
    }

    /// Get the number of matching keys
    pub fn len(&self) -> usize {
        self.keys.len()
    }

    /// Decode the Redis protocol response into a KeysOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let keys = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => {
                    let mut keys = Vec::with_capacity(items.len());
                    for item in items {
                        match item {
                            Resp2Frame::BulkString(bytes) => {
                                keys.push(String::from_utf8(bytes).map_err(EpError::parse)?);
                            }
                            Resp2Frame::SimpleString(s) => {
                                keys.push(String::from_utf8(s).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in KEYS response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected KEYS response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Array { data, .. } => {
                    let mut keys = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected array element in KEYS response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp3Frame::Set { data, .. } => {
                    let mut keys = Vec::with_capacity(data.len());
                    for item in data {
                        match item {
                            Resp3Frame::BlobString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            Resp3Frame::SimpleString { data, .. } => {
                                keys.push(String::from_utf8(data).map_err(EpError::parse)?);
                            }
                            other => {
                                return Err(EpError::parse(format!("unexpected set element in KEYS response: {:?}", other)));
                            }
                        }
                    }
                    keys
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected KEYS response: {:?}", other)));
                }
            },
        };

        Ok(Self { keys })
    }
}

impl Serialize for KeysOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("KeysOutput", 1)?;
        state.serialize_field("keys", &self.keys)?;
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
            let input = KeysInput { pattern: RedisKey::String("user:*".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nKEYS\r\n$6\r\nuser:*\r\n");
        }

        #[test]
        fn test_encode_command_all_keys() {
            let input = KeysInput { pattern: RedisKey::String("*".into()) };
            assert_eq!(input.command().to_vec(), b"*2\r\n$4\r\nKEYS\r\n$1\r\n*\r\n");
        }

        #[test]
        fn test_decode_empty_array() {
            let output = KeysOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
            assert_eq!(output.keys(), &[] as &[String]);
        }

        #[test]
        fn test_decode_single_key() {
            let output = KeysOutput::decode(b"*1\r\n$3\r\nfoo\r\n").unwrap();
            assert!(!output.is_empty());
            assert_eq!(output.len(), 1);
            assert_eq!(output.keys(), &["foo"]);
        }

        #[test]
        fn test_decode_multiple_keys() {
            let output = KeysOutput::decode(b"*3\r\n$4\r\nkey1\r\n$4\r\nkey2\r\n$4\r\nkey3\r\n").unwrap();
            assert_eq!(output.len(), 3);
            assert_eq!(output.keys(), &["key1", "key2", "key3"]);
        }

        #[test]
        fn test_decode_error_fails() {
            let err = KeysOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::SetInput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_no_match() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&KeysInput { pattern: RedisKey::String("nonexistent:*".into()) }.command()).await.expect("raw failed");

                    let output = KeysOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty(), "no keys should match");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_after_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("testkey:1".into()),
                        value: RedisJsonValue::String("v1".into()),
                        ..Default::default()
                    })
                    .await;

                    ctx.write(SetInput {
                        key: RedisKey::String("testkey:2".into()),
                        value: RedisJsonValue::String("v2".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&KeysInput { pattern: RedisKey::String("testkey:*".into()) }.command()).await.expect("raw failed");

                    let output = KeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);

                    let mut keys: Vec<_> = output.keys().to_vec();
                    keys.sort();
                    assert_eq!(keys, vec!["testkey:1", "testkey:2"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_exact_match() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("exactkey".into()),
                        value: RedisJsonValue::String("val".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&KeysInput { pattern: RedisKey::String("exactkey".into()) }.command()).await.expect("raw failed");

                    let output = KeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 1);
                    assert_eq!(output.keys(), &["exactkey"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_question_mark_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("key1".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("key2".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("key10".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&KeysInput { pattern: RedisKey::String("key?".into()) }.command()).await.expect("raw failed");

                    let output = KeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2, "? matches single char only");

                    let mut keys: Vec<_> = output.keys().to_vec();
                    keys.sort();
                    assert_eq!(keys, vec!["key1", "key2"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_bracket_pattern() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.write(SetInput {
                        key: RedisKey::String("keya".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("keyb".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;
                    ctx.write(SetInput {
                        key: RedisKey::String("keyc".into()),
                        value: RedisJsonValue::String("v".into()),
                        ..Default::default()
                    })
                    .await;

                    let result = ctx.raw(&KeysInput { pattern: RedisKey::String("key[ab]".into()) }.command()).await.expect("raw failed");

                    let output = KeysOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);

                    let mut keys: Vec<_> = output.keys().to_vec();
                    keys.sort();
                    assert_eq!(keys, vec!["keya", "keyb"]);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_resp2_empty_format() {
            let mut ctx = setup(RespVersion::Resp2, None).await;
            let result = ctx.raw(&KeysInput { pattern: RedisKey::String("missing:*".into()) }.command()).await.expect("raw failed");

            assert_eq!(&result[..], b"*0\r\n", "RESP2 empty array format");
            let output = KeysOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_keys_resp3_empty_format() {
            let mut ctx = setup(RespVersion::Resp3, None).await;
            let result = ctx.raw(&KeysInput { pattern: RedisKey::String("missing:*".into()) }.command()).await.expect("raw failed");

            // RESP3 also uses *0\r\n for empty arrays
            let output = KeysOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }
    }
}
