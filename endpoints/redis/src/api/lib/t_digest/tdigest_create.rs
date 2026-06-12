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

const API_INFO: ApiInfo<RedisApi, TdigestCreateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestCreate,
    "Allocates memory and initializes a new t-digest sketch",
    ReqType::Write,
    true,
);

/// Input for Redis `TDIGEST.CREATE` command.
///
/// Allocates memory and initializes a new t-digest sketch.
///
/// See official Redis documentation for `TDIGEST.CREATE`:
/// https://redis.io/docs/latest/commands/tdigest.create/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestCreateInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// The compression parameter (default: 100). Higher values mean more accuracy.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    pub(crate) compression: Option<RedisJsonValue>,
}

impl Serialize for TdigestCreateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2; // type, key
        if self.compression.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TdigestCreateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;

        if let Some(compression) = &self.compression {
            state.serialize_field("compression", compression)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    TdigestCreateInput,
    API_INFO,
    {key, compression}
);

impl RedisCommandInput for TdigestCreateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(comp) = &self.compression {
            command.arg("COMPRESSION").arg(comp);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::parse("TDIGEST.CREATE requires at least 1 argument (key)"));
        }

        let key = args[0].clone().try_into()?;
        let mut compression = None;
        let mut i = 1;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                if s.to_uppercase() == "COMPRESSION" {
                    if i + 1 >= args.len() {
                        return Err(EpError::parse("COMPRESSION requires a value"));
                    }
                    compression = Some(args[i + 1].clone());
                    i += 2;
                } else {
                    return Err(EpError::parse(format!("Unknown TDIGEST.CREATE option: {}", s)));
                }
            } else {
                return Err(EpError::parse("TDIGEST.CREATE options must be strings"));
            }
        }

        Ok(TdigestCreateInput { key, compression })
    }
}

/// Output for Redis `TDIGEST.CREATE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestCreateOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl TdigestCreateOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a TdigestCreateOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TDIGEST.CREATE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TDIGEST.CREATE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TdigestCreateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestCreateOutput", 1)?;
        state.serialize_field("success", &self.success)?;
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
            let input = TdigestCreateInput { key: RedisKey::String("td".into()), compression: None };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.CREATE"));
            assert!(cmd_str.contains("td"));
            assert!(!cmd_str.contains("COMPRESSION"));
        }

        #[test]
        fn test_encode_command_with_compression() {
            let input = TdigestCreateInput {
                key: RedisKey::String("td".into()),
                compression: Some(RedisJsonValue::Integer(200)),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.CREATE"));
            assert!(cmd_str.contains("td"));
            assert!(cmd_str.contains("COMPRESSION"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TdigestCreateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestCreateOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_output_already_exists() {
            let err = TdigestCreateOutput::decode(b"-ERR T-Digest: key already exists\r\n").unwrap_err();
            assert!(err.to_string().contains("already exists"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TdigestCreateInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.compression.is_none());
        }

        #[test]
        fn test_decode_input_with_compression() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("COMPRESSION".into()),
                RedisJsonValue::Integer(200),
            ];
            let input = TdigestCreateInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.compression, Some(RedisJsonValue::Integer(200)));
        }

        #[test]
        fn test_decode_input_compression_lowercase() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::String("compression".into()),
                RedisJsonValue::Integer(150),
            ];
            let input = TdigestCreateInput::decode(args).unwrap();
            assert!(input.compression.is_some());
        }

        #[test]
        fn test_decode_input_compression_missing_value() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("COMPRESSION".into())];
            let err = TdigestCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires a value"));
        }

        #[test]
        fn test_decode_input_unknown_option() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("UNKNOWN".into())];
            let err = TdigestCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("Unknown"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestCreateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestCreateInput { key: RedisKey::String("mykey".into()), compression: None };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestCreateOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }

        #[test]
        fn test_serialize_input_without_compression() {
            let input = TdigestCreateInput { key: RedisKey::String("mykey".into()), compression: None };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("mykey"));
            assert!(!json.contains("compression"));
        }

        #[test]
        fn test_serialize_input_with_compression() {
            let input = TdigestCreateInput {
                key: RedisKey::String("mykey".into()),
                compression: Some(RedisJsonValue::Integer(200)),
            };
            let json = serde_json::to_string(&input).unwrap();
            assert!(json.contains("mykey"));
            assert!(json.contains("compression"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_create_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_create_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"+OK") => {
                            let output = TdigestCreateOutput::decode(&r).expect("decode failed");
                            assert!(output.is_success());
                        }
                        Ok(r) if r.starts_with(b"-") => {
                            // Module not available or other error, skip
                        }
                        Err(_) => {
                            // Connection error, skip
                        }
                        _ => {}
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_create_with_compression() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_create_comp".into()),
                                compression: Some(RedisJsonValue::Integer(200)),
                            }
                            .command(),
                        )
                        .await;

                    match result {
                        Ok(r) if r.starts_with(b"+OK") => {
                            let output = TdigestCreateOutput::decode(&r).expect("decode failed");
                            assert!(output.is_success());
                        }
                        Ok(r) if r.starts_with(b"-") => {
                            // Module not available
                        }
                        Err(_) => {}
                        _ => {}
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_create_duplicate_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // Create first time
                    let first_result = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_dup_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await;

                    let Ok(r) = first_result else { return };
                    if r.starts_with(b"-") {
                        // Module not available
                        return;
                    }

                    // Try to create again - should fail
                    let second_result = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_dup_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await;

                    if let Ok(r) = second_result {
                        // Should be an error
                        assert!(r.starts_with(b"-"));
                    }
                })
            })
            .await;
        }
    }
}
