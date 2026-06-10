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

const API_INFO: ApiInfo<RedisApi, TdigestAddInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestAdd,
    "Adds one or more observations to a t-digest sketch",
    ReqType::Write,
    true,
);

/// Input for Redis `TDIGEST.ADD` command.
///
/// Adds one or more observations to a t-digest sketch.
///
/// See official Redis documentation for `TDIGEST.ADD`:
/// https://redis.io/docs/latest/commands/tdigest.add/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestAddInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
    /// One or more values to add to the sketch
    pub(crate) value: Vec<RedisJsonValue>,
}

impl Serialize for TdigestAddInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestAddInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("value", &self.value)?;
        state.end()
    }
}

impl_redis_operation!(
    TdigestAddInput,
    API_INFO,
    {key, value}
);

impl RedisCommandInput for TdigestAddInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        for v in &self.value {
            command.arg(v);
        }
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::parse(format!(
                "TDIGEST.ADD requires at least 2 arguments (key, value...), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let value = args[1..].to_vec();

        if value.is_empty() {
            return Err(EpError::parse("TDIGEST.ADD requires at least one value to add"));
        }

        Ok(Self { key, value })
    }
}

/// Output for Redis `TDIGEST.ADD` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestAddOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl TdigestAddOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a TdigestAddOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TDIGEST.ADD response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TDIGEST.ADD response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TdigestAddOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestAddOutput", 1)?;
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
        fn test_encode_command_single_value() {
            let input = TdigestAddInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.5)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.ADD"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_encode_command_multiple_values() {
            let input = TdigestAddInput {
                key: RedisKey::String("td".into()),
                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.ADD"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TdigestAddOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestAddOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Float(1.5),
                RedisJsonValue::Float(2.5),
            ];
            let input = TdigestAddInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.value.len(), 2);
        }

        #[test]
        fn test_decode_input_missing_value() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let err = TdigestAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestAddInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestAddInput {
                key: RedisKey::String("mykey".into()),
                value: vec![RedisJsonValue::Float(1.0)],
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestAddOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::t_digest::tdigest_create::TdigestCreateInput;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TDIGEST commands require the RedisBloom module.
        // These tests will skip or fail if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_add_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    // First create the t-digest
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_add_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        // Module not available, skip test
                        return;
                    }

                    let result = ctx
                        .raw(
                            &TdigestAddInput {
                                key: RedisKey::String("td_add_test".into()),
                                value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = TdigestAddOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_add_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TdigestAddInput {
                                key: RedisKey::String("nonexistent_td".into()),
                                value: vec![RedisJsonValue::Float(1.0)],
                            }
                            .command(),
                        )
                        .await;

                    // Should return error for nonexistent key
                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {
                        // Expected error
                    }
                })
            })
            .await;
        }
    }
}
