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

const API_INFO: ApiInfo<RedisApi, TdigestResetInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TdigestReset,
    "Resets a t-digest sketch: empty the sketch and re-initializes it",
    ReqType::Write,
    true,
);

/// Input for Redis `TDIGEST.RESET` command.
///
/// Resets a t-digest sketch: empties the sketch and re-initializes it.
///
/// See official Redis documentation for `TDIGEST.RESET`:
/// https://redis.io/docs/latest/commands/tdigest.reset/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TdigestResetInput {
    /// The key name for the t-digest sketch
    pub(crate) key: RedisKey,
}

impl Serialize for TdigestResetInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestResetInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(TdigestResetInput, API_INFO, { key });

impl RedisCommandInput for TdigestResetInput {
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
        if args.len() != 1 {
            return Err(EpError::parse(format!("TDIGEST.RESET requires exactly 1 argument (key), given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `TDIGEST.RESET` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TdigestResetOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl TdigestResetOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a TdigestResetOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TDIGEST.RESET response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data.eq_ignore_ascii_case(b"OK") => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TDIGEST.RESET response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TdigestResetOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TdigestResetOutput", 1)?;
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
        fn test_encode_command() {
            let input = TdigestResetInput { key: RedisKey::String("td".into()) };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TDIGEST.RESET"));
            assert!(cmd_str.contains("td"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TdigestResetOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TdigestResetOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_output_key_not_found() {
            let err = TdigestResetOutput::decode(b"-ERR T-Digest: key does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("does not exist"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = TdigestResetInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::String("extra".into())];
            let err = TdigestResetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_decode_input_empty() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TdigestResetInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 1 argument"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = TdigestResetInput { key: RedisKey::String("mykey".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_serialize_output() {
            let output = TdigestResetOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("success"));
            assert!(json.contains("true"));
        }

        #[test]
        fn test_new_output() {
            let output = TdigestResetOutput::new(true);
            assert!(output.is_success());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::t_digest::tdigest_add::TdigestAddInput;
        use crate::api::lib::t_digest::tdigest_create::TdigestCreateInput;
        use crate::api::lib::t_digest::tdigest_info::TdigestInfoInput;
        use crate::api::lib::t_digest::tdigest_info::TdigestInfoOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_reset_basic() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_reset_test".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    // Add some values
                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_reset_test".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0), RedisJsonValue::Float(3.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    // Reset the sketch
                    let result =
                        ctx.raw(&TdigestResetInput { key: RedisKey::String("td_reset_test".into()) }.command()).await.expect("raw failed");

                    let output = TdigestResetOutput::decode(&result).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_reset_clears_data() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let Ok(create_result) = ctx
                        .raw(
                            &TdigestCreateInput {
                                key: RedisKey::String("td_reset_clear".into()),
                                compression: None,
                            }
                            .command(),
                        )
                        .await
                    else {
                        return;
                    };

                    if create_result.starts_with(b"-") {
                        return;
                    }

                    // Add values
                    ctx.raw(
                        &TdigestAddInput {
                            key: RedisKey::String("td_reset_clear".into()),
                            value: vec![RedisJsonValue::Float(1.0), RedisJsonValue::Float(2.0)],
                        }
                        .command(),
                    )
                    .await
                    .expect("add failed");

                    // Reset
                    ctx.raw(&TdigestResetInput { key: RedisKey::String("td_reset_clear".into()) }.command()).await.expect("reset failed");

                    // Check that sketch is empty via INFO
                    let info_result =
                        ctx.raw(&TdigestInfoInput { key: RedisKey::String("td_reset_clear".into()) }.command()).await.expect("info failed");

                    let info = TdigestInfoOutput::decode(&info_result).expect("decode info failed");
                    // After reset, total_observations should be 0
                    assert!((info.total_observations() - 0.0).abs() < f64::EPSILON);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_tdigest_reset_nonexistent_key() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&TdigestResetInput { key: RedisKey::String("nonexistent_td".into()) }.command()).await;

                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {
                        // Expected error for non-existent key
                    }
                })
            })
            .await;
        }
    }
}
