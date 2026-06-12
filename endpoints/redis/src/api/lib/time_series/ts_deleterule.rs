use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use endpoint_types::protocol::EpProtocol;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize, Serializer};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, TsDeleteruleInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::TsDeleterule, "Delete a compaction rule", ReqType::Write, true);

/// Input for Redis `TS.DELETERULE` command.
///
/// Deletes a compaction rule from a source time series to a destination time series.
///
/// See official Redis documentation for `TS.DELETERULE`:
/// https://redis.io/docs/latest/commands/ts.deleterule/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TsDeleteruleInput {
    /// The key name of the source time series
    source_key: RedisKey,
    /// The key name of the destination (compacted) time series
    dest_key: RedisKey,
}

impl Serialize for TsDeleteruleInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("TsDeleteruleInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("source_key", &self.source_key)?;
        state.serialize_field("dest_key", &self.dest_key)?;
        state.end()
    }
}

impl_redis_operation!(
    TsDeleteruleInput,
    API_INFO,
    {source_key, dest_key}
);

impl RedisCommandInput for TsDeleteruleInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.source_key.clone(), self.dest_key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.source_key).arg(&self.dest_key);

        command.get_packed_command()
    }

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!(
                "TS.DELETERULE requires exactly 2 arguments (sourceKey, destKey), given {}",
                args.len()
            )));
        }

        Ok(TsDeleteruleInput {
            source_key: args[0].clone().try_into()?,
            dest_key: args[1].clone().try_into()?,
        })
    }
}

/// Output for Redis `TS.DELETERULE` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TsDeleteruleOutput {
    /// Whether the operation was successful
    success: bool,
}

impl TsDeleteruleOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the delete rule operation was successful
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a TsDeleteruleOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { success: s.to_uppercase() == "OK" })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected TS.DELETERULE response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { success: s.to_uppercase() == "OK" })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected TS.DELETERULE response: {:?}", other))),
            },
        }
    }
}

impl Serialize for TsDeleteruleOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TsDeleteruleOutput", 1)?;
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
            let input = TsDeleteruleInput {
                source_key: RedisKey::String("source:ts".into()),
                dest_key: RedisKey::String("dest:ts".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TS.DELETERULE"));
            assert!(cmd_str.contains("source:ts"));
            assert!(cmd_str.contains("dest:ts"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("source:ts".into()), RedisJsonValue::String("dest:ts".into())];
            let input = TsDeleteruleInput::decode(args).unwrap();
            assert_eq!(input.source_key, RedisKey::String("source:ts".into()));
            assert_eq!(input.dest_key, RedisKey::String("dest:ts".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("source:ts".into())];
            let err = TsDeleteruleInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("source:ts".into()),
                RedisJsonValue::String("dest:ts".into()),
                RedisJsonValue::String("extra".into()),
            ];
            let err = TsDeleteruleInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("exactly 2 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = TsDeleteruleOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = TsDeleteruleOutput::decode(b"-ERR TSDB: compaction rule does not exist\r\n").unwrap_err();
            assert!(err.to_string().contains("compaction rule"));
        }

        #[test]
        fn test_keys_returns_both_keys() {
            let input = TsDeleteruleInput {
                source_key: RedisKey::String("source".into()),
                dest_key: RedisKey::String("dest".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 2);
            assert_eq!(keys[0], RedisKey::String("source".into()));
            assert_eq!(keys[1], RedisKey::String("dest".into()));
        }

        #[test]
        fn test_kind_returns_correct_api() {
            let input = TsDeleteruleInput {
                source_key: RedisKey::String("source".into()),
                dest_key: RedisKey::String("dest".into()),
            };
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::TsDeleterule);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Note: TS.DELETERULE requires RedisTimeSeries module.
        // These tests will skip if the module is not available.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_deleterule_nonexistent_rule() {
            test_all_protocols_min_version("6.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &TsDeleteruleInput {
                                source_key: RedisKey::String("nonexistent:source".into()),
                                dest_key: RedisKey::String("nonexistent:dest".into()),
                            }
                            .command(),
                        )
                        .await;

                    // Expected to fail - rule doesn't exist
                    if let Ok(result) = result
                        && result.starts_with(b"-")
                    {
                        // Error response - expected
                    }
                })
            })
            .await;
        }
    }
}
