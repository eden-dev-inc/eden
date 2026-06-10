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

const API_INFO: ApiInfo<RedisApi, CmsInitbydimInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CmsInitbydim,
    "Initializes a Count-Min Sketch to dimensions specified by user",
    ReqType::Write,
    true,
);

/// Input for Redis `CMS.INITBYDIM` command.
///
/// Initializes a Count-Min Sketch with specified width and depth dimensions.
///
/// See official Redis documentation for `CMS.INITBYDIM`:
/// https://redis.io/docs/latest/commands/cms.initbydim/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CmsInitbydimInput {
    /// The key name for the Count-Min Sketch
    pub(crate) key: RedisKey,
    /// Number of counters in each array (width)
    pub(crate) width: RedisJsonValue,
    /// Number of counter-arrays (depth)
    pub(crate) depth: RedisJsonValue,
}

impl Serialize for CmsInitbydimInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CmsInitbydimInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("width", &self.width)?;
        state.serialize_field("depth", &self.depth)?;
        state.end()
    }
}

impl_redis_operation!(CmsInitbydimInput, API_INFO, { key, width, depth });

impl RedisCommandInput for CmsInitbydimInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.width).arg(&self.depth);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::parse(format!("CMS.INITBYDIM requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            width: args[1].clone(),
            depth: args[2].clone(),
        })
    }
}

/// Output for Redis `CMS.INITBYDIM` command.
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CmsInitbydimOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl CmsInitbydimOutput {
    /// Create a new CmsInitbydimOutput
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the operation succeeded
    pub fn is_success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a CmsInitbydimOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CMS.INITBYDIM response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self { success: true }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CMS.INITBYDIM response: {:?}", other))),
            },
        }
    }
}

impl Serialize for CmsInitbydimOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CmsInitbydimOutput", 1)?;
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
            let input = CmsInitbydimInput {
                key: RedisKey::String("cms_key".into()),
                width: RedisJsonValue::Integer(1000),
                depth: RedisJsonValue::Integer(5),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CMS.INITBYDIM"));
            assert!(cmd_str.contains("cms_key"));
            assert!(cmd_str.contains("1000"));
            assert!(cmd_str.contains("5"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = CmsInitbydimOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CmsInitbydimOutput::decode(b"-ERR unknown command\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(5),
            ];
            let input = CmsInitbydimInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(1000)];
            let err = CmsInitbydimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::Integer(5),
                RedisJsonValue::Integer(99),
            ];
            let err = CmsInitbydimInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = CmsInitbydimInput {
                key: RedisKey::String("mykey".into()),
                width: RedisJsonValue::Integer(1000),
                depth: RedisJsonValue::Integer(5),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_new() {
            let output = CmsInitbydimOutput::new(true);
            assert!(output.is_success());
        }

        #[test]
        fn test_output_serialize() {
            let output = CmsInitbydimOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_initbydim_basic() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &CmsInitbydimInput {
                                key: RedisKey::String("cms_dim_test".into()),
                                width: RedisJsonValue::Integer(1000),
                                depth: RedisJsonValue::Integer(5),
                            }
                            .command(),
                        )
                        .await;

                    // RedisBloom module may not be available
                    if let Ok(result) = result {
                        if result.starts_with(b"-") {
                            return; // Module not loaded
                        }
                        let output = CmsInitbydimOutput::decode(&result).expect("decode failed");
                        assert!(output.is_success());
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_initbydim_already_exists() {
            test_all_protocols_min_version("4.0", |ctx| {
                Box::pin(async move {
                    let input = CmsInitbydimInput {
                        key: RedisKey::String("cms_dim_exists".into()),
                        width: RedisJsonValue::Integer(1000),
                        depth: RedisJsonValue::Integer(5),
                    };

                    let first = ctx.raw(&input.command()).await;
                    if let Ok(first) = first {
                        if first.starts_with(b"-") {
                            return; // Module not loaded
                        }

                        // Second creation should fail
                        let second = ctx.raw(&input.command()).await.expect("raw failed");
                        assert!(second.starts_with(b"-"), "Creating existing key should error");
                    }
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_initbydim_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_r2".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(result) = result
                && !result.starts_with(b"-")
            {
                assert!(result.starts_with(b"+"), "RESP2 should return simple string");
                let output = CmsInitbydimOutput::decode(&result).expect("decode failed");
                assert!(output.is_success());
            }

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cms_initbydim_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &CmsInitbydimInput {
                        key: RedisKey::String("cms_r3".into()),
                        width: RedisJsonValue::Integer(100),
                        depth: RedisJsonValue::Integer(3),
                    }
                    .command(),
                )
                .await;

            if let Ok(result) = result
                && !result.starts_with(b"-")
            {
                let output = CmsInitbydimOutput::decode(&result).expect("decode failed");
                assert!(output.is_success());
            }

            ctx.stop().await;
        }
    }
}
