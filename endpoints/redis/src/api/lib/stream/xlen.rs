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

const API_INFO: ApiInfo<RedisApi, XlenInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Xlen, "Returns the number of entries in a stream", ReqType::Read, true);

/// Input for Redis `XLEN` command.
///
/// Returns the number of entries inside a stream.
///
/// See official Redis documentation for `XLEN`:
/// https://redis.io/docs/latest/commands/xlen/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct XlenInput {
    /// The key of the stream
    key: RedisKey,
}

impl Serialize for XlenInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("XlenInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.end()
    }
}

impl_redis_operation!(XlenInput, API_INFO, { key });

impl RedisCommandInput for XlenInput {
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
            return Err(EpError::parse(format!("XLEN requires 1 argument, given {}", args.len())));
        }

        Ok(Self { key: args[0].clone().try_into()? })
    }
}

/// Output for Redis `XLEN` command.
///
/// Returns the number of entries in the stream, or 0 if the key does not exist.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct XlenOutput {
    /// The number of entries in the stream
    length: i64,
}

impl XlenOutput {
    /// Create a new XlenOutput
    pub fn new(length: i64) -> Self {
        Self { length }
    }

    /// Get the stream length
    pub fn length(&self) -> i64 {
        self.length
    }

    /// Check if the stream is empty
    pub fn is_empty(&self) -> bool {
        self.length == 0
    }

    /// Decode the Redis protocol response into an XlenOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let length = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Integer(n) => n,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected XLEN response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Number { data, .. } => data,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected XLEN response: {:?}", other)));
                }
            },
        };

        Ok(Self { length })
    }
}

impl Serialize for XlenOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("XlenOutput", 1)?;
        state.serialize_field("length", &self.length)?;
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
            let input = XlenInput { key: RedisKey::String("mystream".into()) };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*2\r\n"));
            assert!(cmd.windows(4).any(|w| w == b"XLEN"));
            assert!(cmd.windows(8).any(|w| w == b"mystream"));
        }

        #[test]
        fn test_decode_output_zero() {
            let output = XlenOutput::decode(b":0\r\n").unwrap();
            assert_eq!(output.length(), 0);
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_output_positive() {
            let output = XlenOutput::decode(b":42\r\n").unwrap();
            assert_eq!(output.length(), 42);
            assert!(!output.is_empty());
        }

        #[test]
        fn test_decode_output_large() {
            let output = XlenOutput::decode(b":1000000\r\n").unwrap();
            assert_eq!(output.length(), 1000000);
        }

        #[test]
        fn test_decode_output_error_fails() {
            let err = XlenOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_keys_accessor() {
            let input = XlenInput { key: RedisKey::String("mystream".into()) };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mystream".into())];
            let input = XlenInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mystream".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = XlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::String("stream1".into()), RedisJsonValue::String("stream2".into())];
            let err = XlenInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 1 argument"));
        }

        #[test]
        fn test_output_new() {
            let output = XlenOutput::new(5);
            assert_eq!(output.length(), 5);
        }

        #[test]
        fn test_output_serialize() {
            let output = XlenOutput::new(10);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"length\":10"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // Helper to create a stream entry using XADD
        async fn xadd_entry(ctx: &mut TestContext, key: &str, field: &str, value: &str) -> String {
            let cmd = format!(
                "*5\r\n$4\r\nXADD\r\n${}\r\n{}\r\n$1\r\n*\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                key.len(),
                key,
                field.len(),
                field,
                value.len(),
                value
            );
            let result = ctx.raw(cmd.as_bytes()).await.expect("XADD failed");
            let response = String::from_utf8_lossy(&result);
            if response.starts_with('$') {
                response.lines().nth(1).unwrap_or("").trim().to_string()
            } else if let Some(stripped) = response.strip_prefix('+') {
                stripped.trim().to_string()
            } else {
                response.trim().to_string()
            }
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_nonexistent_stream() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    let result =
                        ctx.raw(&XlenInput { key: RedisKey::String("nonexistent_stream".into()) }.command()).await.expect("raw failed");

                    let output = XlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 0);
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_single_entry() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    xadd_entry(ctx, "xlen_single", "field", "value").await;

                    let result = ctx.raw(&XlenInput { key: RedisKey::String("xlen_single".into()) }.command()).await.expect("raw failed");

                    let output = XlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 1);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_multiple_entries() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    for i in 0..5 {
                        xadd_entry(ctx, "xlen_multi", "field", &format!("value{}", i)).await;
                    }

                    let result = ctx.raw(&XlenInput { key: RedisKey::String("xlen_multi".into()) }.command()).await.expect("raw failed");

                    let output = XlenOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.length(), 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_wrong_type() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create a string key instead of stream
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$14\r\nxlen_wrongtype\r\n$5\r\nvalue\r\n").await.expect("SET failed");

                    let result =
                        ctx.raw(&XlenInput { key: RedisKey::String("xlen_wrongtype".into()) }.command()).await.expect("raw failed");

                    let err = XlenOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            xadd_entry(&mut ctx, "xlen_r2", "field", "value").await;

            let result = ctx.raw(&XlenInput { key: RedisKey::String("xlen_r2".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b":"), "RESP2 should return integer");
            let output = XlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            xadd_entry(&mut ctx, "xlen_r3", "field", "value").await;

            let result = ctx.raw(&XlenInput { key: RedisKey::String("xlen_r3".into()) }.command()).await.expect("raw failed");

            let output = XlenOutput::decode(&result).expect("decode failed");
            assert_eq!(output.length(), 1);

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_xlen_pipeline() {
            test_all_protocols_min_version("5.0", |ctx| {
                Box::pin(async move {
                    // Create two streams with different lengths
                    xadd_entry(ctx, "xlen_pipe1", "f", "v").await;
                    xadd_entry(ctx, "xlen_pipe2", "f", "v").await;
                    xadd_entry(ctx, "xlen_pipe2", "f", "v").await;

                    let mut pipeline = Vec::new();
                    pipeline.extend_from_slice(&XlenInput { key: RedisKey::String("xlen_pipe1".into()) }.command());
                    pipeline.extend_from_slice(&XlenInput { key: RedisKey::String("xlen_pipe2".into()) }.command());

                    let result = ctx.raw(&pipeline).await.expect("raw failed");
                    let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("parse pipeline");

                    assert_eq!(responses.len(), 2);

                    let out1 = XlenOutput::decode(responses[0]).expect("decode first");
                    assert_eq!(out1.length(), 1);

                    let out2 = XlenOutput::decode(responses[1]).expect("decode second");
                    assert_eq!(out2.length(), 2);
                })
            })
            .await;
        }
    }
}
