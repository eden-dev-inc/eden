use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::protocol::RedisProtocol;
use crate::protocol::decoder::{DecoderRespFrame, Resp2Frame, Resp3Frame};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use borsh::{BorshDeserialize, BorshSerialize};
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

const API_INFO: ApiInfo<RedisApi, TopkReserveInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::TopkReserve,
    "Initializes a TopK with specified parameters",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `TOPK.RESERVE`
/// https://redis.io/docs/latest/commands/topk.reserve/
///
/// Available since RedisBloom 2.0.0
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct TopkReserveInput {
    key: RedisKey,
    topk: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<TopkReserveParams>,
}

impl Serialize for TopkReserveInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 3; // type, key, topk
        if self.params.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("TopkReserveInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("topk", &self.topk)?;

        if let Some(params) = &self.params {
            state.serialize_field("params", params)?;
        }
        state.end()
    }
}

/// Optional parameters for TOPK.RESERVE
#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
pub struct TopkReserveParams {
    /// Width of the underlying Count-Min Sketch
    width: RedisJsonValue,
    /// Depth of the underlying Count-Min Sketch
    depth: RedisJsonValue,
    /// Decay factor (probability of reducing a counter)
    decay: RedisJsonValue,
}

impl_redis_operation!(
    TopkReserveInput,
    API_INFO,
    { key, topk, params }
);

impl RedisCommandInput for TopkReserveInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.topk);

        if let Some(params) = &self.params {
            command.arg(&params.width).arg(&params.depth).arg(&params.decay);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 2 {
            return Err(EpError::request("TOPK.RESERVE requires at least 2 arguments (key, topk)"));
        }

        let key = args[0].clone().try_into()?;
        let topk = args[1].clone();

        let params = if args.len() >= 5 {
            Some(TopkReserveParams {
                width: args[2].clone(),
                depth: args[3].clone(),
                decay: args[4].clone(),
            })
        } else if args.len() > 2 {
            return Err(EpError::request("TOPK.RESERVE optional params require all 3 values (width, depth, decay)"));
        } else {
            None
        };

        Ok(TopkReserveInput { key, topk, params })
    }
}

/// Output for Redis TOPK.RESERVE command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct TopkReserveOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl TopkReserveOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the reserve operation succeeded
    pub fn is_success(&self) -> bool {
        self.success
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::SimpleString(s)) => Ok(Self { success: s == b"OK" }),
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleString { data, .. }) => Ok(Self { success: data == b"OK" }),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            _ => Err(EpError::parse("unexpected TOPK.RESERVE response format")),
        }
    }
}

impl Serialize for TopkReserveOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("TopkReserveOutput", 1)?;
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
            let input = TopkReserveInput {
                key: RedisKey::String("mytopk".into()),
                topk: RedisJsonValue::Integer(10),
                params: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.RESERVE"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("10"));
        }

        #[test]
        fn test_encode_command_with_params() {
            let input = TopkReserveInput {
                key: RedisKey::String("mytopk".into()),
                topk: RedisJsonValue::Integer(10),
                params: Some(TopkReserveParams {
                    width: RedisJsonValue::Integer(2000),
                    depth: RedisJsonValue::Integer(7),
                    decay: RedisJsonValue::String("0.9".into()),
                }),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("TOPK.RESERVE"));
            assert!(cmd_str.contains("mytopk"));
            assert!(cmd_str.contains("2000"));
            assert!(cmd_str.contains("7"));
            assert!(cmd_str.contains("0.9"));
        }

        #[test]
        fn test_decode_input_basic() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(10)];
            let input = TopkReserveInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.topk, RedisJsonValue::Integer(10));
            assert!(input.params.is_none());
        }

        #[test]
        fn test_decode_input_with_params() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(10),
                RedisJsonValue::Integer(2000),
                RedisJsonValue::Integer(7),
                RedisJsonValue::String("0.9".into()),
            ];
            let input = TopkReserveInput::decode(args).unwrap();
            assert!(input.params.is_some());
            let params = input.params.unwrap();
            assert_eq!(params.width, RedisJsonValue::Integer(2000));
            assert_eq!(params.depth, RedisJsonValue::Integer(7));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = TopkReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_one_arg_fails() {
            let args = vec![RedisJsonValue::String("key".into())];
            let err = TopkReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 2 arguments"));
        }

        #[test]
        fn test_decode_input_partial_params_fails() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(10),
                RedisJsonValue::Integer(2000), // width only, missing depth and decay
            ];
            let err = TopkReserveInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("all 3 values"));
        }

        #[test]
        fn test_keys_returns_key() {
            let input = TopkReserveInput {
                key: RedisKey::String("mykey".into()),
                topk: RedisJsonValue::Integer(10),
                params: None,
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_output_decode_ok() {
            let output = TopkReserveOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_success());
        }

        #[test]
        fn test_output_decode_error() {
            let err = TopkReserveOutput::decode(b"-ERR key already exists\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = TopkReserveOutput::new(true);
            assert!(output.is_success());

            let output = TopkReserveOutput::new(false);
            assert!(!output.is_success());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_reserve_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up first
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nreserve_basic\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkReserveInput {
                                key: RedisKey::String("reserve_basic".into()),
                                topk: RedisJsonValue::Integer(10),
                                params: None,
                            }
                            .command(),
                        )
                        .await;

                    // Skip if RedisBloom not available
                    if result.is_err() {
                        return;
                    }
                    let bytes = result.unwrap();
                    if bytes.starts_with(b"-ERR unknown command") {
                        return;
                    }

                    let output = TopkReserveOutput::decode(&bytes).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_reserve_with_params() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$14\r\nreserve_params\r\n").await.ok();

                    let result = ctx
                        .raw(
                            &TopkReserveInput {
                                key: RedisKey::String("reserve_params".into()),
                                topk: RedisJsonValue::Integer(5),
                                params: Some(TopkReserveParams {
                                    width: RedisJsonValue::Integer(1000),
                                    depth: RedisJsonValue::Integer(5),
                                    decay: RedisJsonValue::String("0.925".into()),
                                }),
                            }
                            .command(),
                        )
                        .await;

                    if result.is_err() {
                        return;
                    }
                    let bytes = result.unwrap();
                    if bytes.starts_with(b"-ERR unknown command") {
                        return;
                    }

                    let output = TopkReserveOutput::decode(&bytes).expect("decode failed");
                    assert!(output.is_success());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_topk_reserve_duplicate_fails() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nreserve_dup\r\n").await.ok();

                    // Create first time
                    let first = ctx
                        .raw(
                            &TopkReserveInput {
                                key: RedisKey::String("reserve_dup".into()),
                                topk: RedisJsonValue::Integer(10),
                                params: None,
                            }
                            .command(),
                        )
                        .await;

                    if first.is_err() || first.as_ref().unwrap().starts_with(b"-ERR unknown") {
                        return;
                    }

                    // Try to create again - should fail
                    let second = ctx
                        .raw(
                            &TopkReserveInput {
                                key: RedisKey::String("reserve_dup".into()),
                                topk: RedisJsonValue::Integer(10),
                                params: None,
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    assert!(
                        TopkReserveOutput::decode(&second).is_err()
                            || !TopkReserveOutput::decode(&second).unwrap().is_success()
                            || second.starts_with(b"-")
                    );
                })
            })
            .await;
        }
    }
}
