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
use serde::Serializer;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, SwapdbInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Swapdb, "Swaps two Redis databases", ReqType::Write, true);

/// See official Redis documentation for `SWAPDB`
/// https://redis.io/docs/latest/commands/swapdb/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SwapdbInput {
    /// First database index to swap
    index1: RedisJsonValue,
    /// Second database index to swap
    index2: RedisJsonValue,
}

impl Serialize for SwapdbInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SwapdbInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("index1", &self.index1)?;
        state.serialize_field("index2", &self.index2)?;
        state.end()
    }
}

impl_redis_operation!(
    SwapdbInput,
    API_INFO,
    { index1, index2 }
);

impl RedisCommandInput for SwapdbInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.index1).arg(&self.index2);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 2 {
            return Err(EpError::request(format!("SWAPDB requires 2 arguments, given {}", args.len())));
        }

        Ok(Self { index1: args[0].clone(), index2: args[1].clone() })
    }
}

/// Output for Redis SWAPDB command
///
/// Returns OK on success. The command atomically swaps two databases,
/// so that all connections connected to a given database will immediately
/// see the data of the other database, and vice-versa.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SwapdbOutput {
    /// Whether the swap was successful
    success: bool,
}

impl SwapdbOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Check if the swap was successful
    pub fn success(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a SwapdbOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self::new(true)),
                Resp2Frame::SimpleString(s) => Err(EpError::parse(format!("unexpected SWAPDB response: {}", String::from_utf8_lossy(&s)))),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SWAPDB response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } if data == b"OK" => Ok(Self::new(true)),
                Resp3Frame::SimpleString { data, .. } => {
                    Err(EpError::parse(format!("unexpected SWAPDB response: {}", String::from_utf8_lossy(&data))))
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SWAPDB response: {:?}", other))),
            },
        }
    }
}

impl Serialize for SwapdbOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SwapdbOutput", 1)?;
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
            let input = SwapdbInput {
                index1: RedisJsonValue::Integer(0),
                index2: RedisJsonValue::Integer(1),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SWAPDB"));
            assert!(cmd_str.contains("0"));
            assert!(cmd_str.contains("1"));
        }

        #[test]
        fn test_decode_ok() {
            let output = SwapdbOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.success());
        }

        #[test]
        fn test_decode_error() {
            let err = SwapdbOutput::decode(b"-ERR invalid DB index\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid DB"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(1)];
            let input = SwapdbInput::decode(args).unwrap();
            assert_eq!(input.index1, RedisJsonValue::Integer(0));
            assert_eq!(input.index2, RedisJsonValue::Integer(1));
        }

        #[test]
        fn test_decode_input_string_indices() {
            let args = vec![RedisJsonValue::String("0".into()), RedisJsonValue::String("1".into())];
            let input = SwapdbInput::decode(args).unwrap();
            assert_eq!(input.index1, RedisJsonValue::String("0".into()));
            assert_eq!(input.index2, RedisJsonValue::String("1".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::Integer(0)];
            let err = SwapdbInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![RedisJsonValue::Integer(0), RedisJsonValue::Integer(1), RedisJsonValue::Integer(2)];
            let err = SwapdbInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 2 arguments"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = SwapdbInput {
                index1: RedisJsonValue::Integer(0),
                index2: RedisJsonValue::Integer(1),
            };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = SwapdbInput {
                index1: RedisJsonValue::Integer(0),
                index2: RedisJsonValue::Integer(1),
            };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::Swapdb);
        }

        #[test]
        fn test_serialize_output() {
            let output = SwapdbOutput::new(true);
            let json = serde_json::to_string(&output).unwrap();
            assert!(json.contains("\"success\":true"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        // SWAPDB requires Redis 4.0+
        const MIN_VERSION: &str = "4.0";

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_swapdb_basic() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    let mut conn = ctx.pinned_connection().await.expect("pinned connection failed");

                    // Set a key in DB 0
                    TestContext::raw_on_pinned(&mut conn, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n").await.expect("SELECT 0 failed");

                    TestContext::raw_on_pinned(&mut conn, b"*3\r\n$3\r\nSET\r\n$8\r\nswaptest\r\n$8\r\ndb0value\r\n")
                        .await
                        .expect("SET in DB 0 failed");

                    // Set a different key in DB 1
                    TestContext::raw_on_pinned(&mut conn, b"*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n").await.expect("SELECT 1 failed");

                    TestContext::raw_on_pinned(&mut conn, b"*3\r\n$3\r\nSET\r\n$8\r\nswaptest\r\n$8\r\ndb1value\r\n")
                        .await
                        .expect("SET in DB 1 failed");

                    // Switch back to DB 0 and swap
                    TestContext::raw_on_pinned(&mut conn, b"*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n").await.expect("SELECT 0 failed");

                    let result = TestContext::raw_on_pinned(
                        &mut conn,
                        &SwapdbInput {
                            index1: RedisJsonValue::Integer(0),
                            index2: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    let output = SwapdbOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SWAPDB should succeed");

                    // Now DB 0 should have DB 1's value
                    let get_result =
                        TestContext::raw_on_pinned(&mut conn, b"*2\r\n$3\r\nGET\r\n$8\r\nswaptest\r\n").await.expect("GET failed");

                    assert!(
                        get_result.windows(8).any(|w| w == b"db1value"),
                        "after swap, DB 0 should have DB 1's value, got: {:?}",
                        String::from_utf8_lossy(&get_result)
                    );

                    // Clean up - swap back
                    TestContext::raw_on_pinned(
                        &mut conn,
                        &SwapdbInput {
                            index1: RedisJsonValue::Integer(0),
                            index2: RedisJsonValue::Integer(1),
                        }
                        .command(),
                    )
                    .await
                    .expect("swap back failed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_swapdb_same_index() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Swapping same DB should succeed (no-op)
                    let result = ctx
                        .raw(
                            &SwapdbInput {
                                index1: RedisJsonValue::Integer(0),
                                index2: RedisJsonValue::Integer(0),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SwapdbOutput::decode(&result).expect("decode failed");
                    assert!(output.success(), "SWAPDB same index should succeed");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_swapdb_invalid_index() {
            test_all_protocols_min_version(MIN_VERSION, |ctx| {
                Box::pin(async move {
                    // Try to swap with invalid DB index (usually max is 15)
                    let result = ctx
                        .raw(
                            &SwapdbInput {
                                index1: RedisJsonValue::Integer(0),
                                index2: RedisJsonValue::Integer(9999),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let err = SwapdbOutput::decode(&result);
                    assert!(err.is_err(), "SWAPDB with invalid index should fail");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_swapdb_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result = ctx
                .raw(
                    &SwapdbInput {
                        index1: RedisJsonValue::Integer(0),
                        index2: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP2 should return simple string OK");
            let output = SwapdbOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_swapdb_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result = ctx
                .raw(
                    &SwapdbInput {
                        index1: RedisJsonValue::Integer(0),
                        index2: RedisJsonValue::Integer(1),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert_eq!(&result[..], b"+OK\r\n", "RESP3 should return simple string OK");
            let output = SwapdbOutput::decode(&result).expect("decode failed");
            assert!(output.success());

            ctx.stop().await;
        }
    }
}
