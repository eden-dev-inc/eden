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

const API_INFO: ApiInfo<RedisApi, BfLoadchunkInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::BfLoadchunk,
    "Restores a filter previously saved using SCANDUMP",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `BF.LOADCHUNK`
/// https://redis.io/docs/latest/commands/bf.loadchunk/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct BfLoadchunkInput {
    pub(crate) key: RedisKey,
    pub(crate) iterator: RedisJsonValue,
    pub(crate) data: RedisJsonValue,
}

impl Serialize for BfLoadchunkInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("BfLoadchunkInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("iterator", &self.iterator)?;
        state.serialize_field("data", &self.data)?;
        state.end()
    }
}

impl_redis_operation!(BfLoadchunkInput, API_INFO, { key, iterator, data });

impl RedisCommandInput for BfLoadchunkInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key).arg(&self.iterator).arg(&self.data);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!("BF.LOADCHUNK requires 3 arguments, given {}", args.len())));
        }

        if args.len() > 3 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "BF.LOADCHUNK expects 3 arguments, given {}",
                audience = LogAudience::Client,
                args_given = args.len()
            );
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            iterator: args[1].clone(),
            data: args[2].clone(),
        })
    }
}

/// Output for Redis BF.LOADCHUNK command
///
/// Returns OK on success.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct BfLoadchunkOutput {
    status: String,
}

impl BfLoadchunkOutput {
    pub fn new() -> Self {
        Self { status: "OK".to_string() }
    }

    /// Get the status message
    pub fn status(&self) -> &str {
        &self.status
    }

    /// Check if the operation was successful
    pub fn is_ok(&self) -> bool {
        self.status == "OK"
    }

    /// Decode the Redis protocol response into a BfLoadchunkOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let status = String::from_utf8(s).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected BF.LOADCHUNK response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let status = String::from_utf8(data).map_err(EpError::parse)?;
                    Ok(Self { status })
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected BF.LOADCHUNK response: {:?}", other))),
            },
        }
    }
}

impl Default for BfLoadchunkOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for BfLoadchunkOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("BfLoadchunkOutput", 1)?;
        state.serialize_field("status", &self.status)?;
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
            let input = BfLoadchunkInput {
                key: RedisKey::String("myfilter".into()),
                iterator: RedisJsonValue::Integer(1),
                data: RedisJsonValue::String("binarydata".into()),
            };
            let cmd = input.command();
            assert!(cmd.starts_with(b"*4\r\n$12\r\nBF.LOADCHUNK\r\n"));
        }

        #[test]
        fn test_decode_ok_response() {
            let output = BfLoadchunkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert_eq!(output.status(), "OK");
        }

        #[test]
        fn test_decode_error_response() {
            let err = BfLoadchunkOutput::decode(b"-ERR invalid data\r\n").unwrap_err();
            assert!(err.to_string().contains("invalid data"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("filter".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("data".into()),
            ];
            let input = BfLoadchunkInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("filter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("filter".into()), RedisJsonValue::Integer(1)];
            let err = BfLoadchunkInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires 3 arguments"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = BfLoadchunkInput {
                key: RedisKey::String("testkey".into()),
                iterator: RedisJsonValue::Integer(0),
                data: RedisJsonValue::String("data".into()),
            };
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::lib::bloom_filter::bf_add::BfAddInput;
        use crate::api::lib::bloom_filter::bf_exists::BfExistsInput;
        use crate::api::lib::bloom_filter::bf_exists::BfExistsOutput;
        use crate::api::lib::bloom_filter::bf_scandump::BfScandumpInput;
        use crate::api::lib::bloom_filter::bf_scandump::BfScandumpOutput;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_bf_loadchunk_roundtrip() {
            test_all_protocols_with_stack(|ctx| {
                Box::pin(async move {
                    // Create source filter
                    ctx.raw(
                        &BfAddInput {
                            key: RedisKey::String("bf_src".into()),
                            item: RedisJsonValue::String("test_item".into()),
                        }
                        .command(),
                    )
                    .await
                    .expect("raw failed");

                    // Dump all chunks
                    let mut chunks: Vec<(i64, Vec<u8>)> = Vec::new();
                    let mut iterator = 0i64;

                    loop {
                        let result = ctx
                            .raw(
                                &BfScandumpInput {
                                    key: RedisKey::String("bf_src".into()),
                                    iterator: RedisJsonValue::Integer(iterator),
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = BfScandumpOutput::decode(&result).expect("scandump decode");

                        if output.is_complete() {
                            break;
                        }

                        if let Some(data) = output.data() {
                            chunks.push((output.iterator(), data.to_vec()));
                        }
                        iterator = output.iterator();

                        if chunks.len() > 100 {
                            panic!("Too many chunks");
                        }
                    }

                    // Load chunks into new filter
                    for (iter, data) in chunks {
                        let result = ctx
                            .raw(
                                &BfLoadchunkInput {
                                    key: RedisKey::String("bf_dst".into()),
                                    iterator: RedisJsonValue::Integer(iter),
                                    data: RedisJsonValue::Bytes(data),
                                }
                                .command(),
                            )
                            .await
                            .expect("raw failed");

                        let output = BfLoadchunkOutput::decode(&result).expect("loadchunk decode");
                        assert!(output.is_ok());
                    }

                    // Verify item exists in destination
                    let result = ctx
                        .raw(
                            &BfExistsInput {
                                key: RedisKey::String("bf_dst".into()),
                                item: RedisJsonValue::String("test_item".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = BfExistsOutput::decode(&result).expect("exists decode");
                    assert!(output.may_exist());
                })
            })
            .await;
        }
    }
}
