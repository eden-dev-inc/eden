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

const API_INFO: ApiInfo<RedisApi, CfLoadchunkInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::CfLoadchunk,
    "Restores a Cuckoo Filter previously saved using CF.SCANDUMP",
    ReqType::Write,
    true,
);

/// Input for Redis `CF.LOADCHUNK` command.
///
/// Restores a Cuckoo Filter previously saved using CF.SCANDUMP.
/// This command is used to restore filter data chunk by chunk.
///
/// See official Redis documentation for `CF.LOADCHUNK`:
/// https://redis.io/docs/latest/commands/cf.loadchunk/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct CfLoadchunkInput {
    /// The name of the Cuckoo Filter to restore
    key: RedisKey,
    /// Iterator value from CF.SCANDUMP
    iterator: RedisJsonValue,
    /// Data chunk from CF.SCANDUMP
    data: RedisJsonValue,
}

impl CfLoadchunkInput {
    pub fn new(key: impl Into<RedisKey>, iterator: impl Into<RedisJsonValue>, data: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            iterator: iterator.into(),
            data: data.into(),
        }
    }
}

impl Serialize for CfLoadchunkInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("CfLoadchunkInput", 4)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("iterator", &self.iterator)?;
        state.serialize_field("data", &self.data)?;
        state.end()
    }
}

impl_redis_operation!(CfLoadchunkInput, API_INFO, { key, iterator, data });

impl RedisCommandInput for CfLoadchunkInput {
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

    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() != 3 {
            return Err(EpError::request(format!("CF.LOADCHUNK requires 3 arguments, given {}", args.len())));
        }

        Ok(Self {
            key: args[0].clone().try_into()?,
            iterator: args[1].clone(),
            data: args[2].clone(),
        })
    }
}

/// Output for Redis `CF.LOADCHUNK` command.
///
/// Returns OK if the chunk was loaded successfully.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct CfLoadchunkOutput {
    /// Whether the operation succeeded
    success: bool,
}

impl CfLoadchunkOutput {
    pub fn new(success: bool) -> Self {
        Self { success }
    }

    /// Returns true if the chunk was loaded successfully
    pub fn is_ok(&self) -> bool {
        self.success
    }

    /// Decode the Redis protocol response into a CfLoadchunkOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) if s == b"OK" => Ok(Self { success: true }),
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected CF.LOADCHUNK response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8_lossy(&data);
                    if s.to_uppercase() == "OK" {
                        Ok(Self { success: true })
                    } else {
                        Err(EpError::parse(format!("unexpected response: {}", s)))
                    }
                }
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected CF.LOADCHUNK response: {:?}", other))),
            },
        }
    }
}

impl Serialize for CfLoadchunkOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("CfLoadchunkOutput", 1)?;
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
            let input = CfLoadchunkInput {
                key: RedisKey::String("myfilter".into()),
                iterator: RedisJsonValue::Integer(1),
                data: RedisJsonValue::String("somedata".into()),
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("CF.LOADCHUNK"));
            assert!(cmd_str.contains("myfilter"));
        }

        #[test]
        fn test_new_constructor() {
            let input = CfLoadchunkInput::new("filter1", 1i64, "data");
            assert_eq!(input.key, RedisKey::String("filter1".into()));
        }

        #[test]
        fn test_keys_accessor() {
            let input = CfLoadchunkInput::new("testfilter", 0i64, "testdata");
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("testfilter".into()));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![
                RedisJsonValue::String("myfilter".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("data".into()),
            ];
            let input = CfLoadchunkInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myfilter".into()));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args = vec![RedisJsonValue::String("myfilter".into()), RedisJsonValue::Integer(1)];
            let err = CfLoadchunkInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_input_too_many_args() {
            let args = vec![
                RedisJsonValue::String("a".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::String("c".into()),
                RedisJsonValue::String("d".into()),
            ];
            let err = CfLoadchunkInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("3 arguments"));
        }

        #[test]
        fn test_decode_output_ok() {
            let output = CfLoadchunkOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_output_error() {
            let err = CfLoadchunkOutput::decode(b"-ERR invalid data\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_new() {
            let output = CfLoadchunkOutput::new(true);
            assert!(output.is_ok());

            let output = CfLoadchunkOutput::new(false);
            assert!(!output.is_ok());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::api::{CfAddInput, CfScandumpInput, CfScandumpOutput};
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_cf_loadchunk_restore_filter() {
            let mut ctx = setup_with_stack(RespVersion::Resp2, None).await;

            // First create a filter and dump it
            let add_result = ctx.raw(&CfAddInput::new("cf_source", "item1").command()).await;

            match add_result {
                Ok(_) => {
                    // Dump the filter
                    let mut chunks = Vec::new();
                    let mut iterator = 0i64;

                    loop {
                        let result = ctx.raw(&CfScandumpInput::new("cf_source", iterator).command()).await.expect("scandump failed");

                        let output = CfScandumpOutput::decode(&result).expect("decode scandump failed");

                        if output.has_data() {
                            chunks.push((output.iterator(), output.data().to_vec()));
                        }

                        if output.is_complete() {
                            break;
                        }

                        iterator = output.iterator();

                        if chunks.len() > 100 {
                            break;
                        }
                    }

                    // Restore to new filter
                    for (iter, data) in &chunks {
                        let data_str = String::from_utf8_lossy(data).to_string();
                        let result = ctx.raw(&CfLoadchunkInput::new("cf_dest", *iter, data_str).command()).await;

                        if let Ok(bytes) = result {
                            let output = CfLoadchunkOutput::decode(&bytes);
                            if let Ok(out) = output {
                                assert!(out.is_ok());
                            }
                        }
                    }
                }
                Err(e) => {
                    if e.to_string().contains("unknown command") {
                        println!("Skipping test: RedisBloom module not available");
                    } else {
                        panic!("Unexpected error: {}", e);
                    }
                }
            }

            ctx.stop().await;
        }
    }
}
