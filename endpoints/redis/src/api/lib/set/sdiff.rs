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

const API_INFO: ApiInfo<RedisApi, SdiffInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Sdiff, "Returns the difference of multiple sets", ReqType::Read, true);

/// See official Redis documentation for `SDIFF`
/// https://redis.io/docs/latest/commands/sdiff/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SdiffInput {
    keys: Vec<RedisKey>,
}

impl Serialize for SdiffInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SdiffInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(SdiffInput, API_INFO, { keys });

impl RedisCommandInput for SdiffInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        self.keys.clone()
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.keys);

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SDIFF requires at least 1 key"));
        }

        let mut keys = vec![];
        for key in args.into_iter() {
            keys.push(key.try_into()?)
        }

        Ok(Self { keys })
    }
}

/// Output for Redis SDIFF command
///
/// Returns the difference between the first set and all successive sets.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SdiffOutput {
    members: Vec<String>,
}

impl SdiffOutput {
    pub fn new(members: Vec<String>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => {
                let mut members = Vec::new();
                for frame in arr {
                    let member = match frame {
                        Resp2Frame::BulkString(data) => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected bulk string for member")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    let member = match frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected blob string for member")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Set { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    let member = match frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("Expected blob string for member")),
                    };
                    members.push(member);
                }
                Ok(Self { members })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("SDIFF must return an array or set")),
        }
    }
}

impl Serialize for SdiffOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SdiffOutput", 1)?;
        state.serialize_field("members", &self.members)?;
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
            let input = SdiffInput {
                keys: vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())],
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SDIFF"));
            assert!(cmd_str.contains("set1"));
            assert!(cmd_str.contains("set2"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("set1".into()), RedisJsonValue::String("set2".into())];
            let input = SdiffInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SdiffInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = SdiffInput {
                keys: vec![
                    RedisKey::String("a".into()),
                    RedisKey::String("b".into()),
                    RedisKey::String("c".into()),
                ],
            };
            assert_eq!(input.keys().len(), 3);
        }

        #[test]
        fn test_decode_output_basic() {
            // *2\r\n$1\r\na\r\n$1\r\nc\r\n
            let output = SdiffOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nc\r\n").unwrap();
            assert_eq!(output.len(), 2);
            assert!(output.members().contains(&"a".to_string()));
            assert!(output.members().contains(&"c".to_string()));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = SdiffOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_error() {
            let err = SdiffOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sdiff_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nsdiff_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$10\r\nsdiff_set2\r\n").await.expect("raw failed");

                    // SADD sdiff_set1 a b c
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$10\r\nsdiff_set1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    // SADD sdiff_set2 b d
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$10\r\nsdiff_set2\r\n$1\r\nb\r\n$1\r\nd\r\n").await.expect("raw failed");

                    // SDIFF sdiff_set1 sdiff_set2
                    let result = ctx
                        .raw(
                            &SdiffInput {
                                keys: vec![RedisKey::String("sdiff_set1".into()), RedisKey::String("sdiff_set2".into())],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SdiffOutput::decode(&result).expect("decode failed");
                    // set1 - set2 = {a, c} (b is in both)
                    assert_eq!(output.len(), 2);
                    let members = output.members();
                    assert!(members.contains(&"a".to_string()));
                    assert!(members.contains(&"c".to_string()));
                    assert!(!members.contains(&"b".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sdiff_empty_result() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsdiff_empty_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsdiff_empty_set2\r\n").await.expect("raw failed");

                    // set1 is subset of set2
                    // SADD sdiff_empty_set1 a
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsdiff_empty_set1\r\n$1\r\na\r\n").await.expect("raw failed");

                    // SADD sdiff_empty_set2 a b
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsdiff_empty_set2\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("raw failed");

                    // SDIFF sdiff_empty_set1 sdiff_empty_set2
                    let result = ctx
                        .raw(
                            &SdiffInput {
                                keys: vec![
                                    RedisKey::String("sdiff_empty_set1".into()),
                                    RedisKey::String("sdiff_empty_set2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SdiffOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sdiff_nonexistent_keys() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsdiff_noexist_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsdiff_noexist_set2\r\n").await.expect("raw failed");

                    // SDIFF on nonexistent keys
                    let result = ctx
                        .raw(
                            &SdiffInput {
                                keys: vec![
                                    RedisKey::String("sdiff_noexist_set1".into()),
                                    RedisKey::String("sdiff_noexist_set2".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SdiffOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sdiff_multiple_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsdiff_multi_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsdiff_multi_set2\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsdiff_multi_set3\r\n").await.expect("raw failed");

                    // SADD sdiff_multi_set1 a b c d
                    ctx.raw(b"*6\r\n$4\r\nSADD\r\n$16\r\nsdiff_multi_set1\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n$1\r\nd\r\n")
                        .await
                        .expect("raw failed");

                    // SADD sdiff_multi_set2 b
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsdiff_multi_set2\r\n$1\r\nb\r\n").await.expect("raw failed");

                    // SADD sdiff_multi_set3 c
                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$16\r\nsdiff_multi_set3\r\n$1\r\nc\r\n").await.expect("raw failed");

                    // SDIFF sdiff_multi_set1 sdiff_multi_set2 sdiff_multi_set3
                    let result = ctx
                        .raw(
                            &SdiffInput {
                                keys: vec![
                                    RedisKey::String("sdiff_multi_set1".into()),
                                    RedisKey::String("sdiff_multi_set2".into()),
                                    RedisKey::String("sdiff_multi_set3".into()),
                                ],
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SdiffOutput::decode(&result).expect("decode failed");
                    // set1 - set2 - set3 = {a, d} (b and c are removed)
                    assert_eq!(output.len(), 2);
                    let members = output.members();
                    assert!(members.contains(&"a".to_string()));
                    assert!(members.contains(&"d".to_string()));
                    assert!(!members.contains(&"b".to_string()));
                    assert!(!members.contains(&"c".to_string()));
                })
            })
            .await;
        }
    }
}
