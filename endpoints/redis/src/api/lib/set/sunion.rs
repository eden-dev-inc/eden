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

const API_INFO: ApiInfo<RedisApi, SunionInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Sunion, "Returns the union of multiple sets", ReqType::Read, true);

/// See official Redis documentation for `SUNION`
/// https://redis.io/docs/latest/commands/sunion/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SunionInput {
    keys: Vec<RedisKey>,
}

impl SunionInput {
    pub fn new(keys: Vec<impl Into<RedisKey>>) -> Self {
        let keys: Vec<RedisKey> = keys.into_iter().map(|k| k.into()).collect();
        Self { keys }
    }
}

impl Serialize for SunionInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SunionInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("keys", &self.keys)?;
        state.end()
    }
}

impl_redis_operation!(SunionInput, API_INFO, { keys });

impl RedisCommandInput for SunionInput {
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
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request("SUNION requires at least 1 key"));
        }

        let mut keys = vec![];
        for key in args.into_iter() {
            keys.push(key.try_into()?);
        }

        Ok(Self { keys })
    }
}

/// Output for Redis SUNION command
///
/// Returns the union of multiple sets as an array of members.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SunionOutput {
    members: Vec<String>,
}

impl SunionOutput {
    pub fn new(members: Vec<String>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &[String] {
        &self.members
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let members = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Array(arr)) => Self::parse_resp2_array(&arr)?,
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => Self::parse_resp3_array(&data)?,
            DecoderRespFrame::Resp3(Resp3Frame::Set { data, .. }) => {
                let mut members = Vec::new();
                for frame in data {
                    let member = match frame {
                        Resp3Frame::BlobString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                        _ => return Err(EpError::parse("expected string member")),
                    };
                    members.push(member);
                }
                members
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("expected array or set response")),
        };

        Ok(Self { members })
    }

    fn parse_resp2_array(arr: &[Resp2Frame]) -> Result<Vec<String>, EpError> {
        let mut members = Vec::new();
        for frame in arr {
            let member = match frame {
                Resp2Frame::BulkString(b) => String::from_utf8(b.to_vec()).map_err(EpError::parse)?,
                _ => return Err(EpError::parse("expected string member")),
            };
            members.push(member);
        }
        Ok(members)
    }

    fn parse_resp3_array(arr: &[Resp3Frame]) -> Result<Vec<String>, EpError> {
        let mut members = Vec::new();
        for frame in arr {
            let member = match frame {
                Resp3Frame::BlobString { data, .. } => String::from_utf8(data.to_vec()).map_err(EpError::parse)?,
                _ => return Err(EpError::parse("expected string member")),
            };
            members.push(member);
        }
        Ok(members)
    }
}

impl Serialize for SunionOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SunionOutput", 1)?;
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
            let input = SunionInput::new(vec![RedisKey::String("set1".into()), RedisKey::String("set2".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SUNION"));
        }

        #[test]
        fn test_encode_command_single_key() {
            let input = SunionInput::new(vec![RedisKey::String("set1".into())]);
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SUNION"));
        }

        #[test]
        fn test_decode_output_basic() {
            // *3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n
            let output = SunionOutput::decode(b"*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").unwrap();
            assert_eq!(output.len(), 3);
            let members = output.members();
            assert!(members.contains(&"a".to_string()));
            assert!(members.contains(&"b".to_string()));
            assert!(members.contains(&"c".to_string()));
        }

        #[test]
        fn test_decode_output_empty() {
            let output = SunionOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
            assert_eq!(output.len(), 0);
        }

        #[test]
        fn test_decode_output_single_member() {
            // *1\r\n$1\r\nx\r\n
            let output = SunionOutput::decode(b"*1\r\n$1\r\nx\r\n").unwrap();
            assert_eq!(output.len(), 1);
            assert_eq!(output.members()[0], "x");
        }

        #[test]
        fn test_decode_error() {
            let err = SunionOutput::decode(b"-WRONGTYPE Operation\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("set1".into()), RedisJsonValue::String("set2".into())];
            let input = SunionInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 2);
        }

        #[test]
        fn test_decode_input_single_key() {
            let args = vec![RedisJsonValue::String("set1".into())];
            let input = SunionInput::decode(args).unwrap();
            assert_eq!(input.keys.len(), 1);
        }

        #[test]
        fn test_decode_input_empty_args() {
            let args = vec![];
            let err = SunionInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("SUNION requires at least 1 key"));
        }

        #[test]
        fn test_keys_returns_all_keys() {
            let input = SunionInput::new(vec![
                RedisKey::String("a".into()),
                RedisKey::String("b".into()),
                RedisKey::String("c".into()),
            ]);
            assert_eq!(input.keys().len(), 3);
        }

        #[test]
        fn test_sunion_output_new() {
            let members = vec!["a".to_string(), "b".to_string()];
            let output = SunionOutput::new(members.clone());
            assert_eq!(output.members(), &members);
            assert_eq!(output.len(), 2);
            assert!(!output.is_empty());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunion_basic() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsunion_set1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$11\r\nsunion_set2\r\n").await.expect("raw failed");

                    // SADD sunion_set1 a b
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$11\r\nsunion_set1\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("raw failed");

                    // SADD sunion_set2 b c
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$11\r\nsunion_set2\r\n$1\r\nb\r\n$1\r\nc\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionInput::new(vec![RedisKey::String("sunion_set1".into()), RedisKey::String("sunion_set2".into())])
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3); // a, b, c
                    let members = output.members();
                    assert!(members.contains(&"a".to_string()));
                    assert!(members.contains(&"b".to_string()));
                    assert!(members.contains(&"c".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunion_single_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$13\r\nsunion_single\r\n").await.expect("raw failed");

                    // SADD sunion_single x y z
                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$13\r\nsunion_single\r\n$1\r\nx\r\n$1\r\ny\r\n$1\r\nz\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SunionInput::new(vec![RedisKey::String("sunion_single".into())]).command()).await.expect("raw failed");

                    let output = SunionOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 3);
                    let members = output.members();
                    assert!(members.contains(&"x".to_string()));
                    assert!(members.contains(&"y".to_string()));
                    assert!(members.contains(&"z".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunion_empty_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsunion_empty_s1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsunion_empty_s2\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionInput::new(vec![
                                RedisKey::String("sunion_empty_s1".into()),
                                RedisKey::String("sunion_empty_s2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunion_multiple_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    // Clean up
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nsunion_set_a\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nsunion_set_b\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$12\r\nsunion_set_c\r\n").await.expect("raw failed");

                    // SADD sunion_set_a 1 2
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$12\r\nsunion_set_a\r\n$1\r\n1\r\n$1\r\n2\r\n").await.expect("raw failed");

                    // SADD sunion_set_b 2 3
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$12\r\nsunion_set_b\r\n$1\r\n2\r\n$1\r\n3\r\n").await.expect("raw failed");

                    // SADD sunion_set_c 3 4
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$12\r\nsunion_set_c\r\n$1\r\n3\r\n$1\r\n4\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionInput::new(vec![
                                RedisKey::String("sunion_set_a".into()),
                                RedisKey::String("sunion_set_b".into()),
                                RedisKey::String("sunion_set_c".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 4); // 1, 2, 3, 4
                    let members = output.members();
                    assert!(members.contains(&"1".to_string()));
                    assert!(members.contains(&"2".to_string()));
                    assert!(members.contains(&"3".to_string()));
                    assert!(members.contains(&"4".to_string()));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_sunion_disjoint_sets() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsunion_disjoint1\r\n").await.expect("raw failed");
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$16\r\nsunion_disjoint2\r\n").await.expect("raw failed");

                    // SADD sunion_disjoint1 a b
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsunion_disjoint1\r\n$1\r\na\r\n$1\r\nb\r\n").await.expect("raw failed");

                    // SADD sunion_disjoint2 c d
                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$16\r\nsunion_disjoint2\r\n$1\r\nc\r\n$1\r\nd\r\n").await.expect("raw failed");

                    let result = ctx
                        .raw(
                            &SunionInput::new(vec![
                                RedisKey::String("sunion_disjoint1".into()),
                                RedisKey::String("sunion_disjoint2".into()),
                            ])
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SunionOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 4); // a, b, c, d
                    let members = output.members();
                    assert!(members.contains(&"a".to_string()));
                    assert!(members.contains(&"b".to_string()));
                    assert!(members.contains(&"c".to_string()));
                    assert!(members.contains(&"d".to_string()));
                })
            })
            .await;
        }
    }
}
