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

const API_INFO: ApiInfo<RedisApi, SrandmemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Srandmember,
    "Get one or multiple random members from a set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `SRANDMEMBER`
/// https://redis.io/docs/latest/commands/srandmember/
#[derive(Debug, Default, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct SrandmemberInput {
    key: RedisKey,
    count: Option<RedisJsonValue>,
}

impl SrandmemberInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into(), count: None }
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for SrandmemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("SrandmemberInput", 3)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("count", &self.count)?;
        state.end()
    }
}

impl_redis_operation!(
    SrandmemberInput,
    API_INFO,
    { key, count }
);

impl RedisCommandInput for SrandmemberInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key);

        if let Some(count) = &self.count {
            command.arg(count);
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.is_empty() {
            return Err(EpError::request(format!("SRANDMEMBER requires at least 1 argument, given {}", args.len())));
        }

        let key = args[0].clone().try_into()?;
        let count = if args.len() >= 2 { Some(args[1].clone()) } else { None };

        Ok(Self { key, count })
    }
}

/// Output for Redis SRANDMEMBER command (single member, no count)
///
/// Returns a single random member, or None if the set is empty.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SrandmemberOutput {
    member: Option<RedisJsonValue>,
}

impl SrandmemberOutput {
    pub fn new(member: Option<RedisJsonValue>) -> Self {
        Self { member }
    }

    pub fn member(&self) -> Option<&RedisJsonValue> {
        self.member.as_ref()
    }

    pub fn exists(&self) -> bool {
        self.member.is_some()
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let member = match frame {
            DecoderRespFrame::Resp2(Resp2Frame::Null) => None,
            DecoderRespFrame::Resp2(Resp2Frame::BulkString(b)) => {
                Some(RedisJsonValue::String(String::from_utf8(b).map_err(EpError::parse)?))
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => {
                return Err(EpError::parse(e));
            }
            DecoderRespFrame::Resp3(Resp3Frame::Null) => None,
            DecoderRespFrame::Resp3(Resp3Frame::BlobString { data, .. }) => {
                Some(RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?))
            }
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => {
                return Err(EpError::parse(data));
            }
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                return Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?));
            }
            _ => return Err(EpError::parse("unexpected response format")),
        };

        Ok(Self { member })
    }
}

impl Serialize for SrandmemberOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SrandmemberOutput", 1)?;
        state.serialize_field("member", &self.member)?;
        state.end()
    }
}

/// Output for Redis SRANDMEMBER command with COUNT (multiple members)
///
/// Returns an array of random members.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct SrandmemberArrayOutput {
    members: Vec<RedisJsonValue>,
}

impl SrandmemberArrayOutput {
    pub fn new(members: Vec<RedisJsonValue>) -> Self {
        Self { members }
    }

    pub fn members(&self) -> &Vec<RedisJsonValue> {
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
                let members = arr.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { members })
            }
            DecoderRespFrame::Resp3(Resp3Frame::Array { data, .. }) => {
                let members = data.into_iter().map(RedisJsonValue::try_from).collect::<Result<Vec<_>, _>>()?;
                Ok(Self { members })
            }
            DecoderRespFrame::Resp2(Resp2Frame::Error(e)) => Err(EpError::parse(e)),
            DecoderRespFrame::Resp3(Resp3Frame::SimpleError { data, .. }) => Err(EpError::parse(data)),
            DecoderRespFrame::Resp3(Resp3Frame::BlobError { data, .. }) => {
                Err(EpError::parse(String::from_utf8(data).map_err(EpError::parse)?))
            }
            _ => Err(EpError::parse("SRANDMEMBER with count must return array")),
        }
    }
}

impl Serialize for SrandmemberArrayOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("SrandmemberArrayOutput", 1)?;
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
        fn test_encode_command_no_count() {
            let input = SrandmemberInput::new(RedisKey::String("myset".into()));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*2\r\n$11\r\nSRANDMEMBER\r\n$5\r\nmyset\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = SrandmemberInput::new(RedisKey::String("myset".into())).with_count(RedisJsonValue::Integer(3));
            let cmd = input.command();
            assert_eq!(cmd.to_vec(), b"*3\r\n$11\r\nSRANDMEMBER\r\n$5\r\nmyset\r\n$1\r\n3\r\n");
        }

        #[test]
        fn test_decode_output_single() {
            let output = SrandmemberOutput::decode(b"$6\r\nmember\r\n").unwrap();
            assert!(output.exists());
            assert!(output.member().is_some());
        }

        #[test]
        fn test_decode_output_null() {
            let output = SrandmemberOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_output_array() {
            let output = SrandmemberArrayOutput::decode(b"*2\r\n$1\r\na\r\n$1\r\nb\r\n").unwrap();
            assert_eq!(output.len(), 2);
        }

        #[test]
        fn test_decode_output_array_empty() {
            let output = SrandmemberArrayOutput::decode(b"*0\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_error() {
            let err = SrandmemberOutput::decode(b"-WRONGTYPE Operation against a key holding the wrong kind of value\r\n").unwrap_err();
            assert!(err.to_string().contains("WRONGTYPE"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("myset".into())];
            let input = SrandmemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("myset".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("myset".into()), RedisJsonValue::Integer(5)];
            let input = SrandmemberInput::decode(args).unwrap();
            assert!(input.count.is_some());
            assert_eq!(input.count.as_ref().unwrap(), &RedisJsonValue::Integer(5));
        }

        #[test]
        fn test_decode_input_too_few_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = SrandmemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = SrandmemberInput::new(RedisKey::String("myset".into()));
            assert_eq!(input.keys().len(), 1);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_single() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$18\r\nsrandmember_single\r\n").await.expect("raw failed");

                    ctx.raw(b"*4\r\n$4\r\nSADD\r\n$18\r\nsrandmember_single\r\n$3\r\none\r\n$3\r\ntwo\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SrandmemberInput::new(RedisKey::String("srandmember_single".into())).command()).await.expect("raw failed");

                    let output = SrandmemberOutput::decode(&result).expect("decode failed");
                    assert!(output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_with_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsrandmember_count\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$17\r\nsrandmember_count\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    let result = ctx
                        .raw(
                            &SrandmemberInput::new(RedisKey::String("srandmember_count".into()))
                                .with_count(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SrandmemberArrayOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_empty_set() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$17\r\nsrandmember_empty\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SrandmemberInput::new(RedisKey::String("srandmember_empty".into())).command()).await.expect("raw failed");

                    let output = SrandmemberOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_negative_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsrandmember_neg\r\n").await.expect("raw failed");

                    ctx.raw(b"*3\r\n$4\r\nSADD\r\n$15\r\nsrandmember_neg\r\n$3\r\none\r\n").await.expect("raw failed");

                    // Negative count allows duplicates
                    let result = ctx
                        .raw(
                            &SrandmemberInput::new(RedisKey::String("srandmember_neg".into()))
                                .with_count(RedisJsonValue::Integer(-5))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SrandmemberArrayOutput::decode(&result).expect("decode failed");
                    // With negative count, can return more elements than set size (with duplicates)
                    assert_eq!(output.len(), 5);
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_wrongtype() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*3\r\n$3\r\nSET\r\n$17\r\nsrandmember_wrong\r\n$5\r\nvalue\r\n").await.expect("raw failed");

                    let result =
                        ctx.raw(&SrandmemberInput::new(RedisKey::String("srandmember_wrong".into())).command()).await.expect("raw failed");

                    let err = SrandmemberOutput::decode(&result).unwrap_err();
                    assert!(err.to_string().contains("WRONGTYPE"));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_srandmember_positive_count() {
            test_all_protocols(|ctx| {
                Box::pin(async move {
                    ctx.raw(b"*2\r\n$3\r\nDEL\r\n$15\r\nsrandmember_pos\r\n").await.expect("raw failed");

                    ctx.raw(b"*5\r\n$4\r\nSADD\r\n$15\r\nsrandmember_pos\r\n$3\r\none\r\n$3\r\ntwo\r\n$5\r\nthree\r\n")
                        .await
                        .expect("raw failed");

                    // Positive count returns unique elements
                    let result = ctx
                        .raw(
                            &SrandmemberInput::new(RedisKey::String("srandmember_pos".into()))
                                .with_count(RedisJsonValue::Integer(2))
                                .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = SrandmemberArrayOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.len(), 2);

                    // Verify uniqueness
                    let members = output.members();
                    assert_ne!(members[0], members[1]);
                })
            })
            .await;
        }
    }
}
