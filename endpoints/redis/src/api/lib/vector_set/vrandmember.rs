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

const API_INFO: ApiInfo<RedisApi, VrandmemberInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Vrandmember,
    "Return one or multiple random members from a vector set",
    ReqType::Read,
    true,
);

/// See official Redis documentation for `VRANDMEMBER`
/// https://redis.io/docs/latest/commands/vrandmember/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct VrandmemberInput {
    key: RedisKey,
    #[serde(skip_serializing_if = "Option::is_none")]
    count: Option<RedisJsonValue>,
}

impl VrandmemberInput {
    pub fn new(key: impl Into<RedisKey>) -> Self {
        Self { key: key.into(), count: None }
    }

    pub fn with_count(mut self, count: impl Into<RedisJsonValue>) -> Self {
        self.count = Some(count.into());
        self
    }
}

impl Serialize for VrandmemberInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 2;
        if self.count.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("VrandmemberInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        if let Some(count) = &self.count {
            state.serialize_field("count", count)?;
        }
        state.end()
    }
}

impl_redis_operation!(VrandmemberInput, API_INFO, { key, count });

impl RedisCommandInput for VrandmemberInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.key);
        // Only add count if present
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
            return Err(EpError::request("VRANDMEMBER requires at least 1 argument"));
        }

        let key = args[0].clone().try_into()?;
        let count = if args.len() > 1 { Some(args[1].clone()) } else { None };

        Ok(VrandmemberInput { key, count })
    }
}

/// Output for Redis VRANDMEMBER command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct VrandmemberOutput {
    /// Single member or array of members
    members: VrandmemberResult,
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum VrandmemberResult {
    Single(Option<String>),
    Multiple(Vec<String>),
}

impl VrandmemberOutput {
    pub fn single(member: Option<String>) -> Self {
        Self { members: VrandmemberResult::Single(member) }
    }

    pub fn multiple(members: Vec<String>) -> Self {
        Self { members: VrandmemberResult::Multiple(members) }
    }

    pub fn members(&self) -> &VrandmemberResult {
        &self.members
    }

    pub fn is_empty(&self) -> bool {
        match &self.members {
            VrandmemberResult::Single(m) => m.is_none(),
            VrandmemberResult::Multiple(m) => m.is_empty(),
        }
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let members = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::BulkString(s) => VrandmemberResult::Single(Some(String::from_utf8(s).map_err(EpError::parse)?)),
                Resp2Frame::Array(arr) => {
                    let mut members = Vec::with_capacity(arr.len());
                    for item in arr {
                        if let Resp2Frame::BulkString(s) = item {
                            members.push(String::from_utf8(s).map_err(EpError::parse)?);
                        }
                    }
                    VrandmemberResult::Multiple(members)
                }
                Resp2Frame::Null => VrandmemberResult::Single(None),
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::BlobString { data, .. } => VrandmemberResult::Single(Some(String::from_utf8(data).map_err(EpError::parse)?)),
                Resp3Frame::Array { data, .. } => {
                    let mut members = Vec::with_capacity(data.len());
                    for item in data {
                        if let Resp3Frame::BlobString { data: s, .. } = item {
                            members.push(String::from_utf8(s).map_err(EpError::parse)?);
                        }
                    }
                    VrandmemberResult::Multiple(members)
                }
                Resp3Frame::Null => VrandmemberResult::Single(None),
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => return Err(EpError::parse(format!("unexpected response: {:?}", other))),
            },
        };

        Ok(Self { members })
    }
}

impl Serialize for VrandmemberOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("VrandmemberOutput", 1)?;
        state.serialize_field("members", &self.members)?;
        state.end()
    }
}

impl Serialize for VrandmemberResult {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match self {
            VrandmemberResult::Single(m) => m.serialize(serializer),
            VrandmemberResult::Multiple(m) => m.serialize(serializer),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_count() {
            let input = VrandmemberInput::new("myvset");
            assert_eq!(input.command().to_vec(), b"*2\r\n$11\r\nVRANDMEMBER\r\n$6\r\nmyvset\r\n");
        }

        #[test]
        fn test_encode_command_with_count() {
            let input = VrandmemberInput::new("myvset").with_count(3);
            assert_eq!(input.command().to_vec(), b"*3\r\n$11\r\nVRANDMEMBER\r\n$6\r\nmyvset\r\n$1\r\n3\r\n");
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = VrandmemberOutput::decode(b"$-1\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = VrandmemberOutput::decode(b"_\r\n").unwrap();
            assert!(output.is_empty());
        }

        #[test]
        fn test_decode_single() {
            let output = VrandmemberOutput::decode(b"$4\r\nelem\r\n").unwrap();
            match output.members() {
                VrandmemberResult::Single(Some(s)) => assert_eq!(s, "elem"),
                _ => panic!("expected single member"),
            }
        }

        #[test]
        fn test_decode_error_fails() {
            let err = VrandmemberOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("mykey".into())];
            let input = VrandmemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert!(input.count.is_none());
        }

        #[test]
        fn test_decode_input_with_count() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(5)];
            let input = VrandmemberInput::decode(args).unwrap();
            assert_eq!(input.key, RedisKey::String("mykey".into()));
            assert_eq!(input.count, Some(RedisJsonValue::Integer(5)));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = VrandmemberInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 1"));
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrandmember_empty() {
            test_all_protocols_min_version("8", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&VrandmemberInput::new("empty_vset").command()).await.expect("raw failed");
                    let output = VrandmemberOutput::decode(&result).expect("decode failed");
                    assert!(output.is_empty());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_vrandmember_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("8")).await;
            let result = ctx.raw(&VrandmemberInput::new("empty_vset").command()).await.expect("raw failed");
            let output = VrandmemberOutput::decode(&result).expect("decode failed");
            assert!(output.is_empty());
            ctx.stop().await;
        }
    }
}
