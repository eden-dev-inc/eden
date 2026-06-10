use crate::api::key::RedisKey;
use crate::api::{
    lib::{RedisApi, RedisCommandInput},
    value::RedisJsonValue,
};
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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, AclGetuserInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::AclGetuser, "Lists the ACL rules of a user", ReqType::Read, true);

/// See official Redis documentation for `ACL GETUSER`
/// https://redis.io/docs/latest/commands/acl-getuser/
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub(crate) struct AclGetuserInput {
    pub(crate) username: RedisJsonValue,
}

impl Serialize for AclGetuserInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("AclGetuserInput", 2)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("username", &self.username)?;
        state.end()
    }
}

impl_redis_operation!(AclGetuserInput, API_INFO, { username });

impl RedisCommandInput for AclGetuserInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        Vec::new()
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());
        command.arg(&self.username);
        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() > 1 {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_warn!(
                _ctx,
                "ACL GETUSER expected 1 argument, given {}",
                audience = LogAudience::Internal,
                args_given = args.len()
            );
        }

        match args.first() {
            Some(username) => Ok(Self { username: username.clone() }),
            None => Err(EpError::parse("ACL GETUSER requires a username")),
        }
    }
}

/// Output for Redis ACL GETUSER command
///
/// Returns the ACL configuration for a user as a map of properties.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub(crate) struct AclGetuserOutput {
    /// Whether the user exists
    exists: bool,
    /// User flags (e.g., "on", "off")
    flags: Vec<String>,
    /// Password hashes
    passwords: Vec<String>,
    /// Allowed commands
    commands: String,
    /// Allowed keys patterns
    keys: Vec<String>,
    /// Allowed channels patterns
    channels: Vec<String>,
    /// Selectors (Redis 7.0+)
    selectors: Vec<String>,
    /// Raw properties map
    properties: HashMap<String, RedisJsonValue>,
}

impl AclGetuserOutput {
    pub fn new_not_found() -> Self {
        Self {
            exists: false,
            flags: Vec::new(),
            passwords: Vec::new(),
            commands: String::new(),
            keys: Vec::new(),
            channels: Vec::new(),
            selectors: Vec::new(),
            properties: HashMap::new(),
        }
    }

    /// Check if the user exists
    pub fn exists(&self) -> bool {
        self.exists
    }

    /// Get the user flags
    pub fn flags(&self) -> &[String] {
        &self.flags
    }

    /// Get the password hashes
    pub fn passwords(&self) -> &[String] {
        &self.passwords
    }

    /// Get the commands string
    pub fn commands(&self) -> &str {
        &self.commands
    }

    /// Get the key patterns
    pub fn keys_patterns(&self) -> &[String] {
        &self.keys
    }

    /// Get the channel patterns
    pub fn channels(&self) -> &[String] {
        &self.channels
    }

    /// Get the raw properties
    pub fn properties(&self) -> &HashMap<String, RedisJsonValue> {
        &self.properties
    }

    /// Decode the Redis protocol response into an AclGetuserOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => Self::decode_resp2(resp2_frame),
            DecoderRespFrame::Resp3(resp3_frame) => Self::decode_resp3(resp3_frame),
        }
    }

    fn decode_resp2(frame: Resp2Frame) -> Result<Self, EpError> {
        match frame {
            Resp2Frame::Null => Ok(Self::new_not_found()),
            Resp2Frame::Array(arr) => {
                let mut properties = HashMap::new();
                let mut flags = Vec::new();
                let mut passwords = Vec::new();
                let mut commands = String::new();
                let mut keys = Vec::new();
                let mut channels = Vec::new();
                let mut selectors = Vec::new();

                let mut iter = arr.into_iter();
                while let Some(key_frame) = iter.next() {
                    let key = match key_frame {
                        Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => {
                            String::from_utf8(bytes).map_err(EpError::parse)?
                        }
                        _ => continue,
                    };

                    if let Some(value_frame) = iter.next() {
                        match key.as_str() {
                            "flags" => {
                                if let Resp2Frame::Array(arr) = &value_frame {
                                    for item in arr {
                                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = item {
                                            flags.push(String::from_utf8(bytes.clone()).map_err(EpError::parse)?);
                                        }
                                    }
                                }
                            }
                            "passwords" => {
                                if let Resp2Frame::Array(arr) = &value_frame {
                                    for item in arr {
                                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = item {
                                            passwords.push(String::from_utf8(bytes.clone()).map_err(EpError::parse)?);
                                        }
                                    }
                                }
                            }
                            "commands" => {
                                if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = &value_frame {
                                    commands = String::from_utf8(bytes.clone()).map_err(EpError::parse)?;
                                }
                            }
                            "keys" => {
                                if let Resp2Frame::Array(arr) = &value_frame {
                                    for item in arr {
                                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = item {
                                            keys.push(String::from_utf8(bytes.clone()).map_err(EpError::parse)?);
                                        }
                                    }
                                }
                            }
                            "channels" => {
                                if let Resp2Frame::Array(arr) = &value_frame {
                                    for item in arr {
                                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = item {
                                            channels.push(String::from_utf8(bytes.clone()).map_err(EpError::parse)?);
                                        }
                                    }
                                }
                            }
                            "selectors" => {
                                if let Resp2Frame::Array(arr) = &value_frame {
                                    for item in arr {
                                        if let Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) = item {
                                            selectors.push(String::from_utf8(bytes.clone()).map_err(EpError::parse)?);
                                        }
                                    }
                                }
                            }
                            _ => {}
                        }
                        properties.insert(key, Self::frame_to_json_value_resp2(value_frame)?);
                    }
                }

                Ok(Self {
                    exists: true,
                    flags,
                    passwords,
                    commands,
                    keys,
                    channels,
                    selectors,
                    properties,
                })
            }
            Resp2Frame::Error(e) => Err(EpError::parse(e)),
            other => Err(EpError::parse(format!("unexpected ACL GETUSER response: {:?}", other))),
        }
    }

    fn decode_resp3(frame: Resp3Frame) -> Result<Self, EpError> {
        match frame {
            Resp3Frame::Null => Ok(Self::new_not_found()),
            Resp3Frame::Map { data, .. } => {
                let mut properties = HashMap::new();
                let mut flags = Vec::new();
                let mut passwords = Vec::new();
                let mut commands = String::new();
                let mut keys = Vec::new();
                let mut channels = Vec::new();
                let mut selectors = Vec::new();

                for (key_frame, value_frame) in data {
                    let key = match key_frame {
                        Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                            String::from_utf8(data).map_err(EpError::parse)?
                        }
                        _ => continue,
                    };

                    match key.as_str() {
                        "flags" => {
                            if let Resp3Frame::Array { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        flags.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            } else if let Resp3Frame::Set { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        flags.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            }
                        }
                        "passwords" => {
                            if let Resp3Frame::Array { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        passwords.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            } else if let Resp3Frame::Set { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        passwords.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            }
                        }
                        "commands" => {
                            if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = &value_frame {
                                commands = String::from_utf8(data.clone()).map_err(EpError::parse)?;
                            }
                        }
                        "keys" => {
                            if let Resp3Frame::Array { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        keys.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            } else if let Resp3Frame::Set { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        keys.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            }
                        }
                        "channels" => {
                            if let Resp3Frame::Array { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        channels.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            } else if let Resp3Frame::Set { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        channels.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            }
                        }
                        "selectors" => {
                            if let Resp3Frame::Array { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        selectors.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            } else if let Resp3Frame::Set { data: arr, .. } = &value_frame {
                                for item in arr {
                                    if let Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } = item {
                                        selectors.push(String::from_utf8(data.clone()).map_err(EpError::parse)?);
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                    properties.insert(key, Self::frame_to_json_value_resp3(value_frame)?);
                }

                Ok(Self {
                    exists: true,
                    flags,
                    passwords,
                    commands,
                    keys,
                    channels,
                    selectors,
                    properties,
                })
            }
            Resp3Frame::Array { data, .. } => {
                Self::decode_resp2(Resp2Frame::Array(data.into_iter().map(Self::resp3_to_resp2_frame).collect()))
            }
            Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
            Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
            other => Err(EpError::parse(format!("unexpected ACL GETUSER response: {:?}", other))),
        }
    }

    fn resp3_to_resp2_frame(frame: Resp3Frame) -> Resp2Frame {
        match frame {
            Resp3Frame::BlobString { data, .. } => Resp2Frame::BulkString(data),
            Resp3Frame::SimpleString { data, .. } => Resp2Frame::SimpleString(data),
            Resp3Frame::Number { data, .. } => Resp2Frame::Integer(data),
            Resp3Frame::Null => Resp2Frame::Null,
            Resp3Frame::Array { data, .. } => Resp2Frame::Array(data.into_iter().map(Self::resp3_to_resp2_frame).collect()),
            _ => Resp2Frame::Null,
        }
    }

    fn frame_to_json_value_resp2(frame: Resp2Frame) -> Result<RedisJsonValue, EpError> {
        match frame {
            Resp2Frame::BulkString(bytes) | Resp2Frame::SimpleString(bytes) => {
                Ok(RedisJsonValue::String(String::from_utf8(bytes).map_err(EpError::parse)?))
            }
            Resp2Frame::Integer(n) => Ok(RedisJsonValue::Integer(n)),
            Resp2Frame::Array(arr) => {
                let items: Result<Vec<_>, _> = arr.into_iter().map(Self::frame_to_json_value_resp2).collect();
                Ok(RedisJsonValue::Array(items?))
            }
            Resp2Frame::Null => Ok(RedisJsonValue::Null),
            _ => Ok(RedisJsonValue::Null),
        }
    }

    fn frame_to_json_value_resp3(frame: Resp3Frame) -> Result<RedisJsonValue, EpError> {
        Ok(match frame {
            Resp3Frame::BlobString { data, .. } | Resp3Frame::SimpleString { data, .. } => {
                RedisJsonValue::String(String::from_utf8(data).map_err(EpError::parse)?)
            }
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(data),
            Resp3Frame::Array { data, .. } => {
                let items: Result<Vec<_>, _> = data.into_iter().map(Self::frame_to_json_value_resp3).collect();
                RedisJsonValue::Array(items?)
            }
            Resp3Frame::Set { data, .. } => {
                let items: Result<Vec<_>, _> = data.into_iter().map(Self::frame_to_json_value_resp3).collect();
                RedisJsonValue::Array(items?)
            }
            Resp3Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        })
    }
}

impl Serialize for AclGetuserOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("AclGetuserOutput", 7)?;
        state.serialize_field("exists", &self.exists)?;
        state.serialize_field("flags", &self.flags)?;
        state.serialize_field("passwords", &self.passwords)?;
        state.serialize_field("commands", &self.commands)?;
        state.serialize_field("keys", &self.keys)?;
        state.serialize_field("channels", &self.channels)?;
        state.serialize_field("selectors", &self.selectors)?;
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
            let input = AclGetuserInput { username: RedisJsonValue::String("default".into()) };
            // ACL GETUSER splits into: ACL, GETUSER, default
            assert_eq!(input.command().to_vec(), b"*3\r\n$3\r\nACL\r\n$7\r\nGETUSER\r\n$7\r\ndefault\r\n");
        }

        #[test]
        fn test_decode_null_resp2() {
            let output = AclGetuserOutput::decode(b"$-1\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_null_resp3() {
            let output = AclGetuserOutput::decode(b"_\r\n").unwrap();
            assert!(!output.exists());
        }

        #[test]
        fn test_decode_error_fails() {
            let err = AclGetuserOutput::decode(b"-ERR unknown user\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_input_valid() {
            let args = vec![RedisJsonValue::String("testuser".into())];
            let input = AclGetuserInput::decode(args).unwrap();
            assert_eq!(input.username, RedisJsonValue::String("testuser".into()));
        }

        #[test]
        fn test_decode_input_empty_fails() {
            let args: Vec<RedisJsonValue> = vec![];
            let err = AclGetuserInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires a username"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = AclGetuserInput { username: RedisJsonValue::String("user".into()) };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = AclGetuserInput { username: RedisJsonValue::String("user".into()) };
            assert_eq!(crate::api::lib::RedisCommandInput::kind(&input), RedisApi::AclGetuser);
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_getuser_default() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(&AclGetuserInput { username: RedisJsonValue::String("default".into()) }.command())
                        .await
                        .expect("raw failed");

                    let output = AclGetuserOutput::decode(&result).expect("decode failed");
                    assert!(output.exists(), "default user should exist");
                    assert!(!output.flags().is_empty(), "should have flags");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_getuser_nonexistent() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &AclGetuserInput {
                                username: RedisJsonValue::String("nonexistent_user_xyz".into()),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = AclGetuserOutput::decode(&result).expect("decode failed");
                    assert!(!output.exists(), "nonexistent user should not exist");
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_getuser_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.4")).await;

            let result =
                ctx.raw(&AclGetuserInput { username: RedisJsonValue::String("default".into()) }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = AclGetuserOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_acl_getuser_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.4")).await;

            let result =
                ctx.raw(&AclGetuserInput { username: RedisJsonValue::String("default".into()) }.command()).await.expect("raw failed");

            let output = AclGetuserOutput::decode(&result).expect("decode failed");
            assert!(output.exists());

            ctx.stop().await;
        }
    }
}
