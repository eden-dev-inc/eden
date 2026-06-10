use crate::api::lib::connection_management::{Auth, Protover};
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
use std::collections::HashMap;
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, HelloInput> =
    ApiInfo::new(EpKind::Redis, RedisApi::Hello, "Handshakes with the Redis server", ReqType::Read, false);

/// See official Redis documentation for `HELLO`
/// https://redis.io/docs/latest/commands/hello/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct HelloInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    fields: Option<Protover>,
}

impl Serialize for HelloInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if let Some(protover) = &self.fields {
            fields += 1; // protover
            if protover.auth.is_some() {
                fields += 2;
            }
            if protover.set_name.is_some() {
                fields += 1;
            }
        }

        let mut state = serializer.serialize_struct("HelloInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        if let Some(protover) = &self.fields {
            state.serialize_field("protover", &protover.protover)?;
            if let Some(auth) = &protover.auth {
                state.serialize_field("username", &auth.username)?;
                state.serialize_field("password", &auth.password)?;
            }
            if let Some(set_name) = &protover.set_name {
                state.serialize_field("set_name", set_name)?;
            }
        }
        state.end()
    }
}

impl_redis_operation!(HelloInput, API_INFO, { fields });

impl RedisCommandInput for HelloInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(protover) = &self.fields {
            command.arg(&protover.protover);

            if let Some(auth) = &protover.auth {
                command.arg("AUTH").arg(&auth.username).arg(&auth.password);
            }

            if let Some(set_name) = &protover.set_name {
                command.arg("SETNAME").arg(set_name);
            }
        }

        command.get_packed_command()
    }
    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut fields = None;

        if !args.is_empty() {
            let protover = args[0].clone();
            let mut auth = None;
            let mut set_name = None;
            let mut i = 1;

            while i < args.len() {
                if let RedisJsonValue::String(s) = &args[i] {
                    let upper = s.to_uppercase();
                    match upper.as_str() {
                        "AUTH" => {
                            if i + 2 < args.len() {
                                auth = Some(Auth { username: args[i + 1].clone(), password: args[i + 2].clone() });
                                i += 3;
                            } else {
                                return Err(EpError::request("AUTH requires username and password arguments"));
                            }
                        }
                        "SETNAME" => {
                            if i + 1 < args.len() {
                                set_name = Some(args[i + 1].clone());
                                i += 2;
                            } else {
                                return Err(EpError::request("SETNAME requires a client name argument"));
                            }
                        }
                        _ => {
                            i += 1;
                        }
                    }
                } else {
                    i += 1;
                }
            }

            fields = Some(Protover { protover, auth, set_name });
        }

        Ok(Self { fields })
    }
}

/// Output for Redis HELLO command
///
/// Returns server information including protocol version, server name, version, etc.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct HelloOutput {
    /// The server name (e.g., "redis")
    server: Option<String>,
    /// The Redis version
    version: Option<String>,
    /// The protocol version being used
    proto: Option<i64>,
    /// The client ID
    id: Option<i64>,
    /// The connection mode (e.g., "standalone", "cluster")
    mode: Option<String>,
    /// The current role (e.g., "master", "slave")
    role: Option<String>,
    /// Raw properties map for any additional fields
    properties: HashMap<String, RedisJsonValue>,
}

impl HelloOutput {
    pub fn new() -> Self {
        Self {
            server: None,
            version: None,
            proto: None,
            id: None,
            mode: None,
            role: None,
            properties: HashMap::new(),
        }
    }

    /// Get the server name
    pub fn server(&self) -> Option<&str> {
        self.server.as_deref()
    }

    /// Get the Redis version
    pub fn version(&self) -> Option<&str> {
        self.version.as_deref()
    }

    /// Get the protocol version
    pub fn proto(&self) -> Option<i64> {
        self.proto
    }

    /// Get the client ID
    pub fn id(&self) -> Option<i64> {
        self.id
    }

    /// Get the connection mode
    pub fn mode(&self) -> Option<&str> {
        self.mode.as_deref()
    }

    /// Get the server role
    pub fn role(&self) -> Option<&str> {
        self.role.as_deref()
    }

    /// Check if connected to a master
    pub fn is_master(&self) -> bool {
        self.role.as_deref() == Some("master")
    }

    /// Get all properties as a map
    pub fn properties(&self) -> &HashMap<String, RedisJsonValue> {
        &self.properties
    }

    /// Decode the Redis protocol response into a HelloOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let mut output = Self::new();

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::Array(items) => {
                    let mut i = 0;
                    while i + 1 < items.len() {
                        if let Resp2Frame::BulkString(key_bytes) = &items[i] {
                            let key = String::from_utf8_lossy(key_bytes).to_string();
                            let value = Self::parse_resp2_value(&items[i + 1]);
                            output.set_property(&key, value);
                        }
                        i += 2;
                    }
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected HELLO response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::Map { data, .. } => {
                    for (key_frame, value_frame) in data {
                        if let Some(key) = Self::extract_resp3_string(&key_frame) {
                            let value = Self::parse_resp3_value(&value_frame);
                            output.set_property(&key, value);
                        }
                    }
                }
                Resp3Frame::Array { data: arr, .. } => {
                    let mut i = 0;
                    while i + 1 < arr.len() {
                        if let Some(key) = Self::extract_resp3_string(&arr[i]) {
                            let value = Self::parse_resp3_value(&arr[i + 1]);
                            output.set_property(&key, value);
                        }
                        i += 2;
                    }
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected HELLO response: {:?}", other)));
                }
            },
        };

        Ok(output)
    }

    fn set_property(&mut self, key: &str, value: RedisJsonValue) {
        match key {
            "server" => {
                if let RedisJsonValue::String(s) = &value {
                    self.server = Some(s.clone());
                }
            }
            "version" => {
                if let RedisJsonValue::String(s) = &value {
                    self.version = Some(s.clone());
                }
            }
            "proto" => {
                if let RedisJsonValue::Integer(n) = &value {
                    self.proto = Some(*n);
                }
            }
            "id" => {
                if let RedisJsonValue::Integer(n) = &value {
                    self.id = Some(*n);
                }
            }
            "mode" => {
                if let RedisJsonValue::String(s) = &value {
                    self.mode = Some(s.clone());
                }
            }
            "role" => {
                if let RedisJsonValue::String(s) = &value {
                    self.role = Some(s.clone());
                }
            }
            _ => {}
        }
        self.properties.insert(key.to_string(), value);
    }

    fn parse_resp2_value(frame: &Resp2Frame) -> RedisJsonValue {
        match frame {
            Resp2Frame::BulkString(bytes) => RedisJsonValue::String(String::from_utf8_lossy(bytes).to_string()),
            Resp2Frame::SimpleString(bytes) => RedisJsonValue::String(String::from_utf8_lossy(bytes).to_string()),
            Resp2Frame::Integer(n) => RedisJsonValue::Integer(*n),
            Resp2Frame::Null => RedisJsonValue::Null,
            Resp2Frame::Array(_) => RedisJsonValue::String("[array]".to_string()),
            _ => RedisJsonValue::Null,
        }
    }

    fn parse_resp3_value(frame: &Resp3Frame) -> RedisJsonValue {
        match frame {
            Resp3Frame::BlobString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
            Resp3Frame::SimpleString { data, .. } => RedisJsonValue::String(String::from_utf8_lossy(data).to_string()),
            Resp3Frame::Number { data, .. } => RedisJsonValue::Integer(*data),
            Resp3Frame::Null => RedisJsonValue::Null,
            _ => RedisJsonValue::Null,
        }
    }

    fn extract_resp3_string(frame: &Resp3Frame) -> Option<String> {
        match frame {
            Resp3Frame::BlobString { data, .. } => String::from_utf8(data.clone()).ok(),
            Resp3Frame::SimpleString { data, .. } => String::from_utf8(data.clone()).ok(),
            _ => None,
        }
    }
}

impl Default for HelloOutput {
    fn default() -> Self {
        Self::new()
    }
}

impl Serialize for HelloOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut fields = 0;
        if self.server.is_some() {
            fields += 1;
        }
        if self.version.is_some() {
            fields += 1;
        }
        if self.proto.is_some() {
            fields += 1;
        }
        if self.id.is_some() {
            fields += 1;
        }
        if self.mode.is_some() {
            fields += 1;
        }
        if self.role.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("HelloOutput", fields)?;
        if let Some(ref server) = self.server {
            state.serialize_field("server", server)?;
        }
        if let Some(ref version) = self.version {
            state.serialize_field("version", version)?;
        }
        if let Some(proto) = self.proto {
            state.serialize_field("proto", &proto)?;
        }
        if let Some(id) = self.id {
            state.serialize_field("id", &id)?;
        }
        if let Some(ref mode) = self.mode {
            state.serialize_field("mode", mode)?;
        }
        if let Some(ref role) = self.role {
            state.serialize_field("role", role)?;
        }
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_no_args() {
            let input = HelloInput { fields: None };
            assert_eq!(input.command().to_vec(), b"*1\r\n$5\r\nHELLO\r\n");
        }

        #[test]
        fn test_encode_command_with_protover() {
            let input = HelloInput {
                fields: Some(Protover {
                    protover: RedisJsonValue::Integer(3),
                    auth: None,
                    set_name: None,
                }),
            };
            assert_eq!(input.command().to_vec(), b"*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n");
        }

        #[test]
        fn test_encode_command_with_auth() {
            let input = HelloInput {
                fields: Some(Protover {
                    protover: RedisJsonValue::Integer(3),
                    auth: Some(Auth {
                        username: RedisJsonValue::String("user".into()),
                        password: RedisJsonValue::String("pass".into()),
                    }),
                    set_name: None,
                }),
            };
            let cmd = input.command();
            assert!(cmd.windows(4).any(|w| w == b"AUTH"));
            assert!(cmd.windows(4).any(|w| w == b"user"));
            assert!(cmd.windows(4).any(|w| w == b"pass"));
        }

        #[test]
        fn test_encode_command_with_setname() {
            let input = HelloInput {
                fields: Some(Protover {
                    protover: RedisJsonValue::Integer(3),
                    auth: None,
                    set_name: Some(RedisJsonValue::String("my-client".into())),
                }),
            };
            let cmd = input.command();
            assert!(cmd.windows(7).any(|w| w == b"SETNAME"));
            assert!(cmd.windows(9).any(|w| w == b"my-client"));
        }

        #[test]
        fn test_decode_input_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = HelloInput::decode(args).unwrap();
            assert!(input.fields.is_none());
        }

        #[test]
        fn test_decode_input_with_protover() {
            let args = vec![RedisJsonValue::Integer(3)];
            let input = HelloInput::decode(args).unwrap();
            assert!(input.fields.is_some());
            let fields = input.fields.unwrap();
            assert_eq!(fields.protover, RedisJsonValue::Integer(3));
        }

        #[test]
        fn test_decode_input_with_auth() {
            let args = vec![
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("AUTH".into()),
                RedisJsonValue::String("user".into()),
                RedisJsonValue::String("pass".into()),
            ];
            let input = HelloInput::decode(args).unwrap();
            let fields = input.fields.unwrap();
            let auth = fields.auth.unwrap();
            assert_eq!(auth.username, RedisJsonValue::String("user".into()));
            assert_eq!(auth.password, RedisJsonValue::String("pass".into()));
        }

        #[test]
        fn test_decode_input_auth_missing_args() {
            let args = vec![
                RedisJsonValue::Integer(3),
                RedisJsonValue::String("AUTH".into()),
                RedisJsonValue::String("user".into()),
            ];
            let err = HelloInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("AUTH requires"));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = HelloInput { fields: None };
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_default() {
            let input = HelloInput::default();
            assert!(input.fields.is_none());
        }

        #[test]
        fn test_hello_output_is_master() {
            let mut output = HelloOutput::new();
            output.role = Some("master".to_string());
            assert!(output.is_master());

            output.role = Some("slave".to_string());
            assert!(!output.is_master());
        }
    }

    #[cfg(feature = "integration")]
    mod integration {
        use super::*;
        use crate::test_utils::*;
        use serial_test::serial;

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_no_args() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&HelloInput { fields: None }.command()).await.expect("raw failed");

                    let output = HelloOutput::decode(&result).expect("decode failed");
                    assert!(output.server().is_some());
                    assert!(output.version().is_some());
                    assert!(output.proto().is_some());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_with_proto_2() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx
                        .raw(
                            &HelloInput {
                                fields: Some(Protover {
                                    protover: RedisJsonValue::Integer(2),
                                    auth: None,
                                    set_name: None,
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HelloOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.proto(), Some(2));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_with_proto_3() {
            // HELLO 3 switches the protocol to RESP3. We can only test this on
            // RESP3 contexts because sending HELLO 3 on a RESP2 connection causes
            // the server to respond in RESP3 format but the client expects RESP2.
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Skip RESP2 contexts - HELLO 3 switches protocol mid-connection
                    if ctx.resp_version != RespVersion::Resp3 {
                        return;
                    }

                    let result = ctx
                        .raw(
                            &HelloInput {
                                fields: Some(Protover {
                                    protover: RedisJsonValue::Integer(3),
                                    auth: None,
                                    set_name: None,
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HelloOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.proto(), Some(3));
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_returns_server_info() {
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    let result = ctx.raw(&HelloInput { fields: None }.command()).await.expect("raw failed");

                    let output = HelloOutput::decode(&result).expect("decode failed");
                    assert_eq!(output.server(), Some("redis"));
                    assert!(output.version().is_some());
                    assert!(output.id().is_some());
                    assert_eq!(output.mode(), Some("standalone"));
                    assert!(output.is_master());
                })
            })
            .await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_resp2_format() {
            let mut ctx = setup(RespVersion::Resp2, Some("7.0")).await;

            let result = ctx.raw(&HelloInput { fields: None }.command()).await.expect("raw failed");

            assert!(result.starts_with(b"*"), "RESP2 should return array");
            let output = HelloOutput::decode(&result).expect("decode failed");
            assert!(output.server().is_some());

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_resp3_format() {
            let mut ctx = setup(RespVersion::Resp3, Some("7.0")).await;

            let result = ctx
                .raw(
                    &HelloInput {
                        fields: Some(Protover {
                            protover: RedisJsonValue::Integer(3),
                            auth: None,
                            set_name: None,
                        }),
                    }
                    .command(),
                )
                .await
                .expect("raw failed");

            assert!(result.starts_with(b"%"), "RESP3 should return map");
            let output = HelloOutput::decode(&result).expect("decode failed");
            assert!(output.server().is_some());
            assert_eq!(output.proto(), Some(3));

            ctx.stop().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        async fn test_hello_with_setname() {
            // HELLO 3 with SETNAME switches the protocol to RESP3. We can only test this
            // on RESP3 contexts because sending HELLO 3 on a RESP2 connection causes
            // the server to respond in RESP3 format but the client expects RESP2.
            test_all_protocols_min_version("6", |ctx| {
                Box::pin(async move {
                    // Skip RESP2 contexts - HELLO 3 switches protocol mid-connection
                    if ctx.resp_version != RespVersion::Resp3 {
                        return;
                    }

                    let result = ctx
                        .raw(
                            &HelloInput {
                                fields: Some(Protover {
                                    protover: RedisJsonValue::Integer(3),
                                    auth: None,
                                    set_name: Some(RedisJsonValue::String("test-client".into())),
                                }),
                            }
                            .command(),
                        )
                        .await
                        .expect("raw failed");

                    let output = HelloOutput::decode(&result).expect("decode failed");
                    assert!(output.server().is_some());
                })
            })
            .await;
        }
    }
}
