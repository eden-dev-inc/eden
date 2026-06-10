use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{Auth, MigrateStatus, key::RedisKey, value::RedisJsonValue};
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

const API_INFO: ApiInfo<RedisApi, MigrateInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Migrate,
    "Atomically transfers a key from one Redis instance to another",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `MIGRATE`
/// https://redis.io/docs/latest/commands/migrate/
///
/// Syntax: MIGRATE host port key|"" destination-db timeout [COPY] [REPLACE] [AUTH password | AUTH2 username password] [KEYS key [key ...]]
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema, Default)]
#[builder(default)]
pub struct MigrateInput {
    /// Target Redis host
    host: String,
    /// Target Redis port
    port: u16,
    /// Key to migrate (use empty string when using KEYS option)
    #[builder(default)]
    key: RedisKey,
    /// Target database number
    #[serde(rename = "destination-db")]
    destination_db: u32,
    /// Timeout in milliseconds
    timeout: u64,
    /// Copy instead of move (available since Redis 3.0)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    copy: Option<bool>,
    /// Replace existing key on destination
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    replace: Option<bool>,
    /// Authentication options
    #[serde(flatten)]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    auth: Option<Auth>,
    /// Multiple keys to migrate (available since Redis 3.0.6)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[builder(default)]
    keys: Option<Vec<RedisKey>>,
}

impl Serialize for MigrateInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 6; // type, host, port, key, destination_db, timeout
        if self.copy.is_some() {
            fields += 1;
        }
        if self.replace.is_some() {
            fields += 1;
        }
        if self.auth.is_some() {
            fields += 1;
        }
        if self.keys.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("MigrateInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("host", &self.host)?;
        state.serialize_field("port", &self.port)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("destination-db", &self.destination_db)?;
        state.serialize_field("timeout", &self.timeout)?;

        if let Some(copy) = &self.copy {
            state.serialize_field("copy", copy)?;
        }
        if let Some(replace) = &self.replace {
            state.serialize_field("replace", replace)?;
        }
        if let Some(auth) = &self.auth {
            state.serialize_field("auth", auth)?;
        }
        if let Some(keys) = &self.keys {
            state.serialize_field("keys", keys)?;
        }
        state.end()
    }
}

impl_redis_operation!(
    MigrateInput,
    API_INFO,
    { host, port, key, destination_db, timeout, copy, replace, auth, keys }
);

impl RedisCommandInput for MigrateInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }

    fn keys(&self) -> Vec<RedisKey> {
        let mut result = vec![];
        if !matches!(&self.key, RedisKey::String(s) if s.is_empty()) {
            result.push(self.key.clone());
        }
        if let Some(keys) = &self.keys {
            result.extend(keys.clone());
        }
        result
    }

    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.host).arg(self.port).arg(&self.key).arg(self.destination_db).arg(self.timeout);

        if self.copy == Some(true) {
            command.arg("COPY");
        }

        if self.replace == Some(true) {
            command.arg("REPLACE");
        }

        if let Some(auth) = &self.auth {
            match auth {
                Auth::Auth { password } => {
                    command.arg("AUTH").arg(password);
                }
                Auth::Auth2 { username, password } => {
                    command.arg("AUTH2").arg(username).arg(password);
                }
            }
        }

        if let Some(ref keys) = self.keys {
            command.arg("KEYS");
            for key in keys {
                command.arg(key);
            }
        }

        command.get_packed_command()
    }

    #[named]
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 5 {
            return Err(EpError::request(format!(
                "MIGRATE requires at least 5 arguments (host, port, key, destination-db, timeout), given {}",
                args.len()
            )));
        }

        let host = match &args[0] {
            RedisJsonValue::String(s) => s.clone(),
            other => {
                return Err(EpError::parse(format!("host must be a string, got {:?}", other)));
            }
        };

        let port = match &args[1] {
            RedisJsonValue::Integer(n) => *n as u16,
            RedisJsonValue::String(s) => s.parse().map_err(|_| EpError::parse("invalid port"))?,
            other => {
                return Err(EpError::parse(format!("port must be an integer, got {:?}", other)));
            }
        };

        let key: RedisKey = args[2].clone().try_into()?;

        let destination_db = match &args[3] {
            RedisJsonValue::Integer(n) => *n as u32,
            RedisJsonValue::String(s) => s.parse().map_err(|_| EpError::parse("invalid destination-db"))?,
            other => {
                return Err(EpError::parse(format!("destination-db must be an integer, got {:?}", other)));
            }
        };

        let timeout = match &args[4] {
            RedisJsonValue::Integer(n) => *n as u64,
            RedisJsonValue::String(s) => s.parse().map_err(|_| EpError::parse("invalid timeout"))?,
            other => {
                return Err(EpError::parse(format!("timeout must be an integer, got {:?}", other)));
            }
        };

        let mut copy = None;
        let mut replace = None;
        let mut auth = None;
        let mut keys = None;
        let mut i = 5;

        while i < args.len() {
            match &args[i] {
                RedisJsonValue::String(s) => match s.to_uppercase().as_str() {
                    "COPY" => {
                        copy = Some(true);
                        i += 1;
                    }
                    "REPLACE" => {
                        replace = Some(true);
                        i += 1;
                    }
                    "AUTH" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("AUTH requires a password"));
                        }
                        let password = match &args[i + 1] {
                            RedisJsonValue::String(s) => s.clone(),
                            other => {
                                return Err(EpError::parse(format!("AUTH password must be a string, got {:?}", other)));
                            }
                        };
                        auth = Some(Auth::Auth { password });
                        i += 2;
                    }
                    "AUTH2" => {
                        if i + 2 >= args.len() {
                            return Err(EpError::request("AUTH2 requires username and password"));
                        }
                        let username = match &args[i + 1] {
                            RedisJsonValue::String(s) => s.clone(),
                            other => {
                                return Err(EpError::parse(format!("AUTH2 username must be a string, got {:?}", other)));
                            }
                        };
                        let password = match &args[i + 2] {
                            RedisJsonValue::String(s) => s.clone(),
                            other => {
                                return Err(EpError::parse(format!("AUTH2 password must be a string, got {:?}", other)));
                            }
                        };
                        auth = Some(Auth::Auth2 { username, password });
                        i += 3;
                    }
                    "KEYS" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::request("KEYS requires at least one key"));
                        }
                        let mut key_list = vec![];
                        for k in args[i + 1..].iter() {
                            key_list.push(k.clone().try_into()?);
                        }
                        keys = Some(key_list);
                        break;
                    }
                    unknown => {
                        let _ctx = ctx_with_trace!().with_feature("redis");
                        log_warn!(
                            _ctx,
                            "Unknown MIGRATE option: {}",
                            audience = LogAudience::Internal,
                            details = format!("{}", unknown)
                        );
                        i += 1;
                    }
                },
                _ => i += 1,
            }
        }

        Ok(Self {
            host,
            port,
            key,
            destination_db,
            timeout,
            copy,
            replace,
            auth,
            keys,
        })
    }
}

/// Output for Redis MIGRATE command
///
/// Returns OK on success, or NOKEY if no keys were found in the source instance.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct MigrateOutput {
    /// The result: "OK" on success, "NOKEY" if key doesn't exist
    status: MigrateStatus,
}

impl MigrateOutput {
    pub fn new(status: MigrateStatus) -> Self {
        Self { status }
    }

    /// Get the status
    pub fn status(&self) -> &MigrateStatus {
        &self.status
    }

    /// Check if migration was successful
    pub fn is_ok(&self) -> bool {
        self.status == MigrateStatus::Ok
    }

    /// Check if key was not found
    pub fn is_nokey(&self) -> bool {
        self.status == MigrateStatus::NoKey
    }

    /// Decode the Redis protocol response into a MigrateOutput
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let status = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    let s = String::from_utf8(s).map_err(EpError::parse)?;
                    match s.as_str() {
                        "OK" => MigrateStatus::Ok,
                        "NOKEY" => MigrateStatus::NoKey,
                        other => {
                            return Err(EpError::parse(format!("unexpected MIGRATE response: {}", other)));
                        }
                    }
                }
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected MIGRATE response type: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => {
                    let s = String::from_utf8(data).map_err(EpError::parse)?;
                    match s.as_str() {
                        "OK" => MigrateStatus::Ok,
                        "NOKEY" => MigrateStatus::NoKey,
                        other => {
                            return Err(EpError::parse(format!("unexpected MIGRATE response: {}", other)));
                        }
                    }
                }
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected MIGRATE response type: {:?}", other)));
                }
            },
        };

        Ok(Self { status })
    }
}

impl Serialize for MigrateOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("MigrateOutput", 1)?;
        state.serialize_field(
            "status",
            match &self.status {
                MigrateStatus::Ok => "OK",
                MigrateStatus::NoKey => "NOKEY",
            },
        )?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_basic_command() {
            let input = MigrateInput {
                host: "192.168.1.100".into(),
                port: 6379,
                key: RedisKey::String("mykey".into()),
                destination_db: 0,
                timeout: 5000,
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);

            assert!(cmd_str.contains("MIGRATE"));
            assert!(cmd_str.contains("192.168.1.100"));
            assert!(cmd_str.contains("6379"));
            assert!(cmd_str.contains("mykey"));
            assert!(cmd_str.contains("5000"));
        }

        #[test]
        fn test_encode_with_copy_replace() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("key1".into()),
                destination_db: 1,
                timeout: 1000,
                copy: Some(true),
                replace: Some(true),
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);

            assert!(cmd_str.contains("COPY"));
            assert!(cmd_str.contains("REPLACE"));
        }

        #[test]
        fn test_encode_with_auth() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("key1".into()),
                destination_db: 0,
                timeout: 1000,
                auth: Some(Auth::Auth { password: "secret".into() }),
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);

            assert!(cmd_str.contains("AUTH"));
            assert!(cmd_str.contains("secret"));
        }

        #[test]
        fn test_encode_with_auth2() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("key1".into()),
                destination_db: 0,
                timeout: 1000,
                auth: Some(Auth::Auth2 { username: "user".into(), password: "pass".into() }),
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);

            assert!(cmd_str.contains("AUTH2"));
            assert!(cmd_str.contains("user"));
            assert!(cmd_str.contains("pass"));
        }

        #[test]
        fn test_encode_with_keys() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("".into()), // Empty when using KEYS
                destination_db: 0,
                timeout: 1000,
                keys: Some(vec![
                    RedisKey::String("k1".into()),
                    RedisKey::String("k2".into()),
                    RedisKey::String("k3".into()),
                ]),
                ..Default::default()
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);

            assert!(cmd_str.contains("KEYS"));
            assert!(cmd_str.contains("k1"));
            assert!(cmd_str.contains("k2"));
            assert!(cmd_str.contains("k3"));
        }

        #[test]
        fn test_keys_method_single_key() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("mykey".into()),
                destination_db: 0,
                timeout: 1000,
                ..Default::default()
            };

            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_keys_method_multiple_keys() {
            let input = MigrateInput {
                host: "localhost".into(),
                port: 6380,
                key: RedisKey::String("".into()),
                destination_db: 0,
                timeout: 1000,
                keys: Some(vec![RedisKey::String("k1".into()), RedisKey::String("k2".into())]),
                ..Default::default()
            };

            let keys = input.keys();
            assert_eq!(keys.len(), 2);
        }

        #[test]
        fn test_decode_ok_response() {
            let output = MigrateOutput::decode(b"+OK\r\n").unwrap();
            assert!(output.is_ok());
            assert!(!output.is_nokey());
        }

        #[test]
        fn test_decode_nokey_response() {
            let output = MigrateOutput::decode(b"+NOKEY\r\n").unwrap();
            assert!(!output.is_ok());
            assert!(output.is_nokey());
        }

        #[test]
        fn test_decode_error_response() {
            let err = MigrateOutput::decode(b"-ERR IOERR error\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_args_basic() {
            let args = vec![
                RedisJsonValue::String("192.168.1.100".into()),
                RedisJsonValue::Integer(6379),
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(5000),
            ];

            let input = MigrateInput::decode(args).unwrap();
            assert_eq!(input.host, "192.168.1.100");
            assert_eq!(input.port, 6379);
            assert_eq!(input.destination_db, 0);
            assert_eq!(input.timeout, 5000);
        }

        #[test]
        fn test_decode_args_with_options() {
            let args = vec![
                RedisJsonValue::String("localhost".into()),
                RedisJsonValue::Integer(6380),
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(1),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("COPY".into()),
                RedisJsonValue::String("REPLACE".into()),
            ];

            let input = MigrateInput::decode(args).unwrap();
            assert_eq!(input.copy, Some(true));
            assert_eq!(input.replace, Some(true));
        }

        #[test]
        fn test_decode_args_with_auth() {
            let args = vec![
                RedisJsonValue::String("localhost".into()),
                RedisJsonValue::Integer(6380),
                RedisJsonValue::String("key".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("AUTH".into()),
                RedisJsonValue::String("secret".into()),
            ];

            let input = MigrateInput::decode(args).unwrap();
            assert!(matches!(input.auth, Some(Auth::Auth { password }) if password == "secret"));
        }

        #[test]
        fn test_decode_args_with_keys() {
            let args = vec![
                RedisJsonValue::String("localhost".into()),
                RedisJsonValue::Integer(6380),
                RedisJsonValue::String("".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("KEYS".into()),
                RedisJsonValue::String("k1".into()),
                RedisJsonValue::String("k2".into()),
            ];

            let input = MigrateInput::decode(args).unwrap();
            assert!(input.keys.is_some());
            assert_eq!(input.keys.unwrap().len(), 2);
        }

        #[test]
        fn test_decode_args_insufficient() {
            let args = vec![RedisJsonValue::String("localhost".into()), RedisJsonValue::Integer(6380)];

            let err = MigrateInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("at least 5 arguments"));
        }
    }

    // Note: Integration tests for MIGRATE are complex as they require two Redis instances.
    // These would typically be implemented separately with a multi-container test setup.
    #[cfg(feature = "integration")]
    mod integration {
        use serial_test::serial;

        // MIGRATE integration tests require a second Redis instance as destination.
        // This is a placeholder showing the test structure.
        // In practice, you'd need to spin up two containers.

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "requires multi-container setup"]
        async fn test_migrate_basic() {
            // Would need: source container, destination container
            // Setup keys in source, migrate to destination, verify
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "requires multi-container setup"]
        async fn test_migrate_with_copy() {
            // Test COPY option - key should exist in both after migration
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "requires multi-container setup"]
        async fn test_migrate_nonexistent_key() {
            // Should return NOKEY
        }

        #[tokio::test(flavor = "multi_thread")]
        #[serial]
        #[ignore = "requires multi-container setup"]
        async fn test_migrate_multiple_keys() {
            // Test KEYS option for atomic multi-key migration
        }
    }
}
