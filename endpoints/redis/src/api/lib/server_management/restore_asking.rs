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

const API_INFO: ApiInfo<RedisApi, RestoreAskingInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::RestoreAsking,
    "An internal command for migrating keys in a cluster",
    ReqType::Write,
    true,
);

/// See official Redis documentation for `RESTORE-ASKING`
/// https://redis.io/docs/latest/commands/restore-asking/
///
/// This is an internal command used during cluster migrations.
/// It's similar to RESTORE but used when a key is being migrated.
#[derive(Debug, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct RestoreAskingInput {
    key: RedisKey,
    ttl: RedisJsonValue,
    serialized_value: RedisJsonValue,
    #[builder(default)]
    replace: Option<bool>,
    #[builder(default)]
    absttl: Option<bool>,
    #[builder(default)]
    idletime: Option<RedisJsonValue>,
    #[builder(default)]
    freq: Option<RedisJsonValue>,
}

impl Default for RestoreAskingInput {
    fn default() -> Self {
        Self {
            key: RedisKey::String(String::new()),
            ttl: RedisJsonValue::Integer(0),
            serialized_value: RedisJsonValue::Null,
            replace: None,
            absttl: None,
            idletime: None,
            freq: None,
        }
    }
}

impl RestoreAskingInput {
    pub fn new(key: impl Into<RedisKey>, ttl: impl Into<RedisJsonValue>, serialized_value: impl Into<RedisJsonValue>) -> Self {
        Self {
            key: key.into(),
            ttl: ttl.into(),
            serialized_value: serialized_value.into(),
            ..Default::default()
        }
    }

    pub fn key(&self) -> &RedisKey {
        &self.key
    }

    pub fn ttl(&self) -> &RedisJsonValue {
        &self.ttl
    }

    pub fn serialized_value(&self) -> &RedisJsonValue {
        &self.serialized_value
    }

    pub fn replace(&self) -> Option<bool> {
        self.replace
    }

    pub fn absttl(&self) -> Option<bool> {
        self.absttl
    }

    pub fn idletime(&self) -> Option<&RedisJsonValue> {
        self.idletime.as_ref()
    }

    pub fn freq(&self) -> Option<&RedisJsonValue> {
        self.freq.as_ref()
    }

    pub fn with_replace(mut self) -> Self {
        self.replace = Some(true);
        self
    }

    pub fn with_absttl(mut self) -> Self {
        self.absttl = Some(true);
        self
    }

    pub fn with_idletime(mut self, idletime: impl Into<RedisJsonValue>) -> Self {
        self.idletime = Some(idletime.into());
        self
    }

    pub fn with_freq(mut self, freq: impl Into<RedisJsonValue>) -> Self {
        self.freq = Some(freq.into());
        self
    }
}

impl Serialize for RestoreAskingInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 4; // type, key, ttl, serialized_value
        if self.replace.is_some() {
            fields += 1;
        }
        if self.absttl.is_some() {
            fields += 1;
        }
        if self.idletime.is_some() {
            fields += 1;
        }
        if self.freq.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("RestoreAskingInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("ttl", &self.ttl)?;
        state.serialize_field("serialized_value", &self.serialized_value)?;

        if let Some(replace) = &self.replace {
            state.serialize_field("replace", replace)?;
        }
        if let Some(absttl) = &self.absttl {
            state.serialize_field("absttl", absttl)?;
        }
        if let Some(idletime) = &self.idletime {
            state.serialize_field("idletime", idletime)?;
        }
        if let Some(freq) = &self.freq {
            state.serialize_field("freq", freq)?;
        }

        state.end()
    }
}

impl_redis_operation!(
    RestoreAskingInput,
    API_INFO,
    {key, ttl, serialized_value, replace, absttl, idletime, freq}
);

impl RedisCommandInput for RestoreAskingInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![self.key.clone()]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        command.arg(&self.key).arg(&self.ttl).arg(&self.serialized_value);

        if let Some(replace) = &self.replace
            && *replace
        {
            command.arg("REPLACE");
        }

        if let Some(absttl) = &self.absttl
            && *absttl
        {
            command.arg("ABSTTL");
        }

        if let Some(idletime) = &self.idletime {
            command.arg("IDLETIME").arg(idletime);
        }

        if let Some(freq) = &self.freq {
            command.arg("FREQ").arg(freq);
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        if args.len() < 3 {
            return Err(EpError::parse(format!(
                "RESTORE-ASKING requires at least 3 arguments (key, ttl, serialized_value), given {}",
                args.len()
            )));
        }

        let key = args[0].clone().try_into()?;
        let ttl = args[1].clone();
        let serialized_value = args[2].clone();

        let mut replace = None;
        let mut absttl = None;
        let mut idletime = None;
        let mut freq = None;
        let mut i = 3;

        while i < args.len() {
            if let RedisJsonValue::String(s) = &args[i] {
                match s.to_uppercase().as_str() {
                    "REPLACE" => {
                        replace = Some(true);
                        i += 1;
                    }
                    "ABSTTL" => {
                        absttl = Some(true);
                        i += 1;
                    }
                    "IDLETIME" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("IDLETIME requires a value"));
                        }
                        idletime = Some(args[i + 1].clone());
                        i += 2;
                    }
                    "FREQ" => {
                        if i + 1 >= args.len() {
                            return Err(EpError::parse("FREQ requires a value"));
                        }
                        freq = Some(args[i + 1].clone());
                        i += 2;
                    }
                    _ => i += 1, // Skip unknown options
                }
            } else {
                i += 1;
            }
        }

        Ok(Self { key, ttl, serialized_value, replace, absttl, idletime, freq })
    }
}

/// Output for Redis RESTORE-ASKING command
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct RestoreAskingOutput {
    message: String,
}

impl RestoreAskingOutput {
    pub fn new(message: String) -> Self {
        Self { message }
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn is_ok(&self) -> bool {
        self.message == "OK"
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        let message = match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => String::from_utf8(s).map_err(EpError::parse)?,
                Resp2Frame::Error(e) => return Err(EpError::parse(e)),
                other => {
                    return Err(EpError::parse(format!("unexpected RESTORE-ASKING response: {:?}", other)));
                }
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => String::from_utf8(data).map_err(EpError::parse)?,
                Resp3Frame::SimpleError { data, .. } => return Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => {
                    return Err(EpError::parse(String::from_utf8_lossy(&data).to_string()));
                }
                other => {
                    return Err(EpError::parse(format!("unexpected RESTORE-ASKING response: {:?}", other)));
                }
            },
        };

        Ok(Self { message })
    }
}

impl Serialize for RestoreAskingOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("RestoreAskingOutput", 1)?;
        state.serialize_field("message", &self.message)?;
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
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            );
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("RESTORE-ASKING"));
            assert!(cmd_str.contains("mykey"));
        }

        #[test]
        fn test_encode_command_with_replace() {
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            )
            .with_replace();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("REPLACE"));
        }

        #[test]
        fn test_encode_command_with_absttl() {
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("serialized".into()),
            )
            .with_absttl();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("ABSTTL"));
        }

        #[test]
        fn test_encode_command_with_idletime() {
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            )
            .with_idletime(RedisJsonValue::Integer(100));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("IDLETIME"));
            assert!(cmd_str.contains("100"));
        }

        #[test]
        fn test_encode_command_with_freq() {
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            )
            .with_freq(RedisJsonValue::Integer(50));
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("FREQ"));
            assert!(cmd_str.contains("50"));
        }

        #[test]
        fn test_decode_basic() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.key(), &RedisKey::String("mykey".into()));
            assert_eq!(input.ttl(), &RedisJsonValue::Integer(0));
            assert!(input.replace().is_none());
            assert!(input.absttl().is_none());
        }

        #[test]
        fn test_decode_with_replace() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("REPLACE".into()),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.replace(), Some(true));
        }

        #[test]
        fn test_decode_with_absttl() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(1000),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("ABSTTL".into()),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.absttl(), Some(true));
        }

        #[test]
        fn test_decode_with_idletime() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("IDLETIME".into()),
                RedisJsonValue::Integer(100),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.idletime(), Some(&RedisJsonValue::Integer(100)));
        }

        #[test]
        fn test_decode_with_freq() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("FREQ".into()),
                RedisJsonValue::Integer(50),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.freq(), Some(&RedisJsonValue::Integer(50)));
        }

        #[test]
        fn test_decode_too_few_args() {
            let args = vec![RedisJsonValue::String("mykey".into()), RedisJsonValue::Integer(0)];
            let err = RestoreAskingInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("requires at least 3 arguments"));
        }

        #[test]
        fn test_decode_idletime_missing_value() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("IDLETIME".into()),
            ];
            let err = RestoreAskingInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("IDLETIME requires a value"));
        }

        #[test]
        fn test_decode_freq_missing_value() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("FREQ".into()),
            ];
            let err = RestoreAskingInput::decode(args).unwrap_err();
            assert!(err.to_string().contains("FREQ requires a value"));
        }

        #[test]
        fn test_keys_returns_single_key() {
            let input = RestoreAskingInput::new(
                RedisKey::String("mykey".into()),
                RedisJsonValue::Integer(0),
                RedisJsonValue::String("serialized".into()),
            );
            let keys = input.keys();
            assert_eq!(keys.len(), 1);
            assert_eq!(keys[0], RedisKey::String("mykey".into()));
        }

        #[test]
        fn test_kind() {
            let input = RestoreAskingInput::default();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::RestoreAsking);
        }

        #[test]
        fn test_decode_ok_response() {
            let output = RestoreAskingOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), "OK");
            assert!(output.is_ok());
        }

        #[test]
        fn test_decode_error_response() {
            let err = RestoreAskingOutput::decode(b"-ERR unknown\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_decode_all_options() {
            let args = vec![
                RedisJsonValue::String("mykey".into()),
                RedisJsonValue::Integer(5000),
                RedisJsonValue::String("serialized".into()),
                RedisJsonValue::String("REPLACE".into()),
                RedisJsonValue::String("ABSTTL".into()),
                RedisJsonValue::String("IDLETIME".into()),
                RedisJsonValue::Integer(100),
                RedisJsonValue::String("FREQ".into()),
                RedisJsonValue::Integer(50),
            ];
            let input = RestoreAskingInput::decode(args).unwrap();
            assert_eq!(input.replace(), Some(true));
            assert_eq!(input.absttl(), Some(true));
            assert_eq!(input.idletime(), Some(&RedisJsonValue::Integer(100)));
            assert_eq!(input.freq(), Some(&RedisJsonValue::Integer(50)));
        }
    }

    // Note: Integration tests for RESTORE-ASKING require a Redis cluster setup
    // with active migration, which is complex to set up in automated tests.
}
