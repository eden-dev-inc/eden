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

const API_INFO: ApiInfo<RedisApi, ShutdownInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::Shutdown,
    "Synchronously saves the database(s) to disk and shuts down the Redis server",
    ReqType::Write,
    true,
);

/// Save behavior for SHUTDOWN command
#[derive(
    Debug, Default, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, PartialEq, Eq, ToSchema, JsonSchema,
)]
pub enum ShutdownSave {
    /// Don't save before shutting down
    NoSave,
    /// Save before shutting down (default behavior)
    #[default]
    Save,
}

/// See official Redis documentation for `SHUTDOWN`
/// https://redis.io/docs/latest/commands/shutdown/
#[derive(Debug, Deserialize, Clone, Default, Builder, ToSchema, DocumentInput, JsonSchema)]
#[builder(default)]
pub struct ShutdownInput {
    /// Whether to save or not before shutdown
    #[builder(default)]
    save: Option<ShutdownSave>,
    /// Skip waiting for lagging replicas (Redis 7.0+)
    #[builder(default)]
    now: Option<bool>,
    /// Force shutdown even if AOF child is running (Redis 7.0+)
    #[builder(default)]
    force: Option<bool>,
    /// Abort an ongoing shutdown (Redis 7.0+)
    #[builder(default)]
    abort: Option<bool>,
}

impl ShutdownInput {
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a shutdown command with NOSAVE option
    pub fn nosave() -> Self {
        Self { save: Some(ShutdownSave::NoSave), ..Default::default() }
    }

    /// Create a shutdown command with SAVE option
    pub fn save() -> Self {
        Self { save: Some(ShutdownSave::Save), ..Default::default() }
    }

    /// Create a shutdown command with ABORT option
    pub fn abort() -> Self {
        Self { abort: Some(true), ..Default::default() }
    }

    /// Add NOW modifier
    pub fn with_now(mut self) -> Self {
        self.now = Some(true);
        self
    }

    /// Add FORCE modifier
    pub fn with_force(mut self) -> Self {
        self.force = Some(true);
        self
    }

    pub fn save_option(&self) -> Option<&ShutdownSave> {
        self.save.as_ref()
    }

    pub fn is_now(&self) -> bool {
        self.now.unwrap_or(false)
    }

    pub fn is_force(&self) -> bool {
        self.force.unwrap_or(false)
    }

    pub fn is_abort(&self) -> bool {
        self.abort.unwrap_or(false)
    }
}

impl Serialize for ShutdownInput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut fields = 1; // type
        if self.save.is_some() {
            fields += 1;
        }
        if self.now.is_some() {
            fields += 1;
        }
        if self.force.is_some() {
            fields += 1;
        }
        if self.abort.is_some() {
            fields += 1;
        }

        let mut state = serializer.serialize_struct("ShutdownInput", fields)?;
        state.serialize_field("type", &API_INFO.api.to_string())?;

        if let Some(save) = &self.save {
            state.serialize_field("save", save)?;
        }
        if let Some(now) = &self.now {
            state.serialize_field("now", now)?;
        }
        if let Some(force) = &self.force {
            state.serialize_field("force", force)?;
        }
        if let Some(abort) = &self.abort {
            state.serialize_field("abort", abort)?;
        }

        state.end()
    }
}

impl_redis_operation!(ShutdownInput, API_INFO, {save, now, force, abort});

impl RedisCommandInput for ShutdownInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let mut command = crate::command::cmd(&API_INFO.api.to_string());

        if let Some(save) = &self.save {
            match save {
                ShutdownSave::NoSave => {
                    command.arg("NOSAVE");
                }
                ShutdownSave::Save => {
                    command.arg("SAVE");
                }
            };
        }

        if let Some(now) = &self.now
            && *now
        {
            command.arg("NOW");
        }

        if let Some(force) = &self.force
            && *force
        {
            command.arg("FORCE");
        }

        if let Some(abort) = &self.abort
            && *abort
        {
            command.arg("ABORT");
        }

        command.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        let mut save = None;
        let mut now = None;
        let mut force = None;
        let mut abort = None;

        for arg in args {
            if let RedisJsonValue::String(s) = arg {
                match s.to_uppercase().as_str() {
                    "SAVE" => save = Some(ShutdownSave::Save),
                    "NOSAVE" => save = Some(ShutdownSave::NoSave),
                    "NOW" => now = Some(true),
                    "FORCE" => force = Some(true),
                    "ABORT" => abort = Some(true),
                    _ => {} // Unknown options are ignored
                }
            }
        }

        Ok(Self { save, now, force, abort })
    }
}

/// Output for Redis SHUTDOWN command
///
/// Note: On success, the connection will be closed and no response is received.
/// A response is only received if there's an error or if ABORT is used.
#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub struct ShutdownOutput {
    /// The response message (error message or OK for abort)
    message: Option<String>,
    /// Whether the shutdown was aborted
    aborted: bool,
}

impl ShutdownOutput {
    pub fn new(message: Option<String>, aborted: bool) -> Self {
        Self { message, aborted }
    }

    /// Create output for a successful abort
    pub fn aborted() -> Self {
        Self { message: Some("OK".to_string()), aborted: true }
    }

    /// Get the response message
    pub fn message(&self) -> Option<&str> {
        self.message.as_deref()
    }

    /// Check if this was an abort response
    pub fn was_aborted(&self) -> bool {
        self.aborted
    }

    /// Decode the Redis protocol response into a ShutdownOutput
    ///
    /// Note: A successful SHUTDOWN doesn't return a response (connection closes).
    /// This decodes error responses or ABORT responses.
    pub fn decode(bytes: &[u8]) -> Result<Self, EpError> {
        let (frame, _) = RedisProtocol::decode_buffer(bytes).ok_or_else(|| EpError::parse("incomplete RESP frame"))?;

        match frame {
            DecoderRespFrame::Resp2(resp2_frame) => match resp2_frame {
                Resp2Frame::SimpleString(s) => {
                    // OK response from SHUTDOWN ABORT
                    Ok(Self {
                        message: Some(String::from_utf8(s).map_err(EpError::parse)?),
                        aborted: true,
                    })
                }
                Resp2Frame::Error(e) => Err(EpError::parse(e)),
                other => Err(EpError::parse(format!("unexpected SHUTDOWN response: {:?}", other))),
            },
            DecoderRespFrame::Resp3(resp3_frame) => match resp3_frame {
                Resp3Frame::SimpleString { data, .. } => Ok(Self {
                    message: Some(String::from_utf8(data).map_err(EpError::parse)?),
                    aborted: true,
                }),
                Resp3Frame::SimpleError { data, .. } => Err(EpError::parse(data)),
                Resp3Frame::BlobError { data, .. } => Err(EpError::parse(String::from_utf8_lossy(&data).to_string())),
                other => Err(EpError::parse(format!("unexpected SHUTDOWN response: {:?}", other))),
            },
        }
    }
}

impl Serialize for ShutdownOutput {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let mut state = serializer.serialize_struct("ShutdownOutput", 2)?;
        state.serialize_field("message", &self.message)?;
        state.serialize_field("aborted", &self.aborted)?;
        state.end()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    mod unit {
        use super::*;

        #[test]
        fn test_encode_command_default() {
            let input = ShutdownInput::new();
            assert_eq!(input.command().to_vec(), b"*1\r\n$8\r\nSHUTDOWN\r\n");
        }

        #[test]
        fn test_encode_command_nosave() {
            let input = ShutdownInput::nosave();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SHUTDOWN"));
            assert!(cmd_str.contains("NOSAVE"));
        }

        #[test]
        fn test_encode_command_save() {
            let input = ShutdownInput::save();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SHUTDOWN"));
            assert!(cmd_str.contains("SAVE"));
        }

        #[test]
        fn test_encode_command_abort() {
            let input = ShutdownInput::abort();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SHUTDOWN"));
            assert!(cmd_str.contains("ABORT"));
        }

        #[test]
        fn test_encode_command_with_now() {
            let input = ShutdownInput::nosave().with_now();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NOSAVE"));
            assert!(cmd_str.contains("NOW"));
        }

        #[test]
        fn test_encode_command_with_force() {
            let input = ShutdownInput::save().with_force();
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("SAVE"));
            assert!(cmd_str.contains("FORCE"));
        }

        #[test]
        fn test_encode_command_all_options() {
            let input = ShutdownInput {
                save: Some(ShutdownSave::NoSave),
                now: Some(true),
                force: Some(true),
                abort: None,
            };
            let cmd = input.command();
            let cmd_str = String::from_utf8_lossy(&cmd);
            assert!(cmd_str.contains("NOSAVE"));
            assert!(cmd_str.contains("NOW"));
            assert!(cmd_str.contains("FORCE"));
        }

        #[test]
        fn test_decode_no_args() {
            let args: Vec<RedisJsonValue> = vec![];
            let input = ShutdownInput::decode(args).unwrap();
            assert!(input.save.is_none());
            assert!(input.now.is_none());
            assert!(input.force.is_none());
            assert!(input.abort.is_none());
        }

        #[test]
        fn test_decode_save() {
            let args = vec![RedisJsonValue::String("SAVE".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.save, Some(ShutdownSave::Save));
        }

        #[test]
        fn test_decode_nosave() {
            let args = vec![RedisJsonValue::String("NOSAVE".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.save, Some(ShutdownSave::NoSave));
        }

        #[test]
        fn test_decode_now() {
            let args = vec![RedisJsonValue::String("NOW".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.now, Some(true));
        }

        #[test]
        fn test_decode_force() {
            let args = vec![RedisJsonValue::String("FORCE".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.force, Some(true));
        }

        #[test]
        fn test_decode_abort() {
            let args = vec![RedisJsonValue::String("ABORT".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.abort, Some(true));
        }

        #[test]
        fn test_decode_case_insensitive() {
            let args = vec![RedisJsonValue::String("nosave".into()), RedisJsonValue::String("now".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.save, Some(ShutdownSave::NoSave));
            assert_eq!(input.now, Some(true));
        }

        #[test]
        fn test_decode_multiple_options() {
            let args = vec![
                RedisJsonValue::String("NOSAVE".into()),
                RedisJsonValue::String("NOW".into()),
                RedisJsonValue::String("FORCE".into()),
            ];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.save, Some(ShutdownSave::NoSave));
            assert_eq!(input.now, Some(true));
            assert_eq!(input.force, Some(true));
        }

        #[test]
        fn test_decode_unknown_option_ignored() {
            let args = vec![RedisJsonValue::String("SAVE".into()), RedisJsonValue::String("UNKNOWN".into())];
            let input = ShutdownInput::decode(args).unwrap();
            assert_eq!(input.save, Some(ShutdownSave::Save));
        }

        #[test]
        fn test_keys_returns_empty() {
            let input = ShutdownInput::new();
            assert!(input.keys().is_empty());
        }

        #[test]
        fn test_kind() {
            let input = ShutdownInput::new();
            assert_eq!(RedisCommandInput::kind(&input), RedisApi::Shutdown);
        }

        #[test]
        fn test_builder_methods() {
            let input = ShutdownInput::nosave();
            assert_eq!(input.save_option(), Some(&ShutdownSave::NoSave));
            assert!(!input.is_now());
            assert!(!input.is_force());
            assert!(!input.is_abort());

            let input_with_now = input.with_now();
            assert!(input_with_now.is_now());
        }

        #[test]
        fn test_decode_ok_response() {
            // SHUTDOWN ABORT returns OK
            let output = ShutdownOutput::decode(b"+OK\r\n").unwrap();
            assert_eq!(output.message(), Some("OK"));
            assert!(output.was_aborted());
        }

        #[test]
        fn test_decode_error_response() {
            let err = ShutdownOutput::decode(b"-ERR not supported\r\n").unwrap_err();
            assert!(err.to_string().contains("ERR"));
        }

        #[test]
        fn test_output_aborted() {
            let output = ShutdownOutput::aborted();
            assert!(output.was_aborted());
            assert_eq!(output.message(), Some("OK"));
        }
    }

    // Note: Integration tests for SHUTDOWN are not practical as they would
    // terminate the Redis server. SHUTDOWN should be tested manually.
}
