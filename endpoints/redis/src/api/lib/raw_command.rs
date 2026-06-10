use crate::api::lib::{RedisApi, RedisCommandInput};
use crate::api::{key::RedisKey, value::RedisJsonValue};
use crate::{ApiInfo, ReqType, impl_redis_operation};
use derive_builder::Builder;
use endpoint_derive::DocumentInput;
use format::endpoint::EpKind;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<RedisApi, RawCommandInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::RawCommand,
    "Execute a raw Redis command string (for tools compatibility)",
    ReqType::Write,
    false,
);

#[derive(Debug, Serialize, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RawCommandInput {
    /// The full Redis command string
    pub command: String,
}

pub(crate) fn decode_raw_command_args(command_name: &str, args: Vec<RedisJsonValue>) -> Result<String, EpError> {
    if args.is_empty() {
        return Err(EpError::request(format!("{command_name} requires at least 1 argument, given 0")));
    }

    let mut args = args.into_iter();
    if let Some(first) = args.next() {
        if args.len() == 0 {
            return raw_command_arg_to_string(command_name, first);
        }

        let mut parts = Vec::with_capacity(args.len() + 1);
        parts.push(shell_words::quote(&raw_command_arg_to_string(command_name, first)?).into_owned());
        for arg in args {
            parts.push(shell_words::quote(&raw_command_arg_to_string(command_name, arg)?).into_owned());
        }
        return Ok(parts.join(" "));
    }

    Err(EpError::request(format!("{command_name} requires at least 1 argument, given 0")))
}

fn raw_command_arg_to_string(command_name: &str, arg: RedisJsonValue) -> Result<String, EpError> {
    match arg {
        RedisJsonValue::String(value) => Ok(value),
        RedisJsonValue::Bytes(value) => {
            String::from_utf8(value).map_err(|e| EpError::request(format!("{command_name} argument is not UTF-8: {e}")))
        }
        other => Ok(other.to_string()),
    }
}

impl_redis_operation!(RawCommandInput, API_INFO, { command });

impl RedisCommandInput for RawCommandInput {
    fn kind(&self) -> RedisApi {
        API_INFO.api
    }
    fn keys(&self) -> Vec<RedisKey> {
        vec![]
    }
    fn command(&self) -> bytes::Bytes {
        let parts = match shell_words::split(&self.command) {
            Ok(parts) => parts,
            Err(_) => return crate::command::cmd("").get_packed_command(),
        };

        if parts.is_empty() {
            return crate::command::cmd("").get_packed_command();
        }

        let mut cmd = crate::command::cmd(&parts[0]);

        for part in parts.iter().skip(1) {
            cmd.arg(part);
        }

        cmd.get_packed_command()
    }
    fn decode(args: Vec<RedisJsonValue>) -> Result<Self, EpError>
    where
        Self: Sized,
    {
        Ok(Self { command: decode_raw_command_args("RAW_COMMAND", args)? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_single_full_command_string() {
        let input = RawCommandInput::decode(vec![RedisJsonValue::String("SET raw:key value".to_string())]).expect("decode raw command");

        assert_eq!(input.command, "SET raw:key value");
    }

    #[test]
    fn decodes_command_words_into_shell_safe_command_string() {
        let input = RawCommandInput::decode(vec![
            RedisJsonValue::String("SET".to_string()),
            RedisJsonValue::String("raw:key".to_string()),
            RedisJsonValue::String("value with spaces".to_string()),
        ])
        .expect("decode raw command");

        assert_eq!(input.command, "SET raw:key 'value with spaces'");
    }
}
