use crate::api::lib::raw_command::decode_raw_command_args;
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

const API_INFO: ApiInfo<RedisApi, RawCommandReadOnlyInput> = ApiInfo::new(
    EpKind::Redis,
    RedisApi::RawCommandReadOnly,
    "Execute a read-only raw Redis command string (for tools compatibility)",
    ReqType::Read,
    false,
);

#[derive(Debug, Serialize, Deserialize, Clone, Builder, ToSchema, DocumentInput, JsonSchema)]
pub struct RawCommandReadOnlyInput {
    /// The full Redis command string
    pub command: String,
}

impl_redis_operation!(RawCommandReadOnlyInput, API_INFO, { command });

impl RedisCommandInput for RawCommandReadOnlyInput {
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
        Ok(Self {
            command: decode_raw_command_args("RAW_COMMAND_READ_ONLY", args)?,
        })
    }
}
