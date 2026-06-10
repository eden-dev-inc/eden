use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use derive_builder::Builder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

mod auth;
mod client_caching;
mod client_getname;
mod client_getredir;
mod client_id;
mod client_info;
mod client_kill;
mod client_list;
mod client_no_evict;
mod client_no_touch;
mod client_pause;
mod client_setinfo;
mod client_setname;
mod client_tracking;
mod client_trackinginfo;
mod client_unblock;
mod client_unpause;
mod echo;
mod hello;
pub mod ping;
mod quit;
mod reset;
mod select;

pub use auth::*;
pub use client_caching::*;
pub use client_getname::*;
pub use client_getredir::*;
pub use client_id::*;
pub use client_info::*;
pub use client_kill::*;
pub use client_list::*;
pub use client_no_evict::*;
pub use client_no_touch::*;
pub use client_pause::*;
pub use client_setinfo::*;
pub use client_setname::*;
pub use client_tracking::*;
pub use client_trackinginfo::*;
pub use client_unblock::*;
pub use client_unpause::*;
pub use echo::*;
pub use hello::*;
pub use ping::*;
pub use quit::*;
pub use reset::*;
pub use select::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Input {
    Addr(Addr),
    Filters(Vec<Filter>),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Filter {
    IP(Addr),
    ID(RedisJsonValue),
    TYPE(Type),
    USER(RedisJsonValue),
    ADDR(Addr),
    LADDR(Addr),
    SKIPME(bool),
    MAXAGE(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub struct Addr {
    pub(crate) ip: RedisJsonValue,
    pub(crate) port: RedisJsonValue,
}

impl Addr {
    pub(crate) fn cmd(&self, command: &mut crate::command::Cmd) {
        command.arg(format!("{}:{}", self.ip, self.port));
    }
}

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Type {
    #[default]
    NORMAL,
    MASTER,
    SLAVE,
    REPLICA,
    PUBSUB,
}

#[derive(Debug, Deserialize, Clone, ToSchema, JsonSchema)]
pub enum ClientKillResult {
    Count(i64),
    Ok,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum Pause {
    WRITE,
    #[default]
    ALL,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub enum Info {
    LibName(RedisJsonValue),
    LibVer(RedisJsonValue),
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, ToSchema, JsonSchema)]
pub enum UnblockType {
    #[default]
    TIMEOUT,
    ERROR,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Default, Builder, ToSchema, JsonSchema)]
#[allow(private_interfaces)]
pub struct Protover {
    pub(crate) protover: RedisJsonValue,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) auth: Option<Auth>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) set_name: Option<RedisJsonValue>,
}

#[derive(Debug, Serialize, Deserialize, borsh::BorshSerialize, borsh::BorshDeserialize, Clone, Builder, ToSchema, JsonSchema)]
struct Auth {
    pub(crate) username: RedisJsonValue,
    pub(crate) password: RedisJsonValue,
}
