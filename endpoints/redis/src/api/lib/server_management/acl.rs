#![allow(clippy::upper_case_acronyms)] // Intentional: protocol/command acronyms (ACL, GEO, etc.)
use crate::api::value::RedisJsonValue;
use borsh::{BorshDeserialize, BorshSerialize};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use utoipa::ToSchema;

pub mod acl_cat;
mod acl_deluser;
mod acl_dryrun;
mod acl_genpass;
mod acl_getuser;
mod acl_list;
mod acl_load;
mod acl_log;
mod acl_save;
mod acl_setuser;
mod acl_users;
mod acl_whoami;

pub(crate) use acl_cat::*;
pub(crate) use acl_deluser::*;
pub(crate) use acl_dryrun::*;
pub(crate) use acl_genpass::*;
pub(crate) use acl_getuser::*;
pub(crate) use acl_list::*;
pub(crate) use acl_load::*;
pub(crate) use acl_log::*;
pub(crate) use acl_save::*;
pub(crate) use acl_setuser::*;
pub(crate) use acl_users::*;
pub(crate) use acl_whoami::*;

#[derive(Debug, Serialize, Deserialize, BorshSerialize, BorshDeserialize, Clone, ToSchema, JsonSchema)]
pub(crate) enum Events {
    COUNT(RedisJsonValue),
    RESET,
}
