use borsh::{BorshDeserialize, BorshSerialize};
use ep_core;
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(
    Debug,
    Default,
    Clone,
    PartialEq,
    Serialize,
    Deserialize,
    BorshSerialize,
    BorshDeserialize,
    ToSchema,
)]
pub struct MysqlConnection {
    pub url: String,
}

impl_connection!(MysqlConnection, EpKind::Mysql);
