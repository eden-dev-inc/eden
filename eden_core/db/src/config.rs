use std::fmt;

use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, BorshDeserialize, BorshSerialize, Debug, Clone, PartialEq, Eq, Default)]
pub enum DBKind {
    Redis,
    #[default]
    Postgres,
    Clickhouse,
}

impl fmt::Display for DBKind {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::Redis => write!(f, "redis"),
            Self::Clickhouse => write!(f, "clickhouse"),
        }
    }
}

impl DBKind {
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Postgres => "postgres".as_bytes(),
            Self::Redis => "redis".as_bytes(),
            Self::Clickhouse => "clickhouse".as_bytes(),
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, Default)]
pub struct ConnectionParameters {
    pub name: String,
    pub url: String,
    pub db_type: DBKind,
    pub node: String,
    pub database: String,
    pub read_only: bool,
}
