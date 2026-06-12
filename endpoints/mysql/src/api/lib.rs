use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub mod query;
// mod query_fold;
// mod query_fold_read_only;
mod query_one;
mod query_one_read_only;
mod query_read_only;

pub use query::*;
// pub use query_fold::*;
// pub use query_fold_read_only::*;
pub use query_one::*;
pub use query_one_read_only::*;
pub use query_read_only::*;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "MysqlApiBuilder")]
pub enum MysqlApi {
    Query,
    QueryReadOnly,
    // QueryFold,
    // QueryFoldReadOnly,
    QueryOne,
    QueryOneReadOnly,
}

impl MysqlApi {
    pub fn name() -> String {
        "MysqlApi".to_string()
    }
    pub fn db_kind() -> String {
        "mysql".to_string()
    }
}

impl Display for MysqlApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Query => write!(f, "query"),
            Self::QueryReadOnly => write!(f, "query_read_only"),
            // Self::QueryFold => write!(f, "query_fold"),
            // Self::QueryFoldReadOnly => write!(f, "query_fold_read_only"),
            Self::QueryOne => write!(f, "query_one"),
            Self::QueryOneReadOnly => write!(f, "query_one_read_only"),
        }
    }
}
