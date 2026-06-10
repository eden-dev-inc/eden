pub mod query;
pub mod query_read_only;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[allow(unused_imports)]
pub use query::*;
pub use query_read_only::*;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "MssqlApiBuilder")]
pub enum MssqlApi {
    Query,
    QueryReadOnly,
    #[noinput]
    Read,
}

impl MssqlApi {
    pub fn name() -> String {
        "MssqlApi".to_string()
    }

    pub fn db_kind() -> String {
        "mssql".to_string()
    }
}

impl Display for MssqlApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Query => write!(f, "query"),
            Self::QueryReadOnly => write!(f, "query_read_only"),
            Self::Read => write!(f, "read"),
        }
    }
}
