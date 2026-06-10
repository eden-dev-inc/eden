mod execute;
mod query;

pub use execute::*;
pub use query::*;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "SnowflakeApiBuilder")]
pub enum SnowflakeApi {
    Execute,
    Query,
}

impl SnowflakeApi {
    pub fn name() -> String {
        "SnowflakeApi".to_string()
    }

    pub fn db_kind() -> String {
        "snowflake".to_string()
    }

    #[allow(dead_code)]
    pub(crate) fn as_type(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }
}

impl std::fmt::Display for SnowflakeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Execute => write!(f, "execute"),
            Self::Query => write!(f, "query"),
        }
    }
}
