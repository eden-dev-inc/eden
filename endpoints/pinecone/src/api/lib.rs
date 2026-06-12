mod delete;
mod describe_index_stats;
mod fetch;
mod list;
mod query;
mod update;
mod upsert;

pub use delete::*;
pub use describe_index_stats::*;
pub use fetch::*;
pub use list::*;
pub use query::*;
pub use update::*;
pub use upsert::*;

use std::fmt::Display;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "PineconeApiBuilder")]
pub enum PineconeApi {
    Delete,
    DescribeIndexStats,
    Fetch,
    List,
    Query,
    Update,
    Upsert,
}

impl PineconeApi {
    pub fn name() -> String {
        "PineconeApi".to_string()
    }

    pub fn db_kind() -> String {
        "pinecone".to_string()
    }
}

impl Display for PineconeApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Delete => write!(f, "delete"),
            Self::DescribeIndexStats => write!(f, "describe_index_stats"),
            Self::Fetch => write!(f, "fetch"),
            Self::List => write!(f, "list"),
            Self::Query => write!(f, "query"),
            Self::Update => write!(f, "update"),
            Self::Upsert => write!(f, "upsert"),
        }
    }
}
