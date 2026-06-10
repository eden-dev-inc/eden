mod batch;
// mod execute_single_page;
// mod execute_unpaged;
mod query;
mod query_iter;
mod query_read_only;
mod query_single_page;
mod query_single_page_read_only;
mod query_unpaged;
mod query_unpaged_read_only;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[allow(ambiguous_glob_reexports)]
pub use batch::*;
// pub use execute_single_page::*;
// pub use execute_unpaged::*;
pub use query::*;
pub use query_read_only::*;
pub use query_single_page::*;
pub use query_single_page_read_only::*;
pub use query_unpaged::*;
pub use query_unpaged_read_only::*;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "CassandraApiBuilder")]
pub enum CassandraApi {
    Batch,
    // commands not implemented yet, commented out not to interfere with DocumentAPI
    // ExecuteSinglePage,
    // ExecuteUnpaged,
    // GetClusterData,
    // GetDefaultExecutionProfileHandler,
    // GetKeyspace,
    // GetMetrics,
    // GetTracingInfo,
    // Prepare,
    // PrepareBatch,
    Query,
    // QueryIter,
    QueryReadOnly,
    QuerySinglePage,
    QuerySinglePageReadOnly,
    QueryUnpaged,
    QueryUnpagedReadOnly,
    // UseKeyspace,
}

impl CassandraApi {
    pub fn name() -> String {
        "CassandraApi".to_string()
    }

    pub fn db_kind() -> String {
        "cassandra".to_string()
    }
}

impl Display for CassandraApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Batch => write!(f, "batch"),
            // Self::ExecuteSinglePage => write!(f, "execute_single_page"),
            // Self::ExecuteUnpaged => write!(f, "execute_unpaged"),
            Self::Query => write!(f, "query"),
            // Self::QueryIter => write!(f, "query_iter"),
            Self::QueryReadOnly => write!(f, "query_read_only"),
            Self::QuerySinglePage => write!(f, "query_single_page"),
            Self::QuerySinglePageReadOnly => write!(f, "query_single_page_read_only"),
            Self::QueryUnpaged => write!(f, "query_unpage"),
            Self::QueryUnpagedReadOnly => write!(f, "query_unpaged_read_only"),
            // Self::UseKeyspace => write!(f, "use_keyspace"),
        }
    }
}
