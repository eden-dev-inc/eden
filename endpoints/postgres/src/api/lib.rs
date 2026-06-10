pub mod batch_execute;
pub mod cancel_token;
pub mod clear_typed_cache;
pub mod copy_in;
pub mod copy_out;
pub mod execute;
pub mod is_closed;
pub mod query;
pub mod query_one;
pub mod query_one_read_only;
pub mod query_opt;
pub mod query_opt_read_only;
mod query_raw;
mod query_raw_read_only;
pub mod query_read_only;
pub mod query_typed;
pub mod query_typed_read_only;
pub mod simple_query;
pub mod simple_query_read_only;

use batch_execute::*;
use cancel_token::*;
use clear_typed_cache::*;
use copy_in::*;
use copy_out::*;
use execute::*;
use is_closed::*;
use query::*;
use query_one::*;
use query_one_read_only::*;
use query_read_only::*;
use query_typed::*;
use query_typed_read_only::*;
use simple_query::*;
use simple_query_read_only::*;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "PostgresApiBuilder")]
pub enum PostgresApi {
    BatchExecute,
    CancelToken,
    ClearTypeCache,
    CopyIn,
    CopyOut,
    Execute,
    IsClosed,
    #[noinput]
    IsValid,
    #[noinput]
    Notifications,
    #[noinput]
    Prepare,
    #[noinput]
    PrepareTyped,
    Query,
    QueryOne,
    QueryOneReadOnly,
    #[noinput]
    QueryOpt,
    #[noinput]
    QueryOptReadOnly,
    #[noinput]
    QueryRaw,
    #[noinput]
    QueryRawReadOnly,
    QueryReadOnly,
    QueryTyped,
    QueryTypedReadOnly,
    #[noinput]
    QueryTypedRaw,
    SimpleQuery,
    SimpleQueryReadOnly,
}

impl PostgresApi {
    pub fn name() -> String {
        "PostgresApi".to_string()
    }

    pub fn db_kind() -> String {
        "postgres".to_string()
    }
}

impl Display for PostgresApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::BatchExecute => f.write_str("batch_execute"),
            Self::CancelToken => f.write_str("cancel_token"),
            Self::ClearTypeCache => f.write_str("clear_type_cache"),
            Self::CopyIn => f.write_str("copy_in"),
            Self::CopyOut => f.write_str("copy_out"),
            Self::Execute => f.write_str("execute"),
            Self::IsClosed => f.write_str("is_closed"),
            Self::IsValid => f.write_str("is_valid"),
            Self::Notifications => f.write_str("notifications"),
            Self::Prepare => f.write_str("prepare"),
            Self::PrepareTyped => f.write_str("prepare_typed"),
            Self::Query => f.write_str("query"),
            Self::QueryOne => f.write_str("query_one"),
            Self::QueryOneReadOnly => f.write_str("query_one_read_only"),
            Self::QueryOpt => f.write_str("query_opt"),
            Self::QueryOptReadOnly => f.write_str("query_opt_read_only"),
            Self::QueryRaw => f.write_str("query_raw"),
            Self::QueryRawReadOnly => f.write_str("query_raw_read_only"),
            Self::QueryReadOnly => f.write_str("query_read_only"),
            Self::QueryTyped => f.write_str("query_typed"),
            Self::QueryTypedReadOnly => f.write_str("query_typed_read_only"),
            Self::QueryTypedRaw => f.write_str("query_typed_raw"),
            Self::SimpleQuery => f.write_str("simple"),
            Self::SimpleQueryReadOnly => f.write_str("simple_read_only"),
        }
    }
}
