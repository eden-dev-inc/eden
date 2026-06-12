mod batch;
mod call_timeout;
mod change_password;
mod clear_object_type_cache;
mod current_schema;
mod edition;
mod execute;
mod execute_named;
mod external_name;
mod info;
mod internal_name;
mod is_new_connection;
mod last_warning;
mod object_type;
mod ping;
pub(crate) mod query;
mod query_as;
mod query_as_named;
mod query_as_named_read_only;
mod query_as_read_only;
mod query_named;
mod query_named_read_only;
mod query_read_only;
mod query_row;
mod query_row_as;
mod query_row_as_named;
mod query_row_as_named_read_only;
mod query_row_as_read_only;
mod query_row_named;
mod query_row_named_read_only;
mod query_row_read_only;
mod server_version;
mod set_action;
mod set_call_timeout;
mod set_client_identifier;
mod set_client_info;
mod set_current_schema;
mod set_db_op;
mod set_external_name;
mod set_internal_name;
mod set_module;
mod set_stmt_cache_size;
mod shutdown_database;
mod startup_database;
mod statement;
mod status;
mod stmt_cache_size;
mod tag;
mod tag_found;

use batch::*;
use call_timeout::*;
use change_password::*;
use clear_object_type_cache::*;
use current_schema::*;
use edition::*;
use execute::*;
use execute_named::*;
use external_name::*;
use info::*;
use internal_name::*;
use is_new_connection::*;
use last_warning::*;
use object_type::*;
use ping::*;
use query::*;
use query_as::*;
use query_as_named::*;
use query_as_named_read_only::*;
use query_as_read_only::*;
use query_named::*;
use query_named_read_only::*;
use query_read_only::*;
use query_row::*;
use query_row_as::*;
use query_row_as_named::*;
use query_row_as_named_read_only::*;
use query_row_as_read_only::*;
use query_row_named::*;
use query_row_named_read_only::*;
use query_row_read_only::*;
use server_version::*;
use set_action::*;
use set_call_timeout::*;
use set_client_identifier::*;
use set_client_info::*;
use set_current_schema::*;
use set_db_op::*;
use set_external_name::*;
use set_internal_name::*;
use set_module::*;
use set_stmt_cache_size::*;
use shutdown_database::*;
use startup_database::*;
use statement::*;
use status::*;
use stmt_cache_size::*;
use tag::*;
use tag_found::*;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "OracleApiBuilder")]
pub enum OracleApi {
    Batch,
    CallTimeout,
    ChangePassword,
    ClearObjectTypeCache,
    CurrentSchema,
    Edition,
    Execute,
    ExecuteNamed,
    ExternalName,
    Info,
    InternalName,
    IsNewConnection,
    LastWarning,
    ObjectType,
    Ping,
    Query,
    QueryReadOnly,
    QueryAs,
    QueryAsReadOnly,
    QueryAsNamed,
    QueryAsNamedReadOnly,
    QueryNamed,
    QueryNamedReadOnly,
    QueryRow,
    QueryRowReadOnly,
    QueryRowAs,
    QueryRowAsReadOnly,
    QueryRowAsNamed,
    QueryRowAsNamedReadOnly,
    QueryRowNamed,
    QueryRowNamedReadOnly,
    ServerVersion,
    SetAction,
    SetCallTimeout,
    SetClientIdentifier,
    SetClientInfo,
    SetCurrentSchema,
    SetDbOp,
    SetExternalName,
    SetInternalName,
    SetModule,
    SetStmtCacheSize,
    ShutdownDatabase,
    StartupDatabase,
    Statement,
    Status,
    StmtCacheSize,
    Tag,
    TagFound,
}

impl OracleApi {
    pub fn name() -> String {
        "OracleApi".to_string()
    }

    pub fn db_kind() -> String {
        "oracle".to_string()
    }
}

impl Display for OracleApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Batch => write!(f, "batch"),
            Self::CallTimeout => write!(f, "call_timeout"),
            Self::ChangePassword => write!(f, "change_password"),
            Self::ClearObjectTypeCache => write!(f, "clear_object_cache"),
            Self::CurrentSchema => write!(f, "current_schema"),
            Self::Edition => write!(f, "edition"),
            Self::Execute => write!(f, "execute"),
            Self::ExecuteNamed => write!(f, "execute_named"),
            Self::ExternalName => write!(f, "external_name"),
            Self::Info => write!(f, "info"),
            Self::InternalName => write!(f, "internal_name"),
            Self::IsNewConnection => write!(f, "is_new_connection"),
            Self::LastWarning => write!(f, "last_warning"),
            Self::ObjectType => write!(f, "object_type"),
            Self::Ping => write!(f, "ping"),
            Self::Query => write!(f, "query"),
            Self::QueryReadOnly => write!(f, "query_read_only"),
            Self::QueryAs => write!(f, "query_as"),
            Self::QueryAsReadOnly => write!(f, "query_as_read_only"),
            Self::QueryAsNamed => write!(f, "query_as_named"),
            Self::QueryAsNamedReadOnly => write!(f, "query_as_named_read_only"),
            Self::QueryNamed => write!(f, "query_named"),
            Self::QueryNamedReadOnly => write!(f, "query_named_read_only"),
            Self::QueryRow => write!(f, "query_row"),
            Self::QueryRowReadOnly => write!(f, "query_row_read_only"),
            Self::QueryRowAs => write!(f, "query_row_as"),
            Self::QueryRowAsReadOnly => write!(f, "query_row_as_read_only"),
            Self::QueryRowAsNamed => write!(f, "query_row_as_named"),
            Self::QueryRowAsNamedReadOnly => write!(f, "query_row_as_named_read_only"),
            Self::QueryRowNamed => write!(f, "query_row_named"),
            Self::QueryRowNamedReadOnly => write!(f, "query_row_named_read_only"),
            Self::ServerVersion => write!(f, "server_version"),
            Self::SetAction => write!(f, "set_action"),
            Self::SetCallTimeout => write!(f, "set_call_timeout"),
            Self::SetClientIdentifier => write!(f, "set_client_identifier"),
            Self::SetClientInfo => write!(f, "set_client_info"),
            Self::SetCurrentSchema => write!(f, "set_current_schema"),
            Self::SetDbOp => write!(f, "set_db_op"),
            Self::SetExternalName => write!(f, "set_external_name"),
            Self::SetInternalName => write!(f, "set_internal_name"),
            Self::SetModule => write!(f, "set_module"),
            Self::SetStmtCacheSize => write!(f, "set_stmt_cache_size"),
            Self::ShutdownDatabase => write!(f, "shutdown_database"),
            Self::StartupDatabase => write!(f, "startup_database"),
            Self::Statement => write!(f, "statement"),
            Self::Status => write!(f, "status"),
            Self::StmtCacheSize => write!(f, "stmt_cache_size"),
            Self::Tag => write!(f, "tag"),
            Self::TagFound => write!(f, "tag_found"),
        }
    }
}
