pub mod create_record;
pub mod delete_record;
pub mod describe_global;
pub mod describe_object;
pub mod get_record;
pub mod query;
pub mod search;
pub mod update_record;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum SalesforceApi {
    Query,
    Search,
    GetRecord,
    DescribeObject,
    DescribeGlobal,
    CreateRecord,
    UpdateRecord,
    DeleteRecord,
}

impl Display for SalesforceApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Query => write!(f, "query"),
            Self::Search => write!(f, "search"),
            Self::GetRecord => write!(f, "get_record"),
            Self::DescribeObject => write!(f, "describe_object"),
            Self::DescribeGlobal => write!(f, "describe_global"),
            Self::CreateRecord => write!(f, "create_record"),
            Self::UpdateRecord => write!(f, "update_record"),
            Self::DeleteRecord => write!(f, "delete_record"),
        }
    }
}
