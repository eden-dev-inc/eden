mod ddl;
mod execute;
mod fetch;
mod fetch_all;
mod fetch_bytes;
mod fetch_one;
mod fetch_optional;
mod insert;
mod query;
mod query_read_only;

#[allow(ambiguous_glob_reexports)]
pub use ddl::*;
pub use execute::*;
pub use fetch::*;
pub use fetch_all::*;
pub use fetch_bytes::*;
pub use fetch_one::*;
pub use fetch_optional::*;
pub use insert::*;
pub use query::*;
pub use query_read_only::*;

use crate::output::ClickhouseRow;
use clickhouse_client::query::Query;
use endpoint_derive::{ApiBuilder, DocumentAPI};
use error::EpError;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "ClickhouseApiBuilder")]
pub enum ClickhouseApi {
    Ddl,
    Execute,
    Fetch,
    FetchAll,
    FetchBytes,
    FetchOne,
    FetchOptional,
    Insert,
    Query,
    QueryReadOnly,
}

impl ClickhouseApi {
    pub fn name() -> String {
        "ClickhouseApi".to_string()
    }

    pub fn db_kind() -> String {
        "clickhouse".to_string()
    }

    #[allow(dead_code)]
    pub(crate) fn as_type(&self) -> String {
        format!("{:?}", self).to_lowercase()
    }
}

impl std::fmt::Display for ClickhouseApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ddl => write!(f, "ddl"),
            Self::Execute => write!(f, "execute"),
            Self::Fetch => write!(f, "fetch"),
            Self::FetchAll => write!(f, "fetch_all"),
            Self::FetchBytes => write!(f, "fetch_bytes"),
            Self::FetchOne => write!(f, "fetch_one"),
            Self::FetchOptional => write!(f, "fetch_optional"),
            Self::Query => write!(f, "query"),
            Self::QueryReadOnly => write!(f, "query_read_only"),
            Self::Insert => write!(f, "insert"),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct Param {
    name: String,
    value: fetch::JsonValue,
}

pub async fn fetch_all_rows(query: Query) -> Result<Vec<ClickhouseRow>, EpError> {
    let mut bytes_cursor = query.fetch_bytes("JSON").map_err(EpError::request)?;

    let mut json_bytes = Vec::new();
    while let Some(chunk) = bytes_cursor.next().await.map_err(EpError::request)? {
        json_bytes.extend_from_slice(&chunk);
    }

    let response: serde_json::Value =
        serde_json::from_slice(&json_bytes).map_err(|e| EpError::request(format!("Failed to parse JSON response: {}", e)))?;

    let data = response.get("data").and_then(|d| d.as_array()).ok_or_else(|| EpError::request("Missing 'data' array in response"))?;

    let mut rows: Vec<ClickhouseRow> = vec![];

    for row_value in data {
        if let serde_json::Value::Object(map) = row_value {
            let mut current_row = vec![];
            for (key, value) in map {
                current_row.push((key.clone(), value.clone()));
            }
            rows.push(ClickhouseRow::from(current_row));
        } else {
            return Err(EpError::request("Expected JSON object for row data"));
        }
    }

    Ok(rows)
}
