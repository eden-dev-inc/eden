mod batch_objects;
mod create_object;
mod delete_object;
mod get_object;
mod get_schema;
mod graphql;
mod list_objects;
mod update_object;

pub use batch_objects::*;
pub use create_object::*;
pub use delete_object::*;
pub use get_object::*;
pub use get_schema::*;
pub use graphql::*;
pub use list_objects::*;
pub use update_object::*;

use std::fmt::Display;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "WeaviateApiBuilder")]
pub enum WeaviateApi {
    GraphQL,
    CreateObject,
    GetObject,
    ListObjects,
    UpdateObject,
    DeleteObject,
    BatchObjects,
    GetSchema,
}

impl WeaviateApi {
    pub fn name() -> String {
        "WeaviateApi".to_string()
    }

    pub fn db_kind() -> String {
        "weaviate".to_string()
    }
}

impl Display for WeaviateApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::GraphQL => write!(f, "graphql"),
            Self::CreateObject => write!(f, "create_object"),
            Self::GetObject => write!(f, "get_object"),
            Self::ListObjects => write!(f, "list_objects"),
            Self::UpdateObject => write!(f, "update_object"),
            Self::DeleteObject => write!(f, "delete_object"),
            Self::BatchObjects => write!(f, "batch_objects"),
            Self::GetSchema => write!(f, "get_schema"),
        }
    }
}
