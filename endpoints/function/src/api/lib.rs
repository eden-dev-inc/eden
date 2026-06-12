mod invoke;

pub use invoke::*;

use endpoint_derive::{ApiBuilder, DocumentAPI};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "FunctionApiBuilder")]
pub enum FunctionApi {
    Invoke,
}

impl FunctionApi {
    pub fn name() -> String {
        "FunctionApi".to_string()
    }

    pub fn db_kind() -> String {
        "function".to_string()
    }
}

impl Display for FunctionApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Invoke => write!(f, "invoke"),
        }
    }
}
