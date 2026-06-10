mod request;
mod set_param;

use endpoint_derive::{ApiBuilder, DocumentAPI};

use serde::{Deserialize, Serialize};
use std::fmt::Display;

pub use request::*;
pub use set_param::*;

#[derive(Debug, Serialize, Deserialize, Clone, DocumentAPI, ApiBuilder)]
#[api_builder(builder_name = "LlmApiBuilder")]
pub enum LlmApi {
    Request,
    SetParam,
}

impl LlmApi {
    pub fn name() -> String {
        "LlmApi".to_string()
    }

    pub fn db_kind() -> String {
        "llm".to_string()
    }
}

impl Display for LlmApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Request => write!(f, "request"),
            Self::SetParam => write!(f, "set_param"),
        }
    }
}
