use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum HttpApi {
    Read,
}

impl Display for HttpApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Read => write!(f, "read"),
        }
    }
}
