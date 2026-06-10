pub mod custom;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum GoogleWorkspaceApi {
    Custom,
}

impl Display for GoogleWorkspaceApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Custom => write!(f, "custom"),
        }
    }
}
