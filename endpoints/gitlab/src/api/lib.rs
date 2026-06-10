pub mod custom;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum GitlabApi {
    Custom,
}

impl Display for GitlabApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Custom => write!(f, "custom"),
        }
    }
}
