pub mod crawl;
pub mod extract;
pub mod map;
pub mod research;
pub mod research_status;
pub mod search;

use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Serialize, Deserialize, Clone, utoipa::ToSchema)]
pub enum TavilyApi {
    Search,
    Extract,
    Crawl,
    Map,
    Research,
    ResearchStatus,
}

impl Display for TavilyApi {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Self::Search => write!(f, "search"),
            Self::Extract => write!(f, "extract"),
            Self::Crawl => write!(f, "crawl"),
            Self::Map => write!(f, "map"),
            Self::Research => write!(f, "research"),
            Self::ResearchStatus => write!(f, "research_status"),
        }
    }
}
