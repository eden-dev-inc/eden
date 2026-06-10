//! Marketplace configuration for SkillsMP integration.
//!
//! Maps to `[marketplace]` in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Marketplace configuration for SkillsMP skill browsing and import.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct MarketplaceConfig {
    /// Enable marketplace integration.
    pub enabled: bool,
    /// SkillsMP API base URL.
    pub base_url: String,
    /// SkillsMP API key (falls back to `SKILLSMP_API_KEY` env var).
    #[serde(skip_serializing)]
    pub api_key: Option<String>,
    /// Search cache TTL in seconds.
    pub search_cache_ttl_secs: u64,
    /// GitHub token for fetching SKILL.md from private repos.
    #[serde(skip_serializing)]
    pub github_token: Option<String>,
}

impl Default for MarketplaceConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            base_url: "https://skillsmp.com/api/v1".into(),
            api_key: None,
            search_cache_ttl_secs: 300,
            github_token: None,
        }
    }
}
