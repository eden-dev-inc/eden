//! Internal LLM service configuration (provider, model tiers, API key).
//!
//! Maps to the `[services.llm]` section in `eden.toml`.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// A single LLM tier (large, medium, small).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct LlmTier {
    pub model: String,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}

/// Internal LLM service configuration.
///
/// Supports a flat single-model config (backward-compatible) and named
/// tiers. Tier-specific fields fall back to top-level defaults.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct InternalLlmConfig {
    pub provider: Option<String>,
    /// Used when no tier is specified.
    pub model: Option<String>,
    pub api_key: Option<String>,
    pub base_url: Option<String>,
    pub system_prompt: Option<String>,
    pub temperature: Option<f32>,
    pub max_tokens: Option<u32>,
    /// Optional path where eden-service publishes the data-plane LLM gateway
    /// control-plane snapshot for standalone eden_gateway processes.
    pub gateway_snapshot_publish_path: Option<String>,
    /// Optional publish interval for `gateway_snapshot_publish_path`.
    pub gateway_snapshot_publish_interval_secs: Option<u64>,
    /// Named tiers.
    #[serde(default)]
    pub tiers: HashMap<String, LlmTier>,
}

impl InternalLlmConfig {
    /// Resolve a tier by name; per-tier fields override top-level defaults.
    /// Returns `None` when no matching tier or top-level model exists.
    pub fn resolve_tier(&self, tier_name: &str) -> Option<ResolvedLlmTier> {
        if let Some(tier) = self.tiers.get(tier_name) {
            if tier.model.is_empty() {
                return None;
            }
            Some(ResolvedLlmTier {
                model: tier.model.clone(),
                provider: tier.provider.clone().or_else(|| self.provider.clone()),
                base_url: tier.base_url.clone().or_else(|| self.base_url.clone()),
                api_key: tier.api_key.clone().or_else(|| self.api_key.clone()),
            })
        } else {
            // Top-level model fallback.
            let model = self.model.as_ref().filter(|m| !m.is_empty())?.clone();
            Some(ResolvedLlmTier {
                model,
                provider: self.provider.clone(),
                base_url: self.base_url.clone(),
                api_key: self.api_key.clone(),
            })
        }
    }
}

/// Resolved LLM tier with concrete provider/model/key/url.
#[derive(Debug, Clone)]
pub struct ResolvedLlmTier {
    pub model: String,
    pub provider: Option<String>,
    pub base_url: Option<String>,
    pub api_key: Option<String>,
}
