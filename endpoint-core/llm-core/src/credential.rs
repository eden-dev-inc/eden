use crate::connection::{AzureOpenAiClassicConfig, LlmConnectionDefaults, LlmProvider};
use borsh::{BorshDeserialize, BorshSerialize};
use error::EpError;
use serde::{Deserialize, Serialize};
use std::fmt;
use utoipa::ToSchema;
use uuid::Uuid;

/// Persistent credential information shared across LLM endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct LlmCredential {
    pub id: Uuid,
    pub provider: LlmProvider,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,
}

impl LlmCredential {
    /// Returns true when the credential carries a non-empty API key.
    pub fn has_api_key(&self) -> bool {
        self.api_key.as_ref().map(|key| !key.trim().is_empty()).unwrap_or(false)
    }
}

/// Fully resolved connection used at runtime by the LLM client pool.
#[derive(Debug, Clone)]
pub struct ResolvedLlmConnection {
    pub provider: LlmProvider,
    pub credential_id: Option<Uuid>,
    pub api_key: Option<String>,
    pub credential_base_url: Option<String>,
    pub defaults: LlmConnectionDefaults,
    /// Provider-specific config carried through from `LlmTarget`. Currently
    /// only Azure OpenAI populates a variant.
    pub provider_config: ResolvedProviderConfig,
}

/// Per-provider runtime config that needs to travel alongside the resolved
/// connection. Stays an enum (rather than a generic `Value`) so the route
/// handler can pattern-match it without re-parsing.
#[derive(Debug, Clone, Default)]
pub enum ResolvedProviderConfig {
    #[default]
    None,
    AzureClassic(AzureOpenAiClassicConfig),
}

impl ResolvedLlmConnection {
    /// Determines the effective base URL taking into account overrides and credential defaults.
    pub fn base_url(&self) -> Result<String, EpError> {
        fn normalize_url(value: &str) -> String {
            value.trim().trim_end_matches('/').to_string()
        }

        if let Some(url) = &self.defaults.base_url_override {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                return Ok(normalize_url(trimmed));
            }
        }

        if let Some(url) = &self.credential_base_url {
            let trimmed = url.trim();
            if !trimmed.is_empty() {
                return Ok(normalize_url(trimmed));
            }
        }

        if let Some(default_url) = self.provider.default_base_url() {
            return Ok(normalize_url(default_url));
        }

        Err(EpError::connect(format!("Missing base URL for provider `{}`", self.provider)))
    }

    pub fn effective_model(&self, override_model: Option<String>) -> String {
        override_model.unwrap_or_else(|| self.defaults.model.clone())
    }
}

impl fmt::Display for LlmCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "id: {}, provider: {}, label: {:?}, description: {:?}",
            self.id, self.provider, self.label, self.description
        )
    }
}
