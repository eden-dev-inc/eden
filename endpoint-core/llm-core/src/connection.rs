use crate::credential::{LlmCredential, ResolvedLlmConnection};
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::EpConnection;
use ep_core::impl_connection;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use llm::builder::LLMBackend;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use utoipa::ToSchema;
use uuid::Uuid;

/// Declarative configuration for connecting to an LLM provider.
///
/// Wire format flattens the provider-tagged `target` to the top level so the
/// shape is `{"provider": "...", "defaults": {...}, "credential_id": "...",
/// ...}` and any provider-specific fields (e.g. Azure deployment/api-version)
/// appear alongside.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct LlmConnection {
    #[serde(flatten)]
    pub target: LlmTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_api_key: Option<String>,
}

impl LlmConnection {
    pub fn provider(&self) -> LlmProvider {
        self.target.provider()
    }

    pub fn defaults(&self) -> &LlmConnectionDefaults {
        self.target.defaults()
    }

    pub fn defaults_mut(&mut self) -> &mut LlmConnectionDefaults {
        self.target.defaults_mut()
    }

    fn normalized_inline_api_key(&self) -> Option<String> {
        normalize_optional_string(self.inline_api_key.clone())
    }
}

/// Default inference parameters applied when no per-request override is supplied.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, Default)]
pub struct LlmConnectionDefaults {
    pub model: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_k: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub base_url_override: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(rename_all = "lowercase", example = "openai")]
pub enum LlmProvider {
    OpenAI,
    Anthropic,
    Ollama,
    OpenRouter,
    /// Azure OpenAI using the classic deployment-path API
    /// (`{endpoint}/openai/deployments/{deployment}/chat/completions?api-version=…`).
    AzureOpenAI,
}

impl LlmProvider {
    /// Canonical lowercase string identifier used in the API wire format,
    /// the DB `provider` column, and log output.
    pub const fn as_str(&self) -> &'static str {
        match self {
            LlmProvider::OpenAI => "openai",
            LlmProvider::Anthropic => "anthropic",
            LlmProvider::Ollama => "ollama",
            LlmProvider::OpenRouter => "openrouter",
            LlmProvider::AzureOpenAI => "azureopenai",
        }
    }

    /// Whether this provider's API natively supports parallel tool calls
    /// in a single assistant turn. Used to set a sensible default for
    /// `parallel_tool_calls` when the caller hasn't pinned it.
    pub const fn supports_parallel_tool_calls(&self) -> bool {
        matches!(
            self,
            LlmProvider::OpenAI | LlmProvider::Anthropic | LlmProvider::OpenRouter | LlmProvider::AzureOpenAI
        )
    }

    /// Providers that use OpenAI-compatible request/schema constraints.
    pub const fn is_openai_family(&self) -> bool {
        matches!(self, LlmProvider::OpenAI | LlmProvider::AzureOpenAI)
    }
}

impl TryFrom<&LlmProvider> for LLMBackend {
    type Error = EpError;

    fn try_from(value: &LlmProvider) -> Result<Self, Self::Error> {
        match value {
            LlmProvider::OpenAI => Ok(LLMBackend::OpenAI),
            LlmProvider::Anthropic => Ok(LLMBackend::Anthropic),
            LlmProvider::Ollama => Ok(LLMBackend::Ollama),
            LlmProvider::OpenRouter => Ok(LLMBackend::OpenRouter),
            // Azure goes through a hand-rolled HTTP path (see
            // `ChatRoute::AzureOpenAiClassic`) because the upstream `llm`
            // crate's Azure backend hardcodes the `/openai/v1/` unified API.
            LlmProvider::AzureOpenAI => Err(EpError::connect(
                "LLMBackend conversion not supported for Azure OpenAI — handled by the classic route",
            )),
        }
    }
}

impl fmt::Display for LlmProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for LlmProvider {
    type Err = EpError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_ascii_lowercase().as_str() {
            "openai" => Ok(LlmProvider::OpenAI),
            "anthropic" => Ok(LlmProvider::Anthropic),
            "ollama" => Ok(LlmProvider::Ollama),
            "openrouter" => Ok(LlmProvider::OpenRouter),
            // Canonical form is `azureopenai`; accept the more readable
            // hyphen/underscore variants as well.
            "azureopenai" | "azure-openai" | "azure_openai" => Ok(LlmProvider::AzureOpenAI),
            other => Err(EpError::request(format!("unknown LLM provider `{}`", other))),
        }
    }
}

impl Serialize for LlmProvider {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for LlmProvider {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = <String as Deserialize>::deserialize(deserializer)?;
        raw.parse().map_err(serde::de::Error::custom)
    }
}

impl_connection!(LlmConnection, EpKind::Llm);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — provider tag and default inference parameters.
///
/// Tagged on the wire by a lowercase `provider` discriminant so additional
/// provider-specific fields (e.g. Azure's deployment + api-version) can be
/// added to individual variants without leaking through to the others.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[serde(tag = "provider", rename_all = "lowercase")]
pub enum LlmTarget {
    #[serde(alias = "OpenAI", alias = "openAI")]
    OpenAI {
        #[serde(default)]
        defaults: LlmConnectionDefaults,
    },
    #[serde(alias = "Anthropic")]
    Anthropic {
        #[serde(default)]
        defaults: LlmConnectionDefaults,
    },
    #[serde(alias = "Ollama")]
    Ollama {
        #[serde(default)]
        defaults: LlmConnectionDefaults,
    },
    #[serde(alias = "OpenRouter", alias = "openRouter")]
    OpenRouter {
        #[serde(default)]
        defaults: LlmConnectionDefaults,
    },
    /// Azure OpenAI using the classic per-deployment URL shape.
    ///
    /// `base_url` (or `base_url_override`) must be the resource host, e.g.
    /// `https://my-resource.openai.azure.com`; the route appends
    /// `/openai/deployments/{deployment_id}/chat/completions` and the
    /// `api-version` query parameter from `classic`.
    #[serde(alias = "AzureOpenAI", alias = "azureOpenAI", alias = "azure_openai", alias = "azure-openai")]
    AzureOpenAI {
        #[serde(default)]
        defaults: LlmConnectionDefaults,
        classic: AzureOpenAiClassicConfig,
    },
}

impl Default for LlmTarget {
    fn default() -> Self {
        LlmTarget::OpenAI { defaults: LlmConnectionDefaults::default() }
    }
}

impl LlmTarget {
    /// Construct a target from a provider tag and shared inference defaults.
    /// Variants that need extra typed config (Azure) use their type-level
    /// default for that config.
    pub fn new(provider: LlmProvider, defaults: LlmConnectionDefaults) -> Self {
        match provider {
            LlmProvider::OpenAI => LlmTarget::OpenAI { defaults },
            LlmProvider::Anthropic => LlmTarget::Anthropic { defaults },
            LlmProvider::Ollama => LlmTarget::Ollama { defaults },
            LlmProvider::OpenRouter => LlmTarget::OpenRouter { defaults },
            LlmProvider::AzureOpenAI => LlmTarget::AzureOpenAI { defaults, classic: AzureOpenAiClassicConfig::default() },
        }
    }

    pub fn provider(&self) -> LlmProvider {
        match self {
            LlmTarget::OpenAI { .. } => LlmProvider::OpenAI,
            LlmTarget::Anthropic { .. } => LlmProvider::Anthropic,
            LlmTarget::Ollama { .. } => LlmProvider::Ollama,
            LlmTarget::OpenRouter { .. } => LlmProvider::OpenRouter,
            LlmTarget::AzureOpenAI { .. } => LlmProvider::AzureOpenAI,
        }
    }

    pub fn defaults(&self) -> &LlmConnectionDefaults {
        match self {
            LlmTarget::OpenAI { defaults }
            | LlmTarget::Anthropic { defaults }
            | LlmTarget::Ollama { defaults }
            | LlmTarget::OpenRouter { defaults }
            | LlmTarget::AzureOpenAI { defaults, .. } => defaults,
        }
    }

    pub fn defaults_mut(&mut self) -> &mut LlmConnectionDefaults {
        match self {
            LlmTarget::OpenAI { defaults }
            | LlmTarget::Anthropic { defaults }
            | LlmTarget::Ollama { defaults }
            | LlmTarget::OpenRouter { defaults }
            | LlmTarget::AzureOpenAI { defaults, .. } => defaults,
        }
    }

    /// Azure classic config, if this is an Azure target. Returns `None`
    /// for all other variants.
    pub fn azure_classic(&self) -> Option<&AzureOpenAiClassicConfig> {
        match self {
            LlmTarget::AzureOpenAI { classic, .. } => Some(classic),
            _ => None,
        }
    }
}

/// Configuration for the Azure OpenAI classic (per-deployment) API.
///
/// The request URL is
/// `{base_url}/openai/deployments/{deployment_id}/chat/completions?api-version={api_version}`
/// with an `api-key` auth header. The deployment selects the model, so the
/// request body does **not** include a `model` field.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, Default)]
pub struct AzureOpenAiClassicConfig {
    /// Deployment name as configured in the Azure resource. Substituted into
    /// the URL path; not the same as `defaults.model`, which Eden keeps for
    /// inference identity (context-window estimates, response metadata).
    pub deployment_id: String,
    /// Azure API version (e.g. `2024-08-01-preview`). Sent as the
    /// `api-version` query parameter.
    pub api_version: String,
    /// Which token-limit field to serialize on the request body. Newer Azure
    /// deployments (o-series, GPT-5 family on recent api-versions) require
    /// `max_completion_tokens`; older ones accept `max_tokens`. `Auto`
    /// defaults to `max_completion_tokens`, which is the safer choice for
    /// recently-provisioned deployments.
    #[serde(default)]
    pub max_tokens_field: AzureMaxTokensField,
}

/// Which token-limit field name to send to Azure OpenAI on the chat
/// completions request body.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum AzureMaxTokensField {
    /// Pick automatically (currently `max_completion_tokens`).
    #[default]
    Auto,
    MaxTokens,
    MaxCompletionTokens,
}

/// Connection credentials — credential reference or inline key.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct LlmCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_id: Option<Uuid>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inline_api_key: Option<String>,
}

impl LlmConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &LlmTarget, creds: &LlmCredentials) -> Self {
        Self {
            target: target.clone(),
            credential_id: creds.credential_id,
            inline_api_key: creds.inline_api_key.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(LlmTarget, LlmCredentials)> {
        Ok((
            self.target.clone(),
            LlmCredentials {
                credential_id: self.credential_id,
                inline_api_key: self.inline_api_key.clone(),
            },
        ))
    }
}

impl LlmProvider {
    pub fn default_base_url(&self) -> Option<&'static str> {
        match self {
            LlmProvider::OpenAI => Some("https://api.openai.com/v1"),
            LlmProvider::Anthropic => Some("https://api.anthropic.com"),
            LlmProvider::Ollama => Some("http://localhost:11434"),
            LlmProvider::OpenRouter => Some("https://openrouter.ai/api/v1"),
            // Azure OpenAI URLs are per-resource (each customer has their own
            // subdomain), so there is no sensible global default.
            LlmProvider::AzureOpenAI => None,
        }
    }
}

impl LlmConnection {
    pub fn resolve(&self, credential: Option<&LlmCredential>) -> Result<ResolvedLlmConnection, EpError> {
        let provider = self.provider();
        let defaults = self.defaults().clone().normalized();
        if defaults.model.trim().is_empty() {
            return Err(EpError::connect(format!("Missing default model for provider `{}`", provider)));
        }
        validate_azure_base_url(provider, defaults.base_url_override.as_deref())?;
        let provider_config = self.target.resolved_provider_config()?;

        let inline_api_key = self.normalized_inline_api_key();

        match (&self.credential_id, credential) {
            (Some(expected), Some(store)) => {
                if expected != &store.id {
                    return Err(EpError::connect(format!(
                        "Mismatched credential id for provider `{}`: expected {}, got {}",
                        provider, expected, store.id
                    )));
                }

                if store.provider != provider {
                    return Err(EpError::connect(format!(
                        "Credential `{}` belongs to provider `{}` but the connection is configured for `{}`",
                        store.id, store.provider, provider
                    )));
                }

                let credential_base_url = normalize_optional_string(store.base_url.clone());
                validate_azure_base_url(provider, credential_base_url.as_deref())?;

                Ok(ResolvedLlmConnection {
                    provider,
                    credential_id: Some(*expected),
                    api_key: normalize_optional_string(store.api_key.clone()),
                    credential_base_url,
                    defaults,
                    provider_config,
                })
            }
            (Some(expected), None) => Err(EpError::connect(format!("Missing credential `{}` for provider `{}`", expected, provider))),
            (None, _) => Ok(ResolvedLlmConnection {
                provider,
                credential_id: None,
                api_key: inline_api_key,
                credential_base_url: None,
                defaults,
                provider_config,
            }),
        }
    }
}

fn validate_azure_base_url(provider: LlmProvider, base_url: Option<&str>) -> Result<(), EpError> {
    if provider != LlmProvider::AzureOpenAI {
        return Ok(());
    }

    let Some(base_url) = base_url.map(str::trim).filter(|base_url| !base_url.is_empty()) else {
        return Ok(());
    };

    let parsed = reqwest::Url::parse(base_url)
        .map_err(|err| EpError::connect(format!("Azure OpenAI base URL `{base_url}` is not a valid URL: {err}")))?;
    let path = parsed.path();
    if !path.is_empty() && path != "/" {
        return Err(EpError::connect(
            "Azure OpenAI base URL must be the resource root and must not include a path, query, or fragment",
        ));
    }
    if parsed.query().is_some() || parsed.fragment().is_some() {
        return Err(EpError::connect(
            "Azure OpenAI base URL must be the resource root and must not include a path, query, or fragment",
        ));
    }

    Ok(())
}

impl LlmTarget {
    /// Build the runtime-side `ResolvedProviderConfig`, validating any
    /// per-variant invariants (e.g. Azure requires a non-empty
    /// `deployment_id` and `api_version`).
    fn resolved_provider_config(&self) -> Result<crate::credential::ResolvedProviderConfig, EpError> {
        match self {
            LlmTarget::AzureOpenAI { classic, .. } => {
                if classic.deployment_id.trim().is_empty() {
                    return Err(EpError::connect("Azure OpenAI requires a non-empty `deployment_id`"));
                }
                if classic.api_version.trim().is_empty() {
                    return Err(EpError::connect("Azure OpenAI requires a non-empty `api_version`"));
                }
                Ok(crate::credential::ResolvedProviderConfig::AzureClassic(classic.clone()))
            }
            _ => Ok(crate::credential::ResolvedProviderConfig::None),
        }
    }
}

impl LlmConnectionDefaults {
    fn normalized(mut self) -> Self {
        if let Some(url) = self.base_url_override.take() {
            let trimmed = url.trim().to_string();
            if !trimmed.is_empty() {
                self.base_url_override = Some(trimmed);
            }
        }
        self
    }
}

fn trimmed_string(value: String) -> String {
    value.trim().to_string()
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value.map(trimmed_string).filter(|trimmed| !trimmed.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modern_json_round_trips() {
        let json = r#"
        {
            "provider": "anthropic",
            "credential_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
            "inline_api_key": null,
            "defaults": {
                "model": "claude-3-sonnet",
                "temperature": 0.4,
                "max_tokens": 1024,
                "base_url_override": "https://api.anthropic.com"
            }
        }
        "#;

        let conn: LlmConnection = serde_json::from_str(json).expect("modern json should parse");
        assert_eq!(conn.provider(), LlmProvider::Anthropic);
        assert!(conn.normalized_inline_api_key().is_none());
        assert_eq!(
            conn.credential_id,
            Some(Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap_or_default())
        );

        let defaults = conn.defaults().clone().normalized();
        assert_eq!(defaults.model, "claude-3-sonnet");
        assert_eq!(defaults.temperature, Some(0.4_f32));
        assert_eq!(defaults.max_tokens, Some(1024));
        assert_eq!(defaults.base_url_override.as_deref(), Some("https://api.anthropic.com"));

        // Round-trip serialization emits canonical lowercase, flattened.
        let reserialized = serde_json::to_value(&conn).expect("serialize");
        assert_eq!(reserialized["provider"], "anthropic");
        assert_eq!(reserialized["defaults"]["model"], "claude-3-sonnet");
    }

    #[test]
    fn provider_parse_is_case_insensitive() {
        assert_eq!("openai".parse::<LlmProvider>().unwrap(), LlmProvider::OpenAI);
        assert_eq!("OpenAI".parse::<LlmProvider>().unwrap(), LlmProvider::OpenAI);
        assert_eq!("  OPENAI  ".parse::<LlmProvider>().unwrap(), LlmProvider::OpenAI);
        assert!("ollamaa".parse::<LlmProvider>().is_err());
    }

    #[test]
    fn azure_provider_accepts_canonical_and_aliases() {
        // Canonical form is lowercase no-separator (mirrors `openai`/`openrouter`).
        assert_eq!("azureopenai".parse::<LlmProvider>().unwrap(), LlmProvider::AzureOpenAI);
        assert_eq!("Azure-OpenAI".parse::<LlmProvider>().unwrap(), LlmProvider::AzureOpenAI);
        assert_eq!("azure_openai".parse::<LlmProvider>().unwrap(), LlmProvider::AzureOpenAI);
        assert_eq!(LlmProvider::AzureOpenAI.to_string(), "azureopenai");
        // Display emits canonical, serde round-trip preserves it.
        let value = serde_json::to_value(LlmProvider::AzureOpenAI).expect("serialize");
        assert_eq!(value, serde_json::json!("azureopenai"));
    }

    #[test]
    fn azure_target_round_trips() {
        let json = r#"
        {
            "provider": "azureopenai",
            "defaults": {"model": "gpt-4o"},
            "classic": {
                "deployment_id": "prod-gpt4o",
                "api_version": "2024-08-01-preview",
                "max_tokens_field": "max_completion_tokens"
            },
            "credential_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
        }
        "#;
        let conn: LlmConnection = serde_json::from_str(json).expect("azure connection parses");
        assert_eq!(conn.provider(), LlmProvider::AzureOpenAI);
        let classic = conn.target.azure_classic().expect("azure classic config");
        assert_eq!(classic.deployment_id, "prod-gpt4o");
        assert_eq!(classic.api_version, "2024-08-01-preview");
        assert_eq!(classic.max_tokens_field, AzureMaxTokensField::MaxCompletionTokens);

        // Reserialize and verify the wire shape is preserved.
        let reserialized = serde_json::to_value(&conn).expect("serialize");
        assert_eq!(reserialized["provider"], "azureopenai");
        assert_eq!(reserialized["classic"]["deployment_id"], "prod-gpt4o");
    }

    #[test]
    fn target_provider_aliases_deserialize_and_emit_canonical_lowercase() {
        let cases = [
            ("OpenAI", "openai"),
            ("Anthropic", "anthropic"),
            ("Ollama", "ollama"),
            ("OpenRouter", "openrouter"),
            ("openai", "openai"),
            ("anthropic", "anthropic"),
            ("ollama", "ollama"),
            ("openrouter", "openrouter"),
            ("azureopenai", "azureopenai"),
        ];

        for (wire_provider, canonical_provider) in cases {
            let value = if wire_provider == "azureopenai" {
                serde_json::json!({
                    "provider": wire_provider,
                    "defaults": {"model": "gpt-4o"},
                    "classic": {
                        "deployment_id": "prod-gpt4o",
                        "api_version": "2024-08-01-preview"
                    }
                })
            } else {
                serde_json::json!({
                    "provider": wire_provider,
                    "defaults": {"model": "gpt-4o"}
                })
            };

            let target: LlmTarget = serde_json::from_value(value).expect("target provider alias should deserialize");
            let reserialized = serde_json::to_value(&target).expect("target should serialize");
            assert_eq!(reserialized["provider"], canonical_provider);
        }
    }

    #[test]
    fn azure_resolve_rejects_missing_deployment_or_api_version() {
        let conn = LlmConnection {
            target: LlmTarget::AzureOpenAI {
                defaults: LlmConnectionDefaults { model: "gpt-4o".into(), ..Default::default() },
                classic: AzureOpenAiClassicConfig {
                    deployment_id: "".into(),
                    api_version: "2024-08-01".into(),
                    ..Default::default()
                },
            },
            credential_id: None,
            inline_api_key: Some("k".into()),
        };
        let err = conn.resolve(None).expect_err("missing deployment_id should fail");
        assert!(format!("{err}").contains("deployment_id"));
    }

    #[test]
    fn azure_resolve_rejects_base_url_with_path() {
        let conn = azure_connection_with_base_url("https://example.openai.azure.com/openai");

        let err = conn.resolve(None).expect_err("base URL path should fail");

        assert!(format!("{err}").contains("must be the resource root"));
    }

    #[test]
    fn azure_resolve_rejects_base_url_with_query() {
        let conn = azure_connection_with_base_url("https://example.openai.azure.com?api-version=preview");

        let err = conn.resolve(None).expect_err("base URL query should fail");

        assert!(format!("{err}").contains("must be the resource root"));
    }

    #[test]
    fn resolve_fails_when_model_missing() {
        let conn = LlmConnection {
            target: LlmTarget::OpenAI {
                defaults: LlmConnectionDefaults { model: "".into(), ..Default::default() },
            },
            credential_id: None,
            inline_api_key: Some("key".into()),
        };

        let err = conn.resolve(None).expect_err("missing model should fail");
        assert!(format!("{err}").contains("Missing default model"));
    }

    fn azure_connection_with_base_url(base_url: &str) -> LlmConnection {
        LlmConnection {
            target: LlmTarget::AzureOpenAI {
                defaults: LlmConnectionDefaults {
                    model: "gpt-4o".into(),
                    base_url_override: Some(base_url.to_string()),
                    ..Default::default()
                },
                classic: AzureOpenAiClassicConfig {
                    deployment_id: "prod-gpt4o".into(),
                    api_version: "2024-08-01-preview".into(),
                    ..Default::default()
                },
            },
            credential_id: None,
            inline_api_key: Some("k".into()),
        }
    }
}
