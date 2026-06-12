use borsh::{BorshDeserialize, BorshSerialize};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Structured error from an LLM provider API call.
///
/// Includes category information (rate-limit, server fault, auth, parse) so
/// callers can make retry decisions without string matching.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct LlmProviderError {
    /// Provider name, e.g. `"anthropic"` or `"openai"`.
    pub provider: String,
    /// Model identifier used when the error occurred.
    pub model: String,
    /// HTTP status code from the provider, if a response was received.
    pub http_status: Option<u16>,
    /// Whether the error is transient and worth retrying.
    ///
    /// `true` for HTTP 429, 500, 502, 503 and Anthropic's 529.
    pub retryable: bool,
    /// Description of the failure (truncated at source).
    pub message: String,
}

impl LlmProviderError {
    /// Construct a provider error, deriving `retryable` from `http_status`.
    pub fn new(provider: impl Into<String>, model: impl Into<String>, http_status: Option<u16>, message: impl Into<String>) -> Self {
        let retryable = http_status.map(is_retryable_status).unwrap_or(false);
        Self {
            provider: provider.into(),
            model: model.into(),
            http_status,
            retryable,
            message: message.into(),
        }
    }

    /// Returns the sub-error code: `0x01` retryable, `0x02` non-retryable.
    pub fn error_code(&self) -> u8 {
        if self.retryable { 0x01 } else { 0x02 }
    }
}

/// Returns `true` for transient HTTP status codes worth retrying.
pub fn is_retryable_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 529)
}

impl fmt::Display for LlmProviderError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.http_status {
            Some(status) => write!(
                f,
                "provider={} model={} status={} retryable={}: {}",
                self.provider, self.model, status, self.retryable, self.message
            ),
            None => write!(f, "provider={} model={} retryable={}: {}", self.provider, self.model, self.retryable, self.message),
        }
    }
}
