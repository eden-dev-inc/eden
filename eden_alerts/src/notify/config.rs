//! Notification configuration and defaults.

use reqwest::Url;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use super::NotifyError;

/// Default values for notification settings.
pub mod defaults {
    pub const ENABLED: bool = true;
    pub const RATE_LIMIT_MAX_PER_WINDOW: usize = 20;
    pub const RATE_LIMIT_WINDOW_SECS: u64 = 60;
    pub const DEDUP_WINDOW_SECS: u64 = 300;
}

/// Top-level notification configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub dedup: DedupConfig,
    #[serde(default)]
    pub backends: Vec<BackendConfig>,
}

/// Rate limiting configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RateLimitConfig {
    pub max_per_window: usize,
    pub window_secs: u64,
}

/// Deduplication configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DedupConfig {
    pub window_secs: u64,
}

/// Backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BackendConfig {
    Slack(SlackConfig),
    Webhook(WebhookConfig),
}

/// Slack backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackConfig {
    pub webhook_url: String,
    pub channel: Option<String>,
    pub username: Option<String>,
    pub icon_emoji: Option<String>,
}

/// Generic webhook backend configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    pub url: String,
    #[serde(default)]
    pub headers: Vec<WebhookHeader>,
}

/// Webhook header configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookHeader {
    pub name: String,
    pub value: String,
}

fn default_enabled() -> bool {
    defaults::ENABLED
}

impl Default for NotifyConfig {
    fn default() -> Self {
        Self {
            enabled: defaults::ENABLED,
            rate_limit: RateLimitConfig::default(),
            dedup: DedupConfig::default(),
            backends: Vec::new(),
        }
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            max_per_window: defaults::RATE_LIMIT_MAX_PER_WINDOW,
            window_secs: defaults::RATE_LIMIT_WINDOW_SECS,
        }
    }
}

impl Default for DedupConfig {
    fn default() -> Self {
        Self { window_secs: defaults::DEDUP_WINDOW_SECS }
    }
}

impl RateLimitConfig {
    pub fn window(&self) -> Duration {
        Duration::from_secs(self.window_secs)
    }

    pub fn validate(&self) -> Result<(), NotifyError> {
        if self.max_per_window == 0 {
            return Err(NotifyError::Config("rate_limit.max_per_window must be > 0".into()));
        }
        if self.window_secs == 0 {
            return Err(NotifyError::Config("rate_limit.window_secs must be > 0".into()));
        }
        Ok(())
    }
}

impl DedupConfig {
    pub fn window(&self) -> Duration {
        Duration::from_secs(self.window_secs)
    }

    pub fn validate(&self) -> Result<(), NotifyError> {
        if self.window_secs == 0 {
            return Err(NotifyError::Config("dedup.window_secs must be > 0".into()));
        }
        Ok(())
    }
}

impl NotifyConfig {
    /// Validate notification configuration settings.
    pub fn validate(&self) -> Result<(), NotifyError> {
        self.rate_limit.validate()?;
        self.dedup.validate()?;
        for backend in &self.backends {
            backend.validate()?;
        }
        Ok(())
    }
}

impl BackendConfig {
    pub fn validate(&self) -> Result<(), NotifyError> {
        match self {
            BackendConfig::Slack(config) => {
                if config.webhook_url.trim().is_empty() {
                    return Err(NotifyError::Config("slack.webhook_url must be set".into()));
                }
                validate_url("slack.webhook_url", &config.webhook_url)?;
            }
            BackendConfig::Webhook(config) => {
                if config.url.trim().is_empty() {
                    return Err(NotifyError::Config("webhook.url must be set".into()));
                }
                validate_url("webhook.url", &config.url)?;
            }
        }
        Ok(())
    }
}

fn validate_url(field: &str, url: &str) -> Result<(), NotifyError> {
    let parsed = Url::parse(url).map_err(|err| NotifyError::Config(format!("{} must be a valid URL: {}", field, err)))?;
    let scheme = parsed.scheme();
    if scheme != "http" && scheme != "https" {
        return Err(NotifyError::Config(format!("{} must use http or https", field)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = NotifyConfig::default();
        // Default has no backends, which is valid (just won't send anything)
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_rate_limit_zero_invalid() {
        let mut config = NotifyConfig::default();
        config.rate_limit.max_per_window = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_slack_empty_url_invalid() {
        let config = NotifyConfig {
            backends: vec![BackendConfig::Slack(SlackConfig {
                webhook_url: "".to_string(),
                channel: None,
                username: None,
                icon_emoji: None,
            })],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_valid_slack_config() {
        let config = NotifyConfig {
            backends: vec![BackendConfig::Slack(SlackConfig {
                webhook_url: "https://hooks.slack.com/services/T00/B00/XXX".to_string(),
                channel: Some("#alerts".to_string()),
                username: None,
                icon_emoji: None,
            })],
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }
}
