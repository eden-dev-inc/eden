//! Service configuration.

use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::notify::NotifyConfig;
use crate::provider::ClickhouseConfig;
use crate::rules::AlertRulesConfig;

/// Default values for service configuration.
pub mod defaults {
    pub const POLL_INTERVAL_SECS: u64 = 30;
    pub const WINDOW_MINUTES: i64 = 5;
}

/// Top-level alerts service configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertsConfig {
    /// Polling interval in seconds.
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,

    /// Time window for queries in minutes.
    #[serde(default = "default_window_minutes")]
    pub window_minutes: i64,

    /// Notification configuration.
    #[serde(default)]
    pub notify: NotifyConfig,

    /// Alert rules configuration.
    #[serde(default)]
    pub rules: AlertRulesConfig,

    /// ClickHouse connection configuration.
    #[serde(default)]
    pub clickhouse: ClickhouseConfigWrapper,
}

/// Wrapper for ClickHouse config with serde support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClickhouseConfigWrapper {
    #[serde(default = "default_clickhouse_url")]
    pub url: String,
    #[serde(default = "default_clickhouse_database")]
    pub database: String,
    pub user: Option<String>,
    pub password: Option<String>,
}

impl Default for ClickhouseConfigWrapper {
    fn default() -> Self {
        Self {
            url: default_clickhouse_url(),
            database: default_clickhouse_database(),
            user: None,
            password: None,
        }
    }
}

impl From<ClickhouseConfigWrapper> for ClickhouseConfig {
    fn from(wrapper: ClickhouseConfigWrapper) -> Self {
        ClickhouseConfig {
            url: wrapper.url,
            database: wrapper.database,
            user: wrapper.user,
            password: wrapper.password,
        }
    }
}

fn default_poll_interval() -> u64 {
    defaults::POLL_INTERVAL_SECS
}

fn default_window_minutes() -> i64 {
    defaults::WINDOW_MINUTES
}

fn default_clickhouse_url() -> String {
    "http://localhost:8123".to_string()
}

fn default_clickhouse_database() -> String {
    "analytics".to_string()
}

impl Default for AlertsConfig {
    fn default() -> Self {
        Self {
            poll_interval_secs: defaults::POLL_INTERVAL_SECS,
            window_minutes: defaults::WINDOW_MINUTES,
            notify: NotifyConfig::default(),
            rules: AlertRulesConfig::default(),
            clickhouse: ClickhouseConfigWrapper::default(),
        }
    }
}

impl AlertsConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        let contents = std::fs::read_to_string(path.as_ref()).map_err(|e| ConfigError::Io(format!("failed to read config file: {}", e)))?;
        Self::from_toml(&contents)
    }

    /// Parse configuration from a TOML string.
    pub fn from_toml(contents: &str) -> Result<Self, ConfigError> {
        toml::from_str(contents).map_err(|e| ConfigError::Parse(e.to_string()))
    }

    /// Load configuration from environment variables.
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(val) = std::env::var("EDEN_ALERTS_POLL_INTERVAL_SECS")
            && let Ok(secs) = val.parse()
        {
            config.poll_interval_secs = secs;
        }

        if let Ok(val) = std::env::var("EDEN_ALERTS_WINDOW_MINUTES")
            && let Ok(mins) = val.parse()
        {
            config.window_minutes = mins;
        }

        // ClickHouse
        if let Ok(url) = std::env::var("CLICKHOUSE_URL") {
            config.clickhouse.url = url;
        }
        if let Ok(db) = std::env::var("CLICKHOUSE_DATABASE") {
            config.clickhouse.database = db;
        }
        if let Ok(user) = std::env::var("CLICKHOUSE_USER") {
            config.clickhouse.user = Some(user);
        }
        if let Ok(password) = std::env::var("CLICKHOUSE_PASSWORD") {
            config.clickhouse.password = Some(password);
        }

        // Slack (quick setup)
        if let Ok(webhook_url) = std::env::var("EDEN_ALERTS_SLACK_WEBHOOK_URL") {
            use crate::notify::{BackendConfig, SlackConfig};
            config.notify.backends.push(BackendConfig::Slack(SlackConfig {
                webhook_url,
                channel: std::env::var("EDEN_ALERTS_SLACK_CHANNEL").ok(),
                username: std::env::var("EDEN_ALERTS_SLACK_USERNAME").ok(),
                icon_emoji: std::env::var("EDEN_ALERTS_SLACK_ICON").ok(),
            }));
        }

        config
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.poll_interval_secs == 0 {
            return Err(ConfigError::Validation("poll_interval_secs must be > 0".into()));
        }
        if self.window_minutes <= 0 {
            return Err(ConfigError::Validation("window_minutes must be > 0".into()));
        }

        self.notify.validate().map_err(|e| ConfigError::Validation(e.to_string()))?;
        self.rules.validate().map_err(ConfigError::Validation)?;

        Ok(())
    }
}

/// Configuration error.
#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(String),
    #[error("parse error: {0}")]
    Parse(String),
    #[error("validation error: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_is_valid() {
        let config = AlertsConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_parse_minimal_toml() {
        let toml_str = "poll_interval_secs = 60\nwindow_minutes = 10\n";
        let config = AlertsConfig::from_toml(toml_str).unwrap();
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.window_minutes, 10);
    }
}
