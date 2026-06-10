//! External metering compatibility configuration.
//!
//! This module keeps the historical `[licensing]` config section for backward
//! compatibility while treating all external metering/export settings as
//! opt-in.

use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::ConfigError;

/// Configuration for optional external metering compatibility.
///
/// The section name remains `[licensing]` for compatibility with existing
/// deployments, but open-source builds do not require a token or external
/// service.
///
/// ## Usage
///
/// ```rust
/// use eden_config::licensing;
///
/// let cfg = licensing();
/// if cfg.is_enabled() {
///     println!("External metering enabled for cluster: {:?}", cfg.cluster_uid);
/// }
/// ```
///
/// ## Configuration Sources
///
/// Can be configured via:
/// - TOML file: `[licensing]` section
/// - Legacy environment variables: `EDEN_LICENSE_KEY`, `EDEN_CLUSTER_UID`, etc.
/// - Nested environment variables: `EDEN__LICENSING__LICENSE_KEY`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct LicensingClientConfig {
    /// Optional signed entitlement token.
    /// Leave unset unless an external metering sink requires it.
    pub license_key: Option<String>,

    /// Unique cluster identifier.
    pub cluster_uid: Option<String>,

    /// Optional external metering base URL.
    pub phone_home_url: String,

    /// External sync interval in seconds.
    pub heartbeat_interval_secs: u64,

    /// Disable external compatibility exports entirely.
    pub disabled: bool,

    /// Enable metering export to `/v1/metering/events`.
    pub metering_enabled: bool,

    /// Optional full URL override for metering ingestion endpoint.
    /// If unset, derives from `phone_home_url` when that base URL is set.
    pub metering_endpoint_url: Option<String>,

    /// Dedicated bearer token for metering ingestion auth (`POST /v1/metering/events`).
    ///
    /// If unset, metering exporter remains disabled even when metering is enabled.
    pub metering_ingest_api_key: Option<String>,

    /// Metering exporter flush interval in seconds.
    pub metering_flush_interval_secs: u64,

    /// Maximum number of events to include per ingest batch.
    pub metering_max_batch_size: usize,

    /// Maximum retry attempts for transient metering export failures.
    pub metering_retry_max_attempts: u32,

    /// Base retry backoff in milliseconds.
    pub metering_retry_base_delay_ms: u64,

    /// Maximum retry backoff in milliseconds.
    pub metering_retry_max_delay_ms: u64,
}

impl Default for LicensingClientConfig {
    fn default() -> Self {
        Self {
            license_key: None,
            cluster_uid: None,
            phone_home_url: String::new(),
            heartbeat_interval_secs: 86400, // 24 hours
            disabled: false,
            metering_enabled: false,
            metering_endpoint_url: None,
            metering_ingest_api_key: None,
            metering_flush_interval_secs: 60,
            metering_max_batch_size: 100,
            metering_retry_max_attempts: 5,
            metering_retry_base_delay_ms: 500,
            metering_retry_max_delay_ms: 30000,
        }
    }
}

impl LicensingClientConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.disabled {
            return Ok(());
        }
        match (self.license_key.as_ref(), self.cluster_uid.as_ref()) {
            (None, None) => Ok(()), // Pre-rollout: unconfigured is allowed
            (Some(_), Some(_)) if self.heartbeat_interval_secs == 0 => {
                Err(ConfigError::InvalidValue("licensing.heartbeat_interval_secs must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_flush_interval_secs == 0 => {
                Err(ConfigError::InvalidValue("licensing.metering_flush_interval_secs must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_max_batch_size == 0 => {
                Err(ConfigError::InvalidValue("licensing.metering_max_batch_size must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_retry_max_attempts == 0 => {
                Err(ConfigError::InvalidValue("licensing.metering_retry_max_attempts must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_retry_base_delay_ms == 0 => {
                Err(ConfigError::InvalidValue("licensing.metering_retry_base_delay_ms must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_retry_max_delay_ms == 0 => {
                Err(ConfigError::InvalidValue("licensing.metering_retry_max_delay_ms must be > 0".into()))
            }
            (Some(_), Some(_)) if self.metering_retry_max_delay_ms < self.metering_retry_base_delay_ms => Err(ConfigError::InvalidValue(
                "licensing.metering_retry_max_delay_ms must be >= licensing.metering_retry_base_delay_ms".into(),
            )),
            (Some(_), Some(_)) => Ok(()),
            (Some(_), None) | (None, Some(_)) => Err(ConfigError::InvalidValue(
                "licensing.license_key and licensing.cluster_uid must be set together".into(),
            )),
        }
    }

    /// Convert heartbeat interval to a Duration for use with tokio intervals.
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.heartbeat_interval_secs)
    }

    /// Returns the configured metering ingest endpoint URL.
    pub fn metering_endpoint_url(&self) -> String {
        if let Some(url) = &self.metering_endpoint_url {
            return url.clone();
        }
        let base = self.phone_home_url.trim_end_matches('/');
        if base.is_empty() {
            String::new()
        } else {
            format!("{base}/v1/metering/events")
        }
    }

    /// Check if external metering compatibility is effectively enabled.
    ///
    /// Returns true if:
    /// - `disabled` is false
    /// - `license_key` is provided
    /// - `cluster_uid` is provided
    pub fn is_enabled(&self) -> bool {
        !self.disabled && self.license_key.is_some() && self.cluster_uid.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_licensing_config() {
        let cfg = LicensingClientConfig::default();
        assert_eq!(cfg.phone_home_url, "");
        assert_eq!(cfg.heartbeat_interval_secs, 86400);
        assert!(!cfg.disabled);
        assert!(!cfg.metering_enabled);
        assert!(!cfg.is_enabled()); // No license key or cluster UID
    }

    #[test]
    fn test_is_enabled_requires_keys() {
        let mut cfg = LicensingClientConfig {
            license_key: Some("test-key".to_string()),
            ..Default::default()
        };
        assert!(!cfg.is_enabled()); // Still missing cluster_uid

        cfg.cluster_uid = Some("test-cluster".to_string());
        assert!(cfg.is_enabled()); // Now enabled

        cfg.disabled = true;
        assert!(!cfg.is_enabled()); // Explicitly disabled
    }

    #[test]
    fn test_heartbeat_interval_conversion() {
        let cfg = LicensingClientConfig { heartbeat_interval_secs: 3600, ..Default::default() };
        assert_eq!(cfg.heartbeat_interval(), Duration::from_secs(3600));
    }

    #[test]
    fn test_disabled_overrides_keys() {
        let cfg = LicensingClientConfig {
            license_key: Some("test-key".to_string()),
            cluster_uid: Some("test-cluster".to_string()),
            disabled: true,
            ..Default::default()
        };
        assert!(!cfg.is_enabled());
    }

    #[test]
    fn test_validate_partial_config() {
        // license_key set, cluster_uid missing → Err containing "set together"
        let cfg = LicensingClientConfig {
            license_key: Some("test-key".to_string()),
            cluster_uid: None,
            disabled: false,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("set together"), "unexpected error: {err}");

        // cluster_uid set, license_key missing → Err containing "set together"
        let cfg = LicensingClientConfig {
            license_key: None,
            cluster_uid: Some("test-cluster".to_string()),
            disabled: false,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("set together"), "unexpected error: {err}");

        // license_key set, cluster_uid missing, but disabled → Ok
        let cfg = LicensingClientConfig {
            license_key: Some("test-key".to_string()),
            cluster_uid: None,
            disabled: true,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());

        // cluster_uid set, license_key missing, but disabled → Ok
        let cfg = LicensingClientConfig {
            license_key: None,
            cluster_uid: Some("test-cluster".to_string()),
            disabled: true,
            ..Default::default()
        };
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_metering_endpoint_derives_from_external_base_url() {
        let cfg = LicensingClientConfig {
            phone_home_url: "https://metering.example.com/".to_string(),
            metering_endpoint_url: None,
            ..Default::default()
        };
        assert_eq!(cfg.metering_endpoint_url(), "https://metering.example.com/v1/metering/events");
    }

    #[test]
    fn test_metering_endpoint_empty_without_base_url() {
        let cfg = LicensingClientConfig::default();
        assert_eq!(cfg.metering_endpoint_url(), "");
    }

    #[test]
    fn test_metering_retry_validation() {
        let cfg = LicensingClientConfig {
            license_key: Some("test-key".to_string()),
            cluster_uid: Some("test-cluster".to_string()),
            metering_retry_base_delay_ms: 2_000,
            metering_retry_max_delay_ms: 1_000,
            ..Default::default()
        };
        let err = cfg.validate().unwrap_err().to_string();
        assert!(err.contains("must be >="), "unexpected error: {err}");
    }
}
