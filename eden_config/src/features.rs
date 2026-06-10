//! Feature flags for toggling system capabilities.
//!
//! Maps to the `[features]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Policy enforcement mode for security rules.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PolicyMode {
    #[default]
    /// Log policy violations without blocking.
    Observe,
    /// Log warnings for policy violations.
    Warn,
    /// Block operations that violate policy.
    Block,
}

/// Feature flags for toggling system capabilities.
///
/// RA-specific sampling toggles (burst, discovery, divergence, wire metrics,
/// PII detection) live in `SamplingConfig` — not here.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct FeatureFlags {
    /// Master switch for analytics data collection.
    pub analytics_enabled: bool,
    /// Security policy enforcement mode.
    pub policy_enforcement_mode: PolicyMode,
    /// Enable Redis PSYNC functionality at proxy layer.
    pub redis_psync: bool,
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self {
            analytics_enabled: true,
            policy_enforcement_mode: PolicyMode::default(),
            redis_psync: false,
        }
    }
}
