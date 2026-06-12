//! Eden main service configuration (host, port, JWT, node identity).
//!
//! Maps to the `[services.eden]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GatewayCpuAffinityMode {
    #[default]
    Auto,
    Off,
    Perf,
}

/// Eden service configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EdenServiceConfig {
    pub host: String,
    pub port: u16,
    /// Base64-encoded JWT secret.
    pub jwt_secret: Option<String>,
    /// Node UUID for cluster identification.
    pub node_uuid: Option<String>,
    /// Token for creating new organizations.
    pub new_org_token: Option<String>,
    /// Proxy runtime CPU affinity mode.
    pub gateway_cpu_affinity: GatewayCpuAffinityMode,
}

impl Default for EdenServiceConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 8000,
            jwt_secret: None,
            node_uuid: None,
            new_org_token: None,
            gateway_cpu_affinity: GatewayCpuAffinityMode::Auto,
        }
    }
}
