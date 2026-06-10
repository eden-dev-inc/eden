//! Engine service configuration for gRPC communication.
//!
//! Maps to the `[services.engine]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Engine service configuration for gRPC communication.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct EngineServiceConfig {
    pub host: String,
    pub port: u16,
}

impl Default for EngineServiceConfig {
    fn default() -> Self {
        Self { host: "localhost".to_string(), port: 8001 }
    }
}
