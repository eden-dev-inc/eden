//! Internal PostgreSQL connection configuration.
//!
//! Maps to the `[databases.postgres]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Internal PostgreSQL database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InternalPostgresConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub database: String,
}

impl Default for InternalPostgresConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 5432,
            username: "postgres".to_string(),
            password: String::new(),
            database: "postgres".to_string(),
        }
    }
}
