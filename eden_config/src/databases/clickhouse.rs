//! Internal ClickHouse connection configuration.
//!
//! Maps to the `[databases.clickhouse]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Internal ClickHouse database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InternalClickhouseConfig {
    pub url: String,
    pub username: String,
    pub password: String,
    pub database: Option<String>,
}

impl Default for InternalClickhouseConfig {
    fn default() -> Self {
        Self {
            url: "http://localhost:8123".to_string(),
            username: String::new(),
            password: String::new(),
            database: None,
        }
    }
}
