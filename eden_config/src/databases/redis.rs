//! Internal Redis connection configuration.
//!
//! Maps to the `[databases.redis]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Internal Redis database configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct InternalRedisConfig {
    pub host: String,
    pub port: u16,
    pub username: String,
    pub password: String,
    pub db_number: u8,
}

impl Default for InternalRedisConfig {
    fn default() -> Self {
        Self {
            host: "localhost".to_string(),
            port: 6379,
            username: String::new(),
            password: String::new(),
            db_number: 0,
        }
    }
}
