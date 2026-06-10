//! Configuration error types.
//!
//! Covers load failures, validation violations, and Figment deserialization errors.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("Configuration loading failed: {0}")]
    LoadError(String),

    #[error("Invalid configuration value: {0}")]
    InvalidValue(String),

    #[error("Figment error: {0}")]
    Figment(#[from] figment::Error),
}
