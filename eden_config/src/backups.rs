//! Backup configuration for backup and restore.
//!
//! Maps to the `[backup]` section in `eden.toml`.

use serde::{Deserialize, Serialize};

/// Backup configuration for backup and restore.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct BackupConfig {
    /// Path to backup file.
    pub path: Option<String>,
    /// Password for backup encryption.
    pub password: Option<String>,
    /// Directory for backup storage.
    pub dir: Option<String>,
}
