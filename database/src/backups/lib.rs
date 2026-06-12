//! Backup system for database backup and restoration
//!
//! This module provides a complete backup system for Eden's database state,
//! including PostgreSQL and a raw internal ShardMap cache snapshot. Postgres is
//! the source of truth; restored cache entries are a warm-start optimization.
//!
//! # Features
//!
//! - **Encryption**: AES-256-GCM with Argon2 key derivation
//! - **Integrity**: SHA256 checksums for verification
//! - **Persistence**: Configurable ephemeral or persistent storage
//! - **Metadata**: JSON metadata files for backup management
//!
//! # Architecture
//!
//! ```text
//! EncryptedBackup
//! ├── created_at: i64
//! ├── config: BackupConfig
//! │   ├── description: Option<String>
//! │   └── source_node: Option<String>
//! ├── postgres: EncryptedBackupData
//! ├── redis_rbac: EncryptedBackupData  // compatibility alias for redis_cache on new backups
//! └── redis_cache: EncryptedBackupData // raw ShardMap snapshot
//!
//! EncryptedBackupData
//! ├── dump_path: PathBuf (encrypted dump file)
//! ├── checksum: String (SHA256 of plaintext)
//! ├── settings: EncryptionSettings (salt, nonce)
//! └── persistent: bool (controls Drop behavior)
//! ```
//!
//! # Usage
//!
//! ## Creating a Backup
//!
//! ```ignore
//! use database::backups::{create_backup_with_config, BackupConfig};
//! use std::path::PathBuf;
//!
//! let config = BackupConfig::persistent("backups")
//!     .with_description("Production backup")
//!     .with_source_node("eden-prod-1");
//!
//! let backup = create_backup_with_config(
//!     &db_manager,
//!     "postgres_password",
//!     "encryption_password",
//!     config
//! ).await?;
//!
//! // Backup files created:
//! // - backups/backup-{timestamp}.metadata.json
//! // - backups/pg-backup-{timestamp}.dump
//! // - backups/redis-cache-backup-{timestamp}.dump  // raw ShardMap snapshot payload
//! //   `redis_rbac` metadata points at the same file for compatibility.
//! ```
//!
//! ## Restoring from Metadata
//!
//! ```ignore
//! use database::backups::restore_from_metadata;
//!
//! restore_from_metadata(
//!     &db_manager,
//!     "backups/backup-1234567890.metadata.json",
//!     "postgres_password",
//!     "encryption_password"
//! ).await?;
//! ```
//!
//! ## Listing Backups
//!
//! ```ignore
//! use database::backups::list_backups;
//!
//! let backups = list_backups("backups").await?;
//! for backup in backups {
//!     println!("Backup at {}: {:?}",
//!         backup.created_at,
//!         backup.config.description
//!     );
//! }
//! ```
//!
//! # Security
//!
//! - Encryption password is **never** stored on disk
//! - Metadata JSON contains only encryption parameters (salt, nonce)
//! - Password must be provided at restore time
//! - All dump files are encrypted with AES-256-GCM
//! - SHA256 checksums verify data integrity
//!
//! # File Format
//!
//! **Metadata JSON** (`backup-{timestamp}.metadata.json`):
//! ```json
//! {
//!   "created_at": 1234567890,
//!   "description": "Production backup",
//!   "source_node": "eden-prod-1",
//!   "postgres": {
//!     "dump_path": "backups/pg-backup-1234567890.dump",
//!     "checksum": "abc123...",
//!     "persistent": true,
//!     "settings": {
//!       "salt_b64": "...",
//!       "nonce_prefix_b64": "..."
//!     }
//!   },
//!   "redis_rbac": { ... },
//!   "redis_cache": { ... }
//! }
//! ```
//!
//! **Encrypted Dump Files**: Binary files encrypted with AES-256-GCM

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const NONCE_SIZE: usize = 12;
pub const SALT_SIZE: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedBackup {
    pub created_at: i64,
    pub postgres: EncryptedBackupData,
    pub redis_rbac: EncryptedBackupData,
    pub redis_cache: EncryptedBackupData,
    #[serde(flatten)]
    pub config: BackupConfig,
    #[serde(skip)]
    pub metadata_path: Option<PathBuf>,
}

impl EncryptedBackup {
    pub const CURRENT_VERSION: u32 = 1;

    /// Generate standard metadata filename for a backup
    ///
    /// # Example
    /// ```ignore
    /// let filename = EncryptedBackup::metadata_filename(1234567890);
    /// assert_eq!(filename, "backup-1234567890.metadata.json");
    /// ```
    pub fn metadata_filename(created_at: i64) -> String {
        format!("backup-{}.metadata.json", created_at)
    }
}

/// Configuration for backup behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BackupMode {
    /// Files are deleted when EncryptedBackupData is dropped
    #[default]
    Ephemeral,
    /// Files persist on disk, metadata is saved for later restoration
    Persistent,
}

/// Extended backup configuration
///
/// Controls where backups are stored and whether they persist.
/// Description and source_node are serialized to metadata JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackupConfig {
    #[serde(skip)]
    pub output_dir: PathBuf,
    #[serde(skip)]
    pub mode: BackupMode,

    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub source_node: Option<String>,
}

impl Default for BackupConfig {
    fn default() -> Self {
        Self {
            output_dir: PathBuf::from("backups"),
            mode: BackupMode::Ephemeral,
            description: None,
            source_node: None,
        }
    }
}

impl BackupConfig {
    pub fn persistent(output_dir: impl Into<PathBuf>) -> Self {
        Self {
            output_dir: output_dir.into(),
            mode: BackupMode::Persistent,
            description: None,
            source_node: None,
        }
    }

    pub fn with_description(mut self, desc: impl Into<String>) -> Self {
        self.description = Some(desc.into());
        self
    }

    pub fn with_source_node(mut self, node: impl Into<String>) -> Self {
        self.source_node = Some(node.into());
        self
    }
}

/// Encrypted dump file metadata
///
/// Contains the path to an encrypted dump file, its checksum, encryption settings,
/// and persistence flag. When dropped, the dump file is deleted if `persistent` is false.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedBackupData {
    pub dump_path: PathBuf,
    pub checksum: String,
    pub settings: EncryptionSettings,

    /// Controls whether file is deleted on drop
    /// Defaults to true when deserializing (assumes persistent backups)
    #[serde(default = "default_persistent")]
    pub(crate) persistent: bool,
}

/// Default value for persistent field during deserialization
fn default_persistent() -> bool {
    true
}

impl EncryptedBackupData {
    pub fn new(dump_path: PathBuf, checksum: String, settings: EncryptionSettings, persistent: bool) -> Self {
        Self { dump_path, checksum, settings, persistent }
    }

    /// Convert to non-persistent (will delete on drop)
    pub fn into_ephemeral(mut self) -> Self {
        self.persistent = false;
        self
    }

    /// Convert to persistent (will NOT delete on drop)
    pub fn into_persistent(mut self) -> Self {
        self.persistent = true;
        self
    }
}

impl Drop for EncryptedBackupData {
    fn drop(&mut self) {
        if !self.persistent {
            let _ = std::fs::remove_file(&self.dump_path);
        }
    }
}

/// Encryption settings for a dump file
///
/// Contains the salt and nonce used for AES-256-GCM encryption.
/// Both are stored as base64-encoded strings in metadata JSON.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionSettings {
    pub salt_b64: String,
    pub nonce_prefix_b64: String,
}

impl EncryptionSettings {
    /// Create encryption settings from raw bytes
    ///
    /// # Arguments
    /// * `salt` - Salt bytes for Argon2 (should be SALT_SIZE bytes)
    /// * `nonce_bytes` - Nonce bytes for AES-GCM (should be NONCE_SIZE bytes)
    pub fn new(salt: &[u8], nonce_bytes: &[u8]) -> Self {
        use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
        Self {
            salt_b64: BASE64.encode(salt),
            nonce_prefix_b64: BASE64.encode(nonce_bytes),
        }
    }
}
