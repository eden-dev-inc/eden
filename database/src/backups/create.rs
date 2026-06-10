use chrono::Utc;
use eden_core::error::{EpError, ResultEP};
use std::path::Path;
use tokio::io::{self, AsyncReadExt};
use tokio::process::Command;

use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::lib::ShardCache;

use super::helpers::{checksum, encrypt, write_file};
use super::lib::{BackupConfig, BackupMode, EncryptedBackup, EncryptedBackupData, EncryptionSettings};
use super::metadata::save_backup_metadata;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Create an ephemeral, encrypted backup of Eden's database system.
    ///
    /// Orchestrates the backup process by:
    /// 1. Creating a local `backups` directory if it does not exist.
    /// 2. Exporting the Postgres database using `pg_dump`.
    /// 3. Exporting the internal ShardMap cache snapshot.
    /// 4. Encrypting all generated dump files using the provided `encrypt_password`.
    ///
    /// # System Requirements
    /// **CRITICAL**: Relies on the `pg_dump` command-line utility which **must** be
    /// installed and available in the system's `PATH`.
    pub async fn create_backup(&self, pg_password: &str, encrypt_password: &str) -> ResultEP<EncryptedBackup> {
        self.create_backup_with_config(pg_password, encrypt_password, BackupConfig::default()).await
    }

    /// Create an encrypted backup with custom configuration.
    ///
    /// Same as [`Self::create_backup`] but accepts a [`BackupConfig`] to control
    /// output directory, persistence mode, description, and source node metadata.
    pub async fn create_backup_with_config(
        &self,
        pg_password: &str,
        encrypt_password: &str,
        config: BackupConfig,
    ) -> ResultEP<EncryptedBackup> {
        tokio::fs::create_dir_all(&config.output_dir).await?;

        let created_at = Utc::now().timestamp_millis();
        let persistent = config.mode == BackupMode::Persistent;

        // Track created files so we can clean up on partial failure.
        let mut created_files: Vec<std::path::PathBuf> = Vec::new();

        let cleanup = |files: Vec<std::path::PathBuf>| async move {
            for f in files {
                let _ = tokio::fs::remove_file(&f).await;
            }
        };

        let postgres = match export_postgres(self.pg_url(), created_at, pg_password, encrypt_password, persistent, &config.output_dir).await
        {
            Ok(data) => {
                created_files.push(data.dump_path.clone());
                data
            }
            Err(e) => return Err(e),
        };

        let snapshot = self.internal_cache().snapshot().await?;
        let redis_cache =
            match export_internal_cache_snapshot(&snapshot, created_at, encrypt_password, true, persistent, &config.output_dir).await {
                Ok(data) => {
                    created_files.push(data.dump_path.clone());
                    data
                }
                Err(e) => {
                    cleanup(created_files).await;
                    return Err(e);
                }
            };

        // Compatibility: the public backup metadata still has `redis_cache`
        // and `redis_rbac` fields, but the internal ShardMap snapshot is now a
        // single raw namespaced payload. Store one file and point both fields
        // at it so restore can handle old and new metadata shapes.
        let redis_rbac = redis_cache.clone();

        let mut backup = EncryptedBackup {
            postgres,
            redis_rbac,
            redis_cache,
            created_at,
            config,
            metadata_path: None,
        };

        // If persistent mode, save metadata
        if persistent {
            let metadata_path = backup.config.output_dir.join(EncryptedBackup::metadata_filename(created_at));
            save_backup_metadata(&backup, &metadata_path).await?;
            backup.metadata_path = Some(metadata_path);
        }

        Ok(backup)
    }
}

async fn export_postgres(
    conn_str: &str,
    created_at: i64,
    pg_password: &str,
    encrypt_password: &str,
    persistent: bool,
    output_dir: &Path,
) -> ResultEP<EncryptedBackupData> {
    let filename = format!("pg-backup-{}.dump", created_at);
    let output_path = output_dir.join(filename);

    let mut cmd = Command::new("pg_dump");
    cmd.arg("--format=custom")
        .arg("--dbname")
        .arg(conn_str)
        .env("PGPASSWORD", pg_password)
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped());

    let mut child = cmd.spawn()?;
    let mut stdout = child.stdout.take().ok_or_else(|| io::Error::other("pg_dump stdout not available"))?;

    let mut data = Vec::new();
    stdout.read_to_end(&mut data).await?;

    let status = child.wait().await?;
    if !status.success() {
        if let Some(mut stderr) = child.stderr.take() {
            let mut buf = Vec::new();
            let _ = stderr.read_to_end(&mut buf).await;
            return Err(EpError::Database(eden_core::error::DatabaseError::Custom(format!(
                "pg_dump failed: {}",
                String::from_utf8_lossy(&buf)
            ))));
        }
        return Err(EpError::Database(eden_core::error::DatabaseError::Custom(format!("pg_dump failed: {status}"))));
    }

    let checksum_val = checksum(&data);
    let (ciphertext, salt, nonce_bytes) = encrypt(&data, encrypt_password)?;

    write_file(&output_path, &ciphertext).await?;

    Ok(EncryptedBackupData::new(
        output_path,
        checksum_val,
        EncryptionSettings::new(&salt, &nonce_bytes),
        persistent,
    ))
}

async fn export_internal_cache_snapshot(
    snapshot: &crate::db::internal_cache::InternalCacheSnapshot,
    created_at: i64,
    encrypt_password: &str,
    is_cache: bool,
    persistent: bool,
    output_dir: &Path,
) -> ResultEP<EncryptedBackupData> {
    let plaintext = serde_json::to_vec(snapshot).map_err(|e| EpError::serde(format!("Serialization failed: {e}")))?;

    let checksum_val = checksum(&plaintext);
    let (ciphertext, salt, nonce_bytes) = encrypt(&plaintext, encrypt_password)?;

    let filename = if is_cache {
        format!("redis-cache-backup-{}.dump", created_at)
    } else {
        format!("redis-rbac-backup-{}.dump", created_at)
    };
    let output_path = output_dir.join(filename);

    write_file(&output_path, &ciphertext).await?;

    Ok(EncryptedBackupData::new(
        output_path,
        checksum_val,
        EncryptionSettings::new(&salt, &nonce_bytes),
        persistent,
    ))
}
