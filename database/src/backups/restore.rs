use eden_core::error::{EpError, ResultEP};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use function_name::named;
use std::path::Path;
use tokio::process::Command;

use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::lib::ShardCache;

use super::helpers::{decrypt_and_verify, is_ignorable_pg_restore_error, write_file};
use super::lib::{EncryptedBackup, EncryptedBackupData};
use super::metadata::load_backup_metadata;

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Restore Eden's database system from a previously created backup.
    ///
    /// Orchestrates the restoration process by:
    /// 1. Decrypting the backup files using the provided `encrypt_password`.
    /// 2. Restoring the Postgres database using `pg_restore`.
    /// 3. Restoring the internal ShardMap cache snapshot.
    ///
    /// # System Requirements
    /// **CRITICAL**: Relies on the `pg_restore` command-line utility which **must** be
    /// installed and available in the system's `PATH`.
    #[cfg(not(embedded_db))]
    pub async fn restore_backup(&self, backup: &EncryptedBackup, pg_password: &str, encrypt_password: &str) -> ResultEP<()> {
        restore_postgres(self.pg_url(), &backup.postgres, pg_password, encrypt_password).await?;

        let cache = self.internal_cache();
        restore_internal_cache_snapshot(&cache, &backup.redis_cache, encrypt_password).await?;
        if backup.redis_rbac.dump_path != backup.redis_cache.dump_path || backup.redis_rbac.checksum != backup.redis_cache.checksum {
            restore_internal_cache_snapshot(&cache, &backup.redis_rbac, encrypt_password).await?;
        }
        Ok(())
    }

    /// Restore Eden's database system from a metadata file (primary bootstrap entry point).
    ///
    /// Loads backup metadata from the file, then delegates to [`Self::restore_backup`].
    pub async fn restore_from_metadata(&self, metadata_path: impl AsRef<Path>, pg_password: &str, encrypt_password: &str) -> ResultEP<()> {
        let backup = load_backup_metadata(metadata_path).await?;
        self.restore_backup(&backup, pg_password, encrypt_password).await
    }
}

#[named]
async fn restore_postgres(conn_str: &str, backup: &EncryptedBackupData, pg_password: &str, encrypt_password: &str) -> ResultEP<()> {
    let data = decrypt_and_verify(
        &backup.dump_path,
        &backup.checksum,
        &backup.settings.salt_b64,
        &backup.settings.nonce_prefix_b64,
        encrypt_password,
    )
    .await?;

    // Write decrypted data to a temporary file to preserve the original encrypted backup.
    let temp_path = backup.dump_path.with_extension("restore.tmp");
    write_file(&temp_path, &data).await?;

    let mut cmd = Command::new("pg_restore");
    cmd.arg("--clean")
        .arg("--no-reconnect")
        .arg("--dbname")
        .arg(conn_str)
        .arg(&temp_path)
        .env("PGPASSWORD", pg_password)
        .stderr(std::process::Stdio::piped());

    let output = cmd.output().await.map_err(std::io::Error::other)?;

    // Always clean up the temporary decrypted file
    let _ = tokio::fs::remove_file(&temp_path).await;

    let status = output.status;
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    if !status.success() {
        if is_ignorable_pg_restore_error(status, &stderr) {
            let ctx = ctx_with_trace!().with_feature("backup.restore");
            log_warn!(
                ctx,
                "pg_restore completed with ignorable warnings",
                audience = LogAudience::Internal,
                stderr = stderr
            );
            return Ok(());
        }

        return Err(EpError::Database(eden_core::error::DatabaseError::Custom(format!(
            "pg_restore failed with status {status}: {stderr}",
        ))));
    }
    Ok(())
}

async fn restore_internal_cache_snapshot(
    cache: &crate::db::internal_cache::InternalCache,
    backup: &EncryptedBackupData,
    encrypt_password: &str,
) -> ResultEP<()> {
    let data = decrypt_and_verify(
        &backup.dump_path,
        &backup.checksum,
        &backup.settings.salt_b64,
        &backup.settings.nonce_prefix_b64,
        encrypt_password,
    )
    .await?;

    let snapshot: crate::db::internal_cache::InternalCacheSnapshot =
        serde_json::from_slice(&data).map_err(|e| EpError::serde(format!("Deserialization failed: {e}")))?;
    cache.restore_snapshot(&snapshot).await?;
    Ok(())
}
