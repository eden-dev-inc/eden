//! Backup restoration at startup
//!
//! This module handles automatic restoration from backups when Eden service starts.
//!
//! # Configuration
//!
//! - `eden_config::backup().path` - Path to backup metadata JSON file (optional)
//! - `eden_config::backup().password` - Password for decrypting backup dumps (required if path is set)
//!
//! # Behavior
//!
//! If `backup.path` is set:
//! 1. Validates that `backup.password` is also set (fails startup if missing)
//! 2. Loads backup metadata from the specified path
//! 3. Restores PostgreSQL, Redis RBAC, and Redis Cache from encrypted dumps
//! 4. Returns `Ok(true)` if restoration succeeded
//!
//! If `backup.path` is not set:
//! - Returns `Ok(false)` immediately (normal startup, no restoration)
//!
//! # Errors
//!
//! Returns `EpError` if:
//! - `backup.path` is set but `backup.password` is missing
//! - Metadata file cannot be loaded
//! - Decryption fails (wrong password or corrupted data)
//! - Database restoration fails

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
    } else {
        use database::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
        use eden_core::error::{EpError, ResultEP};
        use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info};
        use function_name::named;

        /// Attempts to restore from backup if `EDEN_BACKUP_PATH` is set.
        ///
        /// # Arguments
        ///
        /// * `db` - Database manager for restoration target
        /// * `pg_password` - PostgreSQL password for pg_restore authentication
        ///
        /// # Returns
        ///
        /// * `Ok(true)` - Backup was restored successfully
        /// * `Ok(false)` - No backup path configured (normal startup)
        /// * `Err(_)` - Restoration failed (startup should abort)
        ///
        /// # Errors
        ///
        /// Returns `EpError::init()` if:
        /// - `EDEN_BACKUP_PATH` is set but `EDEN_BACKUP_PASSWORD` is missing
        /// - Backup restoration fails for any reason
        #[named]
        pub async fn maybe_restore_backup<R, P, C>(db: &DatabaseManager<R, P, C>, pg_password: &str) -> ResultEP<bool>
        where
            R: EdenRedisConnection + Sync,
            P: EdenPostgresConnection + Sync,
            C: EdenClickhouseConnection + Sync,
        {
            let ctx = ctx_with_trace!().with_feature("backup_restore");

            let backup_path = match eden_config::backup().path.clone() {
                Some(path) => path,
                None => {
                    log_info!(
                        ctx,
                        "No backup restoration requested",
                        audience = LogAudience::Internal,
                        env_var = "EDEN_BACKUP_PATH"
                    );
                    return Ok(false);
                }
            };

            log_info!(
                ctx.clone(),
                "Backup restoration requested",
                audience = LogAudience::Internal,
                backup_path = &backup_path
            );

            let encrypt_password = eden_config::backup().password.clone().ok_or_else(|| {
                log_error!(
                    ctx.clone(),
                    "EDEN_BACKUP_PASSWORD must be set when EDEN_BACKUP_PATH is provided",
                    audience = LogAudience::Internal
                );
                EpError::init("EDEN_BACKUP_PASSWORD environment variable is required for backup restoration")
            })?;

            log_info!(
                ctx.clone(),
                "Starting backup restoration",
                audience = LogAudience::Internal,
                metadata_path = &backup_path
            );

            db.restore_from_metadata(&backup_path, pg_password, &encrypt_password).await.map_err(|e| {
                log_error!(
                    ctx.clone(),
                    "Backup restoration failed",
                    audience = LogAudience::Internal,
                    error = e.to_string(),
                    backup_path = &backup_path
                );
                EpError::init(format!("Backup restoration failed: {}", e))
            })?;

            log_info!(
                ctx,
                "Backup restoration completed successfully",
                audience = LogAudience::Internal,
                backup_path = &backup_path
            );

            Ok(true)
        }
    }
}
