use eden_core::error::{EpError, ResultEP};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use function_name::named;
use std::path::{Path, PathBuf};

use super::lib::EncryptedBackup;

/// Saves backup metadata to a JSON file
pub async fn save_backup_metadata(backup: &EncryptedBackup, output_path: impl AsRef<Path>) -> ResultEP<PathBuf> {
    let json = serde_json::to_string_pretty(backup).map_err(|e| EpError::serde(format!("Failed to serialize metadata: {e}")))?;

    tokio::fs::write(output_path.as_ref(), json.as_bytes()).await?;

    Ok(output_path.as_ref().to_path_buf())
}

/// Loads backup metadata from a JSON file
pub async fn load_backup_metadata(metadata_path: impl AsRef<Path>) -> ResultEP<EncryptedBackup> {
    let json = tokio::fs::read_to_string(metadata_path.as_ref())
        .await
        .map_err(|e| EpError::fs(format!("Failed to read metadata file: {e}")))?;

    let backup: EncryptedBackup =
        serde_json::from_str(&json).map_err(|e| EpError::serde(format!("Failed to deserialize metadata: {e}")))?;

    Ok(backup)
}

/// Lists available backups in a directory
#[named]
pub async fn list_backups(backup_dir: impl AsRef<Path>) -> ResultEP<Vec<EncryptedBackup>> {
    let mut backups = Vec::new();
    let mut entries = tokio::fs::read_dir(backup_dir.as_ref()).await?;

    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("json")
            && let Some(filename) = path.file_name().and_then(|s| s.to_str())
            && filename.starts_with("backup-")
            && filename.ends_with(".metadata.json")
        {
            match load_backup_metadata(&path).await {
                Ok(backup) => backups.push(backup),
                Err(e) => {
                    let ctx = ctx_with_trace!().with_feature("backup.metadata");
                    log_warn!(
                        ctx,
                        "Failed to load backup metadata",
                        audience = LogAudience::Internal,
                        path = format!("{:?}", path),
                        error = e.to_string()
                    );
                }
            }
        }
    }

    // Sort by created_at descending (newest first)
    backups.sort_by(|a, b| b.created_at.cmp(&a.created_at));

    Ok(backups)
}

/// Deletes a backup and all its files
#[named]
pub async fn delete_backup(metadata_path: impl AsRef<Path>) -> ResultEP<()> {
    let backup = load_backup_metadata(metadata_path.as_ref()).await?;

    // Delete dump files
    for path in [
        &backup.postgres.dump_path,
        &backup.redis_rbac.dump_path,
        &backup.redis_cache.dump_path,
    ] {
        if let Err(e) = tokio::fs::remove_file(path).await {
            let ctx = ctx_with_trace!().with_feature("backup.metadata");
            log_warn!(
                ctx,
                "Failed to delete dump file",
                audience = LogAudience::Internal,
                path = format!("{:?}", path),
                error = e.to_string()
            );
        }
    }

    // Delete metadata file
    tokio::fs::remove_file(metadata_path.as_ref()).await?;

    Ok(())
}
