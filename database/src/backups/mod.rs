pub(crate) mod helpers;
mod lib;
mod metadata;
cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
    } else {
        mod create;
        mod restore;

        #[cfg(all(test, feature = "infra-tests"))]
        mod tests;
    }
}

pub use lib::{BackupConfig, BackupMode, EncryptedBackup, EncryptedBackupData, EncryptionSettings, NONCE_SIZE, SALT_SIZE};

pub use metadata::{delete_backup, list_backups, load_backup_metadata, save_backup_metadata};
