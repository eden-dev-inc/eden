use super::*;
use std::path::PathBuf;
use uuid::Uuid;

use crate::lib::ShardCache;
use crate::test_utils::{
    database_test_utils::create_database_manager_dedicated, organization_test_utils::initialize_organization,
    telemetry_test_utils::test_telemetry,
};

use super::helpers::{checksum, decrypt, encrypt};

#[tokio::test]
async fn test_backup() -> Result<(), Box<dyn std::error::Error>> {
    let mut test_telemetry = test_telemetry();

    let pg_password = "postgres";
    let encrypt_password = "password123";

    let rbac_key = format!("rbac_key_{}", Uuid::new_v4());
    let rbac_val = format!("rbac_val_{}", Uuid::new_v4());
    let cache_key = format!("cache_key_{}", Uuid::new_v4());
    let cache_val = format!("cache_val_{}", Uuid::new_v4());

    // SETUP
    let (backup, original_org_id) = {
        let (_r, _p, _c, db) = create_database_manager_dedicated().await;
        let _org = initialize_organization(&db, &mut test_telemetry).await;

        let pg_conn = db.pg_connection().await?;
        db.internal_cache().kv_set(rbac_key.clone(), rbac_val.clone()).await?;
        db.internal_cache().kv_set(cache_key.clone(), cache_val.clone()).await?;

        let row = pg_conn.query_one("SELECT * FROM organizations", &[]).await?;
        let org_id: String = row.try_get(0)?;

        let backup = db.create_backup(pg_password, encrypt_password).await?;

        (backup, org_id)
    };

    // RESTORE
    let (_r, _p, _c, db) = create_database_manager_dedicated().await;

    db.restore_backup(&backup, pg_password, encrypt_password).await?;

    // VERIFY
    let pg_conn = db.pg_connection().await?;
    let rbac_result = db.internal_cache().kv_get(&rbac_key).await?.expect("rbac value should exist");
    let cache_result = db.internal_cache().kv_get(&cache_key).await?.expect("cache value should exist");

    let row = pg_conn.query_one("SELECT * FROM organizations", &[]).await?;
    let new_org_id: String = row.try_get(0)?;

    assert_eq!(new_org_id, original_org_id, "Organization ID should match after restore");
    assert_eq!(rbac_result, rbac_val, "RBAC value should persist");
    assert_eq!(cache_result, cache_val, "Cache value should persist");
    Ok(())
}

#[tokio::test]
async fn test_persistent_backup() -> Result<(), Box<dyn std::error::Error>> {
    let mut test_telemetry = test_telemetry();
    let pg_password = "postgres";
    let encrypt_password = "password123";

    let rbac_key = format!("rbac_key_{}", Uuid::new_v4());
    let rbac_val = format!("rbac_val_{}", Uuid::new_v4());

    let temp_dir = std::env::temp_dir().join(format!("backup_test_{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await?;

    // SETUP and CREATE PERSISTENT BACKUP
    let (metadata_path, created_at) = {
        let (_r, _p, _c, db) = create_database_manager_dedicated().await;
        let _org = initialize_organization(&db, &mut test_telemetry).await;

        db.internal_cache().kv_set(rbac_key.clone(), rbac_val.clone()).await?;

        let config = BackupConfig::persistent(&temp_dir).with_description("Test backup").with_source_node("test-node");

        let backup = db.create_backup_with_config(pg_password, encrypt_password, config).await?;

        if let Some(metadata_path) = backup.metadata_path {
            (metadata_path, backup.created_at)
        } else {
            panic!("Persistent backup should have metadata path");
        }
    };

    // Verify metadata file exists
    assert!(metadata_path.exists(), "Metadata file should exist");

    // Verify dump files exist
    let pg_dump = temp_dir.join(format!("pg-backup-{}.dump", created_at));
    let rbac_dump = temp_dir.join(format!("redis-rbac-backup-{}.dump", created_at));
    let cache_dump = temp_dir.join(format!("redis-cache-backup-{}.dump", created_at));

    assert!(pg_dump.exists(), "PostgreSQL dump should exist");
    assert!(rbac_dump.exists(), "RBAC dump should exist");
    assert!(cache_dump.exists(), "Cache dump should exist");

    // RESTORE FROM METADATA
    let (_r2, _p2, _c2, db2) = create_database_manager_dedicated().await;
    db2.restore_from_metadata(&metadata_path, pg_password, encrypt_password).await?;

    // VERIFY
    let rbac_result = db2.internal_cache().kv_get(&rbac_key).await?.expect("rbac value should exist");
    assert_eq!(rbac_result, rbac_val, "RBAC value should persist after metadata restore");

    // CLEANUP
    tokio::fs::remove_dir_all(&temp_dir).await?;

    Ok(())
}

#[tokio::test]
async fn test_backup_metadata_serialization() -> Result<(), Box<dyn std::error::Error>> {
    let backup = EncryptedBackup {
        created_at: 1234567890,
        postgres: EncryptedBackupData::new(
            PathBuf::from("test.dump"),
            "abc123".to_string(),
            EncryptionSettings {
                salt_b64: "salt".to_string(),
                nonce_prefix_b64: "nonce".to_string(),
            },
            true,
        ),
        redis_rbac: EncryptedBackupData::new(
            PathBuf::from("rbac.dump"),
            "def456".to_string(),
            EncryptionSettings {
                salt_b64: "salt2".to_string(),
                nonce_prefix_b64: "nonce2".to_string(),
            },
            true,
        ),
        redis_cache: EncryptedBackupData::new(
            PathBuf::from("cache.dump"),
            "ghi789".to_string(),
            EncryptionSettings {
                salt_b64: "salt3".to_string(),
                nonce_prefix_b64: "nonce3".to_string(),
            },
            true,
        ),
        config: BackupConfig {
            output_dir: PathBuf::from("backups"),
            mode: BackupMode::Ephemeral,
            description: Some("Test backup".to_string()),
            source_node: Some("test-node".to_string()),
        },
        metadata_path: None,
    };

    let temp_path = std::env::temp_dir().join(format!("metadata_test_{}.json", Uuid::new_v4()));

    // Save and load
    metadata::save_backup_metadata(&backup, &temp_path).await?;
    let loaded = metadata::load_backup_metadata(&temp_path).await?;

    assert_eq!(loaded.created_at, backup.created_at);
    assert_eq!(loaded.config.description, backup.config.description);
    assert_eq!(loaded.config.source_node, backup.config.source_node);
    assert_eq!(loaded.postgres.checksum, backup.postgres.checksum);

    // Cleanup
    tokio::fs::remove_file(&temp_path).await?;

    Ok(())
}

#[tokio::test]
async fn test_list_backups() -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = std::env::temp_dir().join(format!("list_test_{}", Uuid::new_v4()));
    tokio::fs::create_dir_all(&temp_dir).await?;

    // Create some test metadata files
    for i in 0..3 {
        let backup = EncryptedBackup {
            created_at: 1000000000 + i,
            postgres: EncryptedBackupData::new(
                PathBuf::from("pg.dump"),
                "abc".to_string(),
                EncryptionSettings {
                    salt_b64: "salt".to_string(),
                    nonce_prefix_b64: "nonce".to_string(),
                },
                true,
            ),
            redis_rbac: EncryptedBackupData::new(
                PathBuf::from("rbac.dump"),
                "def".to_string(),
                EncryptionSettings {
                    salt_b64: "salt".to_string(),
                    nonce_prefix_b64: "nonce".to_string(),
                },
                true,
            ),
            redis_cache: EncryptedBackupData::new(
                PathBuf::from("cache.dump"),
                "ghi".to_string(),
                EncryptionSettings {
                    salt_b64: "salt".to_string(),
                    nonce_prefix_b64: "nonce".to_string(),
                },
                true,
            ),
            config: BackupConfig {
                output_dir: PathBuf::from("backups"),
                mode: BackupMode::Ephemeral,
                description: None,
                source_node: None,
            },
            metadata_path: None,
        };

        let path = temp_dir.join(EncryptedBackup::metadata_filename(1000000000 + i));
        metadata::save_backup_metadata(&backup, &path).await?;
    }

    let backups = metadata::list_backups(&temp_dir).await?;
    assert_eq!(backups.len(), 3, "Should find all 3 backups");

    // Verify sorted by created_at descending
    assert!(backups[0].created_at > backups[1].created_at);
    assert!(backups[1].created_at > backups[2].created_at);

    // Cleanup
    tokio::fs::remove_dir_all(&temp_dir).await?;

    Ok(())
}

#[test]
fn test_encrypt_decrypt_roundtrip() {
    let plaintext = b"test data for encryption";
    let password = "secure_password_123";

    let (ciphertext, salt, nonce) = encrypt(plaintext, password).expect("Failed to encrypt data");
    let decrypted = decrypt(&ciphertext, password, &salt, &nonce).expect("Failed to decrypt data");

    assert_eq!(plaintext.to_vec(), decrypted);
}

#[test]
fn test_wrong_password_fails() {
    let plaintext = b"test data";
    let (ciphertext, salt, nonce) = encrypt(plaintext, "correct").expect("Failed to encrypt data");
    let result = decrypt(&ciphertext, "wrong", &salt, &nonce);

    assert!(result.is_err());
}

#[test]
fn test_checksum() {
    let data = b"some data";
    let checksum1 = checksum(data);
    let checksum2 = checksum(data);

    assert_eq!(checksum1, checksum2);
    assert_ne!(checksum1, checksum(b"different"));
}
