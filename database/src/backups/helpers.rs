use aes_gcm::{
    Aes256Gcm, Nonce,
    aead::{Aead, KeyInit},
};
use argon2::Argon2;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use bb8::PooledConnection;
use bb8_postgres::PostgresConnectionManager;
use eden_core::error::{EpError, ResultEP};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use function_name::named;
use rand::{TryRngCore, rngs::OsRng};
use sha2::{Digest, Sha256};
use std::path::Path;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio_postgres::NoTls;

use super::lib::{NONCE_SIZE, SALT_SIZE};

/// Encrypt data using AES-256-GCM
pub(crate) fn encrypt(plaintext: &[u8], password: &str) -> ResultEP<(Vec<u8>, [u8; SALT_SIZE], [u8; NONCE_SIZE])> {
    let mut salt = [0u8; SALT_SIZE];
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.try_fill_bytes(&mut salt).map_err(|e| EpError::auth(format!("Failed to generate salt: {e}")))?;
    OsRng.try_fill_bytes(&mut nonce_bytes).map_err(|e| EpError::auth(format!("Failed to generate nonce: {e}")))?;

    let key = derive_key(password, &salt)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| EpError::auth(format!("Cipher init failed: {e}")))?;

    let nonce = Nonce::from_slice(&nonce_bytes);
    let ciphertext = cipher.encrypt(nonce, plaintext).map_err(|e| EpError::auth(format!("Encryption failed: {e}")))?;

    Ok((ciphertext, salt, nonce_bytes))
}

/// Decrypt data using AES-256-GCM
pub(crate) fn decrypt(ciphertext: &[u8], password: &str, salt: &[u8], nonce: &[u8]) -> ResultEP<Vec<u8>> {
    let key = derive_key(password, salt)?;
    let cipher = Aes256Gcm::new_from_slice(&key).map_err(|e| EpError::auth(format!("Cipher init failed: {e}")))?;

    let nonce = Nonce::from_slice(nonce);
    cipher.decrypt(nonce, ciphertext).map_err(|e| EpError::auth(format!("Decryption failed: {e}")))
}

/// Derive encryption key from password using Argon2
fn derive_key(password: &str, salt: &[u8]) -> ResultEP<[u8; 32]> {
    let mut key = [0u8; 32];
    Argon2::default()
        .hash_password_into(password.as_bytes(), salt, &mut key)
        .map_err(|e| EpError::auth(format!("Key derivation failed: {e}")))?;
    Ok(key)
}

/// Compute SHA256 checksum
pub(crate) fn checksum(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

/// Write data to file
pub(crate) async fn write_file(path: impl AsRef<Path>, data: &[u8]) -> std::io::Result<()> {
    let mut file = File::create(path).await?;
    file.write_all(data).await?;
    file.flush().await
}

/// Get PostgreSQL connection string from active connection
#[named]
#[allow(dead_code)]
pub(super) async fn get_pg_conn_str(conn: &PooledConnection<'_, PostgresConnectionManager<NoTls>>) -> ResultEP<String> {
    const DEFAULT_PORT: i32 = 5432;
    const DEFAULT_DB: &str = "postgres";
    const DEFAULT_USER: &str = "postgres";
    const DEFAULT_HOST: &str = "localhost";

    let row = conn
        .query_one("SELECT current_database(), current_user, inet_server_addr(), inet_server_port()", &[])
        .await
        .map_err(|e| EpError::Database(eden_core::error::DatabaseError::Custom(e.to_string())))?;

    let ctx = ctx_with_trace!().with_feature("backup");

    let db: String = row.try_get(0).unwrap_or_else(|e| {
        log_warn!(
            ctx.clone(),
            "Could not read current_database() from connection, using default",
            audience = LogAudience::Internal,
            default = DEFAULT_DB,
            error = e.to_string()
        );
        DEFAULT_DB.to_string()
    });
    let user: String = row.try_get(1).unwrap_or_else(|e| {
        log_warn!(
            ctx.clone(),
            "Could not read current_user from connection, using default",
            audience = LogAudience::Internal,
            default = DEFAULT_USER,
            error = e.to_string()
        );
        DEFAULT_USER.to_string()
    });
    let host: String = row.try_get::<_, std::net::IpAddr>(2).map(|h| h.to_string()).unwrap_or_else(|e| {
        log_warn!(
            ctx.clone(),
            "Could not read inet_server_addr() from connection, using default",
            audience = LogAudience::Internal,
            default = DEFAULT_HOST,
            error = e.to_string()
        );
        DEFAULT_HOST.to_string()
    });
    let port: i32 = row.try_get(3).unwrap_or_else(|e| {
        log_warn!(
            ctx.clone(),
            "Could not read inet_server_port() from connection, using default",
            audience = LogAudience::Internal,
            default = DEFAULT_PORT,
            error = e.to_string()
        );
        DEFAULT_PORT
    });

    Ok(format!("postgresql://{user}@{host}:{port}/{db}", user = user, host = host, port = port, db = db))
}

/// Check if pg_restore error is ignorable
pub(super) fn is_ignorable_pg_restore_error(status: std::process::ExitStatus, stderr: &str) -> bool {
    if status.success() {
        return false;
    }

    let has_transaction_timeout_error = stderr.contains("unrecognized configuration parameter \"transaction_timeout\"");
    let has_ignored_errors_warning = stderr.contains("pg_restore: warning: errors ignored on restore:");

    if let Some(code) = status.code() {
        code == 1 && has_transaction_timeout_error && has_ignored_errors_warning
    } else {
        false
    }
}

/// Decrypt backup data and verify checksum
pub(crate) async fn decrypt_and_verify(
    dump_path: &Path,
    checksum_expected: &str,
    salt_b64: &str,
    nonce_b64: &str,
    encrypt_password: &str,
) -> ResultEP<Vec<u8>> {
    let ciphertext = tokio::fs::read(dump_path).await?;
    let salt = BASE64.decode(salt_b64).map_err(|e| EpError::auth(format!("Invalid salt: {e}")))?;
    let nonce = BASE64.decode(nonce_b64).map_err(|e| EpError::auth(format!("Invalid nonce: {e}")))?;

    if salt.len() != SALT_SIZE {
        return Err(EpError::auth(format!("Invalid salt length: expected {SALT_SIZE}, got {}", salt.len())));
    }
    if nonce.len() != NONCE_SIZE {
        return Err(EpError::auth(format!("Invalid nonce length: expected {NONCE_SIZE}, got {}", nonce.len())));
    }

    let plaintext = decrypt(&ciphertext, encrypt_password, &salt, &nonce)?;

    let computed_checksum = checksum(&plaintext);
    if computed_checksum != checksum_expected {
        return Err(EpError::auth("Checksum mismatch - data corrupted"));
    }

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encrypt_decrypt_roundtrip() {
        let plaintext = b"hello world backup data";
        let password = "test-password";

        let (ciphertext, salt, nonce_bytes) = encrypt(plaintext, password).expect("encrypt");
        assert_ne!(ciphertext, plaintext, "ciphertext must differ from plaintext");

        let decrypted = decrypt(&ciphertext, password, &salt, &nonce_bytes).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn decrypt_with_wrong_password_fails() {
        let plaintext = b"secret data";
        let (ciphertext, salt, nonce_bytes) = encrypt(plaintext, "correct").expect("encrypt");

        let result = decrypt(&ciphertext, "wrong", &salt, &nonce_bytes);
        assert!(result.is_err(), "decryption with wrong password should fail");
    }

    #[test]
    fn checksum_is_deterministic() {
        let data = b"deterministic data";
        let c1 = checksum(data);
        let c2 = checksum(data);
        assert_eq!(c1, c2);
    }

    #[test]
    fn checksum_differs_for_different_data() {
        let c1 = checksum(b"data1");
        let c2 = checksum(b"data2");
        assert_ne!(c1, c2);
    }

    #[cfg(unix)]
    #[test]
    fn is_ignorable_pg_restore_error_returns_false_for_success() {
        use std::os::unix::process::ExitStatusExt;
        let status = std::process::ExitStatus::from_raw(0);
        assert!(!is_ignorable_pg_restore_error(status, "anything"));
    }

    #[cfg(unix)]
    #[test]
    fn is_ignorable_pg_restore_error_returns_true_for_transaction_timeout() {
        use std::os::unix::process::ExitStatusExt;
        // Exit code 1 shifted left by 8 for wait status format
        let status = std::process::ExitStatus::from_raw(1 << 8);
        let stderr = "unrecognized configuration parameter \"transaction_timeout\"\npg_restore: warning: errors ignored on restore: 1";
        assert!(is_ignorable_pg_restore_error(status, stderr));
    }

    #[cfg(unix)]
    #[test]
    fn is_ignorable_pg_restore_error_returns_false_for_other_errors() {
        use std::os::unix::process::ExitStatusExt;
        let status = std::process::ExitStatus::from_raw(1 << 8);
        let stderr = "some other error";
        assert!(!is_ignorable_pg_restore_error(status, stderr));
    }

    #[tokio::test]
    async fn decrypt_and_verify_rejects_wrong_salt_length() {
        let dir = std::env::temp_dir().join("eden_test_salt_len");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("test.enc");

        let plaintext = b"test data";
        let password = "pass";
        let (ciphertext, _salt, _nonce) = encrypt(plaintext, password).expect("encrypt");
        let checksum_val = checksum(plaintext);

        write_file(&path, &ciphertext).await.expect("write");

        // Use a salt that's too short
        let bad_salt_b64 = BASE64.encode([0u8; 4]);
        let good_nonce_b64 = BASE64.encode(_nonce);

        let result = decrypt_and_verify(&path, &checksum_val, &bad_salt_b64, &good_nonce_b64, password).await;
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err());
        assert!(err_msg.contains("salt length"), "error should mention salt length: {err_msg}");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn decrypt_and_verify_rejects_wrong_nonce_length() {
        let dir = std::env::temp_dir().join("eden_test_nonce_len");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("test.enc");

        let plaintext = b"test data";
        let password = "pass";
        let (ciphertext, salt, _nonce) = encrypt(plaintext, password).expect("encrypt");
        let checksum_val = checksum(plaintext);

        write_file(&path, &ciphertext).await.expect("write");

        let good_salt_b64 = BASE64.encode(salt);
        let bad_nonce_b64 = BASE64.encode([0u8; 4]);

        let result = decrypt_and_verify(&path, &checksum_val, &good_salt_b64, &bad_nonce_b64, password).await;
        assert!(result.is_err());
        let err_msg = format!("{:?}", result.err());
        assert!(err_msg.contains("nonce length"), "error should mention nonce length: {err_msg}");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn decrypt_and_verify_roundtrip() {
        let dir = std::env::temp_dir().join("eden_test_dav_roundtrip");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("test.enc");

        let plaintext = b"roundtrip test data";
        let password = "roundtrip-pass";
        let (ciphertext, salt, nonce) = encrypt(plaintext, password).expect("encrypt");
        let checksum_val = checksum(plaintext);

        write_file(&path, &ciphertext).await.expect("write");

        let salt_b64 = BASE64.encode(salt);
        let nonce_b64 = BASE64.encode(nonce);

        let result = decrypt_and_verify(&path, &checksum_val, &salt_b64, &nonce_b64, password).await;
        assert!(result.is_ok());
        assert_eq!(result.expect("should succeed"), plaintext);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn write_file_creates_and_writes() {
        let dir = std::env::temp_dir().join("eden_test_write_file");
        let _ = tokio::fs::create_dir_all(&dir).await;
        let path = dir.join("test_output.bin");

        write_file(&path, b"test data").await.expect("write_file");

        let content = tokio::fs::read(&path).await.expect("read back");
        assert_eq!(content, b"test data");

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
