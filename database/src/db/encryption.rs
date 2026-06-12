//! # Encryption Key Hierarchy
//!
//! Envelope encryption for ELS credential configs:
//!
//! ```text
//! Infrastructure (K8s / KMS — outside Eden)
//!   └── Org Key (one per tenant, referenced via org_key_refs table)
//!         └── Endpoint Key / DEK (wrapped by org key, stored in encryption_keys)
//!               └── ELS config (encrypted by DEK, stored in els_policy_versions)
//! ```
//!
//! ## Providers
//!
//! - `EnvKeyProvider` — reads a hex-encoded 256-bit key from an env var (dev/default).
//! - `K8sKeyProvider` — reads from K8s Secrets API (requires etcd encryption at rest).
//! - Stubs for AWS KMS, Azure KV, GCP KMS, HashiCorp Vault.
//!
//! ## Encryption Primitive
//!
//! AES-256-GCM with random 12-byte nonce, prepended to ciphertext.
//! NOT reusing the backup `helpers.rs` code (that uses Argon2 password derivation).

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Nonce};
use base64::Engine;
use eden_core::error::{EpError, ResultEP};

/// Nonce size for AES-256-GCM (96 bits / 12 bytes).
const NONCE_SIZE: usize = 12;
/// AES-256 key size (256 bits / 32 bytes).
pub const KEY_SIZE: usize = 32;

// ---------------------------------------------------------------------------
// AES-256-GCM encryption (raw key, no key derivation)
// ---------------------------------------------------------------------------

/// Encrypt `plaintext` with a raw 256-bit key using AES-256-GCM.
///
/// Returns `nonce || ciphertext` (12 bytes nonce prepended).
///
/// # Nonce safety
///
/// Random 12-byte nonce generated per call. Birthday bound is ~2^48 encryptions
/// per key before collision probability becomes concerning (sqrt(2^96) for a
/// 96-bit nonce). For ELS configs (one DEK per endpoint, encryptions only on
/// draft creation) this is well within safe margins.
pub fn encrypt_with_key(key: &[u8; KEY_SIZE], plaintext: &[u8]) -> ResultEP<Vec<u8>> {
    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| EpError::parse(format!("Invalid AES key: {e}")))?;

    let nonce_bytes: [u8; NONCE_SIZE] = rand::random();
    let nonce = Nonce::from_slice(&nonce_bytes);

    let ciphertext = cipher.encrypt(nonce, plaintext).map_err(|e| EpError::parse(format!("AES-256-GCM encryption failed: {e}")))?;

    // Prepend nonce to ciphertext
    let mut result = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    result.extend_from_slice(&nonce_bytes);
    result.extend_from_slice(&ciphertext);
    Ok(result)
}

/// Decrypt ciphertext produced by [`encrypt_with_key`].
///
/// Expects `nonce || ciphertext` format (12-byte nonce prepended).
pub fn decrypt_with_key(key: &[u8; KEY_SIZE], nonce_and_ciphertext: &[u8]) -> ResultEP<Vec<u8>> {
    if nonce_and_ciphertext.len() < NONCE_SIZE {
        return Err(EpError::parse("Ciphertext too short — missing nonce".to_string()));
    }

    let (nonce_bytes, ciphertext) = nonce_and_ciphertext.split_at(NONCE_SIZE);
    let nonce = Nonce::from_slice(nonce_bytes);

    let cipher = Aes256Gcm::new_from_slice(key).map_err(|e| EpError::parse(format!("Invalid AES key: {e}")))?;

    cipher.decrypt(nonce, ciphertext).map_err(|e| EpError::parse(format!("AES-256-GCM decryption failed: {e}")))
}

// ---------------------------------------------------------------------------
// OrgKeyProvider trait
// ---------------------------------------------------------------------------

/// Abstraction over external key management systems.
///
/// Implementations wrap/unwrap DEKs using an org-level master key stored
/// outside Eden (env var, K8s secret, AWS KMS, etc.).
#[async_trait::async_trait]
pub trait OrgKeyProvider: Send + Sync {
    /// Encrypt (wrap) a plaintext DEK using the org key identified by `key_ref`.
    async fn wrap(&self, key_ref: &str, plaintext: &[u8]) -> ResultEP<Vec<u8>>;

    /// Decrypt (unwrap) a wrapped DEK using the org key identified by `key_ref`.
    async fn unwrap(&self, key_ref: &str, ciphertext: &[u8]) -> ResultEP<Vec<u8>>;

    /// Provider name for logging/auditing.
    fn provider_name(&self) -> &'static str;
}

// ---------------------------------------------------------------------------
// EnvKeyProvider — default for dev / initial deployment
// ---------------------------------------------------------------------------

/// Reads a hex-encoded 256-bit key from an environment variable.
///
/// `key_ref` is the env var name (e.g., `"EDEN_ORG_KEY_ACME"`).
///
/// The org key is used directly as the wrapping key for AES-256-GCM
/// encrypt/decrypt of DEKs. Simple and sufficient for single-node
/// deployments where the env is trusted.
pub struct EnvKeyProvider;

impl EnvKeyProvider {
    fn resolve_key(key_ref: &str) -> ResultEP<[u8; KEY_SIZE]> {
        let hex_key = std::env::var(key_ref).map_err(|_| EpError::parse("Org key env var not set".to_string()))?;

        let bytes = hex::decode(&hex_key).map_err(|_| EpError::parse("Invalid hex in org key env var".to_string()))?;

        if bytes.len() != KEY_SIZE {
            return Err(EpError::parse(format!("Org key is {} bytes, expected {KEY_SIZE}", bytes.len())));
        }

        let mut key = [0u8; KEY_SIZE];
        key.copy_from_slice(&bytes);
        Ok(key)
    }
}

#[async_trait::async_trait]
impl OrgKeyProvider for EnvKeyProvider {
    async fn wrap(&self, key_ref: &str, plaintext: &[u8]) -> ResultEP<Vec<u8>> {
        let key = Self::resolve_key(key_ref)?;
        encrypt_with_key(&key, plaintext)
    }

    async fn unwrap(&self, key_ref: &str, ciphertext: &[u8]) -> ResultEP<Vec<u8>> {
        let key = Self::resolve_key(key_ref)?;
        decrypt_with_key(&key, ciphertext)
    }

    fn provider_name(&self) -> &'static str {
        "env"
    }
}

// ---------------------------------------------------------------------------
// DEK generation
// ---------------------------------------------------------------------------

/// Generate a random 256-bit data encryption key.
pub fn generate_dek() -> [u8; KEY_SIZE] {
    rand::random()
}

// ---------------------------------------------------------------------------
// Config-level encrypt / decrypt
// ---------------------------------------------------------------------------

/// Sentinel key used to mark encrypted JSONB values.
///
/// An encrypted config is stored as `{"__encrypted": "<base64(nonce||ciphertext)>"}`.
/// This avoids a column-type change from JSONB to BYTEA and allows transparent
/// detection of legacy plaintext rows.
const ENCRYPTED_SENTINEL: &str = "__encrypted";

/// Encrypt a `serde_json::Value` config for storage.
///
/// Returns a JSON value of the form `{"__encrypted": "<base64>"}`.
pub fn encrypt_config(dek: &[u8; KEY_SIZE], config: &serde_json::Value) -> ResultEP<serde_json::Value> {
    let plaintext = serde_json::to_vec(config).map_err(EpError::serde)?;
    let ciphertext = encrypt_with_key(dek, &plaintext)?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&ciphertext);
    Ok(serde_json::json!({ ENCRYPTED_SENTINEL: b64 }))
}

/// Decrypt a config value that may or may not be encrypted.
///
/// - If the value is `{"__encrypted": "<base64>"}`, decrypt and return the
///   original JSON.
/// - Otherwise, return the value as-is (legacy plaintext row).
pub fn decrypt_config(dek: &[u8; KEY_SIZE], config: &serde_json::Value) -> ResultEP<serde_json::Value> {
    if let Some(b64) = config.get(ENCRYPTED_SENTINEL).and_then(|v| v.as_str()) {
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| EpError::parse(format!("Invalid base64 in encrypted ELS config: {e}")))?;
        let plaintext = decrypt_with_key(dek, &ciphertext)?;
        serde_json::from_slice(&plaintext).map_err(EpError::serde)
    } else {
        // Legacy plaintext — return as-is
        Ok(config.clone())
    }
}

/// Encrypt a `ResolvedPolicy` JSON string for internal cache storage.
///
/// Returns a string prefixed with `ENC:` followed by base64-encoded ciphertext.
pub fn encrypt_cache_value(dek: &[u8; KEY_SIZE], json_str: &str) -> ResultEP<String> {
    let ciphertext = encrypt_with_key(dek, json_str.as_bytes())?;
    let b64 = base64::engine::general_purpose::STANDARD.encode(&ciphertext);
    Ok(format!("ENC:{b64}"))
}

/// Decrypt an internal cache value that may or may not be encrypted.
///
/// - If the value starts with `ENC:`, decrypt and return the JSON string.
/// - Otherwise, return as-is (legacy unencrypted cache entry).
pub fn decrypt_cache_value(dek: &[u8; KEY_SIZE], value: &str) -> ResultEP<String> {
    if let Some(b64) = value.strip_prefix("ENC:") {
        let ciphertext = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| EpError::parse(format!("Invalid base64 in encrypted ELS cache: {e}")))?;
        let plaintext = decrypt_with_key(dek, &ciphertext)?;
        String::from_utf8(plaintext).map_err(|e| EpError::parse(format!("Decrypted ELS cache is not valid UTF-8: {e}")))
    } else {
        Ok(value.to_owned())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn test_encrypt_decrypt_roundtrip() {
        let key = generate_dek();
        let plaintext = b"AWS secret access key: wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";

        let encrypted = encrypt_with_key(&key, plaintext).expect("encrypt");
        assert_ne!(encrypted, plaintext);
        assert!(encrypted.len() > plaintext.len()); // nonce + tag overhead

        let decrypted = decrypt_with_key(&key, &encrypted).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_nonces_produce_different_ciphertext() {
        let key = generate_dek();
        let plaintext = b"same plaintext";

        let enc1 = encrypt_with_key(&key, plaintext).expect("encrypt 1");
        let enc2 = encrypt_with_key(&key, plaintext).expect("encrypt 2");

        // Random nonces => different ciphertext
        assert_ne!(enc1, enc2);

        // Both decrypt to same plaintext
        assert_eq!(decrypt_with_key(&key, &enc1).expect("dec 1"), plaintext);
        assert_eq!(decrypt_with_key(&key, &enc2).expect("dec 2"), plaintext);
    }

    #[test]
    fn test_wrong_key_fails_decrypt() {
        let key1 = generate_dek();
        let key2 = generate_dek();
        let plaintext = b"secret data";

        let encrypted = encrypt_with_key(&key1, plaintext).expect("encrypt");
        let result = decrypt_with_key(&key2, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_tampered_ciphertext_fails() {
        let key = generate_dek();
        let plaintext = b"secret data";

        let mut encrypted = encrypt_with_key(&key, plaintext).expect("encrypt");
        // Flip a byte in the ciphertext (after nonce)
        if let Some(byte) = encrypted.get_mut(NONCE_SIZE + 1) {
            *byte ^= 0xFF;
        }

        let result = decrypt_with_key(&key, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_too_short_ciphertext_fails() {
        let key = generate_dek();
        let short = vec![0u8; 5]; // less than NONCE_SIZE
        assert!(decrypt_with_key(&key, &short).is_err());
    }

    #[test]
    fn test_empty_plaintext() {
        let key = generate_dek();
        let encrypted = encrypt_with_key(&key, b"").expect("encrypt empty");
        let decrypted = decrypt_with_key(&key, &encrypted).expect("decrypt empty");
        assert_eq!(decrypted, b"");
    }

    #[test]
    fn test_generate_dek_unique() {
        let k1 = generate_dek();
        let k2 = generate_dek();
        assert_ne!(k1, k2);
    }

    #[test]
    #[serial]
    fn test_env_key_provider_resolve_key() {
        // Set a test env var
        let var_name = "EDEN_TEST_ORG_KEY_RESOLVE";
        let key = generate_dek();
        let hex_key = hex::encode(key);
        unsafe { std::env::set_var(var_name, &hex_key) };

        let resolved = EnvKeyProvider::resolve_key(var_name).expect("resolve");
        assert_eq!(resolved, key);

        unsafe { std::env::remove_var(var_name) };
    }

    #[test]
    #[serial]
    fn test_env_key_provider_missing_var() {
        let result = EnvKeyProvider::resolve_key("EDEN_NONEXISTENT_KEY_VAR_12345");
        assert!(result.is_err());
    }

    #[test]
    #[serial]
    fn test_env_key_provider_wrong_length() {
        let var_name = "EDEN_TEST_ORG_KEY_SHORT";
        unsafe { std::env::set_var(var_name, "aabbccdd") }; // only 4 bytes
        let result = EnvKeyProvider::resolve_key(var_name);
        assert!(result.is_err());
        unsafe { std::env::remove_var(var_name) };
    }

    #[tokio::test]
    #[serial]
    async fn test_env_key_provider_wrap_unwrap() {
        let var_name = "EDEN_TEST_ORG_KEY_WRAP";
        let org_key = generate_dek();
        unsafe { std::env::set_var(var_name, hex::encode(org_key)) };

        let provider = EnvKeyProvider;
        let dek = generate_dek();

        let wrapped = provider.wrap(var_name, &dek).await.expect("wrap");
        let unwrapped = provider.unwrap(var_name, &wrapped).await.expect("unwrap");

        assert_eq!(unwrapped, dek);

        unsafe { std::env::remove_var(var_name) };
    }

    #[test]
    fn test_encrypt_decrypt_config_roundtrip() {
        let dek = generate_dek();
        let config = serde_json::json!({"username": "alice", "password": "s3cret!"});

        let encrypted = encrypt_config(&dek, &config).expect("encrypt config");
        // Must have the sentinel wrapper
        assert!(encrypted.get(ENCRYPTED_SENTINEL).is_some());
        // Must not contain the plaintext
        assert!(!encrypted.to_string().contains("s3cret!"));

        let decrypted = decrypt_config(&dek, &encrypted).expect("decrypt config");
        assert_eq!(decrypted, config);
    }

    #[test]
    fn test_decrypt_config_plaintext_passthrough() {
        let dek = generate_dek();
        let plaintext = serde_json::json!({"variables": {"app.tenant_id": "t-1"}});

        // Plaintext config (no sentinel) should pass through unchanged
        let result = decrypt_config(&dek, &plaintext).expect("passthrough");
        assert_eq!(result, plaintext);
    }

    #[test]
    fn test_decrypt_config_wrong_key_fails() {
        let dek1 = generate_dek();
        let dek2 = generate_dek();
        let config = serde_json::json!({"password": "secret"});

        let encrypted = encrypt_config(&dek1, &config).expect("encrypt");
        let result = decrypt_config(&dek2, &encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_decrypt_cache_value_roundtrip() {
        let dek = generate_dek();
        let json = r#"{"strategy":"postgres","config":{"variables":{"app.tenant":"t-1"}}}"#;

        let encrypted = encrypt_cache_value(&dek, json).expect("encrypt cache");
        assert!(encrypted.starts_with("ENC:"));
        assert!(!encrypted.contains("tenant"));

        let decrypted = decrypt_cache_value(&dek, &encrypted).expect("decrypt cache");
        assert_eq!(decrypted, json);
    }

    #[test]
    fn test_decrypt_cache_value_plaintext_passthrough() {
        let dek = generate_dek();
        let plain = r#"{"strategy":"postgres","config":{}}"#;
        let result = decrypt_cache_value(&dek, plain).expect("passthrough");
        assert_eq!(result, plain);
    }
}
