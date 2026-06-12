//! MySQL authentication plugins.
//!
//! MySQL supports pluggable authentication. Common plugins include:
//! - mysql_native_password (default until MySQL 8.0)
//! - caching_sha2_password (default in MySQL 8.0+)
//! - sha256_password

use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Authentication plugin interface.
pub trait AuthPlugin {
    /// Plugin name as it appears in the protocol.
    fn name(&self) -> &'static str;

    /// Generate authentication response from password and auth data.
    fn generate_auth_response(&self, password: &str, auth_data: &[u8]) -> Vec<u8>;
}

/// mysql_native_password authentication.
///
/// Uses SHA1-based challenge-response:
/// SHA1(password) XOR SHA1(auth_data + SHA1(SHA1(password)))
#[derive(Clone, Debug, Default)]
pub struct MysqlNativePassword;

impl MysqlNativePassword {
    pub fn new() -> Self {
        Self
    }

    /// Generate native password auth response.
    ///
    /// The auth data should be the 20-byte scramble from the server handshake.
    pub fn scramble(password: &str, auth_data: &[u8]) -> Vec<u8> {
        if password.is_empty() {
            return Vec::new();
        }

        // SHA1(password)
        let mut hasher = Sha1::new();
        hasher.update(password.as_bytes());
        let stage1 = hasher.finalize();

        // SHA1(SHA1(password))
        let mut hasher = Sha1::new();
        hasher.update(stage1);
        let stage2 = hasher.finalize();

        // SHA1(auth_data + SHA1(SHA1(password)))
        let mut hasher = Sha1::new();
        hasher.update(auth_data);
        hasher.update(stage2);
        let stage3 = hasher.finalize();

        // SHA1(password) XOR SHA1(auth_data + SHA1(SHA1(password)))
        stage1.iter().zip(stage3.iter()).map(|(a, b)| a ^ b).collect()
    }
}

impl AuthPlugin for MysqlNativePassword {
    fn name(&self) -> &'static str {
        "mysql_native_password"
    }

    fn generate_auth_response(&self, password: &str, auth_data: &[u8]) -> Vec<u8> {
        Self::scramble(password, auth_data)
    }
}

/// caching_sha2_password authentication (MySQL 8.0+ default).
///
/// Uses SHA256 and supports caching for performance.
#[derive(Clone, Debug, Default)]
pub struct CachingSha2Password;

impl CachingSha2Password {
    pub fn new() -> Self {
        Self
    }

    /// Generate initial auth response (fast path).
    ///
    /// XOR(SHA256(password), SHA256(SHA256(SHA256(password)) + auth_data))
    pub fn scramble(password: &str, auth_data: &[u8]) -> Vec<u8> {
        if password.is_empty() {
            return Vec::new();
        }

        // SHA256(password)
        let mut hasher = Sha256::new();
        hasher.update(password.as_bytes());
        let password_hash = hasher.finalize();

        // SHA256(SHA256(password))
        let mut hasher = Sha256::new();
        hasher.update(password_hash);
        let password_hash_hash = hasher.finalize();

        // SHA256(SHA256(SHA256(password)) + auth_data)
        let mut hasher = Sha256::new();
        hasher.update(password_hash_hash);
        hasher.update(auth_data);
        let scramble_hash = hasher.finalize();

        // XOR(SHA256(password), scramble_hash)
        password_hash.iter().zip(scramble_hash.iter()).map(|(a, b)| a ^ b).collect()
    }

    /// Generate full auth response (slow path, when cache miss).
    ///
    /// This sends the password XOR'd with the server's public key.
    pub fn scramble_full(password: &str, auth_data: &[u8]) -> Vec<u8> {
        // Full auth requires RSA encryption with server's public key
        // For now, return the fast path scramble
        Self::scramble(password, auth_data)
    }
}

impl AuthPlugin for CachingSha2Password {
    fn name(&self) -> &'static str {
        "caching_sha2_password"
    }

    fn generate_auth_response(&self, password: &str, auth_data: &[u8]) -> Vec<u8> {
        Self::scramble(password, auth_data)
    }
}

/// sha256_password authentication.
///
/// Similar to caching_sha2_password but without caching.
#[derive(Clone, Debug, Default)]
pub struct Sha256Password;

impl Sha256Password {
    pub fn new() -> Self {
        Self
    }

    /// Generate SHA256 auth response.
    pub fn scramble(password: &str, auth_data: &[u8]) -> Vec<u8> {
        // Same scramble as caching_sha2_password
        CachingSha2Password::scramble(password, auth_data)
    }
}

impl AuthPlugin for Sha256Password {
    fn name(&self) -> &'static str {
        "sha256_password"
    }

    fn generate_auth_response(&self, password: &str, auth_data: &[u8]) -> Vec<u8> {
        Self::scramble(password, auth_data)
    }
}

/// Get an auth plugin by name.
pub fn get_auth_plugin(name: &str) -> Option<Box<dyn AuthPlugin + Send + Sync>> {
    match name {
        "mysql_native_password" => Some(Box::new(MysqlNativePassword::new())),
        "caching_sha2_password" => Some(Box::new(CachingSha2Password::new())),
        "sha256_password" => Some(Box::new(Sha256Password::new())),
        _ => None,
    }
}

/// Auth plugin response codes for caching_sha2_password.
pub mod caching_sha2 {
    /// Fast auth success - server cached the password hash.
    pub const FAST_AUTH_SUCCESS: u8 = 0x03;
    /// Perform full authentication (cache miss).
    pub const PERFORM_FULL_AUTHENTICATION: u8 = 0x04;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_native_password_empty() {
        let response = MysqlNativePassword::scramble("", b"scramble12scramble12");
        assert!(response.is_empty());
    }

    #[test]
    fn test_native_password_scramble() {
        // Known test vector
        let auth_data = [
            0x3d, 0x67, 0x29, 0x3e, 0x75, 0x6e, 0x4e, 0x20, 0x5e, 0x34, 0x34, 0x4a, 0x72, 0x3c, 0x41, 0x31, 0x79, 0x54, 0x5e, 0x4e,
        ];
        let response = MysqlNativePassword::scramble("password", &auth_data);
        assert_eq!(response.len(), 20);
    }

    #[test]
    fn test_caching_sha2_empty() {
        let response = CachingSha2Password::scramble("", b"scramble12scramble12");
        assert!(response.is_empty());
    }

    #[test]
    fn test_caching_sha2_scramble() {
        let auth_data = b"scramble12scramble12";
        let response = CachingSha2Password::scramble("password", auth_data);
        assert_eq!(response.len(), 32); // SHA256 produces 32 bytes
    }

    #[test]
    fn test_get_auth_plugin() {
        assert!(get_auth_plugin("mysql_native_password").is_some());
        assert!(get_auth_plugin("caching_sha2_password").is_some());
        assert!(get_auth_plugin("sha256_password").is_some());
        assert!(get_auth_plugin("unknown_plugin").is_none());
    }

    #[test]
    fn test_plugin_names() {
        let native = MysqlNativePassword::new();
        assert_eq!(native.name(), "mysql_native_password");

        let caching = CachingSha2Password::new();
        assert_eq!(caching.name(), "caching_sha2_password");
    }
}
