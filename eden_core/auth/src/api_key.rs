use rand::{Rng, rng};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use utoipa::ToSchema;

/// API key for robot (machine account) authentication.
///
/// Stores a salted SHA256 hash of the plaintext API key. The plaintext key
/// is only available at generation time and is never stored.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct ApiKey {
    salt: [u8; 8],
    hash: [u8; 32],
}

impl ApiKey {
    /// Generate a new API key. Returns the plaintext key (to show the user once)
    /// and the hashed `ApiKey` struct (to store in the database).
    pub fn generate() -> (String, Self) {
        let mut rng = rng();

        // Generate a 32-byte random key, hex-encoded to 64 characters
        let key_bytes: [u8; 32] = rng.random();
        let plaintext = format!("eden_{}", hex::encode(key_bytes));

        let salt: [u8; 8] = rng.random();
        let hash = hash_bytes([salt.as_ref(), plaintext.as_bytes()].concat());

        (plaintext, Self { salt, hash })
    }

    /// Create an `ApiKey` from a known plaintext (for testing or import).
    pub fn from_plaintext(plaintext: &str) -> Self {
        let mut rng = rng();
        let salt: [u8; 8] = rng.random();
        let hash = hash_bytes([salt.as_ref(), plaintext.as_bytes()].concat());
        Self { salt, hash }
    }

    /// Verify a plaintext API key against the stored hash.
    pub fn verify(&self, plaintext: &str) -> bool {
        self.hash == hash_bytes([self.salt.as_ref(), plaintext.as_bytes()].concat())
    }
}

fn hash_bytes(bytes: Vec<u8>) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

#[cfg(test)]
mod test {
    use super::ApiKey;

    #[test]
    fn test_api_key_generate_and_verify() {
        let (plaintext, api_key) = ApiKey::generate();

        assert!(plaintext.starts_with("eden_"));
        assert!(api_key.verify(&plaintext));
        assert!(!api_key.verify("wrong_key"));
    }

    #[test]
    fn test_api_key_from_plaintext() {
        let api_key = ApiKey::from_plaintext("test_key_123");

        assert!(api_key.verify("test_key_123"));
        assert!(!api_key.verify("wrong_key"));
    }
}
