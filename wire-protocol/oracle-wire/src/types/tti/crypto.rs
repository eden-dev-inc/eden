//! Oracle authentication cryptographic operations.
//!
//! This module implements the cryptographic operations used in Oracle authentication:
//!
//! - **O8LOGON**: SHA-1 based authentication (Oracle 11g+)
//! - **O9LOGON**: SHA-256 based authentication (Oracle 12c+)
//!
//! The authentication flow works as follows:
//! 1. Client sends username
//! 2. Server sends session key (AUTH_SESSKEY) and salt (AUTH_VFR_DATA)
//! 3. Client computes password verifier using hash(password + salt)
//! 4. Client computes session key response
//! 5. Server validates and establishes session
//!
//! ## Session Encryption
//!
//! After authentication, Oracle uses AES-256-CBC for encrypting session data.
//! The encryption key is derived from the password hash and combined session keys.

use aes::cipher::{BlockDecryptMut, BlockEncryptMut, KeyIvInit};
use hmac::{Hmac, Mac};
use sha1::Sha1;
use sha2::Sha256;

type Aes256CbcEnc = cbc::Encryptor<aes::Aes256>;
type Aes256CbcDec = cbc::Decryptor<aes::Aes256>;
type Aes128CbcEnc = cbc::Encryptor<aes::Aes128>;
type Aes128CbcDec = cbc::Decryptor<aes::Aes128>;

type HmacSha1 = Hmac<Sha1>;
type HmacSha256 = Hmac<Sha256>;

/// Error during cryptographic operations.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("invalid key length")]
    InvalidKeyLength,
    #[error("invalid salt length: expected at least {expected}, got {actual}")]
    InvalidSaltLength { expected: usize, actual: usize },
    #[error("invalid session key length: expected {expected}, got {actual}")]
    InvalidSessionKeyLength { expected: usize, actual: usize },
    #[error("hex decode error: {0}")]
    HexDecode(String),
    #[error("invalid IV length: expected {expected}, got {actual}")]
    InvalidIvLength { expected: usize, actual: usize },
    #[error("invalid ciphertext length: must be multiple of block size")]
    InvalidCiphertextLength,
    #[error("invalid padding")]
    InvalidPadding,
    #[error("data too large for encryption")]
    DataTooLarge,
}

/// Oracle authentication verifier result.
#[derive(Clone, Debug)]
pub struct AuthVerifier {
    /// The computed password verifier.
    pub verifier: Vec<u8>,
    /// The session key response.
    pub session_key_response: Vec<u8>,
}

/// Compute O8LOGON (SHA-1 based) password verifier.
///
/// Oracle 11g+ uses this authentication scheme:
/// 1. H = SHA1(salt || password)
/// 2. Server provides AUTH_SESSKEY (encrypted server session key)
/// 3. Client computes: HMAC-SHA1(H, server_session_key)
///
/// # Arguments
/// * `password` - The user's password (case-sensitive in Oracle 11g+)
/// * `salt` - The AUTH_VFR_DATA from server (typically 10 bytes)
/// * `server_session_key` - The AUTH_SESSKEY from server (typically 48 bytes)
///
/// # Returns
/// The password verifier to send back to the server
pub fn compute_o8logon_verifier(password: &str, salt: &[u8], server_session_key: &[u8]) -> Result<AuthVerifier, CryptoError> {
    use sha1::Digest;

    // Minimum salt length for O8LOGON
    if salt.len() < 10 {
        return Err(CryptoError::InvalidSaltLength { expected: 10, actual: salt.len() });
    }

    // Compute H = SHA1(salt || password)
    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    let password_hash = hasher.finalize();

    // Compute HMAC-SHA1(H, server_session_key)
    let mut mac = HmacSha1::new_from_slice(&password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    mac.update(server_session_key);
    let verifier = mac.finalize().into_bytes().to_vec();

    // Compute session key response (for session establishment)
    let mut session_mac = HmacSha1::new_from_slice(&password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    session_mac.update(&verifier);
    let session_key_response = session_mac.finalize().into_bytes().to_vec();

    Ok(AuthVerifier { verifier, session_key_response })
}

/// Compute O9LOGON (SHA-256 based) password verifier.
///
/// Oracle 12c+ uses this stronger authentication scheme:
/// 1. H = SHA256(salt || password)
/// 2. Server provides AUTH_SESSKEY (encrypted server session key)
/// 3. Client computes: HMAC-SHA256(H, server_session_key)
///
/// # Arguments
/// * `password` - The user's password (case-sensitive)
/// * `salt` - The AUTH_VFR_DATA from server (typically 16 bytes for SHA-256)
/// * `server_session_key` - The AUTH_SESSKEY from server (typically 64 bytes)
///
/// # Returns
/// The password verifier to send back to the server
pub fn compute_o9logon_verifier(password: &str, salt: &[u8], server_session_key: &[u8]) -> Result<AuthVerifier, CryptoError> {
    use sha2::Digest;

    // Minimum salt length for O9LOGON
    if salt.len() < 16 {
        return Err(CryptoError::InvalidSaltLength { expected: 16, actual: salt.len() });
    }

    // Compute H = SHA256(salt || password)
    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    let password_hash = hasher.finalize();

    // Compute HMAC-SHA256(H, server_session_key)
    let mut mac = HmacSha256::new_from_slice(&password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    mac.update(server_session_key);
    let verifier = mac.finalize().into_bytes().to_vec();

    // Compute session key response
    let mut session_mac = HmacSha256::new_from_slice(&password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    session_mac.update(&verifier);
    let session_key_response = session_mac.finalize().into_bytes().to_vec();

    Ok(AuthVerifier { verifier, session_key_response })
}

/// Compute the SHA-1 password hash used in O8LOGON.
///
/// This is the intermediate hash H = SHA1(salt || password).
pub fn sha1_password_hash(password: &str, salt: &[u8]) -> [u8; 20] {
    use sha1::Digest;

    let mut hasher = Sha1::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

/// Compute the SHA-256 password hash used in O9LOGON.
///
/// This is the intermediate hash H = SHA256(salt || password).
pub fn sha256_password_hash(password: &str, salt: &[u8]) -> [u8; 32] {
    use sha2::Digest;

    let mut hasher = Sha256::new();
    hasher.update(salt);
    hasher.update(password.as_bytes());
    hasher.finalize().into()
}

/// Decode hex-encoded authentication data.
///
/// Oracle often sends authentication data (salt, session key) as hex strings.
pub fn decode_hex_auth_data(hex: &str) -> Result<Vec<u8>, CryptoError> {
    hex::decode(hex).map_err(|e| CryptoError::HexDecode(e.to_string()))
}

/// Encode authentication data as hex string.
///
/// Used when sending verifiers back to the server.
pub fn encode_hex_auth_data(data: &[u8]) -> String {
    hex::encode_upper(data)
}

/// Derive encryption key for session data.
///
/// Oracle uses this for encrypting session data after authentication.
/// The key is derived from the password hash and session key.
pub fn derive_session_encryption_key_sha1(password_hash: &[u8; 20], combined_session_key: &[u8]) -> Result<[u8; 20], CryptoError> {
    let mut mac = HmacSha1::new_from_slice(password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    mac.update(combined_session_key);
    Ok(mac.finalize().into_bytes().into())
}

/// Derive encryption key for session data (SHA-256 version).
///
/// Used with O9LOGON for stronger encryption.
pub fn derive_session_encryption_key_sha256(password_hash: &[u8; 32], combined_session_key: &[u8]) -> Result<[u8; 32], CryptoError> {
    let mut mac = HmacSha256::new_from_slice(password_hash).map_err(|_| CryptoError::InvalidKeyLength)?;
    mac.update(combined_session_key);
    Ok(mac.finalize().into_bytes().into())
}

/// XOR two byte slices (for key mixing).
///
/// Used in Oracle's key derivation process.
pub fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter().cycle()).map(|(x, y)| x ^ y).collect()
}

/// Compute combined session key from client and server keys.
///
/// After authentication, both client and server have session keys.
/// The combined key is used for session encryption.
pub fn combine_session_keys(client_key: &[u8], server_key: &[u8]) -> Vec<u8> {
    xor_bytes(client_key, server_key)
}

/// AES block size in bytes.
pub const AES_BLOCK_SIZE: usize = 16;

/// AES-256 key size in bytes.
pub const AES256_KEY_SIZE: usize = 32;

/// AES-128 key size in bytes.
pub const AES128_KEY_SIZE: usize = 16;

/// Encrypt data using AES-256-CBC with PKCS#7 padding.
///
/// Oracle uses AES-256-CBC for session data encryption after authentication.
///
/// # Arguments
/// * `key` - 32-byte AES-256 key
/// * `iv` - 16-byte initialization vector
/// * `plaintext` - Data to encrypt
///
/// # Returns
/// Ciphertext with PKCS#7 padding applied
pub fn aes256_cbc_encrypt(key: &[u8; 32], iv: &[u8; 16], plaintext: &[u8]) -> Vec<u8> {
    let padded = pkcs7_pad(plaintext, AES_BLOCK_SIZE);
    let mut ciphertext = padded;
    let len = ciphertext.len();
    let encryptor = Aes256CbcEnc::new(key.into(), iv.into());
    encryptor
        .encrypt_padded_mut::<aes::cipher::block_padding::NoPadding>(&mut ciphertext, len)
        .expect("buffer has correct length");
    ciphertext
}

/// Decrypt data using AES-256-CBC with PKCS#7 padding.
///
/// # Arguments
/// * `key` - 32-byte AES-256 key
/// * `iv` - 16-byte initialization vector
/// * `ciphertext` - Data to decrypt (must be multiple of block size)
///
/// # Returns
/// Decrypted plaintext with padding removed
pub fn aes256_cbc_decrypt(key: &[u8; 32], iv: &[u8; 16], ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if ciphertext.is_empty() || !ciphertext.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Err(CryptoError::InvalidCiphertextLength);
    }

    let mut plaintext = ciphertext.to_vec();
    let decryptor = Aes256CbcDec::new(key.into(), iv.into());
    decryptor
        .decrypt_padded_mut::<aes::cipher::block_padding::NoPadding>(&mut plaintext)
        .map_err(|_| CryptoError::InvalidPadding)?;

    pkcs7_unpad(&plaintext).map(|p| p.to_vec())
}

/// Encrypt data using AES-128-CBC with PKCS#7 padding.
///
/// Oracle may use AES-128 for older connections.
///
/// # Arguments
/// * `key` - 16-byte AES-128 key
/// * `iv` - 16-byte initialization vector
/// * `plaintext` - Data to encrypt
///
/// # Returns
/// Ciphertext with PKCS#7 padding applied
pub fn aes128_cbc_encrypt(key: &[u8; 16], iv: &[u8; 16], plaintext: &[u8]) -> Vec<u8> {
    let padded = pkcs7_pad(plaintext, AES_BLOCK_SIZE);
    let mut ciphertext = padded;
    let len = ciphertext.len();
    let encryptor = Aes128CbcEnc::new(key.into(), iv.into());
    encryptor
        .encrypt_padded_mut::<aes::cipher::block_padding::NoPadding>(&mut ciphertext, len)
        .expect("buffer has correct length");
    ciphertext
}

/// Decrypt data using AES-128-CBC with PKCS#7 padding.
///
/// # Arguments
/// * `key` - 16-byte AES-128 key
/// * `iv` - 16-byte initialization vector
/// * `ciphertext` - Data to decrypt (must be multiple of block size)
///
/// # Returns
/// Decrypted plaintext with padding removed
pub fn aes128_cbc_decrypt(key: &[u8; 16], iv: &[u8; 16], ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
    if ciphertext.is_empty() || !ciphertext.len().is_multiple_of(AES_BLOCK_SIZE) {
        return Err(CryptoError::InvalidCiphertextLength);
    }

    let mut plaintext = ciphertext.to_vec();
    let decryptor = Aes128CbcDec::new(key.into(), iv.into());
    decryptor
        .decrypt_padded_mut::<aes::cipher::block_padding::NoPadding>(&mut plaintext)
        .map_err(|_| CryptoError::InvalidPadding)?;

    pkcs7_unpad(&plaintext).map(|p| p.to_vec())
}

/// Apply PKCS#7 padding to data.
///
/// Pads the data to a multiple of `block_size` bytes.
pub fn pkcs7_pad(data: &[u8], block_size: usize) -> Vec<u8> {
    let padding_len = block_size - (data.len() % block_size);
    let mut padded = Vec::with_capacity(data.len() + padding_len);
    padded.extend_from_slice(data);
    padded.extend(std::iter::repeat_n(padding_len as u8, padding_len));
    padded
}

/// Remove PKCS#7 padding from data.
///
/// # Errors
/// Returns `CryptoError::InvalidPadding` if padding is invalid.
pub fn pkcs7_unpad(data: &[u8]) -> Result<&[u8], CryptoError> {
    if data.is_empty() {
        return Err(CryptoError::InvalidPadding);
    }

    let padding_len = data[data.len() - 1] as usize;
    if padding_len == 0 || padding_len > AES_BLOCK_SIZE || padding_len > data.len() {
        return Err(CryptoError::InvalidPadding);
    }

    // Verify all padding bytes are correct
    for &byte in &data[data.len() - padding_len..] {
        if byte != padding_len as u8 {
            return Err(CryptoError::InvalidPadding);
        }
    }

    Ok(&data[..data.len() - padding_len])
}

/// Session encryption context for encrypting/decrypting TTI data.
///
/// This holds the derived session key and tracks the IV for CBC mode.
#[derive(Clone)]
pub struct SessionCipher {
    key: [u8; 32],
    iv: [u8; 16],
}

impl SessionCipher {
    /// Create a new session cipher with the given key and initial IV.
    ///
    /// # Arguments
    /// * `key` - 32-byte AES-256 session key
    /// * `iv` - 16-byte initial IV (typically derived from session establishment)
    pub fn new(key: [u8; 32], iv: [u8; 16]) -> Self {
        Self { key, iv }
    }

    /// Create session cipher from SHA-256 derived key.
    ///
    /// Uses the first 16 bytes of the key as the initial IV.
    pub fn from_sha256_key(key: [u8; 32]) -> Self {
        let mut iv = [0u8; 16];
        iv.copy_from_slice(&key[..16]);
        Self { key, iv }
    }

    /// Create session cipher from SHA-1 derived key.
    ///
    /// Expands the 20-byte key to 32 bytes for AES-256.
    /// Uses the first 16 bytes as the initial IV.
    pub fn from_sha1_key(key: [u8; 20]) -> Self {
        // Expand 20-byte key to 32 bytes by repeating
        let mut expanded_key = [0u8; 32];
        expanded_key[..20].copy_from_slice(&key);
        expanded_key[20..].copy_from_slice(&key[..12]);

        let mut iv = [0u8; 16];
        iv.copy_from_slice(&key[..16]);

        Self { key: expanded_key, iv }
    }

    /// Encrypt data and update IV for next operation.
    ///
    /// Uses CBC chaining: the last ciphertext block becomes the next IV.
    pub fn encrypt(&mut self, plaintext: &[u8]) -> Vec<u8> {
        let ciphertext = aes256_cbc_encrypt(&self.key, &self.iv, plaintext);

        // Update IV to last block of ciphertext for chaining
        if ciphertext.len() >= AES_BLOCK_SIZE {
            self.iv.copy_from_slice(&ciphertext[ciphertext.len() - AES_BLOCK_SIZE..]);
        }

        ciphertext
    }

    /// Decrypt data and update IV for next operation.
    ///
    /// Uses CBC chaining: the last ciphertext block becomes the next IV.
    pub fn decrypt(&mut self, ciphertext: &[u8]) -> Result<Vec<u8>, CryptoError> {
        // Save last block before decryption for IV update
        let next_iv: [u8; 16] = if ciphertext.len() >= AES_BLOCK_SIZE {
            let mut iv = [0u8; 16];
            iv.copy_from_slice(&ciphertext[ciphertext.len() - AES_BLOCK_SIZE..]);
            iv
        } else {
            return Err(CryptoError::InvalidCiphertextLength);
        };

        let plaintext = aes256_cbc_decrypt(&self.key, &self.iv, ciphertext)?;
        self.iv = next_iv;
        Ok(plaintext)
    }

    /// Get the current IV.
    pub fn iv(&self) -> &[u8; 16] {
        &self.iv
    }

    /// Set the IV (for resynchronization).
    pub fn set_iv(&mut self, iv: [u8; 16]) {
        self.iv = iv;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sha1_password_hash() {
        let password = "tiger";
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];

        let hash = sha1_password_hash(password, &salt);

        // SHA1 produces 20 bytes
        assert_eq!(hash.len(), 20);

        // Same inputs should produce same hash
        let hash2 = sha1_password_hash(password, &salt);
        assert_eq!(hash, hash2);

        // Different password should produce different hash
        let hash3 = sha1_password_hash("lion", &salt);
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_sha256_password_hash() {
        let password = "tiger";
        let salt = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];

        let hash = sha256_password_hash(password, &salt);

        // SHA256 produces 32 bytes
        assert_eq!(hash.len(), 32);

        // Same inputs should produce same hash
        let hash2 = sha256_password_hash(password, &salt);
        assert_eq!(hash, hash2);

        // Different password should produce different hash
        let hash3 = sha256_password_hash("lion", &salt);
        assert_ne!(hash, hash3);
    }

    #[test]
    fn test_o8logon_verifier() {
        let password = "tiger";
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A];
        let server_session_key = [0xAA; 48]; // Mock server session key

        let result = compute_o8logon_verifier(password, &salt, &server_session_key);
        assert!(result.is_ok());

        let verifier = result.expect("should compute verifier");
        // HMAC-SHA1 produces 20 bytes
        assert_eq!(verifier.verifier.len(), 20);
        assert_eq!(verifier.session_key_response.len(), 20);
    }

    #[test]
    fn test_o9logon_verifier() {
        let password = "tiger";
        let salt = [
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
        ];
        let server_session_key = [0xBB; 64]; // Mock server session key

        let result = compute_o9logon_verifier(password, &salt, &server_session_key);
        assert!(result.is_ok());

        let verifier = result.expect("should compute verifier");
        // HMAC-SHA256 produces 32 bytes
        assert_eq!(verifier.verifier.len(), 32);
        assert_eq!(verifier.session_key_response.len(), 32);
    }

    #[test]
    fn test_o8logon_salt_too_short() {
        let password = "tiger";
        let salt = [0x01, 0x02, 0x03]; // Too short
        let server_session_key = [0xAA; 48];

        let result = compute_o8logon_verifier(password, &salt, &server_session_key);
        assert!(matches!(result, Err(CryptoError::InvalidSaltLength { .. })));
    }

    #[test]
    fn test_o9logon_salt_too_short() {
        let password = "tiger";
        let salt = [0x01, 0x02, 0x03, 0x04, 0x05]; // Too short for SHA-256
        let server_session_key = [0xBB; 64];

        let result = compute_o9logon_verifier(password, &salt, &server_session_key);
        assert!(matches!(result, Err(CryptoError::InvalidSaltLength { .. })));
    }

    #[test]
    fn test_hex_encode_decode() {
        let data = vec![0xDE, 0xAD, 0xBE, 0xEF];

        let encoded = encode_hex_auth_data(&data);
        assert_eq!(encoded, "DEADBEEF");

        let decoded = decode_hex_auth_data(&encoded).expect("should decode");
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_hex_decode_lowercase() {
        let decoded = decode_hex_auth_data("deadbeef").expect("should decode lowercase");
        assert_eq!(decoded, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_hex_decode_invalid() {
        let result = decode_hex_auth_data("not_hex!");
        assert!(matches!(result, Err(CryptoError::HexDecode(_))));
    }

    #[test]
    fn test_xor_bytes() {
        let a = vec![0xFF, 0x00, 0xAA];
        let b = vec![0x0F, 0xF0, 0x55];

        let result = xor_bytes(&a, &b);
        assert_eq!(result, vec![0xF0, 0xF0, 0xFF]);
    }

    #[test]
    fn test_xor_bytes_different_lengths() {
        let a = vec![0xFF, 0xFF, 0xFF, 0xFF];
        let b = vec![0x0F, 0xF0]; // Shorter, will cycle

        let result = xor_bytes(&a, &b);
        assert_eq!(result, vec![0xF0, 0x0F, 0xF0, 0x0F]);
    }

    #[test]
    fn test_combine_session_keys() {
        let client = vec![0xAA, 0xBB, 0xCC, 0xDD];
        let server = vec![0x11, 0x22, 0x33, 0x44];

        let combined = combine_session_keys(&client, &server);
        // XOR of the keys
        assert_eq!(combined, vec![0xBB, 0x99, 0xFF, 0x99]);
    }

    #[test]
    fn test_derive_session_encryption_key_sha1() {
        let password_hash = sha1_password_hash("tiger", &[0x01; 10]);
        let combined_key = vec![0xAB; 40];

        let result = derive_session_encryption_key_sha1(&password_hash, &combined_key);
        assert!(result.is_ok());

        let key = result.expect("should derive key");
        assert_eq!(key.len(), 20);
    }

    #[test]
    fn test_derive_session_encryption_key_sha256() {
        let password_hash = sha256_password_hash("tiger", &[0x01; 16]);
        let combined_key = vec![0xCD; 64];

        let result = derive_session_encryption_key_sha256(&password_hash, &combined_key);
        assert!(result.is_ok());

        let key = result.expect("should derive key");
        assert_eq!(key.len(), 32);
    }

    #[test]
    fn test_verifier_deterministic() {
        let password = "secret123";
        let salt = [0x55; 16];
        let session_key = [0xAA; 64];

        let v1 = compute_o9logon_verifier(password, &salt, &session_key).expect("v1");
        let v2 = compute_o9logon_verifier(password, &salt, &session_key).expect("v2");

        assert_eq!(v1.verifier, v2.verifier);
        assert_eq!(v1.session_key_response, v2.session_key_response);
    }

    #[test]
    fn test_different_passwords_different_verifiers() {
        let salt = [0x55; 16];
        let session_key = [0xAA; 64];

        let v1 = compute_o9logon_verifier("password1", &salt, &session_key).expect("v1");
        let v2 = compute_o9logon_verifier("password2", &salt, &session_key).expect("v2");

        assert_ne!(v1.verifier, v2.verifier);
    }

    #[test]
    fn test_pkcs7_padding() {
        // Test various data lengths
        let data1 = b"hello";
        let padded1 = pkcs7_pad(data1, 16);
        assert_eq!(padded1.len(), 16);
        assert_eq!(&padded1[..5], b"hello");
        assert!(padded1[5..].iter().all(|&b| b == 11));

        // Data exactly block size needs full padding block
        let data2 = [0u8; 16];
        let padded2 = pkcs7_pad(&data2, 16);
        assert_eq!(padded2.len(), 32);
        assert!(padded2[16..].iter().all(|&b| b == 16));
    }

    #[test]
    fn test_pkcs7_unpadding() {
        // Valid padding
        let mut data = b"hello".to_vec();
        data.extend([11u8; 11]);
        let unpadded = pkcs7_unpad(&data).expect("valid padding");
        assert_eq!(unpadded, b"hello");

        // Invalid padding value
        let invalid = vec![0x10; 16]; // All 16s is valid for empty data
        assert!(pkcs7_unpad(&invalid).is_ok());

        // Invalid: padding byte doesn't match count
        let mut invalid2 = vec![0u8; 16];
        invalid2[15] = 3;
        invalid2[14] = 3;
        invalid2[13] = 2; // Wrong!
        assert!(pkcs7_unpad(&invalid2).is_err());
    }

    #[test]
    fn test_aes256_cbc_roundtrip() {
        let key = [0x42u8; 32];
        let iv = [0x24u8; 16];
        let plaintext = b"This is a test message for AES-256-CBC encryption";

        let ciphertext = aes256_cbc_encrypt(&key, &iv, plaintext);
        assert!(ciphertext.len() > plaintext.len());
        assert_eq!(ciphertext.len() % 16, 0);

        let decrypted = aes256_cbc_decrypt(&key, &iv, &ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes256_cbc_empty() {
        let key = [0x42u8; 32];
        let iv = [0x24u8; 16];
        let plaintext = b"";

        let ciphertext = aes256_cbc_encrypt(&key, &iv, plaintext);
        assert_eq!(ciphertext.len(), 16); // One block of padding

        let decrypted = aes256_cbc_decrypt(&key, &iv, &ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes128_cbc_roundtrip() {
        let key = [0x42u8; 16];
        let iv = [0x24u8; 16];
        let plaintext = b"This is a test message for AES-128-CBC encryption";

        let ciphertext = aes128_cbc_encrypt(&key, &iv, plaintext);
        assert!(ciphertext.len() > plaintext.len());
        assert_eq!(ciphertext.len() % 16, 0);

        let decrypted = aes128_cbc_decrypt(&key, &iv, &ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_aes256_cbc_invalid_ciphertext() {
        let key = [0x42u8; 32];
        let iv = [0x24u8; 16];

        // Empty ciphertext
        let result = aes256_cbc_decrypt(&key, &iv, &[]);
        assert!(matches!(result, Err(CryptoError::InvalidCiphertextLength)));

        // Non-block-aligned ciphertext
        let result = aes256_cbc_decrypt(&key, &iv, &[0u8; 15]);
        assert!(matches!(result, Err(CryptoError::InvalidCiphertextLength)));
    }

    #[test]
    fn test_session_cipher_encrypt_decrypt() {
        let key = [0x42u8; 32];
        let iv = [0x24u8; 16];
        let mut cipher = SessionCipher::new(key, iv);

        let plaintext1 = b"First message";
        let ciphertext1 = cipher.encrypt(plaintext1);

        // IV should have changed
        assert_ne!(cipher.iv(), &iv);

        // Decrypt with fresh cipher
        let mut decrypt_cipher = SessionCipher::new(key, iv);
        let decrypted1 = decrypt_cipher.decrypt(&ciphertext1).expect("decrypt");
        assert_eq!(decrypted1, plaintext1);

        // Both ciphers should now have the same IV
        assert_eq!(cipher.iv(), decrypt_cipher.iv());

        // Second message should chain properly
        let plaintext2 = b"Second message";
        let ciphertext2 = cipher.encrypt(plaintext2);
        let decrypted2 = decrypt_cipher.decrypt(&ciphertext2).expect("decrypt");
        assert_eq!(decrypted2, plaintext2);
    }

    #[test]
    fn test_session_cipher_from_sha256_key() {
        let password_hash = sha256_password_hash("test", &[0x01; 16]);
        let combined_key = vec![0xAB; 64];
        let key = derive_session_encryption_key_sha256(&password_hash, &combined_key).expect("derive key");

        let mut cipher = SessionCipher::from_sha256_key(key);

        let plaintext = b"Encrypted session data";
        let ciphertext = cipher.encrypt(plaintext);

        let mut decrypt_cipher = SessionCipher::from_sha256_key(key);
        let decrypted = decrypt_cipher.decrypt(&ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_session_cipher_from_sha1_key() {
        let password_hash = sha1_password_hash("test", &[0x01; 10]);
        let combined_key = vec![0xAB; 40];
        let key = derive_session_encryption_key_sha1(&password_hash, &combined_key).expect("derive key");

        let mut cipher = SessionCipher::from_sha1_key(key);

        let plaintext = b"Encrypted session data with SHA1 key";
        let ciphertext = cipher.encrypt(plaintext);

        let mut decrypt_cipher = SessionCipher::from_sha1_key(key);
        let decrypted = decrypt_cipher.decrypt(&ciphertext).expect("decrypt");
        assert_eq!(decrypted, plaintext);
    }
}
