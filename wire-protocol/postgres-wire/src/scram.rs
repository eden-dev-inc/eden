//! SCRAM-SHA-256 authentication helpers.
//!
//! This module provides helpers for SCRAM-SHA-256 authentication as used by
//! PostgreSQL 10+. SCRAM (Salted Challenge Response Authentication Mechanism)
//! is defined in RFC 5802 and provides secure password-based authentication.
//!
//! # Feature Flag
//!
//! This module requires the `scram` feature to be enabled:
//!
//! ```toml
//! [dependencies]
//! postgres-wire = { version = "0.1", features = ["scram"] }
//! ```
//!
//! # Protocol Overview
//!
//! 1. Client sends initial message containing username and client nonce
//! 2. Server responds with salt, iteration count, and combined nonce
//! 3. Client computes client proof and sends client final message
//! 4. Server verifies proof and sends server signature
//! 5. Client verifies server signature

use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

/// SCRAM-SHA-256 client state machine.
///
/// Use this for implementing client-side SCRAM authentication.
#[derive(Debug)]
pub struct ScramClient {
    /// The username (normalized if needed).
    username: String,
    /// The password.
    password: String,
    /// Client nonce.
    client_nonce: String,
    /// Combined nonce from server.
    combined_nonce: Option<String>,
    /// Salt from server.
    salt: Option<Vec<u8>>,
    /// Iteration count from server.
    iterations: Option<u32>,
    /// Client first message bare (for auth message).
    client_first_message_bare: String,
    /// Server first message (for auth message).
    server_first_message: Option<String>,
    /// Salted password (cached for server signature verification).
    salted_password: Option<[u8; 32]>,
}

impl ScramClient {
    /// Create a new SCRAM client with the given credentials.
    ///
    /// # Arguments
    ///
    /// * `username` - The PostgreSQL username
    /// * `password` - The password
    /// * `nonce` - A random nonce (at least 24 bytes of randomness, base64-encoded)
    ///
    /// # Example
    ///
    /// ```ignore
    /// use postgres_wire::scram::ScramClient;
    ///
    /// let nonce = base64::encode(&rand::random::<[u8; 24]>());
    /// let client = ScramClient::new("postgres", "secret", &nonce);
    /// let initial = client.client_first_message();
    /// ```
    pub fn new(username: &str, password: &str, nonce: &str) -> Self {
        let client_first_message_bare = format!("n={},r={}", sasl_prep(username), nonce);

        Self {
            username: username.to_string(),
            password: password.to_string(),
            client_nonce: nonce.to_string(),
            combined_nonce: None,
            salt: None,
            iterations: None,
            client_first_message_bare,
            server_first_message: None,
            salted_password: None,
        }
    }

    /// Get the username.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Get the client first message to send to the server.
    ///
    /// This is the initial SASL response containing the GS2 header
    /// and client first message bare.
    pub fn client_first_message(&self) -> Vec<u8> {
        // GS2 header: n,, (no channel binding, no authzid)
        // Followed by client first message bare: n=username,r=nonce
        format!("n,,{}", self.client_first_message_bare).into_bytes()
    }

    /// Process the server first message and generate the client final message.
    ///
    /// # Arguments
    ///
    /// * `server_first` - The server's first message
    ///
    /// # Returns
    ///
    /// The client final message to send, or an error if the server response is invalid.
    pub fn process_server_first(&mut self, server_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let server_first_str = std::str::from_utf8(server_first).map_err(|_| ScramError::InvalidEncoding)?;

        self.server_first_message = Some(server_first_str.to_string());

        // Parse server first message: r=nonce,s=salt,i=iterations
        let mut nonce = None;
        let mut salt = None;
        let mut iterations = None;

        for part in server_first_str.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value.to_string());
            } else if let Some(value) = part.strip_prefix("s=") {
                salt = Some(BASE64.decode(value).map_err(|_| ScramError::InvalidBase64)?);
            } else if let Some(value) = part.strip_prefix("i=") {
                iterations = Some(value.parse::<u32>().map_err(|_| ScramError::InvalidIterations)?);
            }
        }

        let combined_nonce = nonce.ok_or(ScramError::MissingNonce)?;
        let salt = salt.ok_or(ScramError::MissingSalt)?;
        let iterations = iterations.ok_or(ScramError::MissingIterations)?;

        // Verify nonce starts with our client nonce
        if !combined_nonce.starts_with(&self.client_nonce) {
            return Err(ScramError::InvalidNonce);
        }

        self.combined_nonce = Some(combined_nonce.clone());
        self.salt = Some(salt.clone());
        self.iterations = Some(iterations);

        // Compute salted password using PBKDF2
        let mut salted_password = [0u8; 32];
        pbkdf2_hmac::<Sha256>(self.password.as_bytes(), &salt, iterations, &mut salted_password);
        self.salted_password = Some(salted_password);

        // Client final message without proof
        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);

        // Auth message = client-first-message-bare + "," + server-first-message + "," + client-final-without-proof
        let auth_message = format!("{},{},{}", self.client_first_message_bare, server_first_str, client_final_without_proof);

        // Calculate client proof
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let client_proof = xor_bytes(&client_key, &client_signature);

        // Client final message with proof
        let client_final = format!("{},p={}", client_final_without_proof, BASE64.encode(client_proof));

        Ok(client_final.into_bytes())
    }

    /// Verify the server final message.
    ///
    /// # Arguments
    ///
    /// * `server_final` - The server's final message
    ///
    /// # Returns
    ///
    /// Ok(()) if the server signature is valid, or an error.
    pub fn verify_server_final(&self, server_final: &[u8]) -> Result<(), ScramError> {
        let server_final_str = std::str::from_utf8(server_final).map_err(|_| ScramError::InvalidEncoding)?;

        // Check for error
        if server_final_str.starts_with("e=") {
            let error = server_final_str.strip_prefix("e=").unwrap_or("unknown");
            return Err(ScramError::ServerError(error.to_string()));
        }

        // Parse server signature
        let server_sig_b64 = server_final_str.strip_prefix("v=").ok_or(ScramError::MissingVerifier)?;

        let received_signature = BASE64.decode(server_sig_b64).map_err(|_| ScramError::InvalidBase64)?;

        // Calculate expected server signature
        let salted_password = self.salted_password.ok_or(ScramError::NotReady)?;
        let server_first = self.server_first_message.as_ref().ok_or(ScramError::NotReady)?;
        let combined_nonce = self.combined_nonce.as_ref().ok_or(ScramError::NotReady)?;

        let client_final_without_proof = format!("c=biws,r={}", combined_nonce);
        let auth_message = format!("{},{},{}", self.client_first_message_bare, server_first, client_final_without_proof);

        let server_key = hmac_sha256(&salted_password, b"Server Key");
        let expected_signature = hmac_sha256(&server_key, auth_message.as_bytes());

        if received_signature != expected_signature {
            return Err(ScramError::InvalidServerSignature);
        }

        Ok(())
    }
}

/// SCRAM-SHA-256 server state machine.
///
/// Use this for implementing server-side SCRAM authentication.
#[derive(Debug)]
pub struct ScramServer {
    /// Server nonce.
    server_nonce: String,
    /// Combined nonce (client + server).
    combined_nonce: Option<String>,
    /// Salt for this user.
    salt: Vec<u8>,
    /// Iteration count.
    iterations: u32,
    /// Stored key (from password file).
    stored_key: [u8; 32],
    /// Server key (from password file).
    server_key: [u8; 32],
    /// Client first message bare.
    client_first_message_bare: Option<String>,
    /// Server first message.
    server_first_message: Option<String>,
}

impl ScramServer {
    /// Create a new SCRAM server with stored credentials.
    ///
    /// # Arguments
    ///
    /// * `server_nonce` - A random server nonce
    /// * `salt` - The salt stored for this user
    /// * `iterations` - The iteration count
    /// * `stored_key` - The StoredKey from the password file
    /// * `server_key` - The ServerKey from the password file
    ///
    /// In PostgreSQL, these values come from `pg_authid.rolpassword` which
    /// stores: `SCRAM-SHA-256$iterations:salt$StoredKey:ServerKey`
    pub fn new(server_nonce: &str, salt: Vec<u8>, iterations: u32, stored_key: [u8; 32], server_key: [u8; 32]) -> Self {
        Self {
            server_nonce: server_nonce.to_string(),
            combined_nonce: None,
            salt,
            iterations,
            stored_key,
            server_key,
            client_first_message_bare: None,
            server_first_message: None,
        }
    }

    /// Create credentials from a password (for testing or user creation).
    ///
    /// In production, you should store the derived values, not the password.
    pub fn create_credentials(password: &str, salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
        let mut salted_password = [0u8; 32];
        pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted_password);

        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = sha256(&client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");

        (stored_key, server_key)
    }

    /// Process the client first message and generate the server first message.
    ///
    /// # Arguments
    ///
    /// * `client_first` - The client's first message (SASL initial response)
    ///
    /// # Returns
    ///
    /// The server first message to send.
    pub fn process_client_first(&mut self, client_first: &[u8]) -> Result<Vec<u8>, ScramError> {
        let client_first_str = std::str::from_utf8(client_first).map_err(|_| ScramError::InvalidEncoding)?;

        // Skip GS2 header (n,, or similar)
        let client_first_bare = client_first_str
            .strip_prefix("n,,")
            .or_else(|| client_first_str.strip_prefix("y,,"))
            .ok_or(ScramError::InvalidGS2Header)?;

        self.client_first_message_bare = Some(client_first_bare.to_string());

        // Parse client nonce
        let mut client_nonce = None;
        for part in client_first_bare.split(',') {
            if let Some(value) = part.strip_prefix("r=") {
                client_nonce = Some(value);
            }
        }

        let client_nonce = client_nonce.ok_or(ScramError::MissingNonce)?;
        let combined = format!("{}{}", client_nonce, self.server_nonce);
        self.combined_nonce = Some(combined.clone());

        // Server first message: r=nonce,s=salt,i=iterations
        let server_first = format!("r={},s={},i={}", combined, BASE64.encode(&self.salt), self.iterations);

        self.server_first_message = Some(server_first.clone());

        Ok(server_first.into_bytes())
    }

    /// Process the client final message and verify authentication.
    ///
    /// # Arguments
    ///
    /// * `client_final` - The client's final message
    ///
    /// # Returns
    ///
    /// The server final message (containing server signature) if authentication succeeds.
    pub fn process_client_final(&self, client_final: &[u8]) -> Result<Vec<u8>, ScramError> {
        let client_final_str = std::str::from_utf8(client_final).map_err(|_| ScramError::InvalidEncoding)?;

        // Parse client final: c=channel-binding,r=nonce,p=proof
        let mut channel_binding = None;
        let mut nonce = None;
        let mut proof_b64 = None;

        for part in client_final_str.split(',') {
            if let Some(value) = part.strip_prefix("c=") {
                channel_binding = Some(value);
            } else if let Some(value) = part.strip_prefix("r=") {
                nonce = Some(value);
            } else if let Some(value) = part.strip_prefix("p=") {
                proof_b64 = Some(value);
            }
        }

        // Verify channel binding (should be base64 of "n,,")
        let cb = channel_binding.ok_or(ScramError::MissingChannelBinding)?;
        if cb != "biws" {
            // "biws" is base64("n,,")
            return Err(ScramError::InvalidChannelBinding);
        }

        // Verify nonce
        let received_nonce = nonce.ok_or(ScramError::MissingNonce)?;
        let expected_nonce = self.combined_nonce.as_ref().ok_or(ScramError::NotReady)?;
        if received_nonce != expected_nonce {
            return Err(ScramError::InvalidNonce);
        }

        // Get client proof
        let proof_b64 = proof_b64.ok_or(ScramError::MissingProof)?;
        let client_proof = BASE64.decode(proof_b64).map_err(|_| ScramError::InvalidBase64)?;

        // Calculate expected client signature
        let client_first_bare = self.client_first_message_bare.as_ref().ok_or(ScramError::NotReady)?;
        let server_first = self.server_first_message.as_ref().ok_or(ScramError::NotReady)?;

        // Client final without proof
        let client_final_without_proof = format!("c=biws,r={}", expected_nonce);
        let auth_message = format!("{},{},{}", client_first_bare, server_first, client_final_without_proof);

        // Verify proof
        let client_signature = hmac_sha256(&self.stored_key, auth_message.as_bytes());
        let client_key = xor_bytes(&client_proof, &client_signature);
        let computed_stored_key = sha256(&client_key);

        if computed_stored_key != self.stored_key {
            return Err(ScramError::AuthenticationFailed);
        }

        // Generate server signature
        let server_signature = hmac_sha256(&self.server_key, auth_message.as_bytes());
        let server_final = format!("v={}", BASE64.encode(server_signature));

        Ok(server_final.into_bytes())
    }
}

/// SCRAM authentication error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ScramError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("invalid base64")]
    InvalidBase64,
    #[error("missing nonce in message")]
    MissingNonce,
    #[error("missing salt in message")]
    MissingSalt,
    #[error("missing iteration count in message")]
    MissingIterations,
    #[error("invalid iteration count")]
    InvalidIterations,
    #[error("invalid nonce (doesn't match client nonce)")]
    InvalidNonce,
    #[error("invalid server signature")]
    InvalidServerSignature,
    #[error("state machine not ready")]
    NotReady,
    #[error("server error: {0}")]
    ServerError(String),
    #[error("missing verifier in server final")]
    MissingVerifier,
    #[error("invalid GS2 header")]
    InvalidGS2Header,
    #[error("missing channel binding")]
    MissingChannelBinding,
    #[error("invalid channel binding")]
    InvalidChannelBinding,
    #[error("missing proof")]
    MissingProof,
    #[error("authentication failed")]
    AuthenticationFailed,
}

// Helper: HMAC-SHA256
fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

// Helper: SHA256
fn sha256(data: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hasher.finalize().into()
}

// Helper: XOR two byte arrays
fn xor_bytes(a: &[u8], b: &[u8]) -> Vec<u8> {
    a.iter().zip(b.iter()).map(|(x, y)| x ^ y).collect()
}

// Simplified SASLprep - just returns the input for now.
// A full implementation would normalize Unicode per RFC 4013.
fn sasl_prep(s: &str) -> &str {
    // PostgreSQL uses SASLprep for username/password normalization.
    // For ASCII inputs, this is typically a no-op.
    // A full implementation should handle Unicode normalization.
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scram_full_exchange() {
        // Simulated exchange between client and server
        let password = "pencil";
        let salt = b"saltysalt1234567".to_vec(); // 16 bytes
        let iterations = 4096;

        // Create server credentials from password
        let (stored_key, server_key) = ScramServer::create_credentials(password, &salt, iterations);

        // Client setup
        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let mut client = ScramClient::new("user", password, client_nonce);

        // Server setup
        let server_nonce = "serverNonce123";
        let mut server = ScramServer::new(server_nonce, salt, iterations, stored_key, server_key);

        // Step 1: Client first message
        let client_first = client.client_first_message();

        // Step 2: Server processes and responds
        let server_first = server.process_client_first(&client_first).unwrap();

        // Step 3: Client processes and responds
        let client_final = client.process_server_first(&server_first).unwrap();

        // Step 4: Server verifies and responds
        let server_final = server.process_client_final(&client_final).unwrap();

        // Step 5: Client verifies server
        client.verify_server_final(&server_final).unwrap();
    }

    #[test]
    fn test_scram_wrong_password() {
        let password = "correct";
        let wrong_password = "wrong";
        let salt = b"saltysalt1234567".to_vec();
        let iterations = 4096;

        let (stored_key, server_key) = ScramServer::create_credentials(password, &salt, iterations);

        let client_nonce = "clientNonce123";
        let mut client = ScramClient::new("user", wrong_password, client_nonce);

        let server_nonce = "serverNonce456";
        let mut server = ScramServer::new(server_nonce, salt, iterations, stored_key, server_key);

        let client_first = client.client_first_message();
        let server_first = server.process_client_first(&client_first).unwrap();
        let client_final = client.process_server_first(&server_first).unwrap();

        // Server should reject wrong password
        let result = server.process_client_final(&client_final);
        assert!(matches!(result, Err(ScramError::AuthenticationFailed)));
    }

    #[test]
    fn test_credential_generation() {
        let password = "secret";
        let salt = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let iterations = 4096;

        let (stored_key, server_key) = ScramServer::create_credentials(password, &salt, iterations);

        // Keys should be 32 bytes (SHA-256)
        assert_eq!(stored_key.len(), 32);
        assert_eq!(server_key.len(), 32);

        // Same input should produce same output
        let (stored_key2, server_key2) = ScramServer::create_credentials(password, &salt, iterations);
        assert_eq!(stored_key, stored_key2);
        assert_eq!(server_key, server_key2);
    }
}
