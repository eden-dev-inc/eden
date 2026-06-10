//! Oracle authentication protocol structures.
//!
//! Oracle uses different authentication protocols depending on version:
//!
//! - **O5LOGON**: Older protocol (Oracle 9i and earlier)
//! - **O7LOGON**: Newer protocol with stronger security (Oracle 10g+)
//! - **O8LOGON**: Enhanced security with SHA-1 (Oracle 11g+)
//! - **O9LOGON**: SHA-256 based authentication (Oracle 12c+)
//!
//! The authentication flow typically involves:
//! 1. Client sends username and requests authentication
//! 2. Server sends session key and authentication parameters
//! 3. Client computes password verifier and sends response
//! 4. Server validates and establishes session
//!
//! ## Cryptographic Operations
//!
//! See the [`crypto`](super::crypto) module for the actual cryptographic
//! implementations used in password verification.

use super::crypto::{
    AuthVerifier, CryptoError, compute_o8logon_verifier, compute_o9logon_verifier, decode_hex_auth_data, encode_hex_auth_data,
};

/// Authentication protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthProtocol {
    /// O5LOGON - Legacy protocol (Oracle 9i and earlier).
    O5Logon,
    /// O7LOGON - Improved security (Oracle 10g+).
    O7Logon,
    /// O8LOGON - SHA-1 based (Oracle 11g+).
    O8Logon,
    /// O9LOGON - SHA-256 based (Oracle 12c+).
    O9Logon,
}

impl AuthProtocol {
    /// Get the protocol identifier byte.
    pub fn protocol_byte(&self) -> u8 {
        match self {
            Self::O5Logon => 0x05,
            Self::O7Logon => 0x07,
            Self::O8Logon => 0x08,
            Self::O9Logon => 0x09,
        }
    }

    /// Create from protocol byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x05 => Some(Self::O5Logon),
            0x07 => Some(Self::O7Logon),
            0x08 => Some(Self::O8Logon),
            0x09 => Some(Self::O9Logon),
            _ => None,
        }
    }

    /// Check if this protocol supports encrypted passwords.
    pub fn supports_encryption(&self) -> bool {
        !matches!(self, Self::O5Logon)
    }

    /// Get the hash algorithm name used by this protocol.
    pub fn hash_algorithm(&self) -> &'static str {
        match self {
            Self::O5Logon => "DES",
            Self::O7Logon => "DES-CBC",
            Self::O8Logon => "SHA-1",
            Self::O9Logon => "SHA-256",
        }
    }
}

/// Authentication request sent by client.
#[derive(Clone, Debug)]
pub struct AuthRequest {
    /// The protocol version to use.
    pub protocol: AuthProtocol,
    /// Username (typically uppercase in Oracle).
    pub username: String,
    /// Terminal identifier (for auditing).
    pub terminal: Option<String>,
    /// Program name (for auditing).
    pub program: Option<String>,
    /// Machine name (for auditing).
    pub machine: Option<String>,
    /// Process ID.
    pub pid: Option<u32>,
    /// Session ID (for existing session reconnect).
    pub session_id: Option<u32>,
}

impl AuthRequest {
    /// Create a new authentication request.
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            protocol: AuthProtocol::O8Logon, // Default to modern protocol
            username: username.into(),
            terminal: None,
            program: None,
            machine: None,
            pid: None,
            session_id: None,
        }
    }

    /// Set the authentication protocol.
    pub fn with_protocol(mut self, protocol: AuthProtocol) -> Self {
        self.protocol = protocol;
        self
    }

    /// Set the terminal identifier.
    pub fn with_terminal(mut self, terminal: impl Into<String>) -> Self {
        self.terminal = Some(terminal.into());
        self
    }

    /// Set the program name.
    pub fn with_program(mut self, program: impl Into<String>) -> Self {
        self.program = Some(program.into());
        self
    }

    /// Set the machine name.
    pub fn with_machine(mut self, machine: impl Into<String>) -> Self {
        self.machine = Some(machine.into());
        self
    }

    /// Set the process ID.
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    /// Encode the request for the wire protocol.
    ///
    /// The format varies by protocol version, but generally includes:
    /// - Protocol byte
    /// - Username length + username
    /// - Optional: terminal, program, machine, pid
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(256);

        // Protocol identifier
        bytes.push(self.protocol.protocol_byte());

        // Username (length-prefixed)
        let username_bytes = self.username.as_bytes();
        bytes.push(username_bytes.len() as u8);
        bytes.extend_from_slice(username_bytes);

        // Optional fields (using key-value format)
        if let Some(ref terminal) = self.terminal {
            bytes.push(AUTH_KEY_TERMINAL);
            let tb = terminal.as_bytes();
            bytes.push(tb.len() as u8);
            bytes.extend_from_slice(tb);
        }

        if let Some(ref program) = self.program {
            bytes.push(AUTH_KEY_PROGRAM);
            let pb = program.as_bytes();
            bytes.push(pb.len() as u8);
            bytes.extend_from_slice(pb);
        }

        if let Some(ref machine) = self.machine {
            bytes.push(AUTH_KEY_MACHINE);
            let mb = machine.as_bytes();
            bytes.push(mb.len() as u8);
            bytes.extend_from_slice(mb);
        }

        if let Some(pid) = self.pid {
            bytes.push(AUTH_KEY_PID);
            bytes.push(4);
            bytes.extend_from_slice(&pid.to_be_bytes());
        }

        bytes
    }
}

/// Authentication challenge from server.
#[derive(Clone, Debug)]
pub struct AuthChallenge {
    /// Session key (random bytes from server).
    pub session_key: Vec<u8>,
    /// Salt for password hashing.
    pub salt: Vec<u8>,
    /// Authentication flags.
    pub flags: AuthFlags,
    /// Server version info.
    pub server_version: Option<u32>,
}

impl AuthChallenge {
    /// Parse from wire format.
    pub fn from_bytes(data: &[u8]) -> Result<Self, AuthParseError> {
        if data.len() < 4 {
            return Err(AuthParseError::TooShort);
        }

        let mut pos = 0;

        // Read session key
        let key_len = data[pos] as usize;
        pos += 1;
        if pos + key_len > data.len() {
            return Err(AuthParseError::TooShort);
        }
        let session_key = data[pos..pos + key_len].to_vec();
        pos += key_len;

        // Read salt
        if pos >= data.len() {
            return Err(AuthParseError::TooShort);
        }
        let salt_len = data[pos] as usize;
        pos += 1;
        if pos + salt_len > data.len() {
            return Err(AuthParseError::TooShort);
        }
        let salt = data[pos..pos + salt_len].to_vec();
        pos += salt_len;

        // Read flags
        let flags = if pos + 2 <= data.len() {
            AuthFlags::from_raw(u16::from_be_bytes([data[pos], data[pos + 1]]))
        } else {
            AuthFlags::default()
        };

        Ok(Self { session_key, salt, flags, server_version: None })
    }

    /// Parse session key from hex-encoded AUTH_SESSKEY.
    ///
    /// Oracle often sends the session key as a hex string.
    pub fn parse_hex_session_key(hex: &str) -> Result<Vec<u8>, CryptoError> {
        decode_hex_auth_data(hex)
    }

    /// Parse salt from hex-encoded AUTH_VFR_DATA.
    ///
    /// Oracle often sends the salt as a hex string.
    pub fn parse_hex_salt(hex: &str) -> Result<Vec<u8>, CryptoError> {
        decode_hex_auth_data(hex)
    }

    /// Compute password verifier for O8LOGON (SHA-1).
    ///
    /// Use this when the server requests O8LOGON authentication.
    pub fn compute_o8_verifier(&self, password: &str) -> Result<AuthVerifier, CryptoError> {
        compute_o8logon_verifier(password, &self.salt, &self.session_key)
    }

    /// Compute password verifier for O9LOGON (SHA-256).
    ///
    /// Use this when the server requests O9LOGON authentication.
    pub fn compute_o9_verifier(&self, password: &str) -> Result<AuthVerifier, CryptoError> {
        compute_o9logon_verifier(password, &self.salt, &self.session_key)
    }

    /// Compute password verifier based on the authentication protocol.
    ///
    /// Automatically selects the correct algorithm based on the protocol.
    pub fn compute_verifier(&self, password: &str, protocol: AuthProtocol) -> Result<AuthVerifier, AuthComputeError> {
        match protocol {
            AuthProtocol::O8Logon => self.compute_o8_verifier(password).map_err(AuthComputeError::Crypto),
            AuthProtocol::O9Logon => self.compute_o9_verifier(password).map_err(AuthComputeError::Crypto),
            AuthProtocol::O5Logon | AuthProtocol::O7Logon => Err(AuthComputeError::UnsupportedProtocol(protocol)),
        }
    }
}

/// Authentication response from client (password verifier).
#[derive(Clone, Debug)]
pub struct AuthResponse {
    /// Password verifier (computed from password + session key + salt).
    pub verifier: Vec<u8>,
    /// Authentication method used.
    pub auth_method: AuthMethod,
}

impl AuthResponse {
    /// Create a new authentication response.
    pub fn new(verifier: Vec<u8>, auth_method: AuthMethod) -> Self {
        Self { verifier, auth_method }
    }

    /// Create an authentication response from computed verifier.
    ///
    /// This is the typical flow: compute the verifier from the challenge,
    /// then create the response to send back to the server.
    pub fn from_verifier(verifier: AuthVerifier, auth_method: AuthMethod) -> Self {
        Self { verifier: verifier.verifier, auth_method }
    }

    /// Encode for the wire protocol.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.verifier.len() + 2);
        bytes.push(self.auth_method.method_byte());
        bytes.push(self.verifier.len() as u8);
        bytes.extend_from_slice(&self.verifier);
        bytes
    }

    /// Encode verifier as hex string (for AUTH_PASSWORD parameter).
    pub fn verifier_hex(&self) -> String {
        encode_hex_auth_data(&self.verifier)
    }
}

/// Authentication method.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthMethod {
    /// Password-based authentication.
    Password,
    /// Kerberos authentication.
    Kerberos,
    /// RADIUS authentication.
    Radius,
    /// SSL certificate authentication.
    Ssl,
    /// Operating system authentication.
    Os,
}

impl AuthMethod {
    /// Get the method identifier byte.
    pub fn method_byte(&self) -> u8 {
        match self {
            Self::Password => 0x01,
            Self::Kerberos => 0x02,
            Self::Radius => 0x03,
            Self::Ssl => 0x04,
            Self::Os => 0x05,
        }
    }

    /// Create from method byte.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            0x01 => Some(Self::Password),
            0x02 => Some(Self::Kerberos),
            0x03 => Some(Self::Radius),
            0x04 => Some(Self::Ssl),
            0x05 => Some(Self::Os),
            _ => None,
        }
    }
}

/// Authentication flags.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct AuthFlags {
    raw: u16,
}

impl AuthFlags {
    /// Create from raw flags value.
    pub fn from_raw(raw: u16) -> Self {
        Self { raw }
    }

    /// Get the raw flags value.
    pub fn raw(&self) -> u16 {
        self.raw
    }

    /// Check if password case sensitivity is required.
    pub fn case_sensitive_password(&self) -> bool {
        self.raw & AUTH_FLAG_CASE_SENSITIVE != 0
    }

    /// Check if session migration is allowed.
    pub fn session_migration(&self) -> bool {
        self.raw & AUTH_FLAG_SESSION_MIGRATION != 0
    }

    /// Check if encryption is required.
    pub fn encryption_required(&self) -> bool {
        self.raw & AUTH_FLAG_ENCRYPTION_REQUIRED != 0
    }
}

/// Error when parsing authentication messages.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthParseError {
    #[error("authentication data too short")]
    TooShort,
    #[error("invalid protocol byte: {0}")]
    InvalidProtocol(u8),
    #[error("invalid auth method: {0}")]
    InvalidMethod(u8),
    #[error("missing required field: {0}")]
    MissingField(&'static str),
}

/// Error when computing authentication verifier.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthComputeError {
    #[error("cryptographic error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("unsupported authentication protocol: {0:?}")]
    UnsupportedProtocol(AuthProtocol),
}

// Key identifiers for authentication fields
const AUTH_KEY_TERMINAL: u8 = 0x01;
const AUTH_KEY_PROGRAM: u8 = 0x02;
const AUTH_KEY_MACHINE: u8 = 0x03;
const AUTH_KEY_PID: u8 = 0x04;

// Authentication flags
const AUTH_FLAG_CASE_SENSITIVE: u16 = 0x0001;
const AUTH_FLAG_SESSION_MIGRATION: u16 = 0x0002;
const AUTH_FLAG_ENCRYPTION_REQUIRED: u16 = 0x0004;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_protocol() {
        assert_eq!(AuthProtocol::O5Logon.protocol_byte(), 0x05);
        assert_eq!(AuthProtocol::O8Logon.protocol_byte(), 0x08);
        assert_eq!(AuthProtocol::O9Logon.protocol_byte(), 0x09);

        assert_eq!(AuthProtocol::from_byte(0x07), Some(AuthProtocol::O7Logon));
        assert_eq!(AuthProtocol::from_byte(0x09), Some(AuthProtocol::O9Logon));
        assert_eq!(AuthProtocol::from_byte(0xFF), None);

        assert!(!AuthProtocol::O5Logon.supports_encryption());
        assert!(AuthProtocol::O8Logon.supports_encryption());
        assert!(AuthProtocol::O9Logon.supports_encryption());

        assert_eq!(AuthProtocol::O8Logon.hash_algorithm(), "SHA-1");
        assert_eq!(AuthProtocol::O9Logon.hash_algorithm(), "SHA-256");
    }

    #[test]
    fn test_auth_request() {
        let request = AuthRequest::new("SCOTT")
            .with_protocol(AuthProtocol::O8Logon)
            .with_terminal("pts/0")
            .with_program("sqlplus")
            .with_machine("workstation")
            .with_pid(12345);

        let bytes = request.to_bytes();

        // Should start with protocol byte
        assert_eq!(bytes[0], 0x08);
        // Then username length and username
        assert_eq!(bytes[1], 5); // "SCOTT" length
        assert_eq!(&bytes[2..7], b"SCOTT");
    }

    #[test]
    fn test_auth_flags() {
        let flags = AuthFlags::from_raw(AUTH_FLAG_CASE_SENSITIVE | AUTH_FLAG_ENCRYPTION_REQUIRED);

        assert!(flags.case_sensitive_password());
        assert!(flags.encryption_required());
        assert!(!flags.session_migration());
    }

    #[test]
    fn test_auth_method() {
        assert_eq!(AuthMethod::Password.method_byte(), 0x01);
        assert_eq!(AuthMethod::from_byte(0x02), Some(AuthMethod::Kerberos));
        assert_eq!(AuthMethod::from_byte(0xFF), None);
    }

    #[test]
    fn test_auth_challenge_compute_o8_verifier() {
        let challenge = AuthChallenge {
            session_key: vec![0xAA; 48],
            salt: vec![0x55; 10],
            flags: AuthFlags::default(),
            server_version: None,
        };

        let verifier = challenge.compute_o8_verifier("tiger").expect("should compute");
        assert_eq!(verifier.verifier.len(), 20); // SHA-1 output
        assert_eq!(verifier.session_key_response.len(), 20);
    }

    #[test]
    fn test_auth_challenge_compute_o9_verifier() {
        let challenge = AuthChallenge {
            session_key: vec![0xBB; 64],
            salt: vec![0x66; 16],
            flags: AuthFlags::default(),
            server_version: None,
        };

        let verifier = challenge.compute_o9_verifier("tiger").expect("should compute");
        assert_eq!(verifier.verifier.len(), 32); // SHA-256 output
        assert_eq!(verifier.session_key_response.len(), 32);
    }

    #[test]
    fn test_auth_challenge_compute_verifier_by_protocol() {
        let challenge = AuthChallenge {
            session_key: vec![0xCC; 64],
            salt: vec![0x77; 16],
            flags: AuthFlags::default(),
            server_version: None,
        };

        // O8Logon
        let v8 = challenge.compute_verifier("password", AuthProtocol::O8Logon).expect("o8");
        assert_eq!(v8.verifier.len(), 20);

        // O9Logon
        let v9 = challenge.compute_verifier("password", AuthProtocol::O9Logon).expect("o9");
        assert_eq!(v9.verifier.len(), 32);

        // O5Logon not supported
        let v5 = challenge.compute_verifier("password", AuthProtocol::O5Logon);
        assert!(matches!(v5, Err(AuthComputeError::UnsupportedProtocol(_))));
    }

    #[test]
    fn test_auth_response_from_verifier() {
        let challenge = AuthChallenge {
            session_key: vec![0xAA; 48],
            salt: vec![0x55; 10],
            flags: AuthFlags::default(),
            server_version: None,
        };

        let verifier = challenge.compute_o8_verifier("tiger").expect("should compute");
        let response = AuthResponse::from_verifier(verifier, AuthMethod::Password);

        assert_eq!(response.verifier.len(), 20);
        assert_eq!(response.auth_method, AuthMethod::Password);

        // Test encoding
        let bytes = response.to_bytes();
        assert_eq!(bytes[0], 0x01); // Password method
        assert_eq!(bytes[1], 20); // Verifier length
    }

    #[test]
    fn test_auth_response_verifier_hex() {
        let response = AuthResponse {
            verifier: vec![0xDE, 0xAD, 0xBE, 0xEF],
            auth_method: AuthMethod::Password,
        };

        assert_eq!(response.verifier_hex(), "DEADBEEF");
    }

    #[test]
    fn test_parse_hex_session_key() {
        let hex = "AABBCCDD";
        let key = AuthChallenge::parse_hex_session_key(hex).expect("should parse");
        assert_eq!(key, vec![0xAA, 0xBB, 0xCC, 0xDD]);
    }

    #[test]
    fn test_parse_hex_salt() {
        let hex = "0102030405060708090A";
        let salt = AuthChallenge::parse_hex_salt(hex).expect("should parse");
        assert_eq!(salt, vec![0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A]);
    }

    #[test]
    fn test_full_auth_flow_o8() {
        // Simulate a complete O8LOGON authentication flow
        let username = "SCOTT";
        let password = "tiger";

        // 1. Client creates auth request
        let request = AuthRequest::new(username).with_protocol(AuthProtocol::O8Logon);
        let request_bytes = request.to_bytes();
        assert!(!request_bytes.is_empty());

        // 2. Server sends challenge (simulated)
        let challenge = AuthChallenge {
            session_key: vec![0xAA; 48], // 48 bytes
            salt: vec![0xAA, 0xBB, 0xCC, 0xDD, 0xEE, 0xFF, 0x11, 0x22, 0x33, 0x44],
            flags: AuthFlags::from_raw(AUTH_FLAG_CASE_SENSITIVE),
            server_version: Some(0x0B200000), // 11.2.0.0
        };

        // 3. Client computes verifier
        let verifier = challenge.compute_verifier(password, AuthProtocol::O8Logon).expect("should compute verifier");

        // 4. Client creates response
        let response = AuthResponse::from_verifier(verifier, AuthMethod::Password);
        let response_bytes = response.to_bytes();

        // Verify response format
        assert_eq!(response_bytes[0], AuthMethod::Password.method_byte());
        assert_eq!(response_bytes[1], 20); // SHA-1 verifier length
        assert_eq!(response_bytes.len(), 22); // method + length + verifier
    }

    #[test]
    fn test_full_auth_flow_o9() {
        // Simulate a complete O9LOGON authentication flow
        let username = "ADMIN";
        let password = "SecurePass123!";

        // 1. Client creates auth request
        let request = AuthRequest::new(username).with_protocol(AuthProtocol::O9Logon);
        let request_bytes = request.to_bytes();
        assert_eq!(request_bytes[0], 0x09); // O9 protocol

        // 2. Server sends challenge (simulated)
        let challenge = AuthChallenge {
            session_key: vec![0xBB; 64], // 64 bytes
            salt: vec![
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10,
            ],
            flags: AuthFlags::from_raw(AUTH_FLAG_CASE_SENSITIVE | AUTH_FLAG_ENCRYPTION_REQUIRED),
            server_version: Some(0x0C020000), // 12.2.0.0
        };

        // 3. Client computes verifier
        let verifier = challenge.compute_verifier(password, AuthProtocol::O9Logon).expect("should compute verifier");

        // 4. Client creates response
        let response = AuthResponse::from_verifier(verifier, AuthMethod::Password);
        let response_bytes = response.to_bytes();

        // Verify response format
        assert_eq!(response_bytes[0], AuthMethod::Password.method_byte());
        assert_eq!(response_bytes[1], 32); // SHA-256 verifier length
        assert_eq!(response_bytes.len(), 34); // method + length + verifier

        // Verify hex encoding works
        let hex = response.verifier_hex();
        assert_eq!(hex.len(), 64); // 32 bytes * 2
    }
}
