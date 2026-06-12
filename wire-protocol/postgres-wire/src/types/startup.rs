//! Startup phase messages.
//!
//! These messages are used during the initial connection handshake.
//! They do NOT have a type byte - only a length and payload.

use crate::error::protocol;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// StartupMessage sent by the client to initiate a connection.
///
/// Format: [length: i32][protocol_version: i32][parameters...]
/// Parameters are key-value pairs as C-strings, terminated by a NUL byte.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StartupMessage {
    /// Protocol version (major << 16 | minor)
    pub protocol_version: i32,
    /// Connection parameters (user, database, options, etc.)
    pub parameters: Vec<(String, String)>,
}

impl StartupMessage {
    /// Protocol version 3.0 (PostgreSQL 7.4 through 17)
    pub const PROTOCOL_VERSION_3_0: i32 = protocol::VERSION_3_0;

    /// Protocol version 3.2 (PostgreSQL 18+)
    /// Supports variable-length cancel keys.
    pub const PROTOCOL_VERSION_3_2: i32 = protocol::VERSION_3_2;

    /// Create a new startup message with Protocol 3.0.
    pub fn new(parameters: Vec<(String, String)>) -> Self {
        Self { protocol_version: Self::PROTOCOL_VERSION_3_0, parameters }
    }

    /// Create a new startup message with Protocol 3.2.
    pub fn new_v32(parameters: Vec<(String, String)>) -> Self {
        Self { protocol_version: Self::PROTOCOL_VERSION_3_2, parameters }
    }

    /// Check if this startup uses Protocol 3.2 or later.
    pub fn supports_variable_cancel_key(&self) -> bool {
        protocol::supports_variable_cancel_key(self.protocol_version)
    }

    /// Get the major protocol version.
    pub fn major_version(&self) -> i16 {
        (self.protocol_version >> 16) as i16
    }

    /// Get the minor protocol version.
    pub fn minor_version(&self) -> i16 {
        (self.protocol_version & 0xFFFF) as i16
    }

    /// Get a parameter value by name.
    pub fn get_parameter(&self, name: &str) -> Option<&str> {
        self.parameters.iter().find(|(k, _)| k == name).map(|(_, v)| v.as_str())
    }

    /// Get the user parameter.
    pub fn user(&self) -> Option<&str> {
        self.get_parameter("user")
    }

    /// Get the database parameter.
    pub fn database(&self) -> Option<&str> {
        self.get_parameter("database")
    }

    /// Encode the startup message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin_startup().write_i32_be(self.protocol_version);

        for (key, value) in &self.parameters {
            builder.write_cstring_str(key).write_cstring_str(value);
        }
        builder.write_u8(0); // Final NUL terminator

        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StartupMessageError {
    #[error("invalid protocol version: {major}.{minor}")]
    InvalidProtocolVersion { major: i16, minor: i16 },
    #[error("invalid parameter string encoding")]
    InvalidEncoding,
    #[error("missing final NUL terminator")]
    MissingTerminator,
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for StartupMessage {
    type ParseError = StartupMessageError;
    type Value<'s>
        = StartupMessage
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Read length (includes itself)
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let payload_length = (length - 4) as usize;

        // Read protocol version
        let protocol_version = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        // Check for special request codes
        if protocol_version == protocol::SSL_REQUEST
            || protocol_version == protocol::CANCEL_REQUEST
            || protocol_version == protocol::GSSENC_REQUEST
        {
            return Err(PgParseError::Parse(StartupMessageError::InvalidProtocolVersion {
                major: (protocol_version >> 16) as i16,
                minor: (protocol_version & 0xFFFF) as i16,
            }));
        }

        // Read parameters (remaining payload minus the 4 bytes for version)
        let mut remaining = payload_length - 4;
        let mut parameters = Vec::new();

        while remaining > 1 {
            // Read key
            let key_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
            remaining -= key_bytes.len() + 1;

            if key_bytes.is_empty() {
                // Empty key means end of parameters
                break;
            }

            let key = String::from_utf8(key_bytes).map_err(|_| PgParseError::Parse(StartupMessageError::InvalidEncoding))?;

            // Read value
            let value_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
            remaining -= value_bytes.len() + 1;

            let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(StartupMessageError::InvalidEncoding))?;

            parameters.push((key, value));
        }

        // Skip any remaining bytes (should be just the final NUL)
        if remaining > 0 {
            stream.skip_bytes_sync(remaining).map_err(PgParseError::Stream)?;
        }

        Ok(StartupMessage { protocol_version, parameters })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for StartupMessage {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let payload_length = (length - 4) as usize;

        let protocol_version = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        if protocol_version == protocol::SSL_REQUEST
            || protocol_version == protocol::CANCEL_REQUEST
            || protocol_version == protocol::GSSENC_REQUEST
        {
            return Err(PgParseError::Parse(StartupMessageError::InvalidProtocolVersion {
                major: (protocol_version >> 16) as i16,
                minor: (protocol_version & 0xFFFF) as i16,
            }));
        }

        let mut remaining = payload_length - 4;
        let mut parameters = Vec::new();

        while remaining > 1 {
            let key_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
            remaining -= key_bytes.len() + 1;

            if key_bytes.is_empty() {
                break;
            }

            let key = String::from_utf8(key_bytes).map_err(|_| PgParseError::Parse(StartupMessageError::InvalidEncoding))?;

            let value_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
            remaining -= value_bytes.len() + 1;

            let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(StartupMessageError::InvalidEncoding))?;

            parameters.push((key, value));
        }

        if remaining > 0 {
            stream.skip_bytes(remaining).await.map_err(PgParseError::Stream)?;
        }

        Ok(StartupMessage { protocol_version, parameters })
    }
}

/// SSLRequest message sent by the client to request SSL/TLS.
///
/// Format: [length: i32 = 8][code: i32 = 80877103]
/// Server responds with 'S' (SSL) or 'N' (no SSL).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SSLRequest;

impl SSLRequest {
    /// The SSL request code.
    pub const CODE: i32 = protocol::SSL_REQUEST;

    /// Encode the SSL request message.
    pub fn encode() -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&8i32.to_be_bytes());
        buf[4..8].copy_from_slice(&Self::CODE.to_be_bytes());
        buf
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SSLRequestError {
    #[error("invalid SSL request code: expected {}, got {0}", protocol::SSL_REQUEST)]
    InvalidCode(i32),
    #[error("invalid length: expected 8, got {0}")]
    InvalidLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for SSLRequest {
    type ParseError = SSLRequestError;
    type Value<'s>
        = SSLRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if length != 8 {
            return Err(PgParseError::Parse(SSLRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if code != SSLRequest::CODE {
            return Err(PgParseError::Parse(SSLRequestError::InvalidCode(code)));
        }

        Ok(SSLRequest)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for SSLRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if length != 8 {
            return Err(PgParseError::Parse(SSLRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if code != SSLRequest::CODE {
            return Err(PgParseError::Parse(SSLRequestError::InvalidCode(code)));
        }

        Ok(SSLRequest)
    }
}

/// CancelRequest message sent to cancel a running query.
///
/// Format: [length: i32 = 16][code: i32 = 80877102][pid: i32][secret: i32]
/// This is sent on a NEW connection, not the existing one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CancelRequest {
    /// The backend process ID.
    pub process_id: i32,
    /// The secret key for cancellation.
    pub secret_key: i32,
}

impl CancelRequest {
    /// The cancel request code.
    pub const CODE: i32 = protocol::CANCEL_REQUEST;

    /// Create a new cancel request.
    pub fn new(process_id: i32, secret_key: i32) -> Self {
        Self { process_id, secret_key }
    }

    /// Encode the cancel request message.
    pub fn encode(&self) -> [u8; 16] {
        let mut buf = [0u8; 16];
        buf[0..4].copy_from_slice(&16i32.to_be_bytes());
        buf[4..8].copy_from_slice(&Self::CODE.to_be_bytes());
        buf[8..12].copy_from_slice(&self.process_id.to_be_bytes());
        buf[12..16].copy_from_slice(&self.secret_key.to_be_bytes());
        buf
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CancelRequestError {
    #[error("invalid cancel request code: expected {}, got {0}", protocol::CANCEL_REQUEST)]
    InvalidCode(i32),
    #[error("invalid length: expected 16, got {0}")]
    InvalidLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CancelRequest {
    type ParseError = CancelRequestError;
    type Value<'s>
        = CancelRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if length != 16 {
            return Err(PgParseError::Parse(CancelRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if code != CancelRequest::CODE {
            return Err(PgParseError::Parse(CancelRequestError::InvalidCode(code)));
        }

        let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let secret_key = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        Ok(CancelRequest { process_id, secret_key })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CancelRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if length != 16 {
            return Err(PgParseError::Parse(CancelRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if code != CancelRequest::CODE {
            return Err(PgParseError::Parse(CancelRequestError::InvalidCode(code)));
        }

        let process_id = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let secret_key = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        Ok(CancelRequest { process_id, secret_key })
    }
}

/// CancelRequest message with variable-length key (Protocol 3.2, PostgreSQL 18+).
///
/// Format: [length: i32][code: i32 = 80877105][pid: i32][key_len: i32][key: bytes]
/// This provides improved security over the fixed 4-byte key in Protocol 3.0.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CancelRequestV2 {
    /// The backend process ID.
    pub process_id: i32,
    /// The variable-length secret key for cancellation.
    pub secret_key: Vec<u8>,
}

impl CancelRequestV2 {
    /// The cancel request code for Protocol 3.2.
    pub const CODE: i32 = protocol::CANCEL_REQUEST_V2;

    /// Create a new cancel request with variable-length key.
    pub fn new(process_id: i32, secret_key: Vec<u8>) -> Self {
        Self { process_id, secret_key }
    }

    /// Encode the cancel request message.
    pub fn encode(&self) -> Vec<u8> {
        // length (4) + code (4) + pid (4) + key_len (4) + key
        let total_len = 16 + self.secret_key.len();
        let mut buf = Vec::with_capacity(total_len);

        buf.extend_from_slice(&(total_len as i32).to_be_bytes());
        buf.extend_from_slice(&Self::CODE.to_be_bytes());
        buf.extend_from_slice(&self.process_id.to_be_bytes());
        buf.extend_from_slice(&(self.secret_key.len() as i32).to_be_bytes());
        buf.extend_from_slice(&self.secret_key);

        buf
    }

    /// Convert to Protocol 3.0 format if the key is exactly 4 bytes.
    pub fn to_v1(&self) -> Option<CancelRequest> {
        if self.secret_key.len() == 4 {
            let key = i32::from_be_bytes([self.secret_key[0], self.secret_key[1], self.secret_key[2], self.secret_key[3]]);
            Some(CancelRequest::new(self.process_id, key))
        } else {
            None
        }
    }
}

impl CancelRequest {
    /// Convert to Protocol 3.2 format with variable-length key.
    pub fn to_v2(&self) -> CancelRequestV2 {
        CancelRequestV2 {
            process_id: self.process_id,
            secret_key: self.secret_key.to_be_bytes().to_vec(),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CancelRequestV2Error {
    #[error("invalid cancel request code: expected {}, got {0}", protocol::CANCEL_REQUEST_V2)]
    InvalidCode(i32),
    #[error("invalid key length: {0}")]
    InvalidKeyLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CancelRequestV2 {
    type ParseError = CancelRequestV2Error;
    type Value<'s>
        = CancelRequestV2
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let code = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if code != CancelRequestV2::CODE {
            return Err(PgParseError::Parse(CancelRequestV2Error::InvalidCode(code)));
        }

        let process_id = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let key_len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if !(0..=256).contains(&key_len) {
            return Err(PgParseError::Parse(CancelRequestV2Error::InvalidKeyLength(key_len)));
        }

        let secret_key = stream.read_bytes_sync(key_len as usize).map_err(PgParseError::Stream)?;

        Ok(CancelRequestV2 { process_id, secret_key })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CancelRequestV2 {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let code = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if code != CancelRequestV2::CODE {
            return Err(PgParseError::Parse(CancelRequestV2Error::InvalidCode(code)));
        }

        let process_id = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let key_len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if !(0..=256).contains(&key_len) {
            return Err(PgParseError::Parse(CancelRequestV2Error::InvalidKeyLength(key_len)));
        }

        let secret_key = stream.read_bytes(key_len as usize).await.map_err(PgParseError::Stream)?;

        Ok(CancelRequestV2 { process_id, secret_key })
    }
}

/// GSSEncRequest message sent by the client to request GSSAPI encryption.
///
/// Format: [length: i32 = 8][code: i32 = 80877104]
/// Server responds with 'G' (GSS) or 'N' (no GSS).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GSSEncRequest;

impl GSSEncRequest {
    /// The GSS encryption request code.
    pub const CODE: i32 = protocol::GSSENC_REQUEST;

    /// Encode the GSS encryption request message.
    pub fn encode() -> [u8; 8] {
        let mut buf = [0u8; 8];
        buf[0..4].copy_from_slice(&8i32.to_be_bytes());
        buf[4..8].copy_from_slice(&Self::CODE.to_be_bytes());
        buf
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum GSSEncRequestError {
    #[error("invalid GSS request code: expected {}, got {0}", protocol::GSSENC_REQUEST)]
    InvalidCode(i32),
    #[error("invalid length: expected 8, got {0}")]
    InvalidLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for GSSEncRequest {
    type ParseError = GSSEncRequestError;
    type Value<'s>
        = GSSEncRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if length != 8 {
            return Err(PgParseError::Parse(GSSEncRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        if code != GSSEncRequest::CODE {
            return Err(PgParseError::Parse(GSSEncRequestError::InvalidCode(code)));
        }

        Ok(GSSEncRequest)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for GSSEncRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if length != 8 {
            return Err(PgParseError::Parse(GSSEncRequestError::InvalidLength(length)));
        }

        let code = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        if code != GSSEncRequest::CODE {
            return Err(PgParseError::Parse(GSSEncRequestError::InvalidCode(code)));
        }

        Ok(GSSEncRequest)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_startup_message_encode_decode() {
        let msg = StartupMessage::new(vec![
            ("user".to_string(), "postgres".to_string()),
            ("database".to_string(), "testdb".to_string()),
        ]);

        let encoded = msg.encode();
        let stream = SliceStream::new(&encoded);
        let decoded = StartupMessage::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.protocol_version, StartupMessage::PROTOCOL_VERSION_3_0);
        assert_eq!(decoded.user(), Some("postgres"));
        assert_eq!(decoded.database(), Some("testdb"));
    }

    #[test]
    fn test_ssl_request_encode_decode() {
        let encoded = SSLRequest::encode();
        assert_eq!(encoded.len(), 8);

        let stream = SliceStream::new(&encoded);
        let _decoded = SSLRequest::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_cancel_request_encode_decode() {
        let request = CancelRequest::new(12345, 67890);
        let encoded = request.encode();
        assert_eq!(encoded.len(), 16);

        let stream = SliceStream::new(&encoded);
        let decoded = CancelRequest::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.process_id, 12345);
        assert_eq!(decoded.secret_key, 67890);
    }

    #[test]
    fn test_gssenc_request_encode_decode() {
        let encoded = GSSEncRequest::encode();
        assert_eq!(encoded.len(), 8);

        let stream = SliceStream::new(&encoded);
        let _decoded = GSSEncRequest::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_cancel_request_v2_encode_decode() {
        let secret = vec![
            0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E, 0x0F, 0x10, 0x11, 0x12, 0x13, 0x14, 0x15,
            0x16, 0x17, 0x18, 0x19, 0x1A, 0x1B, 0x1C, 0x1D, 0x1E, 0x1F, 0x20,
        ];
        let request = CancelRequestV2::new(12345, secret.clone());
        let encoded = request.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = CancelRequestV2::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.process_id, 12345);
        assert_eq!(decoded.secret_key, secret);
    }

    #[test]
    fn test_cancel_request_v1_to_v2() {
        let v1 = CancelRequest::new(12345, 67890);
        let v2 = v1.to_v2();

        assert_eq!(v2.process_id, 12345);
        assert_eq!(v2.secret_key, 67890_i32.to_be_bytes().to_vec());

        // Convert back
        let v1_again = v2.to_v1().expect("should convert back");
        assert_eq!(v1_again.process_id, 12345);
        assert_eq!(v1_again.secret_key, 67890);
    }

    #[test]
    fn test_cancel_request_v2_long_key_no_v1() {
        let secret = vec![0x01; 32]; // 32-byte key
        let v2 = CancelRequestV2::new(12345, secret);

        // Cannot convert to v1 because key is not 4 bytes
        assert!(v2.to_v1().is_none());
    }
}
