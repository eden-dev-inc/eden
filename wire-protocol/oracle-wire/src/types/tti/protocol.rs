//! TTI protocol negotiation.
//!
//! Protocol negotiation is the first exchange after a TNS connection is established.
//! It determines the TTI version and features to use for the session.
//!
//! # Negotiation Sequence
//!
//! 1. Client sends `ProtocolNegotiationRequest` with supported versions and features
//! 2. Server responds with `ProtocolNegotiationResponse` with selected version
//! 3. Client sends `DataTypeNegotiationRequest` with supported data types
//! 4. Server responds with `DataTypeNegotiationResponse`
//! 5. Client sends `VersionRequest` to get server version info
//! 6. Server responds with `VersionResponse`

use super::function_codes::FunctionCode;
use super::message::TtiMessage;

/// Capability flags for protocol negotiation.
pub mod capabilities {
    /// Supports authentication (O5LOGON or newer).
    pub const AUTH: u16 = 0x0001;
    /// Supports compression.
    pub const COMPRESSION: u16 = 0x0002;
    /// Supports encryption.
    pub const ENCRYPTION: u16 = 0x0004;
    /// Supports LOB operations.
    pub const LOB: u16 = 0x0008;
    /// Supports array operations.
    pub const ARRAY: u16 = 0x0010;
    /// Supports statement caching.
    pub const STMT_CACHE: u16 = 0x0020;
    /// Supports scrollable cursors.
    pub const SCROLLABLE: u16 = 0x0040;
    /// Supports implicit results (v12+).
    pub const IMPLICIT_RESULTS: u16 = 0x0080;
    /// Supports DRCP (v11+).
    pub const DRCP: u16 = 0x0100;
    /// Supports piggyback (v11+).
    pub const PIGGYBACK: u16 = 0x0200;
}

/// Protocol negotiation request.
///
/// Sent as the first TTI message after TNS connection establishment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolNegotiationRequest {
    /// Minimum TTI version supported.
    pub version_min: u16,
    /// Maximum TTI version supported.
    pub version_max: u16,
    /// Capability flags.
    pub capabilities: u16,
    /// Character set ID for client.
    pub charset_id: u16,
    /// National character set ID.
    pub ncharset_id: u16,
    /// Client flags.
    pub flags: u32,
}

impl ProtocolNegotiationRequest {
    /// Create a new protocol negotiation request.
    pub fn new(version: u16) -> Self {
        Self {
            version_min: 8,
            version_max: version,
            capabilities: capabilities::AUTH | capabilities::LOB | capabilities::ARRAY | capabilities::STMT_CACHE,
            charset_id: 873,   // AL32UTF8
            ncharset_id: 2000, // AL16UTF16
            flags: 0,
        }
    }

    /// Set capability flags.
    pub fn with_capabilities(mut self, caps: u16) -> Self {
        self.capabilities = caps;
        self
    }

    /// Set character set.
    pub fn with_charset(mut self, charset_id: u16, ncharset_id: u16) -> Self {
        self.charset_id = charset_id;
        self.ncharset_id = ncharset_id;
        self
    }

    /// Enable DRCP support.
    pub fn with_drcp(mut self) -> Self {
        self.capabilities |= capabilities::DRCP;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&self.version_min.to_be_bytes());
        buf.extend_from_slice(&self.version_max.to_be_bytes());
        buf.extend_from_slice(&self.capabilities.to_be_bytes());
        buf.extend_from_slice(&self.charset_id.to_be_bytes());
        buf.extend_from_slice(&self.ncharset_id.to_be_bytes());
        buf.extend_from_slice(&self.flags.to_be_bytes());
        buf
    }

    /// Build as a TtiMessage.
    pub fn to_message(&self) -> TtiMessage {
        TtiMessage::new(FunctionCode::ProtocolNegotiation, 0, self.encode())
    }
}

impl Default for ProtocolNegotiationRequest {
    fn default() -> Self {
        Self::new(12)
    }
}

/// Protocol negotiation response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProtocolNegotiationResponse {
    /// Negotiated TTI version.
    pub version: u16,
    /// Server capability flags.
    pub capabilities: u16,
    /// Server flags.
    pub flags: u32,
    /// Server character set ID.
    pub charset_id: u16,
    /// Server national character set ID.
    pub ncharset_id: u16,
}

impl ProtocolNegotiationResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 12 {
            return Err(ProtocolError::TooShort { expected: 12, actual: data.len() });
        }

        Ok(Self {
            version: u16::from_be_bytes([data[0], data[1]]),
            capabilities: u16::from_be_bytes([data[2], data[3]]),
            flags: u32::from_be_bytes([data[4], data[5], data[6], data[7]]),
            charset_id: u16::from_be_bytes([data[8], data[9]]),
            ncharset_id: u16::from_be_bytes([data[10], data[11]]),
        })
    }

    /// Check if server supports a capability.
    pub fn has_capability(&self, cap: u16) -> bool {
        self.capabilities & cap != 0
    }

    /// Check if server supports DRCP.
    pub fn supports_drcp(&self) -> bool {
        self.has_capability(capabilities::DRCP)
    }

    /// Check if server supports compression.
    pub fn supports_compression(&self) -> bool {
        self.has_capability(capabilities::COMPRESSION)
    }

    /// Check if server supports encryption.
    pub fn supports_encryption(&self) -> bool {
        self.has_capability(capabilities::ENCRYPTION)
    }
}

/// Data type negotiation request.
///
/// Sent after protocol negotiation to negotiate supported data types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataTypeNegotiationRequest {
    /// Number of data types.
    pub type_count: u16,
    /// Supported data type codes.
    pub types: Vec<u8>,
}

impl DataTypeNegotiationRequest {
    /// Create with default supported types.
    pub fn default_types() -> Self {
        Self {
            type_count: 23,
            types: vec![
                1,   // VARCHAR2
                2,   // NUMBER
                8,   // LONG
                12,  // DATE
                23,  // RAW
                24,  // LONG RAW
                96,  // CHAR
                100, // BINARY_FLOAT
                101, // BINARY_DOUBLE
                104, // ROWID
                112, // CLOB
                113, // BLOB
                114, // BFILE
                180, // TIMESTAMP
                181, // TIMESTAMP WITH TZ
                182, // INTERVAL YEAR TO MONTH
                183, // INTERVAL DAY TO SECOND
                208, // UROWID
                231, // TIMESTAMP WITH LOCAL TZ
                252, // BOOLEAN
            ],
        }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.types.len());
        buf.extend_from_slice(&self.type_count.to_be_bytes());
        buf.extend_from_slice(&self.types);
        buf
    }

    /// Build as a TtiMessage.
    pub fn to_message(&self) -> TtiMessage {
        TtiMessage::new(FunctionCode::DataTypeNegotiation, 0, self.encode())
    }
}

impl Default for DataTypeNegotiationRequest {
    fn default() -> Self {
        Self::default_types()
    }
}

/// Data type negotiation response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DataTypeNegotiationResponse {
    /// Number of supported types.
    pub type_count: u16,
    /// Accepted type codes.
    pub accepted_types: Vec<u8>,
}

impl DataTypeNegotiationResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.len() < 2 {
            return Err(ProtocolError::TooShort { expected: 2, actual: data.len() });
        }

        let type_count = u16::from_be_bytes([data[0], data[1]]);
        let accepted_types = if data.len() > 2 { data[2..].to_vec() } else { Vec::new() };

        Ok(Self { type_count, accepted_types })
    }

    /// Check if a type is accepted.
    pub fn accepts_type(&self, type_code: u8) -> bool {
        self.accepted_types.contains(&type_code)
    }
}

/// Version exchange request.
///
/// Sent to retrieve server version information.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct VersionRequest {
    /// Client version string.
    pub client_version: Option<String>,
}

impl VersionRequest {
    /// Create a new version request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set client version.
    pub fn with_client_version(mut self, version: impl Into<String>) -> Self {
        self.client_version = Some(version.into());
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        match &self.client_version {
            Some(v) => {
                let bytes = v.as_bytes();
                let mut buf = Vec::with_capacity(1 + bytes.len());
                buf.push(bytes.len() as u8);
                buf.extend_from_slice(bytes);
                buf
            }
            None => vec![0],
        }
    }

    /// Build as a TtiMessage.
    pub fn to_message(&self) -> TtiMessage {
        TtiMessage::new(FunctionCode::Version, 0, self.encode())
    }
}

/// Version exchange response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionResponse {
    /// Server version string (e.g., "19.0.0.0.0").
    pub version: String,
    /// Server banner/description.
    pub banner: Option<String>,
    /// Server version components.
    pub version_components: Option<VersionComponents>,
}

impl VersionResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, ProtocolError> {
        if data.is_empty() {
            return Err(ProtocolError::TooShort { expected: 1, actual: 0 });
        }

        let version_len = data[0] as usize;
        if data.len() < 1 + version_len {
            return Err(ProtocolError::TooShort { expected: 1 + version_len, actual: data.len() });
        }

        let version = String::from_utf8_lossy(&data[1..1 + version_len]).to_string();

        let banner = if data.len() > 1 + version_len {
            let banner_start = 1 + version_len;
            if banner_start < data.len() {
                let banner_len = data[banner_start] as usize;
                if data.len() >= banner_start + 1 + banner_len {
                    Some(String::from_utf8_lossy(&data[banner_start + 1..banner_start + 1 + banner_len]).to_string())
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let version_components = VersionComponents::parse(&version);

        Ok(Self { version, banner, version_components })
    }

    /// Get major version number.
    pub fn major_version(&self) -> Option<u8> {
        self.version_components.as_ref().map(|v| v.major)
    }
}

/// Parsed version components.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionComponents {
    /// Major version (e.g., 19 for Oracle 19c).
    pub major: u8,
    /// Minor version.
    pub minor: u8,
    /// Patch level.
    pub patch: u8,
    /// Build number.
    pub build: u8,
    /// Port-specific release.
    pub port_release: u8,
}

impl VersionComponents {
    /// Parse version string like "19.3.0.0.0".
    pub fn parse(version: &str) -> Option<Self> {
        let parts: Vec<&str> = version.split('.').collect();
        if parts.len() < 2 {
            return None;
        }

        Some(Self {
            major: parts.first().and_then(|s| s.parse().ok()).unwrap_or(0),
            minor: parts.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
            patch: parts.get(2).and_then(|s| s.parse().ok()).unwrap_or(0),
            build: parts.get(3).and_then(|s| s.parse().ok()).unwrap_or(0),
            port_release: parts.get(4).and_then(|s| s.parse().ok()).unwrap_or(0),
        })
    }

    /// Check if version is at least the specified version.
    pub fn is_at_least(&self, major: u8, minor: u8) -> bool {
        self.major > major || (self.major == major && self.minor >= minor)
    }
}

/// Protocol negotiation error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("data too short: expected {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },

    #[error("version mismatch: server {server}, client min {client_min}, max {client_max}")]
    VersionMismatch { server: u16, client_min: u16, client_max: u16 },

    #[error("capability not supported: {0}")]
    CapabilityNotSupported(String),

    #[error("protocol error: {0}")]
    Other(String),
}

/// Complete protocol negotiation state.
#[derive(Clone, Debug, Default)]
pub struct NegotiatedProtocol {
    /// Negotiated TTI version.
    pub version: u16,
    /// Server capabilities.
    pub capabilities: u16,
    /// Server character set ID.
    pub charset_id: u16,
    /// Server national character set ID.
    pub ncharset_id: u16,
    /// Server version string.
    pub server_version: Option<String>,
    /// Parsed version components.
    pub version_components: Option<VersionComponents>,
    /// Supported data types.
    pub data_types: Vec<u8>,
}

impl NegotiatedProtocol {
    /// Create from negotiation responses.
    pub fn from_responses(
        proto: &ProtocolNegotiationResponse,
        types: Option<&DataTypeNegotiationResponse>,
        version: Option<&VersionResponse>,
    ) -> Self {
        Self {
            version: proto.version,
            capabilities: proto.capabilities,
            charset_id: proto.charset_id,
            ncharset_id: proto.ncharset_id,
            server_version: version.map(|v| v.version.clone()),
            version_components: version.and_then(|v| v.version_components.clone()),
            data_types: types.map(|t| t.accepted_types.clone()).unwrap_or_default(),
        }
    }

    /// Check if a capability is supported.
    pub fn has_capability(&self, cap: u16) -> bool {
        self.capabilities & cap != 0
    }

    /// Check if version is at least the specified TTI version.
    pub fn is_version_at_least(&self, version: u16) -> bool {
        self.version >= version
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_request_encode() {
        let req = ProtocolNegotiationRequest::new(12);
        let encoded = req.encode();
        assert!(!encoded.is_empty());
        assert!(encoded.len() >= 12);
    }

    #[test]
    fn test_protocol_response_parse() {
        let data = vec![
            0, 12, // version = 12
            0, 0x1F, // capabilities
            0, 0, 0, 0, // flags
            0x03, 0x69, // charset = 873 (AL32UTF8)
            0x07, 0xD0, // ncharset = 2000 (AL16UTF16)
        ];

        let resp = ProtocolNegotiationResponse::parse(&data).unwrap();
        assert_eq!(resp.version, 12);
        assert_eq!(resp.charset_id, 873);
    }

    #[test]
    fn test_version_components_parse() {
        let components = VersionComponents::parse("19.3.0.0.0").unwrap();
        assert_eq!(components.major, 19);
        assert_eq!(components.minor, 3);
        assert!(components.is_at_least(19, 0));
        assert!(components.is_at_least(18, 0));
        assert!(!components.is_at_least(20, 0));
    }

    #[test]
    fn test_version_response_parse() {
        let mut data = vec![5]; // version length
        data.extend_from_slice(b"19.3.");

        let resp = VersionResponse::parse(&data).unwrap();
        assert_eq!(resp.version, "19.3.");
    }

    #[test]
    fn test_data_type_request() {
        let req = DataTypeNegotiationRequest::default_types();
        let encoded = req.encode();
        assert!(!encoded.is_empty());
        assert!(encoded.len() > 2);
    }

    #[test]
    fn test_capabilities() {
        let req = ProtocolNegotiationRequest::new(12).with_drcp();
        assert!(req.capabilities & capabilities::DRCP != 0);
    }

    #[test]
    fn test_negotiated_protocol() {
        let proto_resp = ProtocolNegotiationResponse {
            version: 12,
            capabilities: capabilities::AUTH | capabilities::LOB,
            flags: 0,
            charset_id: 873,
            ncharset_id: 2000,
        };

        let negotiated = NegotiatedProtocol::from_responses(&proto_resp, None, None);
        assert_eq!(negotiated.version, 12);
        assert!(negotiated.has_capability(capabilities::AUTH));
        assert!(negotiated.has_capability(capabilities::LOB));
        assert!(!negotiated.has_capability(capabilities::DRCP));
    }
}
