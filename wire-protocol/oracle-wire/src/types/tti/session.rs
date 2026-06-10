//! Session state and capabilities negotiation.
//!
//! This module manages Oracle session state including:
//! - Protocol version negotiation
//! - Session capabilities
//! - Connection parameters
//! - DRCP (Database Resident Connection Pooling)

use super::charset::{CharsetId, NCharsetId};

/// Oracle protocol version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ProtocolVersion {
    /// Major version (e.g., 12 for Oracle 12c).
    pub major: u8,
    /// Minor version.
    pub minor: u8,
}

impl ProtocolVersion {
    /// Oracle 8i (TNS v8).
    pub const V8: Self = Self { major: 8, minor: 0 };
    /// Oracle 9i (TNS v9).
    pub const V9: Self = Self { major: 9, minor: 0 };
    /// Oracle 10g (TNS v10).
    pub const V10: Self = Self { major: 10, minor: 0 };
    /// Oracle 11g (TNS v11).
    pub const V11: Self = Self { major: 11, minor: 0 };
    /// Oracle 12c (TNS v12).
    pub const V12: Self = Self { major: 12, minor: 0 };
    /// Oracle 18c.
    pub const V18: Self = Self { major: 18, minor: 0 };
    /// Oracle 19c.
    pub const V19: Self = Self { major: 19, minor: 0 };
    /// Oracle 21c.
    pub const V21: Self = Self { major: 21, minor: 0 };
    /// Oracle 23ai.
    pub const V23: Self = Self { major: 23, minor: 0 };

    /// Create a new version.
    pub const fn new(major: u8, minor: u8) -> Self {
        Self { major, minor }
    }

    /// Parse from TNS version bytes.
    pub const fn from_tns_version(version: u16) -> Self {
        // TNS version format: major in high byte, minor in low byte
        Self { major: (version >> 8) as u8, minor: (version & 0xFF) as u8 }
    }

    /// Convert to TNS version bytes.
    pub const fn to_tns_version(self) -> u16 {
        ((self.major as u16) << 8) | (self.minor as u16)
    }

    /// Check if this version supports a feature.
    pub fn supports(&self, feature: Feature) -> bool {
        match feature {
            Feature::SessionMultiplexing => *self >= Self::V11,
            Feature::ImplicitResults => *self >= Self::V12,
            Feature::Sharding => *self >= Self::V12,
            Feature::JsonType => *self >= Self::V21,
            Feature::Drcp => *self >= Self::V11,
            Feature::ExtendedDataTypes => *self >= Self::V12,
            Feature::SessionPiggyback => *self >= Self::V11,
            Feature::Boolean => *self >= Self::V23,
        }
    }
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self::V12
    }
}

/// Protocol feature.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Feature {
    /// Session multiplexing (v11+).
    SessionMultiplexing,
    /// Implicit result sets (v12+).
    ImplicitResults,
    /// Database sharding (v12+).
    Sharding,
    /// Native JSON data type (v21+).
    JsonType,
    /// Database Resident Connection Pooling (v11+).
    Drcp,
    /// Extended data types (v12+).
    ExtendedDataTypes,
    /// Session state piggyback (v11+).
    SessionPiggyback,
    /// Native BOOLEAN type (v23+).
    Boolean,
}

/// Session capabilities negotiated during connection.
#[derive(Clone, Debug)]
pub struct SessionCapabilities {
    /// Protocol version.
    pub version: ProtocolVersion,
    /// Maximum SDU (Session Data Unit) size.
    pub sdu_size: u16,
    /// Maximum TDU (Transport Data Unit) size.
    pub tdu_size: u16,
    /// Database character set.
    pub db_charset: CharsetId,
    /// National character set.
    pub nchar_charset: NCharsetId,
    /// Server timezone offset (minutes from UTC).
    pub timezone_offset: i16,
    /// Server version string.
    pub server_version: Option<String>,
    /// Server banner.
    pub server_banner: Option<String>,
    /// Enabled capabilities bitmap.
    capabilities: u64,
}

impl SessionCapabilities {
    /// Create new capabilities with defaults.
    pub fn new(version: ProtocolVersion) -> Self {
        Self {
            version,
            sdu_size: 8192,
            tdu_size: 32767,
            db_charset: CharsetId::AL32UTF8,
            nchar_charset: NCharsetId::AL16UTF16,
            timezone_offset: 0,
            server_version: None,
            server_banner: None,
            capabilities: 0,
        }
    }

    /// Check if a capability is enabled.
    pub fn has_capability(&self, cap: Capability) -> bool {
        (self.capabilities & cap.mask()) != 0
    }

    /// Enable a capability.
    pub fn enable_capability(&mut self, cap: Capability) {
        self.capabilities |= cap.mask();
    }

    /// Disable a capability.
    pub fn disable_capability(&mut self, cap: Capability) {
        self.capabilities &= !cap.mask();
    }

    /// Check if version supports a feature.
    pub fn supports_feature(&self, feature: Feature) -> bool {
        self.version.supports(feature)
    }

    /// Set SDU size.
    pub fn with_sdu_size(mut self, size: u16) -> Self {
        self.sdu_size = size;
        self
    }

    /// Set TDU size.
    pub fn with_tdu_size(mut self, size: u16) -> Self {
        self.tdu_size = size;
        self
    }

    /// Set character sets.
    pub fn with_charsets(mut self, db: CharsetId, nchar: NCharsetId) -> Self {
        self.db_charset = db;
        self.nchar_charset = nchar;
        self
    }
}

impl Default for SessionCapabilities {
    fn default() -> Self {
        Self::new(ProtocolVersion::default())
    }
}

/// Individual capability flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Capability {
    /// Break/Reset support.
    BreakReset,
    /// Native 64-bit operations.
    Native64Bit,
    /// Implicit results.
    ImplicitResults,
    /// Session piggyback.
    SessionPiggyback,
    /// Array DML row counts.
    ArrayDmlRowCounts,
    /// LOB prefetch.
    LobPrefetch,
    /// End-to-end metrics.
    EndToEndMetrics,
    /// Connection authentication.
    ConnectionAuth,
}

impl Capability {
    /// Get the capability bitmask.
    pub const fn mask(self) -> u64 {
        1 << (self as u32)
    }
}

/// Session state that changes during connection lifetime.
#[derive(Clone, Debug, Default)]
pub struct SessionState {
    /// Current schema.
    pub current_schema: Option<String>,
    /// Current user.
    pub current_user: Option<String>,
    /// Session ID.
    pub session_id: Option<u32>,
    /// Serial number.
    pub serial_num: Option<u32>,
    /// Transaction state.
    pub transaction: TransactionState,
    /// Action name (for tracing).
    pub action: Option<String>,
    /// Module name (for tracing).
    pub module: Option<String>,
    /// Client info (for tracing).
    pub client_info: Option<String>,
}

impl SessionState {
    /// Create new session state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if in a transaction.
    pub fn in_transaction(&self) -> bool {
        self.transaction.is_active()
    }

    /// Start a transaction.
    pub fn begin_transaction(&mut self) {
        self.transaction = TransactionState::Active;
    }

    /// Commit the transaction.
    pub fn commit(&mut self) {
        self.transaction = TransactionState::None;
    }

    /// Rollback the transaction.
    pub fn rollback(&mut self) {
        self.transaction = TransactionState::None;
    }
}

/// Transaction state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TransactionState {
    /// No active transaction.
    #[default]
    None,
    /// Transaction is active.
    Active,
    /// Transaction is prepared (for XA).
    Prepared,
}

impl TransactionState {
    /// Check if transaction is active.
    pub const fn is_active(self) -> bool {
        matches!(self, Self::Active | Self::Prepared)
    }
}

/// DRCP (Database Resident Connection Pooling) state.
#[derive(Clone, Debug, Default)]
pub struct DrcpState {
    /// Whether DRCP is enabled.
    pub enabled: bool,
    /// Connection class.
    pub connection_class: Option<String>,
    /// Purity (new or self).
    pub purity: DrcpPurity,
    /// Pool timeout.
    pub pool_timeout: u32,
    /// Session state consistency.
    pub state_consistency: StateConsistency,
}

/// DRCP purity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum DrcpPurity {
    /// Get a new session.
    #[default]
    New,
    /// Get own session if available.
    Own,
}

impl DrcpPurity {
    /// Oracle's internal code.
    pub const fn code(self) -> u8 {
        match self {
            Self::New => 0x01,
            Self::Own => 0x02,
        }
    }
}

/// Session state consistency for DRCP.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum StateConsistency {
    /// Dynamic (default).
    #[default]
    Dynamic,
    /// Static (no session state changes expected).
    Static,
}

/// Connection info for session establishment.
#[derive(Clone, Debug)]
pub struct ConnectionInfo {
    /// Host name or IP.
    pub host: String,
    /// Port number.
    pub port: u16,
    /// Service name.
    pub service_name: String,
    /// SID (alternative to service name).
    pub sid: Option<String>,
    /// Instance name (for RAC).
    pub instance_name: Option<String>,
    /// Server type (dedicated, shared, pooled).
    pub server_type: ServerType,
    /// Connection timeout (seconds).
    pub connect_timeout: u32,
    /// DRCP configuration.
    pub drcp: Option<DrcpState>,
}

impl ConnectionInfo {
    /// Create new connection info.
    pub fn new(host: impl Into<String>, port: u16, service_name: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            service_name: service_name.into(),
            sid: None,
            instance_name: None,
            server_type: ServerType::Dedicated,
            connect_timeout: 60,
            drcp: None,
        }
    }

    /// Use SID instead of service name.
    pub fn with_sid(mut self, sid: impl Into<String>) -> Self {
        self.sid = Some(sid.into());
        self
    }

    /// Set instance name for RAC.
    pub fn with_instance(mut self, instance: impl Into<String>) -> Self {
        self.instance_name = Some(instance.into());
        self
    }

    /// Set server type.
    pub fn with_server_type(mut self, server_type: ServerType) -> Self {
        self.server_type = server_type;
        self
    }

    /// Enable DRCP.
    pub fn with_drcp(mut self, drcp: DrcpState) -> Self {
        self.drcp = Some(drcp);
        self
    }

    /// Build TNS connect string.
    pub fn to_connect_string(&self) -> String {
        let mut parts = Vec::new();

        // Address
        parts.push(format!("(ADDRESS=(PROTOCOL=TCP)(HOST={})(PORT={}))", self.host, self.port));

        // Connect data
        let mut connect_data = Vec::new();

        if let Some(ref sid) = self.sid {
            connect_data.push(format!("(SID={})", sid));
        } else {
            connect_data.push(format!("(SERVICE_NAME={})", self.service_name));
        }

        if let Some(ref instance) = self.instance_name {
            connect_data.push(format!("(INSTANCE_NAME={})", instance));
        }

        connect_data.push(format!("(SERVER={})", self.server_type.as_str()));

        parts.push(format!("(CONNECT_DATA={})", connect_data.join("")));

        format!("(DESCRIPTION={})", parts.join(""))
    }
}

/// Server type.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum ServerType {
    /// Dedicated server process.
    #[default]
    Dedicated,
    /// Shared server (dispatcher).
    Shared,
    /// Pooled (DRCP).
    Pooled,
}

impl ServerType {
    /// String representation.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dedicated => "DEDICATED",
            Self::Shared => "SHARED",
            Self::Pooled => "POOLED",
        }
    }
}

/// Session state piggyback request (function code 0x6B).
///
/// Oracle 11g+ supports piggybacking session state changes onto other requests
/// to reduce round-trips. This allows sending session attribute changes
/// (like NLS settings) along with the next SQL operation.
#[derive(Clone, Debug)]
pub struct SessionStatePiggybackRequest {
    /// Session attributes to change.
    pub attributes: Vec<SessionAttribute>,
}

impl SessionStatePiggybackRequest {
    /// Create a new piggyback request.
    pub fn new() -> Self {
        Self { attributes: Vec::new() }
    }

    /// Add an attribute change.
    pub fn with_attribute(mut self, attr: SessionAttribute) -> Self {
        self.attributes.push(attr);
        self
    }

    /// Set current schema.
    pub fn set_schema(self, schema: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::CurrentSchema(schema.into()))
    }

    /// Set NLS language.
    pub fn set_language(self, language: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::NlsLanguage(language.into()))
    }

    /// Set NLS territory.
    pub fn set_territory(self, territory: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::NlsTerritory(territory.into()))
    }

    /// Set action name (for tracing).
    pub fn set_action(self, action: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::Action(action.into()))
    }

    /// Set module name (for tracing).
    pub fn set_module(self, module: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::Module(module.into()))
    }

    /// Set client info (for tracing).
    pub fn set_client_info(self, info: impl Into<String>) -> Self {
        self.with_attribute(SessionAttribute::ClientInfo(info.into()))
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        use super::function_codes::FunctionCode;

        let mut buf = Vec::with_capacity(256);

        // Function code
        buf.push(FunctionCode::SessionStatePiggyback.as_u8());

        // Number of attributes
        buf.push(self.attributes.len() as u8);

        // Each attribute
        for attr in &self.attributes {
            let (key, value) = attr.encode();
            buf.push(key);
            let value_bytes = value.as_bytes();
            buf.push(value_bytes.len() as u8);
            buf.extend_from_slice(value_bytes);
        }

        buf
    }

    /// Check if this request has any attributes.
    pub fn is_empty(&self) -> bool {
        self.attributes.is_empty()
    }
}

impl Default for SessionStatePiggybackRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// Session attribute for piggyback changes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionAttribute {
    /// ALTER SESSION SET CURRENT_SCHEMA.
    CurrentSchema(String),
    /// ALTER SESSION SET NLS_LANGUAGE.
    NlsLanguage(String),
    /// ALTER SESSION SET NLS_TERRITORY.
    NlsTerritory(String),
    /// ALTER SESSION SET NLS_DATE_FORMAT.
    NlsDateFormat(String),
    /// ALTER SESSION SET NLS_TIMESTAMP_FORMAT.
    NlsTimestampFormat(String),
    /// ALTER SESSION SET NLS_NUMERIC_CHARACTERS.
    NlsNumericCharacters(String),
    /// ALTER SESSION SET TIME_ZONE.
    TimeZone(String),
    /// DBMS_APPLICATION_INFO.SET_ACTION.
    Action(String),
    /// DBMS_APPLICATION_INFO.SET_MODULE.
    Module(String),
    /// DBMS_APPLICATION_INFO.SET_CLIENT_INFO.
    ClientInfo(String),
}

impl SessionAttribute {
    /// Get the attribute key code and value.
    fn encode(&self) -> (u8, &str) {
        match self {
            Self::CurrentSchema(v) => (0x01, v),
            Self::NlsLanguage(v) => (0x02, v),
            Self::NlsTerritory(v) => (0x03, v),
            Self::NlsDateFormat(v) => (0x04, v),
            Self::NlsTimestampFormat(v) => (0x05, v),
            Self::NlsNumericCharacters(v) => (0x06, v),
            Self::TimeZone(v) => (0x07, v),
            Self::Action(v) => (0x10, v),
            Self::Module(v) => (0x11, v),
            Self::ClientInfo(v) => (0x12, v),
        }
    }

    /// Parse from key code and value.
    pub fn from_code(code: u8, value: String) -> Option<Self> {
        match code {
            0x01 => Some(Self::CurrentSchema(value)),
            0x02 => Some(Self::NlsLanguage(value)),
            0x03 => Some(Self::NlsTerritory(value)),
            0x04 => Some(Self::NlsDateFormat(value)),
            0x05 => Some(Self::NlsTimestampFormat(value)),
            0x06 => Some(Self::NlsNumericCharacters(value)),
            0x07 => Some(Self::TimeZone(value)),
            0x10 => Some(Self::Action(value)),
            0x11 => Some(Self::Module(value)),
            0x12 => Some(Self::ClientInfo(value)),
            _ => None,
        }
    }
}

/// DRCP release request (function code 0x6C).
///
/// Releases a DRCP (Database Resident Connection Pooling) connection back
/// to the pool. This should be called when the application no longer needs
/// the connection but wants to keep it available for reuse.
#[derive(Clone, Debug)]
pub struct DrpcReleaseRequest {
    /// Release mode.
    pub mode: DrpcReleaseMode,
    /// Optional tag for the session.
    pub tag: Option<String>,
}

impl DrpcReleaseRequest {
    /// Create a new release request with default settings.
    pub fn new() -> Self {
        Self { mode: DrpcReleaseMode::Normal, tag: None }
    }

    /// Set release mode.
    pub fn with_mode(mut self, mode: DrpcReleaseMode) -> Self {
        self.mode = mode;
        self
    }

    /// Set session tag for later retrieval.
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tag = Some(tag.into());
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        use super::function_codes::FunctionCode;

        let mut buf = Vec::with_capacity(32);

        // Function code
        buf.push(FunctionCode::DrpcRelease.as_u8());

        // Release mode
        buf.push(self.mode.code());

        // Tag (optional, length-prefixed)
        if let Some(ref tag) = self.tag {
            let tag_bytes = tag.as_bytes();
            buf.push(tag_bytes.len() as u8);
            buf.extend_from_slice(tag_bytes);
        } else {
            buf.push(0);
        }

        buf
    }
}

impl Default for DrpcReleaseRequest {
    fn default() -> Self {
        Self::new()
    }
}

/// DRCP release mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DrpcReleaseMode {
    /// Normal release - connection may be reused by any session.
    #[default]
    Normal,
    /// Stateless release - session state is preserved for later retrieval.
    Stateless,
    /// Force release - immediately terminates the session.
    Force,
}

impl DrpcReleaseMode {
    /// Get the mode code.
    pub const fn code(self) -> u8 {
        match self {
            Self::Normal => 0x00,
            Self::Stateless => 0x01,
            Self::Force => 0x02,
        }
    }

    /// Parse from code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x00 => Some(Self::Normal),
            0x01 => Some(Self::Stateless),
            0x02 => Some(Self::Force),
            _ => None,
        }
    }
}

/// Get server info request (function code 0x76).
///
/// Requests detailed server information beyond what's provided in
/// the initial connection handshake.
#[derive(Clone, Debug)]
pub struct GetServerInfoRequest {
    /// Information categories to retrieve.
    pub categories: Vec<ServerInfoCategory>,
}

impl GetServerInfoRequest {
    /// Create a request for all available info.
    pub fn all() -> Self {
        Self {
            categories: vec![
                ServerInfoCategory::Version,
                ServerInfoCategory::Platform,
                ServerInfoCategory::Charset,
                ServerInfoCategory::Instance,
                ServerInfoCategory::Database,
                ServerInfoCategory::Parameters,
            ],
        }
    }

    /// Create a request for specific categories.
    pub fn new(categories: Vec<ServerInfoCategory>) -> Self {
        Self { categories }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        use super::function_codes::FunctionCode;

        let mut buf = Vec::with_capacity(8);

        // Function code
        buf.push(FunctionCode::GetServerInfo.as_u8());

        // Category bitmap
        let mut bitmap: u16 = 0;
        for cat in &self.categories {
            bitmap |= cat.mask();
        }
        buf.extend_from_slice(&bitmap.to_be_bytes());

        buf
    }
}

/// Server info category.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ServerInfoCategory {
    /// Server version info.
    Version,
    /// Platform/OS info.
    Platform,
    /// Character set info.
    Charset,
    /// Instance info (SID, instance name).
    Instance,
    /// Database info (database name, unique name).
    Database,
    /// Server parameters.
    Parameters,
}

impl ServerInfoCategory {
    /// Get the category bitmask.
    pub const fn mask(self) -> u16 {
        1 << (self as u32)
    }
}

/// Server info response.
#[derive(Clone, Debug, Default)]
pub struct GetServerInfoResponse {
    /// Oracle version string (e.g., "19.0.0.0.0").
    pub version: Option<String>,
    /// Full version banner.
    pub version_banner: Option<String>,
    /// Platform/OS name.
    pub platform: Option<String>,
    /// Database character set name.
    pub db_charset: Option<String>,
    /// National character set name.
    pub nchar_charset: Option<String>,
    /// Instance name.
    pub instance_name: Option<String>,
    /// Database name.
    pub database_name: Option<String>,
    /// Database unique name.
    pub db_unique_name: Option<String>,
    /// Is CDB (Container Database).
    pub is_cdb: bool,
    /// Current PDB name (if applicable).
    pub pdb_name: Option<String>,
}

impl GetServerInfoResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, ServerInfoError> {
        if data.is_empty() {
            return Err(ServerInfoError::TooShort);
        }

        let mut response = Self::default();
        let mut pos = 0;

        // Parse key-value pairs
        while pos < data.len() {
            if pos + 2 > data.len() {
                break;
            }

            let key = data[pos];
            let len = data[pos + 1] as usize;
            pos += 2;

            if pos + len > data.len() {
                break;
            }

            let value = String::from_utf8_lossy(&data[pos..pos + len]).to_string();
            pos += len;

            match key {
                0x01 => response.version = Some(value),
                0x02 => response.version_banner = Some(value),
                0x03 => response.platform = Some(value),
                0x04 => response.db_charset = Some(value),
                0x05 => response.nchar_charset = Some(value),
                0x06 => response.instance_name = Some(value),
                0x07 => response.database_name = Some(value),
                0x08 => response.db_unique_name = Some(value),
                0x09 => response.is_cdb = value == "YES",
                0x0A => response.pdb_name = Some(value),
                _ => {} // Unknown key, skip
            }
        }

        Ok(response)
    }
}

/// Error parsing server info response.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ServerInfoError {
    #[error("data too short")]
    TooShort,
    #[error("invalid format")]
    InvalidFormat,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_protocol_version() {
        assert!(ProtocolVersion::V12 > ProtocolVersion::V11);
        assert!(ProtocolVersion::V19 > ProtocolVersion::V12);

        assert!(ProtocolVersion::V12.supports(Feature::ImplicitResults));
        assert!(!ProtocolVersion::V11.supports(Feature::ImplicitResults));
        assert!(ProtocolVersion::V11.supports(Feature::Drcp));
    }

    #[test]
    fn test_version_conversion() {
        let v = ProtocolVersion::new(12, 2);
        let tns = v.to_tns_version();
        let v2 = ProtocolVersion::from_tns_version(tns);
        assert_eq!(v, v2);
    }

    #[test]
    fn test_session_capabilities() {
        let mut caps = SessionCapabilities::new(ProtocolVersion::V12);

        caps.enable_capability(Capability::LobPrefetch);
        assert!(caps.has_capability(Capability::LobPrefetch));

        caps.disable_capability(Capability::LobPrefetch);
        assert!(!caps.has_capability(Capability::LobPrefetch));
    }

    #[test]
    fn test_session_state() {
        let mut state = SessionState::new();
        assert!(!state.in_transaction());

        state.begin_transaction();
        assert!(state.in_transaction());

        state.commit();
        assert!(!state.in_transaction());
    }

    #[test]
    fn test_connection_info() {
        let info = ConnectionInfo::new("localhost", 1521, "ORCL").with_server_type(ServerType::Dedicated);

        let connect_string = info.to_connect_string();
        assert!(connect_string.contains("HOST=localhost"));
        assert!(connect_string.contains("PORT=1521"));
        assert!(connect_string.contains("SERVICE_NAME=ORCL"));
        assert!(connect_string.contains("SERVER=DEDICATED"));
    }

    #[test]
    fn test_drcp_state() {
        let drcp = DrcpState {
            enabled: true,
            connection_class: Some("MYAPP".to_string()),
            purity: DrcpPurity::Own,
            pool_timeout: 300,
            state_consistency: StateConsistency::Dynamic,
        };

        assert!(drcp.enabled);
        assert_eq!(drcp.purity, DrcpPurity::Own);
    }

    #[test]
    fn test_session_state_piggyback() {
        let request = SessionStatePiggybackRequest::new().set_schema("HR").set_action("load_data").set_module("ETL_PROCESS");

        assert!(!request.is_empty());
        assert_eq!(request.attributes.len(), 3);

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0x6B); // SessionStatePiggyback function code
        assert_eq!(encoded[1], 3); // 3 attributes
    }

    #[test]
    fn test_session_attribute() {
        let attr = SessionAttribute::CurrentSchema("HR".to_string());
        let (key, value) = attr.encode();
        assert_eq!(key, 0x01);
        assert_eq!(value, "HR");

        let parsed = SessionAttribute::from_code(0x01, "HR".to_string());
        assert_eq!(parsed, Some(attr));
    }

    #[test]
    fn test_drpc_release_request() {
        let request = DrpcReleaseRequest::new().with_mode(DrpcReleaseMode::Stateless).with_tag("session_123");

        assert_eq!(request.mode, DrpcReleaseMode::Stateless);
        assert_eq!(request.tag, Some("session_123".to_string()));

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0x6C); // DrpcRelease function code
        assert_eq!(encoded[1], 0x01); // Stateless mode
    }

    #[test]
    fn test_drpc_release_mode() {
        assert_eq!(DrpcReleaseMode::Normal.code(), 0x00);
        assert_eq!(DrpcReleaseMode::Stateless.code(), 0x01);
        assert_eq!(DrpcReleaseMode::Force.code(), 0x02);

        assert_eq!(DrpcReleaseMode::from_code(0x01), Some(DrpcReleaseMode::Stateless));
        assert_eq!(DrpcReleaseMode::from_code(0xFF), None);
    }

    #[test]
    fn test_get_server_info_request() {
        let request = GetServerInfoRequest::all();
        assert_eq!(request.categories.len(), 6);

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], 0x76); // GetServerInfo function code
    }

    #[test]
    fn test_get_server_info_response() {
        // Build sample response data: key (1 byte) + length (1 byte) + value
        let mut data = Vec::new();

        // Version
        data.push(0x01);
        data.push(8);
        data.extend_from_slice(b"19.0.0.0");

        // Instance name
        data.push(0x06);
        data.push(4);
        data.extend_from_slice(b"ORCL");

        // Is CDB
        data.push(0x09);
        data.push(3);
        data.extend_from_slice(b"YES");

        let response = GetServerInfoResponse::parse(&data).expect("should parse");
        assert_eq!(response.version, Some("19.0.0.0".to_string()));
        assert_eq!(response.instance_name, Some("ORCL".to_string()));
        assert!(response.is_cdb);
    }
}
