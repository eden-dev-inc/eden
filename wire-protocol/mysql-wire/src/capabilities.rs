//! MySQL capability flags.
//!
//! Capability flags are exchanged during the handshake phase to negotiate
//! which features the client and server will use during the connection.

use std::fmt;

bitflags::bitflags! {
    /// MySQL capability flags.
    ///
    /// These flags are exchanged during handshake to negotiate features.
    /// The server sends its capabilities in the initial handshake packet,
    /// and the client responds with a subset it supports.
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
    pub struct CapabilityFlags: u32 {
        /// Use the improved version of Old Password Authentication.
        const LONG_PASSWORD = 1 << 0;
        /// Send found rows instead of affected rows in EOF_Packet.
        const FOUND_ROWS = 1 << 1;
        /// Get all column flags (longer flags in Protocol::ColumnDefinition320).
        const LONG_FLAG = 1 << 2;
        /// Database (schema) name can be specified on connect in Handshake Response Packet.
        const CONNECT_WITH_DB = 1 << 3;
        /// Don't allow database.table.column syntax.
        const NO_SCHEMA = 1 << 4;
        /// Compression protocol supported.
        const COMPRESS = 1 << 5;
        /// Special handling of ODBC behaviour.
        const ODBC = 1 << 6;
        /// Can use LOAD DATA LOCAL.
        const LOCAL_FILES = 1 << 7;
        /// Parser can ignore spaces before '(' in queries.
        const IGNORE_SPACE = 1 << 8;
        /// Supports the 4.1 protocol.
        const PROTOCOL_41 = 1 << 9;
        /// wait_timeout vs wait_interactive_timeout.
        const INTERACTIVE = 1 << 10;
        /// Supports SSL.
        const SSL = 1 << 11;
        /// Ignore sigpipes.
        const IGNORE_SIGPIPE = 1 << 12;
        /// Client knows about transactions.
        const TRANSACTIONS = 1 << 13;
        /// Old flag for 4.1 protocol (reserved).
        const RESERVED = 1 << 14;
        /// 4.1 authentication (SECURE_CONNECTION).
        const SECURE_CONNECTION = 1 << 15;
        /// Multiple statements support.
        const MULTI_STATEMENTS = 1 << 16;
        /// Multiple result sets support.
        const MULTI_RESULTS = 1 << 17;
        /// Multiple result sets from COM_STMT_EXECUTE.
        const PS_MULTI_RESULTS = 1 << 18;
        /// Plugin authentication.
        const PLUGIN_AUTH = 1 << 19;
        /// Supports connection attributes in handshake response.
        const CONNECT_ATTRS = 1 << 20;
        /// Length of auth response data can be > 255.
        const PLUGIN_AUTH_LENENC_CLIENT_DATA = 1 << 21;
        /// Supports handling of expired passwords.
        const CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22;
        /// Session state tracking in OK packet.
        const SESSION_TRACK = 1 << 23;
        /// EOF packet deprecated, use OK packet instead.
        const DEPRECATE_EOF = 1 << 24;
        /// Client can handle optional metadata (resultset metadata).
        const OPTIONAL_RESULTSET_METADATA = 1 << 25;
        /// zstd compression algorithm support.
        const ZSTD_COMPRESSION_ALGORITHM = 1 << 26;
        /// Query attributes support.
        const QUERY_ATTRIBUTES = 1 << 27;
        /// Multi-factor authentication.
        const MULTI_FACTOR_AUTHENTICATION = 1 << 28;
        /// Capability extension support.
        const CAPABILITY_EXTENSION = 1 << 29;
        /// SSL connection with server certificate verification.
        const SSL_VERIFY_SERVER_CERT = 1 << 30;
        /// Remember options.
        const REMEMBER_OPTIONS = 1 << 31;
    }
}

impl CapabilityFlags {
    /// Standard client capabilities for MySQL 5.x compatibility.
    pub fn client_default_5x() -> Self {
        Self::LONG_PASSWORD
            | Self::LONG_FLAG
            | Self::CONNECT_WITH_DB
            | Self::PROTOCOL_41
            | Self::TRANSACTIONS
            | Self::SECURE_CONNECTION
            | Self::MULTI_STATEMENTS
            | Self::MULTI_RESULTS
            | Self::PS_MULTI_RESULTS
            | Self::PLUGIN_AUTH
    }

    /// Standard client capabilities for MySQL 8.x compatibility.
    pub fn client_default_8x() -> Self {
        Self::client_default_5x() | Self::DEPRECATE_EOF | Self::PLUGIN_AUTH_LENENC_CLIENT_DATA | Self::SESSION_TRACK
    }

    /// Minimal capabilities for a basic connection.
    pub fn minimal() -> Self {
        Self::LONG_PASSWORD | Self::PROTOCOL_41 | Self::SECURE_CONNECTION
    }

    /// Check if 4.1 protocol is supported.
    #[inline]
    pub fn supports_41(&self) -> bool {
        self.contains(Self::PROTOCOL_41)
    }

    /// Check if EOF packets are deprecated (MySQL 5.7.5+).
    #[inline]
    pub fn deprecate_eof(&self) -> bool {
        self.contains(Self::DEPRECATE_EOF)
    }

    /// Check if plugin authentication is supported.
    #[inline]
    pub fn supports_plugin_auth(&self) -> bool {
        self.contains(Self::PLUGIN_AUTH)
    }

    /// Check if transactions are supported.
    #[inline]
    pub fn supports_transactions(&self) -> bool {
        self.contains(Self::TRANSACTIONS)
    }

    /// Check if SSL is supported.
    #[inline]
    pub fn supports_ssl(&self) -> bool {
        self.contains(Self::SSL)
    }

    /// Check if compression is supported.
    #[inline]
    pub fn supports_compression(&self) -> bool {
        self.contains(Self::COMPRESS)
    }

    /// Check if connection attributes are supported.
    #[inline]
    pub fn supports_connect_attrs(&self) -> bool {
        self.contains(Self::CONNECT_ATTRS)
    }

    /// Check if session tracking is supported.
    #[inline]
    pub fn supports_session_track(&self) -> bool {
        self.contains(Self::SESSION_TRACK)
    }

    /// Check if multi-statements are supported.
    #[inline]
    pub fn supports_multi_statements(&self) -> bool {
        self.contains(Self::MULTI_STATEMENTS)
    }

    /// Check if multi-results are supported.
    #[inline]
    pub fn supports_multi_results(&self) -> bool {
        self.contains(Self::MULTI_RESULTS)
    }

    /// Negotiate capabilities between client and server.
    ///
    /// Returns the intersection of client and server capabilities.
    pub fn negotiate(client: Self, server: Self) -> Self {
        client & server
    }

    /// Check if query attributes are supported (MySQL 8.0.25+).
    #[inline]
    pub fn supports_query_attrs(&self) -> bool {
        self.contains(Self::QUERY_ATTRIBUTES)
    }

    /// Check if LOCAL INFILE is supported.
    #[inline]
    pub fn supports_local_files(&self) -> bool {
        self.contains(Self::LOCAL_FILES)
    }

    /// Check if optional result set metadata is supported (MySQL 8.0.14+).
    #[inline]
    pub fn supports_optional_metadata(&self) -> bool {
        self.contains(Self::OPTIONAL_RESULTSET_METADATA)
    }

    /// Check if zstd compression is supported (MySQL 8.0.18+).
    #[inline]
    pub fn supports_zstd(&self) -> bool {
        self.contains(Self::ZSTD_COMPRESSION_ALGORITHM)
    }

    /// Check if multi-factor authentication is supported (MySQL 8.0.27+).
    #[inline]
    pub fn supports_mfa(&self) -> bool {
        self.contains(Self::MULTI_FACTOR_AUTHENTICATION)
    }

    /// Estimate MySQL version based on capabilities.
    ///
    /// Returns a tuple of (major, minor) version numbers.
    /// This is a heuristic based on when capabilities were introduced.
    #[allow(clippy::if_same_then_else)]
    pub fn estimate_version(&self) -> (u8, u8) {
        if self.contains(Self::MULTI_FACTOR_AUTHENTICATION) {
            (8, 27) // MySQL 8.0.27+
        } else if self.contains(Self::QUERY_ATTRIBUTES) {
            (8, 25) // MySQL 8.0.25+
        } else if self.contains(Self::ZSTD_COMPRESSION_ALGORITHM) {
            (8, 18) // MySQL 8.0.18+
        } else if self.contains(Self::OPTIONAL_RESULTSET_METADATA) {
            (8, 14) // MySQL 8.0.14+
        } else if self.contains(Self::DEPRECATE_EOF) {
            (5, 7) // MySQL 5.7.5+ (or 8.0+)
        } else if self.contains(Self::SESSION_TRACK) {
            (5, 7) // MySQL 5.7+
        } else if self.contains(Self::PLUGIN_AUTH) {
            (5, 5) // MySQL 5.5+
        } else if self.contains(Self::PROTOCOL_41) {
            (4, 1) // MySQL 4.1+
        } else {
            (4, 0) // Old protocol
        }
    }

    /// Check if this appears to be MySQL 8.x.
    #[inline]
    pub fn is_mysql_8x(&self) -> bool {
        // MySQL 8.0 servers typically have DEPRECATE_EOF and SESSION_TRACK
        self.contains(Self::DEPRECATE_EOF) && self.contains(Self::SESSION_TRACK)
    }

    /// Check if this appears to be MySQL 5.7.x.
    #[inline]
    pub fn is_mysql_57(&self) -> bool {
        self.contains(Self::DEPRECATE_EOF) && !self.contains(Self::QUERY_ATTRIBUTES)
    }

    /// Check if this appears to be MySQL 5.6.x or earlier.
    #[inline]
    pub fn is_mysql_56_or_earlier(&self) -> bool {
        !self.contains(Self::DEPRECATE_EOF)
    }

    /// Check if mariadb-specific features are present.
    ///
    /// MariaDB uses some capability flags differently than MySQL.
    /// This is a heuristic - the server version string should be checked for definitive identification.
    #[inline]
    pub fn might_be_mariadb(&self) -> bool {
        // MariaDB has CAPABILITY_EXTENSION (bit 29) set in recent versions
        self.contains(Self::CAPABILITY_EXTENSION)
    }
}

impl Default for CapabilityFlags {
    fn default() -> Self {
        Self::client_default_8x()
    }
}

impl fmt::Display for CapabilityFlags {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:#010X}", self.bits())
    }
}

impl From<u32> for CapabilityFlags {
    fn from(bits: u32) -> Self {
        Self::from_bits_truncate(bits)
    }
}

impl From<CapabilityFlags> for u32 {
    fn from(flags: CapabilityFlags) -> Self {
        flags.bits()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client_default_5x() {
        let caps = CapabilityFlags::client_default_5x();
        assert!(caps.supports_41());
        assert!(caps.supports_transactions());
        assert!(caps.supports_plugin_auth());
        assert!(!caps.deprecate_eof());
    }

    #[test]
    fn test_client_default_8x() {
        let caps = CapabilityFlags::client_default_8x();
        assert!(caps.supports_41());
        assert!(caps.deprecate_eof());
        assert!(caps.supports_session_track());
    }

    #[test]
    fn test_negotiate() {
        let client = CapabilityFlags::client_default_8x();
        let server = CapabilityFlags::client_default_5x() | CapabilityFlags::SSL;

        let negotiated = CapabilityFlags::negotiate(client, server);

        // Both support PROTOCOL_41
        assert!(negotiated.supports_41());
        // Server doesn't support DEPRECATE_EOF
        assert!(!negotiated.deprecate_eof());
        // Client doesn't support SSL
        assert!(!negotiated.supports_ssl());
    }

    #[test]
    fn test_from_bits() {
        let bits: u32 = 0x000FA68F;
        let caps = CapabilityFlags::from(bits);
        assert_eq!(caps.bits(), bits);
    }

    #[test]
    fn test_display() {
        let caps = CapabilityFlags::PROTOCOL_41 | CapabilityFlags::SECURE_CONNECTION;
        let s = caps.to_string();
        assert!(s.starts_with("0x"));
    }
}
