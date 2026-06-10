//! Sybase TDS protocol error types.

use std::num::ParseIntError;
use std::str::Utf8Error;

/// Error when an unexpected TDS packet type is encountered.
#[derive(Copy, Clone, Eq, PartialEq, Debug, thiserror::Error)]
#[error("encountered incorrect packet type {encountered:#04x}; expected {expected:#04x}")]
pub struct IncorrectPacketType {
    /// The packet type byte that was actually found.
    pub encountered: u8,
    /// The packet type byte that was expected.
    pub expected: u8,
}

impl IncorrectPacketType {
    /// Returns a human-readable name for the encountered packet type.
    pub fn encountered_name(&self) -> &'static str {
        packet_type_name(self.encountered)
    }

    /// Returns a human-readable name for the expected packet type.
    pub fn expected_name(&self) -> &'static str {
        packet_type_name(self.expected)
    }
}

/// Error when parsing a TDS length or numeric value.
#[derive(Clone, Eq, PartialEq, Debug, thiserror::Error)]
pub enum InvalidLength {
    #[error("length is not an integer")]
    NonNumeric,

    #[error("length is too large")]
    TooLarge,

    #[error("length is invalid UTF-8: {0}")]
    InvalidUtf8(#[from] Utf8Error),

    #[error("length is invalid: {0}")]
    ParseIntError(#[from] ParseIntError),
}

/// General Sybase wire protocol error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum SybaseWireError {
    #[error("packet too short: expected at least {expected} bytes, got {actual}")]
    PacketTooShort { expected: usize, actual: usize },

    #[error("invalid packet type: {0:#04x}")]
    InvalidPacketType(u8),

    #[error("unsupported TDS version: {0}")]
    UnsupportedVersion(u8),

    #[error("invalid packet length: declared {declared}, actual {actual}")]
    InvalidPacketLength { declared: u16, actual: usize },

    #[error("invalid token type: {0:#04x}")]
    InvalidTokenType(u8),

    #[error("invalid data type: {0:#04x}")]
    InvalidDataType(u8),

    #[error("invalid status flags: {0:#04x}")]
    InvalidStatus(u8),

    #[error("login failed: {0}")]
    LoginFailed(String),

    #[error("server error {number}: {message}")]
    ServerError { number: i32, message: String },

    #[error(transparent)]
    IncorrectPacketType(#[from] IncorrectPacketType),

    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
}

impl SybaseWireError {
    pub fn packet_too_short(expected: usize, actual: usize) -> Self {
        Self::PacketTooShort { expected, actual }
    }

    pub fn invalid_packet_length(declared: u16, actual: usize) -> Self {
        Self::InvalidPacketLength { declared, actual }
    }

    pub fn server_error(number: i32, message: impl Into<String>) -> Self {
        Self::ServerError { number, message: message.into() }
    }
}

/// TDS packet type constants.
///
/// These define the first byte of the TDS packet header.
pub mod packet_types {
    /// SQL batch / language command (TDS 4.2)
    pub const QUERY: u8 = 0x01;

    /// Login packet (TDS 4.2)
    pub const LOGIN: u8 = 0x02;

    /// Remote procedure call
    pub const RPC: u8 = 0x03;

    /// Server response packet
    pub const REPLY: u8 = 0x04;

    /// Cancel / attention signal
    pub const CANCEL: u8 = 0x06;

    /// Bulk load data
    pub const BULK: u8 = 0x07;

    /// TDS 5.0 language command
    pub const QUERY5: u8 = 0x0F;

    /// TDS 5.0 login packet
    pub const LOGIN5: u8 = 0x10;

    /// Normal data token stream
    pub const NORMAL: u8 = 0x0F;

    /// Pre-login packet (TDS 7.0+, included for compatibility detection)
    pub const PRELOGIN: u8 = 0x12;

    /// TDS 7.0 login packet (included for compatibility detection)
    pub const LOGIN7: u8 = 0x10;

    /// SSPI authentication message
    pub const SSPI: u8 = 0x11;

    /// Transaction manager request
    pub const TRANS_MGR: u8 = 0x0E;
}

/// TDS packet status flags.
///
/// These define the second byte of the TDS packet header.
pub mod status_flags {
    /// Normal packet, more packets to follow
    pub const NORMAL: u8 = 0x00;

    /// Last packet in request/response (EOM - End of Message)
    pub const EOM: u8 = 0x01;

    /// Ignore this packet (used for attention acknowledgment)
    pub const IGNORE: u8 = 0x02;

    /// Reset connection (TDS 7.1+)
    pub const RESET_CONNECTION: u8 = 0x08;

    /// Reset connection but keep transaction state (TDS 7.3+)
    pub const RESET_CONNECTION_SKIP_TRAN: u8 = 0x10;
}

/// TDS token types used in the data stream.
///
/// Tokens appear within TDS_REPLY packets and describe the data that follows.
pub mod token_types {
    // Fixed-length tokens (0x00-0x7F range, no length field)

    /// Offset information (indicates position in SQL batch)
    pub const OFFSET: u8 = 0x78;

    /// Return status from stored procedure
    pub const RETURNSTATUS: u8 = 0x79;

    /// Column metadata for subsequent rows
    pub const COLMETADATA: u8 = 0x81;

    // Variable-length tokens (0x80-0xFF range, have length field)

    /// Tabular result: column info (TDS 5.0)
    pub const TABNAME: u8 = 0xA4;

    /// Column information
    pub const COLINFO: u8 = 0xA5;

    /// Order by columns
    pub const ORDER: u8 = 0xA9;

    /// Error message from server
    pub const ERROR: u8 = 0xAA;

    /// Informational message from server
    pub const INFO: u8 = 0xAB;

    /// Return parameter from stored procedure
    pub const RETURNVALUE: u8 = 0xAC;

    /// Login acknowledgment
    pub const LOGINACK: u8 = 0xAD;

    /// Feature extension acknowledgment (TDS 7.4+)
    pub const FEATUREEXTACK: u8 = 0xAE;

    /// Row data
    pub const ROW: u8 = 0xD1;

    /// Null-bitmap compressed row (TDS 7.3+)
    pub const NBCROW: u8 = 0xD2;

    /// Alternate row format (TDS 5.0)
    pub const ALTROW: u8 = 0xD3;

    /// Environment change notification
    pub const ENVCHANGE: u8 = 0xE3;

    /// Session state (TDS 7.4+)
    pub const SESSIONSTATE: u8 = 0xE4;

    /// SSPI authentication token
    pub const SSPI_TOKEN: u8 = 0xED;

    /// Indicates end of result set
    pub const DONE: u8 = 0xFD;

    /// End of stored procedure execution
    pub const DONEPROC: u8 = 0xFE;

    /// End of statement within batch
    pub const DONEINPROC: u8 = 0xFF;

    // TDS 5.0 specific tokens

    /// Capability exchange (TDS 5.0)
    pub const CAPABILITY: u8 = 0xE2;

    /// Parameter format (TDS 5.0)
    pub const PARAMFMT: u8 = 0xEC;

    /// Parameter format 2 (TDS 5.0)
    pub const PARAMFMT2: u8 = 0x20;

    /// Language command (TDS 5.0)
    pub const LANGUAGE: u8 = 0x21;

    /// Wide column format (TDS 5.0)
    pub const ORDERBY2: u8 = 0x22;

    /// Control token (TDS 5.0)
    pub const CONTROL: u8 = 0xA7;

    /// Result format (TDS 5.0)
    pub const ROWFMT: u8 = 0xEE;

    /// Wide result format (TDS 5.0)
    pub const ROWFMT2: u8 = 0x61;

    /// Dynamic SQL (TDS 5.0)
    pub const DYNAMIC: u8 = 0xE7;

    /// Dynamic SQL 2 (TDS 5.0)
    pub const DYNAMIC2: u8 = 0x62;

    /// Cursor declare (TDS 5.0)
    pub const CURCLOSE: u8 = 0x80;

    /// Cursor delete (TDS 5.0)
    pub const CURDELETE: u8 = 0x81;

    /// Cursor fetch (TDS 5.0)
    pub const CURFETCH: u8 = 0x82;

    /// Cursor info (TDS 5.0)
    pub const CURINFO: u8 = 0x83;

    /// Cursor open (TDS 5.0)
    pub const CUROPEN: u8 = 0x84;

    /// Cursor update (TDS 5.0)
    pub const CURUPDATE: u8 = 0x85;

    /// Message (TDS 5.0)
    pub const MSG: u8 = 0x65;

    /// Extended error (TDS 5.0)
    pub const EED: u8 = 0xE5;
}

/// TDS data type constants.
///
/// These define the type of data in column definitions and parameters.
pub mod data_types {
    // Fixed-length types

    /// Null type (0 bytes)
    pub const NULLTYPE: u8 = 0x1F;

    /// 1-byte signed integer
    pub const INT1TYPE: u8 = 0x30;

    /// Bit/boolean (1 byte)
    pub const BITTYPE: u8 = 0x32;

    /// 2-byte signed integer
    pub const INT2TYPE: u8 = 0x34;

    /// 4-byte signed integer
    pub const INT4TYPE: u8 = 0x38;

    /// 4-byte datetime (days since 1900-01-01, 1/300 second precision)
    pub const DATETIM4TYPE: u8 = 0x3A;

    /// 4-byte IEEE float
    pub const FLT4TYPE: u8 = 0x3B;

    /// Money (8 bytes, scaled integer)
    pub const MONEYTYPE: u8 = 0x3C;

    /// 8-byte datetime
    pub const DATETIMETYPE: u8 = 0x3D;

    /// 8-byte IEEE float
    pub const FLT8TYPE: u8 = 0x3E;

    /// Small money (4 bytes, scaled integer)
    pub const MONEY4TYPE: u8 = 0x7A;

    /// 8-byte signed integer
    pub const INT8TYPE: u8 = 0x7F;

    // Variable-length types

    /// GUID/uniqueidentifier (16 bytes)
    pub const GUIDTYPE: u8 = 0x24;

    /// Variable-length integer (1, 2, 4, or 8 bytes)
    pub const INTNTYPE: u8 = 0x26;

    /// Decimal number with precision and scale
    pub const DECIMALTYPE: u8 = 0x37;

    /// Numeric (same as decimal)
    pub const NUMERICTYPE: u8 = 0x3F;

    /// Variable-length bit
    pub const BITNTYPE: u8 = 0x68;

    /// Decimal with precision/scale (nullable)
    pub const DECIMALNTYPE: u8 = 0x6A;

    /// Numeric with precision/scale (nullable)
    pub const NUMERICNTYPE: u8 = 0x6C;

    /// Variable-length float
    pub const FLTNTYPE: u8 = 0x6D;

    /// Variable-length money
    pub const MONEYNTYPE: u8 = 0x6E;

    /// Variable-length datetime
    pub const DATETIMNTYPE: u8 = 0x6F;

    // Character/Binary types

    /// Fixed-length character
    pub const CHARTYPE: u8 = 0x2F;

    /// Variable-length character (max 255)
    pub const VARCHARTYPE: u8 = 0x27;

    /// Fixed-length binary
    pub const BINARYTYPE: u8 = 0x2D;

    /// Variable-length binary (max 255)
    pub const VARBINARYTYPE: u8 = 0x25;

    // Large object types

    /// Text (large character data)
    pub const TEXTTYPE: u8 = 0x23;

    /// Image (large binary data)
    pub const IMAGETYPE: u8 = 0x22;

    /// Unicode text
    pub const NTEXTTYPE: u8 = 0x63;

    /// Unicode variable-length character
    pub const NVARCHARTYPE: u8 = 0x67;

    /// Unicode fixed-length character
    pub const NCHARTYPE: u8 = 0x6F;

    // TDS 5.0 specific types

    /// Sensitivity label (TDS 5.0)
    pub const SENSITIVITYTYPE: u8 = 0x67;

    /// Boundary (TDS 5.0)
    pub const BOUNDARYTYPE: u8 = 0x68;

    /// Date (TDS 5.0)
    pub const DATETYPE: u8 = 0x31;

    /// Time (TDS 5.0)
    pub const TIMETYPE: u8 = 0x33;

    /// Interval types (TDS 5.0)
    pub const INTERVALTYPE: u8 = 0x2E;

    /// Unsigned int types (TDS 5.0)
    pub const UINT2TYPE: u8 = 0x41;
    pub const UINT4TYPE: u8 = 0x42;
    pub const UINT8TYPE: u8 = 0x43;
    pub const UINTNTYPE: u8 = 0x44;

    // Long types (TDS 5.0)

    /// Long binary (up to 2GB)
    pub const LONGBINARYTYPE: u8 = 0xE1;

    /// Long character (up to 2GB)
    pub const LONGCHARTYPE: u8 = 0xAF;

    /// Unitext (TDS 5.0 unicode text)
    pub const UNITEXTTYPE: u8 = 0xAE;

    /// XML type
    pub const XMLTYPE: u8 = 0xF1;
}

/// Environment change types.
pub mod env_change_types {
    /// Database changed
    pub const DATABASE: u8 = 0x01;

    /// Language changed
    pub const LANGUAGE: u8 = 0x02;

    /// Character set changed
    pub const CHARSET: u8 = 0x03;

    /// Packet size changed
    pub const PACKET_SIZE: u8 = 0x04;

    /// Sort order changed (TDS 5.0)
    pub const SORT_ORDER: u8 = 0x05;

    /// Unicode sort order changed
    pub const UNICODE_SORT: u8 = 0x06;

    /// Collation changed
    pub const COLLATION: u8 = 0x07;

    /// Transaction began
    pub const BEGIN_TRAN: u8 = 0x08;

    /// Transaction committed
    pub const COMMIT_TRAN: u8 = 0x09;

    /// Transaction rolled back
    pub const ROLLBACK_TRAN: u8 = 0x0A;

    /// Enlisted in distributed transaction
    pub const ENLIST_DTC: u8 = 0x0B;

    /// Defected from distributed transaction
    pub const DEFECT_DTC: u8 = 0x0C;

    /// Real-time log shipping
    pub const RTLS: u8 = 0x0D;

    /// Promote transaction
    pub const PROMOTE_TRAN: u8 = 0x0F;

    /// Transaction manager address
    pub const TRAN_MGR_ADDR: u8 = 0x10;

    /// Transaction ended
    pub const TRAN_ENDED: u8 = 0x11;

    /// Reset connection acknowledgment
    pub const RESET_ACK: u8 = 0x12;

    /// Session state
    pub const SESSION_STATE: u8 = 0x13;
}

/// TDS protocol version constants.
pub mod versions {
    /// TDS version 4.2 (original Sybase)
    pub const TDS_4_2: u32 = 0x04020000;

    /// TDS version 5.0 (Sybase ASE)
    pub const TDS_5_0: u32 = 0x05000000;

    /// TDS version 7.0 (SQL Server 7.0) - for detection only
    pub const TDS_7_0: u32 = 0x07000000;

    /// TDS version 7.1 (SQL Server 2000) - for detection only
    pub const TDS_7_1: u32 = 0x07010000;

    /// Minimum supported Sybase TDS version
    pub const MIN_SUPPORTED: u32 = TDS_4_2;

    /// Maximum supported Sybase TDS version
    pub const MAX_SUPPORTED: u32 = TDS_5_0;

    /// Check if a version is a Sybase TDS version (vs MS SQL Server)
    pub fn is_sybase(version: u32) -> bool {
        version <= TDS_5_0
    }

    /// Check if a version is supported
    pub fn is_supported(version: u32) -> bool {
        (MIN_SUPPORTED..=MAX_SUPPORTED).contains(&version)
    }
}

/// Returns a human-readable name for a packet type.
fn packet_type_name(packet_type: u8) -> &'static str {
    match packet_type {
        packet_types::QUERY => "Query",
        packet_types::LOGIN => "Login",
        packet_types::RPC => "RPC",
        packet_types::REPLY => "Reply",
        packet_types::CANCEL => "Cancel",
        packet_types::BULK => "Bulk",
        packet_types::QUERY5 => "Query5",
        packet_types::LOGIN5 => "Login5",
        packet_types::PRELOGIN => "PreLogin",
        packet_types::SSPI => "SSPI",
        packet_types::TRANS_MGR => "TransMgr",
        _ => "Unknown",
    }
}
