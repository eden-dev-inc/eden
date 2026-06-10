//! Oracle TNS protocol error types.

use std::num::ParseIntError;
use std::str::Utf8Error;

/// Error when an unexpected TNS packet type is encountered.
#[derive(Copy, Clone, Eq, PartialEq, Debug, thiserror::Error)]
#[error("encountered incorrect packet type {encountered}; expected {expected}")]
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

/// Error when parsing a TNS length or numeric value.
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

/// General Oracle wire protocol error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum OracleWireError {
    #[error("packet too short: expected at least {expected} bytes, got {actual}")]
    PacketTooShort { expected: usize, actual: usize },

    #[error("invalid packet type: {0}")]
    InvalidPacketType(u8),

    #[error("unsupported TNS version: {0}")]
    UnsupportedVersion(u16),

    #[error("invalid packet length: declared {declared}, actual {actual}")]
    InvalidPacketLength { declared: u16, actual: usize },

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u16, actual: u16 },

    #[error("invalid connect data: {0}")]
    InvalidConnectData(String),

    #[error("invalid data representation: {0}")]
    InvalidDataRepresentation(String),

    #[error(transparent)]
    IncorrectPacketType(#[from] IncorrectPacketType),

    #[error(transparent)]
    InvalidLength(#[from] InvalidLength),
}

impl OracleWireError {
    pub fn packet_too_short(expected: usize, actual: usize) -> Self {
        Self::PacketTooShort { expected, actual }
    }

    pub fn invalid_packet_length(declared: u16, actual: usize) -> Self {
        Self::InvalidPacketLength { declared, actual }
    }

    pub fn checksum_mismatch(expected: u16, actual: u16) -> Self {
        Self::ChecksumMismatch { expected, actual }
    }
}

/// TNS packet type constants.
///
/// These are common across all TNS versions.
pub mod packet_types {
    /// Connect request packet
    pub const CONNECT: u8 = 1;
    /// Accept packet (connection accepted)
    pub const ACCEPT: u8 = 2;
    /// Acknowledge packet
    pub const ACK: u8 = 3;
    /// Refuse packet (connection refused)
    pub const REFUSE: u8 = 4;
    /// Redirect packet
    pub const REDIRECT: u8 = 5;
    /// Data packet
    pub const DATA: u8 = 6;
    /// Null packet
    pub const NULL: u8 = 7;
    /// Abort packet
    pub const ABORT: u8 = 9;
    /// Resend packet
    pub const RESEND: u8 = 11;
    /// Marker packet
    pub const MARKER: u8 = 12;
    /// Attention packet
    pub const ATTENTION: u8 = 13;
    /// Control packet
    pub const CONTROL: u8 = 14;
    /// Data descriptor packet (TNS v12+)
    pub const DATA_DESCRIPTOR: u8 = 15;
}

/// TNS protocol version constants.
pub mod versions {
    // TNS Version 8 (Oracle 8i)
    pub const TNS_V8: u16 = 8;
    // TNS Version 9 (Oracle 9i)
    pub const TNS_V9: u16 = 9;
    // TNS Version 10 (Oracle 10g)
    pub const TNS_V10: u16 = 10;
    // TNS Version 11 (Oracle 11g)
    pub const TNS_V11: u16 = 11;
    // TNS Version 12 (Oracle 12c)
    pub const TNS_V12: u16 = 12;

    /// Minimum supported TNS version.
    pub const MIN_SUPPORTED: u16 = TNS_V8;
    /// Maximum supported TNS version.
    pub const MAX_SUPPORTED: u16 = TNS_V12;

    /// Check if a version is supported.
    pub fn is_supported(version: u16) -> bool {
        (MIN_SUPPORTED..=MAX_SUPPORTED).contains(&version)
    }
}

/// Data flags used in TNS data packets.
pub mod data_flags {
    /// Send token (used for authentication)
    pub const SEND_TOKEN: u16 = 0x0001;
    /// Request to send (flow control)
    pub const REQUEST_TO_SEND: u16 = 0x0002;
    /// End of file marker
    pub const EOF: u16 = 0x0040;
    /// More data follows
    pub const MORE_DATA: u16 = 0x0020;
    /// Reset marker
    pub const RESET: u16 = 0x0008;
}

/// Connect flags used in TNS connect packets.
pub mod connect_flags {
    // Connect flags byte 1
    /// Services wanted
    pub const SERVICES_WANTED: u8 = 0x01;
    /// Interchange involved
    pub const INTERCHANGE: u8 = 0x02;
    /// Services required
    pub const SERVICES_REQUIRED: u8 = 0x04;
    /// NAU enabled
    pub const NAU_ENABLED: u8 = 0x08;
    /// Strict ANO
    pub const STRICT_ANO: u8 = 0x10;

    // Connect flags byte 2
    /// Can receive attention
    pub const CAN_RECV_ATTENTION: u8 = 0x01;
    /// Can handle big data
    pub const BIG_DATA: u8 = 0x02;
    /// Supports multiplexing
    pub const MULTIPLEXING: u8 = 0x04;
    /// Supports DRCP (Database Resident Connection Pooling) - TNS v11+
    pub const DRCP: u8 = 0x08;
}

/// Returns a human-readable name for a packet type.
fn packet_type_name(packet_type: u8) -> &'static str {
    match packet_type {
        packet_types::CONNECT => "Connect",
        packet_types::ACCEPT => "Accept",
        packet_types::ACK => "Acknowledge",
        packet_types::REFUSE => "Refuse",
        packet_types::REDIRECT => "Redirect",
        packet_types::DATA => "Data",
        packet_types::NULL => "Null",
        packet_types::ABORT => "Abort",
        packet_types::RESEND => "Resend",
        packet_types::MARKER => "Marker",
        packet_types::ATTENTION => "Attention",
        packet_types::CONTROL => "Control",
        packet_types::DATA_DESCRIPTOR => "DataDescriptor",
        _ => "Unknown",
    }
}
