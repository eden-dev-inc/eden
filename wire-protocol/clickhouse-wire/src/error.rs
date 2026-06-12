//! ClickHouse Wire Protocol error types.

use std::str::Utf8Error;
use wire_stream::SliceReadError;

/// Errors that can occur when parsing ClickHouse wire protocol messages.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ClickhouseWireError {
    // ========================================================================
    // Size/length errors
    // ========================================================================
    /// Packet is too short to contain the expected data.
    #[error("packet too short: expected at least {expected} bytes, got {actual}")]
    PacketTooShort {
        /// Expected minimum size.
        expected: usize,
        /// Actual size received.
        actual: usize,
    },

    /// Packet exceeds maximum allowed size.
    #[error("packet too large: {length} exceeds maximum {max}")]
    PacketTooLarge {
        /// Actual packet length.
        length: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Incomplete packet - not enough bytes available.
    #[error("incomplete packet: expected {expected} bytes, got {actual}")]
    IncompletePacket {
        /// Expected number of bytes.
        expected: usize,
        /// Actual bytes available.
        actual: usize,
    },

    // ========================================================================
    // Protocol errors
    // ========================================================================
    /// Unknown packet type identifier.
    #[error("unknown packet type: {0}")]
    UnknownPacketType(u64),

    /// VarUInt encoding exceeds maximum length (9 bytes).
    #[error("invalid VarUInt: exceeds 9 bytes")]
    VarUIntTooLong,

    /// VarUInt value overflows u64.
    #[error("invalid VarUInt: overflow")]
    VarUIntOverflow,

    /// Unsupported protocol version.
    #[error("unsupported protocol version: {0}")]
    UnsupportedProtocolVersion(u64),

    // ========================================================================
    // String errors
    // ========================================================================
    /// String exceeds maximum allowed size.
    #[error("string too large: {length} exceeds maximum {max}")]
    StringTooLarge {
        /// Actual string length.
        length: usize,
        /// Maximum allowed length.
        max: usize,
    },

    /// Invalid UTF-8 in string data.
    #[error("invalid UTF-8 in string: {0}")]
    InvalidUtf8(#[from] Utf8Error),

    // ========================================================================
    // Block/column errors
    // ========================================================================
    /// Invalid block format or structure.
    #[error("invalid block: {0}")]
    InvalidBlock(String),

    /// Unknown or unsupported column type.
    #[error("unknown column type: {0}")]
    UnknownColumnType(String),

    /// Column count doesn't match expected.
    #[error("column count mismatch: expected {expected}, got {actual}")]
    ColumnCountMismatch {
        /// Expected column count.
        expected: usize,
        /// Actual column count.
        actual: usize,
    },

    /// Row count doesn't match expected.
    #[error("row count mismatch: expected {expected}, got {actual}")]
    RowCountMismatch {
        /// Expected row count.
        expected: usize,
        /// Actual row count.
        actual: usize,
    },

    // ========================================================================
    // Compression errors
    // ========================================================================
    /// Checksum validation failed.
    #[error("checksum mismatch: expected {expected:032x}, got {actual:032x}")]
    ChecksumMismatch {
        /// Expected checksum value.
        expected: u128,
        /// Actual computed checksum.
        actual: u128,
    },

    /// Decompression operation failed.
    #[error("decompression failed: {0}")]
    DecompressionFailed(String),

    /// Decompressed size exceeds safety limit.
    #[error("decompression size exceeded: got {actual} bytes, limit is {limit}")]
    DecompressionSizeExceeded {
        /// Actual decompressed size.
        actual: usize,
        /// Maximum allowed size.
        limit: usize,
    },

    /// Unsupported compression method.
    #[error("unsupported compression method: {0:#04x}")]
    UnsupportedCompression(u8),

    // ========================================================================
    // Server exception
    // ========================================================================
    /// Exception received from ClickHouse server.
    #[error("server exception {code}: {message}")]
    ServerException {
        /// Error code from server.
        code: i32,
        /// Exception class name.
        name: String,
        /// Error message.
        message: String,
        /// Stack trace (if available).
        stack_trace: String,
        /// Nested exception (if any).
        nested: Option<Box<ClickhouseWireError>>,
    },

    // ========================================================================
    // Stream error
    // ========================================================================
    /// Underlying stream read error.
    #[error("stream error: {0}")]
    Stream(String),

    // ========================================================================
    // HTTP-specific errors
    // ========================================================================
    /// Invalid header value.
    #[error("invalid header value: {header}: {value}")]
    InvalidHeader {
        /// Header name.
        header: String,
        /// Invalid header value.
        value: String,
    },

    /// Missing required parameter.
    #[error("missing required parameter: {0}")]
    MissingParameter(String),

    /// Invalid query parameter value.
    #[error("invalid query parameter: {param}: {value}")]
    InvalidQueryParam {
        /// Parameter name.
        param: String,
        /// Invalid value.
        value: String,
    },
}

impl ClickhouseWireError {
    /// Create a packet too short error.
    #[inline]
    pub fn packet_too_short(expected: usize, actual: usize) -> Self {
        Self::PacketTooShort { expected, actual }
    }

    /// Create an incomplete packet error.
    #[inline]
    pub fn incomplete(expected: usize, actual: usize) -> Self {
        Self::IncompletePacket { expected, actual }
    }

    /// Create a server exception error.
    #[inline]
    pub fn server_exception(code: i32, name: impl Into<String>, message: impl Into<String>, stack_trace: impl Into<String>) -> Self {
        Self::ServerException {
            code,
            name: name.into(),
            message: message.into(),
            stack_trace: stack_trace.into(),
            nested: None,
        }
    }

    /// Create a server exception with nested exception.
    #[inline]
    pub fn server_exception_nested(
        code: i32,
        name: impl Into<String>,
        message: impl Into<String>,
        stack_trace: impl Into<String>,
        nested: ClickhouseWireError,
    ) -> Self {
        Self::ServerException {
            code,
            name: name.into(),
            message: message.into(),
            stack_trace: stack_trace.into(),
            nested: Some(Box::new(nested)),
        }
    }
}

impl From<SliceReadError> for ClickhouseWireError {
    fn from(e: SliceReadError) -> Self {
        ClickhouseWireError::Stream(e.to_string())
    }
}

impl From<std::string::FromUtf8Error> for ClickhouseWireError {
    fn from(e: std::string::FromUtf8Error) -> Self {
        ClickhouseWireError::InvalidUtf8(e.utf8_error())
    }
}
