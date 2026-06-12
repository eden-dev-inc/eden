//! MongoDB Wire Protocol error types.

use std::borrow::Cow;
use wire_stream::SliceReadError;

#[derive(Clone, Debug, thiserror::Error)]
pub enum MongoWireError {
    #[error("message too short: expected at least {expected} bytes, got {actual}")]
    MessageTooShort { expected: usize, actual: usize },

    #[error("message too large: {length} exceeds maximum {max}")]
    MessageTooLarge { length: usize, max: usize },

    #[error("invalid message length: {0}")]
    InvalidMessageLength(i32),

    #[error("unknown opcode: {0}")]
    UnknownOpCode(i32),

    #[error("incomplete message: expected {expected} bytes, got {actual}")]
    IncompleteMessage { expected: usize, actual: usize },

    #[error("invalid BSON document: {0}")]
    InvalidBson(Cow<'static, str>),

    #[error("document too large: {length} exceeds maximum {max}")]
    DocumentTooLarge { length: usize, max: usize },

    #[error("string too large: {length} exceeds maximum {max}")]
    StringTooLarge { length: usize, max: usize },

    #[error("missing null terminator")]
    MissingNullTerminator,

    #[error("invalid UTF-8 in string: {0}")]
    InvalidUtf8(#[from] std::str::Utf8Error),

    #[error("unsupported section type: {0}")]
    UnsupportedSectionType(u8),

    #[error("invalid flags: reserved bits set (flags={flags:#010x}, reserved={reserved:#010x})")]
    InvalidFlags { flags: u32, reserved: u32 },

    #[error("unknown compressor ID: {0}")]
    UnknownCompressor(u8),

    #[error("missing required field: {0}")]
    MissingField(&'static str),

    #[error("stream error: {0}")]
    Stream(String),

    #[error("checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("decompression size exceeded: got {actual} bytes, limit is {limit}")]
    DecompressionSizeExceeded { actual: usize, limit: usize },

    #[error("too many documents: {count} exceeds maximum {max}")]
    TooManyDocuments { count: usize, max: usize },
}

impl MongoWireError {
    pub fn message_too_short(expected: usize, actual: usize) -> Self {
        Self::MessageTooShort { expected, actual }
    }

    pub fn incomplete(expected: usize, actual: usize) -> Self {
        Self::IncompleteMessage { expected, actual }
    }
}

impl From<SliceReadError> for MongoWireError {
    fn from(e: SliceReadError) -> Self {
        MongoWireError::Stream(e.to_string())
    }
}
