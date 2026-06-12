//! RESP protocol error types.

use std::num::ParseIntError;
use std::str::Utf8Error;

/// Error when an unexpected RESP type tag is encountered.
#[derive(Copy, Clone, Eq, PartialEq, Debug, thiserror::Error)]
#[error("encountered incorrect tag {encountered}; expected {expected}")]
pub struct IncorrectTag {
    /// The tag byte that was actually found.
    pub encountered: u8,
    /// The tag byte that was expected.
    pub expected: u8,
}

impl IncorrectTag {
    /// Returns the encountered tag as a char for display purposes.
    pub fn encountered_char(&self) -> char {
        self.encountered as char
    }

    /// Returns the expected tag as a char for display purposes.
    pub fn expected_char(&self) -> char {
        self.expected as char
    }
}

/// Error when parsing a RESP length value.
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

/// RESP type tags.
pub mod tags {
    // RESP2 tags
    pub const SIMPLE_STRING: u8 = b'+';
    pub const SIMPLE_ERROR: u8 = b'-';
    pub const INTEGER: u8 = b':';
    pub const BULK_STRING: u8 = b'$';
    pub const ARRAY: u8 = b'*';

    // RESP3 tags
    pub const NULL: u8 = b'_';
    pub const BOOLEAN: u8 = b'#';
    pub const DOUBLE: u8 = b',';
    pub const BIG_NUMBER: u8 = b'(';
    pub const BULK_ERROR: u8 = b'!';
    pub const VERBATIM_STRING: u8 = b'=';
    pub const MAP: u8 = b'%';
    pub const SET: u8 = b'~';
    pub const PUSH: u8 = b'>';
    pub const ATTRIBUTES: u8 = b'|';
}
