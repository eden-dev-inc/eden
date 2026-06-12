//! Parsing traits for PostgreSQL wire protocol messages.

use std::fmt;
use wire_stream::{WireRead, WireReadSync};

/// Error type for parse operations.
///
/// Distinguishes between stream errors (I/O, buffer issues) and
/// parse errors (protocol violations, invalid data).
#[derive(Clone, Debug)]
pub enum PgParseError<Serror: std::error::Error, Perror: std::error::Error> {
    /// Error from the underlying stream (I/O, buffer exhaustion, etc.)
    Stream(Serror),
    /// Error parsing the protocol data.
    Parse(Perror),
}

impl<Serror: std::error::Error, Perror: std::error::Error> fmt::Display for PgParseError<Serror, Perror> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PgParseError::Stream(e) => write!(f, "stream error: {}", e),
            PgParseError::Parse(e) => write!(f, "parse error: {}", e),
        }
    }
}

impl<Serror: std::error::Error + 'static, Perror: std::error::Error + 'static> std::error::Error for PgParseError<Serror, Perror> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PgParseError::Stream(e) => Some(e),
            PgParseError::Parse(e) => Some(e),
        }
    }
}

impl<Serror: std::error::Error, Perror: std::error::Error> PgParseError<Serror, Perror> {
    /// Returns true if this is a stream error.
    pub fn is_stream(&self) -> bool {
        matches!(self, PgParseError::Stream(_))
    }

    /// Returns true if this is a parse error.
    pub fn is_parse(&self) -> bool {
        matches!(self, PgParseError::Parse(_))
    }

    /// Map the stream error type.
    pub fn map_stream<F, S2: std::error::Error>(self, f: F) -> PgParseError<S2, Perror>
    where
        F: FnOnce(Serror) -> S2,
    {
        match self {
            PgParseError::Stream(e) => PgParseError::Stream(f(e)),
            PgParseError::Parse(e) => PgParseError::Parse(e),
        }
    }

    /// Map the parse error type.
    pub fn map_parse<F, P2: std::error::Error>(self, f: F) -> PgParseError<Serror, P2>
    where
        F: FnOnce(Perror) -> P2,
    {
        match self {
            PgParseError::Stream(e) => PgParseError::Stream(e),
            PgParseError::Parse(e) => PgParseError::Parse(f(e)),
        }
    }
}

/// Synchronous parsing trait for PostgreSQL messages.
///
/// Implement this trait for types that can be parsed from a complete buffer.
pub trait PgParseSync<S: WireReadSync + ?Sized> {
    /// The specific parse error type for this message.
    type ParseError: std::error::Error;

    /// The parsed value type (may borrow from the stream).
    type Value<'s>
    where
        S: 's;

    /// Parse a value from the stream synchronously.
    ///
    /// This is used when the entire message is already available in a buffer.
    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}

/// Asynchronous parsing trait for PostgreSQL messages.
///
/// Implement this trait for types that can be parsed from a streaming source.
/// This extends `PgParseSync` with async capabilities.
pub trait PgParse<S: WireRead + ?Sized>: PgParseSync<S> {
    /// Parse a value from the stream asynchronously.
    ///
    /// This is used when data may need to be read incrementally.
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's;
}
