//! TNS Abort packet type.
//!
//! Abort packets signal immediate connection termination. They are sent
//! when a fatal error occurs or when a connection needs to be forcefully
//! closed.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// Abort reason codes.
pub mod abort_reasons {
    /// Normal abort (clean shutdown).
    pub const NORMAL: u16 = 0x0000;
    /// Protocol error.
    pub const PROTOCOL_ERROR: u16 = 0x0001;
    /// Timeout.
    pub const TIMEOUT: u16 = 0x0002;
    /// Resource unavailable.
    pub const RESOURCE_UNAVAILABLE: u16 = 0x0003;
    /// Authentication failure.
    pub const AUTH_FAILURE: u16 = 0x0004;
    /// Server shutdown.
    pub const SERVER_SHUTDOWN: u16 = 0x0005;
    /// Client request.
    pub const CLIENT_REQUEST: u16 = 0x0006;
    /// Fatal error.
    pub const FATAL_ERROR: u16 = 0x00FF;
}

/// TNS Abort packet.
///
/// Signals immediate connection termination. After receiving an Abort packet,
/// the connection should be closed without further communication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Abort {
    /// Reason for the abort.
    pub reason: u16,
    /// Additional data (error message or diagnostic info).
    pub data: Vec<u8>,
}

/// Error when parsing an Abort packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AbortError {
    #[error("abort packet too short")]
    TooShort,
}

impl Abort {
    /// Create a new abort packet.
    pub fn new(reason: u16) -> Self {
        Self { reason, data: Vec::new() }
    }

    /// Create an abort with reason and data.
    pub fn with_data(reason: u16, data: Vec<u8>) -> Self {
        Self { reason, data }
    }

    /// Create a normal abort (clean shutdown).
    pub fn normal() -> Self {
        Self::new(abort_reasons::NORMAL)
    }

    /// Create a protocol error abort.
    pub fn protocol_error() -> Self {
        Self::new(abort_reasons::PROTOCOL_ERROR)
    }

    /// Create a timeout abort.
    pub fn timeout() -> Self {
        Self::new(abort_reasons::TIMEOUT)
    }

    /// Create a fatal error abort.
    pub fn fatal_error() -> Self {
        Self::new(abort_reasons::FATAL_ERROR)
    }

    /// Check if this is a normal (clean) abort.
    pub fn is_normal(&self) -> bool {
        self.reason == abort_reasons::NORMAL
    }

    /// Check if this is a fatal abort.
    pub fn is_fatal(&self) -> bool {
        self.reason == abort_reasons::FATAL_ERROR
    }

    /// Get reason name.
    pub fn reason_name(&self) -> &'static str {
        match self.reason {
            abort_reasons::NORMAL => "Normal",
            abort_reasons::PROTOCOL_ERROR => "ProtocolError",
            abort_reasons::TIMEOUT => "Timeout",
            abort_reasons::RESOURCE_UNAVAILABLE => "ResourceUnavailable",
            abort_reasons::AUTH_FAILURE => "AuthFailure",
            abort_reasons::SERVER_SHUTDOWN => "ServerShutdown",
            abort_reasons::CLIENT_REQUEST => "ClientRequest",
            abort_reasons::FATAL_ERROR => "FatalError",
            _ => "Unknown",
        }
    }

    /// Get the data as a string (if valid UTF-8).
    pub fn data_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.data).ok()
    }

    /// Encode to wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.data.len());
        bytes.extend_from_slice(&self.reason.to_be_bytes());
        bytes.extend_from_slice(&self.data);
        bytes
    }
}

impl Default for Abort {
    fn default() -> Self {
        Self::normal()
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Abort {
    type ParseError = AbortError;
    type Value<'s>
        = Abort
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let reason = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        Ok(Abort { reason, data: Vec::new() })
    }
}

impl Abort {
    /// Parse with a known length.
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Abort, OracleParseError<S::ReadError, AbortError>> {
        if length < 2 {
            return Err(OracleParseError::Parse(AbortError::TooShort));
        }

        let reason = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        let data = if length > 2 {
            stream.read_bytes_sync(length - 2).map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(Abort { reason, data })
    }

    /// Parse with a known length (async).
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Abort, OracleParseError<S::ReadError, AbortError>> {
        if length < 2 {
            return Err(OracleParseError::Parse(AbortError::TooShort));
        }

        let reason = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        let data = if length > 2 {
            stream.read_bytes(length - 2).await.map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(Abort { reason, data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Abort {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let reason = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        Ok(Abort { reason, data: Vec::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_abort_new() {
        let abort = Abort::new(0x0001);
        assert_eq!(abort.reason, 0x0001);
        assert!(abort.data.is_empty());
    }

    #[test]
    fn test_abort_normal() {
        let abort = Abort::normal();
        assert!(abort.is_normal());
        assert!(!abort.is_fatal());
        assert_eq!(abort.reason_name(), "Normal");
    }

    #[test]
    fn test_abort_fatal() {
        let abort = Abort::fatal_error();
        assert!(!abort.is_normal());
        assert!(abort.is_fatal());
        assert_eq!(abort.reason_name(), "FatalError");
    }

    #[test]
    fn test_abort_with_data() {
        let abort = Abort::with_data(abort_reasons::PROTOCOL_ERROR, b"bad packet".to_vec());
        assert_eq!(abort.reason, abort_reasons::PROTOCOL_ERROR);
        assert_eq!(abort.data_str(), Some("bad packet"));
    }

    #[test]
    fn test_abort_to_bytes() {
        let abort = Abort::with_data(0x0102, vec![0x03, 0x04]);
        assert_eq!(abort.to_bytes(), vec![0x01, 0x02, 0x03, 0x04]);
    }

    #[test]
    fn test_abort_reason_names() {
        assert_eq!(Abort::normal().reason_name(), "Normal");
        assert_eq!(Abort::protocol_error().reason_name(), "ProtocolError");
        assert_eq!(Abort::timeout().reason_name(), "Timeout");
        assert_eq!(Abort::new(abort_reasons::SERVER_SHUTDOWN).reason_name(), "ServerShutdown");
    }
}
