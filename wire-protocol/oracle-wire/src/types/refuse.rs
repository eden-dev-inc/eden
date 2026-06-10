//! TNS Refuse packet type.
//!
//! The Refuse packet is sent by the server to reject a connection request.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Refuse packet.
///
/// Sent by the server when a connection request cannot be fulfilled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Refuse {
    /// Reason for refusal (user-visible).
    pub reason_user: u8,
    /// Reason for refusal (system-level).
    pub reason_system: u8,
    /// Length of refusal data.
    pub data_length: u16,
    /// Refusal data/message.
    pub data: Vec<u8>,
}

/// Known refusal reason codes.
pub mod reasons {
    // User-level reasons
    /// No listener on connect descriptor.
    pub const NO_LISTENER: u8 = 0;
    /// Redirect required.
    pub const REDIRECT: u8 = 1;
    /// Invalid service name.
    pub const INVALID_SERVICE: u8 = 2;
    /// Authentication failure.
    pub const AUTH_FAILURE: u8 = 3;
    /// Connection refused.
    pub const REFUSED: u8 = 4;

    // System-level reasons
    /// Generic system error.
    pub const SYSTEM_ERROR: u8 = 0;
    /// Resource unavailable.
    pub const RESOURCE_UNAVAILABLE: u8 = 1;
    /// Protocol version mismatch.
    pub const VERSION_MISMATCH: u8 = 2;
}

/// Error when parsing a Refuse packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RefuseError {
    #[error("refuse data extends beyond packet")]
    DataBeyondPacket,
}

impl Refuse {
    /// Returns the refusal data as a UTF-8 string if valid.
    pub fn data_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.data).ok()
    }

    /// Get human-readable description of user reason.
    pub fn user_reason_description(&self) -> &'static str {
        match self.reason_user {
            reasons::NO_LISTENER => "No listener",
            reasons::REDIRECT => "Redirect required",
            reasons::INVALID_SERVICE => "Invalid service name",
            reasons::AUTH_FAILURE => "Authentication failure",
            reasons::REFUSED => "Connection refused",
            _ => "Unknown reason",
        }
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Refuse {
    type ParseError = RefuseError;
    type Value<'s>
        = Refuse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let reason_user = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let reason_system = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        let data = stream.read_bytes_sync(data_length as usize).map_err(OracleParseError::Stream)?.to_vec();

        Ok(Refuse { reason_user, reason_system, data_length, data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Refuse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let reason_user = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let reason_system = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        let data = stream.read_bytes(data_length as usize).await.map_err(OracleParseError::Stream)?.to_vec();

        Ok(Refuse { reason_user, reason_system, data_length, data })
    }
}
