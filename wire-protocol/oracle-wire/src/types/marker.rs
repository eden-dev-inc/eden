//! TNS Marker packet type (TNS v11+).
//!
//! Marker packets are used for break/reset signaling.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync};

/// Marker types.
pub mod marker_types {
    /// Break marker - interrupt current operation.
    pub const BREAK: u8 = 1;
    /// Reset marker - reset connection state.
    pub const RESET: u8 = 2;
    /// Interrupt marker.
    pub const INTERRUPT: u8 = 3;
}

/// TNS Marker packet.
///
/// Used for out-of-band signaling, primarily for cancellation
/// and reset operations. Introduced in TNS v11.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Marker {
    /// Type of marker.
    pub marker_type: u8,
    /// Marker data (interpretation depends on marker_type).
    pub data: u8,
}

/// Error when parsing a Marker packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum MarkerError {
    #[error("invalid marker type: {0}")]
    InvalidType(u8),
}

impl Marker {
    /// Create a break marker.
    pub fn break_marker() -> Self {
        Self { marker_type: marker_types::BREAK, data: 0 }
    }

    /// Create a reset marker.
    pub fn reset_marker() -> Self {
        Self { marker_type: marker_types::RESET, data: 0 }
    }

    /// Check if this is a break marker.
    pub fn is_break(&self) -> bool {
        self.marker_type == marker_types::BREAK
    }

    /// Check if this is a reset marker.
    pub fn is_reset(&self) -> bool {
        self.marker_type == marker_types::RESET
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Marker {
    type ParseError = MarkerError;
    type Value<'s>
        = Marker
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let marker_type = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let data = stream.read_u8_sync().map_err(OracleParseError::Stream)?;

        Ok(Marker { marker_type, data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Marker {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let marker_type = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let data = stream.read_u8().await.map_err(OracleParseError::Stream)?;

        Ok(Marker { marker_type, data })
    }
}
