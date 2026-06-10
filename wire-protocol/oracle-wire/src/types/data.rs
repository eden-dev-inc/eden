//! TNS Data packet type.
//!
//! Data packets carry the actual protocol data between client and server.

use crate::error::data_flags;
use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Data packet.
///
/// Data packets are used for all protocol communication after connection
/// establishment. The structure is consistent across TNS versions.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Data {
    /// Data flags indicating packet properties.
    pub flags: DataFlags,
    /// The actual data payload.
    pub payload: Vec<u8>,
}

/// Data packet flags.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct DataFlags {
    /// Raw flags value.
    raw: u16,
}

impl DataFlags {
    /// Create from raw u16 value.
    pub fn from_raw(raw: u16) -> Self {
        Self { raw }
    }

    /// Get the raw flags value.
    pub fn raw(&self) -> u16 {
        self.raw
    }

    /// Check if SEND_TOKEN flag is set.
    pub fn send_token(&self) -> bool {
        self.raw & data_flags::SEND_TOKEN != 0
    }

    /// Check if REQUEST_TO_SEND flag is set.
    pub fn request_to_send(&self) -> bool {
        self.raw & data_flags::REQUEST_TO_SEND != 0
    }

    /// Check if EOF flag is set.
    pub fn eof(&self) -> bool {
        self.raw & data_flags::EOF != 0
    }

    /// Check if MORE_DATA flag is set.
    pub fn more_data(&self) -> bool {
        self.raw & data_flags::MORE_DATA != 0
    }

    /// Check if RESET flag is set.
    pub fn reset(&self) -> bool {
        self.raw & data_flags::RESET != 0
    }
}

/// Error when parsing a Data packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum DataError {
    #[error("data packet too short")]
    TooShort,
}

impl Data {
    /// Create a new data packet.
    pub fn new(payload: Vec<u8>) -> Self {
        Self { flags: DataFlags::default(), payload }
    }

    /// Create a new data packet with flags.
    pub fn with_flags(payload: Vec<u8>, flags: DataFlags) -> Self {
        Self { flags, payload }
    }

    /// Check if this is the final data packet in a sequence.
    pub fn is_final(&self) -> bool {
        self.flags.eof() || !self.flags.more_data()
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Data {
    type ParseError = DataError;
    type Value<'s>
        = Data
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_length_sync(stream, None)
    }
}

impl Data {
    /// Parse with a known remaining length (from header).
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        remaining_length: Option<usize>,
    ) -> Result<Data, OracleParseError<S::ReadError, DataError>> {
        let flags_raw = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let flags = DataFlags::from_raw(flags_raw);

        // Read remaining bytes as payload
        let payload = if let Some(len) = remaining_length {
            // Subtract 2 bytes for flags
            let payload_len = len.saturating_sub(2);
            stream.read_bytes_sync(payload_len).map_err(OracleParseError::Stream)?.to_vec()
        } else {
            // Read all available data
            Vec::new()
        };

        Ok(Data { flags, payload })
    }

    /// Parse with a known remaining length (from header) - async version.
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        remaining_length: Option<usize>,
    ) -> Result<Data, OracleParseError<S::ReadError, DataError>> {
        let flags_raw = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let flags = DataFlags::from_raw(flags_raw);

        // Read remaining bytes as payload
        let payload = if let Some(len) = remaining_length {
            // Subtract 2 bytes for flags
            let payload_len = len.saturating_sub(2);
            stream.read_bytes(payload_len).await.map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(Data { flags, payload })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Data {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_length(stream, None).await
    }
}
