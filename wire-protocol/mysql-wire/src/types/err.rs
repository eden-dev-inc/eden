//! MySQL ERR packet.
//!
//! Sent by the server to indicate an error.

use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// ERR packet response.
///
/// Sent by the server to indicate an error. The packet header byte is 0xFF.
#[derive(Clone, Debug)]
pub struct ErrPacket {
    /// Error code.
    pub error_code: u16,
    /// SQL state (5 characters, e.g., "HY000").
    pub sql_state: String,
    /// Human-readable error message.
    pub error_message: String,
}

impl ErrPacket {
    /// Create a new ERR packet.
    pub fn new(error_code: u16, sql_state: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error_code,
            sql_state: sql_state.into(),
            error_message: message.into(),
        }
    }

    /// Check if this is a fatal error (connection should be closed).
    pub fn is_fatal(&self) -> bool {
        // Error codes 1000-1999 are typically server errors
        // Error codes 2000-2999 are client errors
        self.error_code >= 2000
    }
}

impl std::fmt::Display for ErrPacket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MySQL Error {}: {} ({})", self.error_code, self.error_message, self.sql_state)
    }
}

impl std::error::Error for ErrPacket {}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ErrPacketError {
    #[error("invalid ERR packet header: expected 0xFF, got {0:#04X}")]
    InvalidHeader(u8),
    #[error("invalid SQL state marker")]
    InvalidSqlStateMarker,
    #[error("invalid error message encoding")]
    InvalidErrorMessage,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for ErrPacket {
    type ParseError = ErrPacketError;
    type Value<'s>
        = ErrPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Header (0xFF)
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != packet_types::ERR {
            return Err(MysqlParseError::Parse(ErrPacketError::InvalidHeader(header)));
        }

        // Error code (2 bytes LE)
        let error_code = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // SQL state marker '#' (if Protocol 4.1)
        // We try to read the marker; if it's '#', we have sql_state
        let first_byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        let (sql_state, error_message) = if first_byte == b'#' {
            // Read 5-character SQL state
            let mut state_bytes = [0u8; 5];
            for byte in &mut state_bytes {
                *byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
            }
            let sql_state = String::from_utf8_lossy(&state_bytes).into_owned();

            // Read error message (rest of packet)
            let mut msg_bytes = Vec::new();
            while let Ok(byte) = stream.read_u8_sync() {
                msg_bytes.push(byte);
            }
            let error_message = String::from_utf8_lossy(&msg_bytes).into_owned();

            (sql_state, error_message)
        } else {
            // Old protocol without SQL state - first_byte is start of error message
            let mut msg_bytes = vec![first_byte];
            while let Ok(byte) = stream.read_u8_sync() {
                msg_bytes.push(byte);
            }
            let error_message = String::from_utf8_lossy(&msg_bytes).into_owned();

            ("HY000".to_string(), error_message)
        };

        Ok(ErrPacket { error_code, sql_state, error_message })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for ErrPacket {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_err_packet_with_state() {
        // ERR packet with SQL state
        let mut data = Vec::new();
        data.push(0xFF); // ERR header
        data.extend_from_slice(&1045u16.to_le_bytes()); // Error code
        data.push(b'#'); // SQL state marker
        data.extend_from_slice(b"28000"); // SQL state
        data.extend_from_slice(b"Access denied"); // Error message

        let stream = SliceStream::new(&data);
        let err = ErrPacket::parse_sync(&stream).unwrap();

        assert_eq!(err.error_code, 1045);
        assert_eq!(err.sql_state, "28000");
        assert_eq!(err.error_message, "Access denied");
    }

    #[test]
    fn test_err_packet_without_state() {
        // ERR packet without SQL state (old protocol)
        let mut data = Vec::new();
        data.push(0xFF); // ERR header
        data.extend_from_slice(&1064u16.to_le_bytes()); // Error code
        data.extend_from_slice(b"Syntax error"); // Error message (no # prefix)

        let stream = SliceStream::new(&data);
        let err = ErrPacket::parse_sync(&stream).unwrap();

        assert_eq!(err.error_code, 1064);
        assert_eq!(err.sql_state, "HY000"); // Default state
    }

    #[test]
    fn test_err_packet_invalid_header() {
        let data = [0x00]; // Wrong header
        let stream = SliceStream::new(&data);

        let result = ErrPacket::parse_sync(&stream);
        assert!(matches!(result, Err(MysqlParseError::Parse(ErrPacketError::InvalidHeader(0x00)))));
    }

    #[test]
    fn test_err_packet_display() {
        let err = ErrPacket::new(1045, "28000", "Access denied");
        let display = err.to_string();
        assert!(display.contains("1045"));
        assert!(display.contains("Access denied"));
        assert!(display.contains("28000"));
    }
}
