//! MySQL EOF packet.
//!
//! EOF packets are deprecated since MySQL 5.7.5 when the DEPRECATE_EOF
//! capability is used. In that case, OK packets are sent instead.

use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// EOF packet response.
///
/// Sent by the server to mark the end of a result set or field list.
/// Deprecated when DEPRECATE_EOF capability is used.
///
/// The packet header byte is 0xFE.
#[derive(Clone, Copy, Debug)]
pub struct EofPacket {
    /// Number of warnings.
    pub warnings: u16,
    /// Server status flags.
    pub status_flags: u16,
}

impl EofPacket {
    /// Create a new EOF packet.
    pub fn new(warnings: u16, status_flags: u16) -> Self {
        Self { warnings, status_flags }
    }

    /// Check if more results follow (multi-result set).
    #[inline]
    pub fn has_more_results(&self) -> bool {
        self.status_flags & crate::error::status_flags::SERVER_MORE_RESULTS_EXISTS != 0
    }

    /// Check if in a transaction.
    #[inline]
    pub fn in_transaction(&self) -> bool {
        self.status_flags & crate::error::status_flags::SERVER_STATUS_IN_TRANS != 0
    }

    /// Check if autocommit is enabled.
    #[inline]
    pub fn autocommit(&self) -> bool {
        self.status_flags & crate::error::status_flags::SERVER_STATUS_AUTOCOMMIT != 0
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum EofPacketError {
    #[error("invalid EOF packet header: expected 0xFE, got {0:#04X}")]
    InvalidHeader(u8),
    #[error("EOF packet payload too short")]
    TooShort,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for EofPacket {
    type ParseError = EofPacketError;
    type Value<'s>
        = EofPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Header (0xFE)
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != packet_types::EOF {
            return Err(MysqlParseError::Parse(EofPacketError::InvalidHeader(header)));
        }

        // Warnings (2 bytes LE)
        let warnings = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Status flags (2 bytes LE)
        let status_flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        Ok(EofPacket { warnings, status_flags })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for EofPacket {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// Check if a packet is an EOF packet.
///
/// An EOF packet has:
/// - Header byte 0xFE
/// - Payload length < 9 bytes (to distinguish from length-encoded strings starting with 0xFE)
#[inline]
pub fn is_eof_packet(header_byte: u8, payload_length: usize) -> bool {
    header_byte == packet_types::EOF && payload_length < 9
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_eof_packet() {
        let data = [
            0xFE, // EOF header
            0x00, 0x00, // warnings = 0
            0x02, 0x00, // status_flags = 2 (AUTOCOMMIT)
        ];
        let stream = SliceStream::new(&data);

        let eof = EofPacket::parse_sync(&stream).unwrap();

        assert_eq!(eof.warnings, 0);
        assert!(eof.autocommit());
    }

    #[test]
    fn test_eof_packet_with_warnings() {
        let data = [
            0xFE, // EOF header
            0x05, 0x00, // warnings = 5
            0x01, 0x00, // status_flags = 1 (IN_TRANS)
        ];
        let stream = SliceStream::new(&data);

        let eof = EofPacket::parse_sync(&stream).unwrap();

        assert_eq!(eof.warnings, 5);
        assert!(eof.in_transaction());
    }

    #[test]
    fn test_eof_packet_invalid_header() {
        let data = [0x00, 0x00, 0x00, 0x00, 0x00];
        let stream = SliceStream::new(&data);

        let result = EofPacket::parse_sync(&stream);
        assert!(matches!(result, Err(MysqlParseError::Parse(EofPacketError::InvalidHeader(0x00)))));
    }

    #[test]
    fn test_is_eof_packet() {
        // Valid EOF: header 0xFE, length < 9
        assert!(is_eof_packet(0xFE, 5));
        assert!(is_eof_packet(0xFE, 0));

        // Not EOF: wrong header
        assert!(!is_eof_packet(0x00, 5));

        // Not EOF: length too large (would be length-encoded int)
        assert!(!is_eof_packet(0xFE, 10));
    }
}
