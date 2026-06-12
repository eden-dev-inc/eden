//! MySQL OK packet.
//!
//! Sent by the server to indicate successful completion of a command.

use crate::capabilities::CapabilityFlags;
use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// OK packet response.
///
/// Sent by the server to indicate success. The packet header byte is 0x00,
/// or 0xFE when DEPRECATE_EOF capability is set.
#[derive(Clone, Debug)]
pub struct OkPacket {
    /// Number of affected rows.
    pub affected_rows: u64,
    /// Last insert ID (if AUTO_INCREMENT column was updated).
    pub last_insert_id: u64,
    /// Server status flags.
    pub status_flags: u16,
    /// Number of warnings.
    pub warnings: u16,
    /// Human-readable info message.
    pub info: String,
    /// Session state changes (if SESSION_TRACK capability).
    pub session_state_changes: Option<Vec<u8>>,
}

impl OkPacket {
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
pub enum OkPacketError {
    #[error("invalid OK packet header: expected 0x00 or 0xFE, got {0:#04X}")]
    InvalidHeader(u8),
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
    #[error("invalid info string")]
    InvalidInfo,
}

impl OkPacket {
    /// Parse an OK packet with given capabilities context.
    ///
    /// The `deprecate_eof` parameter should be true if the connection
    /// has DEPRECATE_EOF capability negotiated.
    pub fn parse_with_capabilities_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        capabilities: CapabilityFlags,
    ) -> Result<Self, MysqlParseError<S::ReadError, OkPacketError>> {
        // Header check (0x00 or 0xFE for EOF_DEPRECATED)
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != packet_types::OK && header != packet_types::EOF {
            return Err(MysqlParseError::Parse(OkPacketError::InvalidHeader(header)));
        }

        // Affected rows (length-encoded int)
        let affected_rows = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(OkPacketError::InvalidLenEnc))?;

        // Last insert ID (length-encoded int)
        let last_insert_id = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(OkPacketError::InvalidLenEnc))?;

        // Status flags and warnings (if PROTOCOL_41 or TRANSACTIONS)
        let (status_flags, warnings) =
            if capabilities.contains(CapabilityFlags::PROTOCOL_41) || capabilities.contains(CapabilityFlags::TRANSACTIONS) {
                let status = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                let warns = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                (status, warns)
            } else {
                (0, 0)
            };

        // Info string and session state - we'll read remaining bytes
        // For simplicity, we don't parse session state changes in detail
        let info = String::new();
        let session_state_changes = None;

        Ok(OkPacket {
            affected_rows,
            last_insert_id,
            status_flags,
            warnings,
            info,
            session_state_changes,
        })
    }
}

// Note: Standard MysqlParseSync requires capabilities context, so we provide
// a default implementation that assumes PROTOCOL_41
impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for OkPacket {
    type ParseError = OkPacketError;
    type Value<'s>
        = OkPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_capabilities_sync(stream, CapabilityFlags::client_default_8x())
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for OkPacket {
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
    fn test_ok_packet_simple() {
        // OK packet: header + affected_rows + last_insert_id + status + warnings
        let data = [
            0x00, // OK header
            0x01, // affected_rows = 1
            0x00, // last_insert_id = 0
            0x02, 0x00, // status_flags = 2 (AUTOCOMMIT)
            0x00, 0x00, // warnings = 0
        ];
        let stream = SliceStream::new(&data);

        let ok = OkPacket::parse_sync(&stream).unwrap();

        assert_eq!(ok.affected_rows, 1);
        assert_eq!(ok.last_insert_id, 0);
        assert!(ok.autocommit());
        assert_eq!(ok.warnings, 0);
    }

    #[test]
    fn test_ok_packet_with_insert_id() {
        let data = [
            0x00, // OK header
            0x01, // affected_rows = 1
            0x2A, // last_insert_id = 42
            0x00, 0x00, // status_flags
            0x00, 0x00, // warnings
        ];
        let stream = SliceStream::new(&data);

        let ok = OkPacket::parse_sync(&stream).unwrap();

        assert_eq!(ok.affected_rows, 1);
        assert_eq!(ok.last_insert_id, 42);
    }

    #[test]
    fn test_ok_packet_invalid_header() {
        let data = [0x01]; // Invalid header
        let stream = SliceStream::new(&data);

        let result = OkPacket::parse_sync(&stream);
        assert!(matches!(result, Err(MysqlParseError::Parse(OkPacketError::InvalidHeader(0x01)))));
    }
}
