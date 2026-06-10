//! Dynamic MySQL packet type.
//!
//! Can represent any MySQL packet type for generic packet handling.

use crate::capabilities::CapabilityFlags;
use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use crate::types::eof::is_eof_packet;
use crate::types::{EofPacket, ErrPacket, OkPacket};
use wire_stream::{WireRead, WireReadSync};

/// A dynamically-typed MySQL packet.
#[derive(Clone, Debug)]
pub enum MysqlPacket {
    /// OK response.
    Ok(OkPacket),
    /// Error response.
    Err(ErrPacket),
    /// EOF packet (deprecated but still used).
    Eof(EofPacket),
    /// Local infile request.
    LocalInfile { filename: String },
    /// ResultSet (first packet is column count).
    ResultSetStart { column_count: u64 },
    /// Unknown/raw packet data.
    Unknown { header: u8, data: Vec<u8> },
}

impl MysqlPacket {
    /// Check if this is an OK packet.
    pub fn is_ok(&self) -> bool {
        matches!(self, MysqlPacket::Ok(_))
    }

    /// Check if this is an error packet.
    pub fn is_err(&self) -> bool {
        matches!(self, MysqlPacket::Err(_))
    }

    /// Check if this is an EOF packet.
    pub fn is_eof(&self) -> bool {
        matches!(self, MysqlPacket::Eof(_))
    }

    /// Check if this is a result set start.
    pub fn is_result_set(&self) -> bool {
        matches!(self, MysqlPacket::ResultSetStart { .. })
    }

    /// Get the OK packet if this is one.
    pub fn as_ok(&self) -> Option<&OkPacket> {
        match self {
            MysqlPacket::Ok(ok) => Some(ok),
            _ => None,
        }
    }

    /// Get the error packet if this is one.
    pub fn as_err(&self) -> Option<&ErrPacket> {
        match self {
            MysqlPacket::Err(err) => Some(err),
            _ => None,
        }
    }

    /// Get the EOF packet if this is one.
    pub fn as_eof(&self) -> Option<&EofPacket> {
        match self {
            MysqlPacket::Eof(eof) => Some(eof),
            _ => None,
        }
    }

    /// Convert error to Result.
    pub fn into_result(self) -> Result<Self, ErrPacket> {
        match self {
            MysqlPacket::Err(err) => Err(err),
            other => Ok(other),
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum DynamicParseError {
    #[error("OK packet error: {0}")]
    Ok(#[from] crate::types::ok::OkPacketError),
    #[error("ERR packet error: {0}")]
    Err(#[from] crate::types::err::ErrPacketError),
    #[error("EOF packet error: {0}")]
    Eof(#[from] crate::types::eof::EofPacketError),
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
}

impl MysqlPacket {
    /// Parse a server response packet with given capabilities.
    ///
    /// The `payload_length` helps distinguish EOF packets from length-encoded integers.
    pub fn parse_response_with_capabilities_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        capabilities: CapabilityFlags,
        payload_length: usize,
    ) -> Result<Self, MysqlParseError<S::ReadError, DynamicParseError>> {
        // Peek at the first byte to determine packet type
        let first = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        match first {
            packet_types::OK => {
                // Need to re-read since OK packet starts with 0x00
                // For now, we construct manually
                let affected_rows = stream
                    .read_lenenc_int_sync()
                    .map_err(MysqlParseError::Stream)?
                    .map_err(|_| MysqlParseError::Parse(DynamicParseError::InvalidLenEnc))?;

                let last_insert_id = stream
                    .read_lenenc_int_sync()
                    .map_err(MysqlParseError::Stream)?
                    .map_err(|_| MysqlParseError::Parse(DynamicParseError::InvalidLenEnc))?;

                let (status_flags, warnings) = if capabilities.supports_41() {
                    let status = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                    let warns = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                    (status, warns)
                } else {
                    (0, 0)
                };

                Ok(MysqlPacket::Ok(OkPacket {
                    affected_rows,
                    last_insert_id,
                    status_flags,
                    warnings,
                    info: String::new(),
                    session_state_changes: None,
                }))
            }

            packet_types::ERR => {
                let error_code = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

                // Read SQL state if present
                let marker = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                let (sql_state, error_message) = if marker == b'#' {
                    let mut state = [0u8; 5];
                    for byte in &mut state {
                        *byte = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
                    }

                    let mut msg = Vec::new();
                    while let Ok(b) = stream.read_u8_sync() {
                        msg.push(b);
                    }

                    (String::from_utf8_lossy(&state).into_owned(), String::from_utf8_lossy(&msg).into_owned())
                } else {
                    let mut msg = vec![marker];
                    while let Ok(b) = stream.read_u8_sync() {
                        msg.push(b);
                    }
                    ("HY000".to_string(), String::from_utf8_lossy(&msg).into_owned())
                };

                Ok(MysqlPacket::Err(ErrPacket { error_code, sql_state, error_message }))
            }

            packet_types::EOF if is_eof_packet(first, payload_length) => {
                // This is an EOF packet, not a length-encoded int
                let warnings = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;
                let status_flags = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

                Ok(MysqlPacket::Eof(EofPacket { warnings, status_flags }))
            }

            packet_types::LOCAL_INFILE => {
                let mut filename_bytes = Vec::new();
                while let Ok(b) = stream.read_u8_sync() {
                    filename_bytes.push(b);
                }
                Ok(MysqlPacket::LocalInfile {
                    filename: String::from_utf8_lossy(&filename_bytes).into_owned(),
                })
            }

            _ => {
                // Likely a ResultSet (first byte is column count as lenenc int)
                // We already consumed the first byte, so we need to reconstruct
                let column_count = match first {
                    0..=0xFA => first as u64,
                    0xFC => stream.read_u16_le_sync().map_err(MysqlParseError::Stream)? as u64,
                    0xFD => stream.read_u24_le_sync().map_err(MysqlParseError::Stream)? as u64,
                    0xFE => stream.read_u64_le_sync().map_err(MysqlParseError::Stream)?,
                    _ => {
                        return Err(MysqlParseError::Parse(DynamicParseError::InvalidLenEnc));
                    }
                };

                Ok(MysqlPacket::ResultSetStart { column_count })
            }
        }
    }

    /// Parse a server response packet with default capabilities.
    pub fn parse_response_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        payload_length: usize,
    ) -> Result<Self, MysqlParseError<S::ReadError, DynamicParseError>> {
        Self::parse_response_with_capabilities_sync(stream, CapabilityFlags::client_default_8x(), payload_length)
    }
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for MysqlPacket {
    type ParseError = DynamicParseError;
    type Value<'s>
        = MysqlPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Without payload length context, assume large (not EOF)
        Self::parse_response_sync(stream, 100)
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for MysqlPacket {
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
    fn test_parse_ok_packet() {
        let data = [
            0x00, // OK header
            0x01, // affected_rows = 1
            0x00, // last_insert_id = 0
            0x00, 0x00, // status_flags
            0x00, 0x00, // warnings
        ];
        let stream = SliceStream::new(&data);

        let packet = MysqlPacket::parse_response_sync(&stream, data.len()).unwrap();

        assert!(packet.is_ok());
        let ok = packet.as_ok().unwrap();
        assert_eq!(ok.affected_rows, 1);
    }

    #[test]
    fn test_parse_err_packet() {
        let mut data = Vec::new();
        data.push(0xFF); // ERR header
        data.extend_from_slice(&1045u16.to_le_bytes());
        data.push(b'#');
        data.extend_from_slice(b"28000");
        data.extend_from_slice(b"Access denied");

        let stream = SliceStream::new(&data);
        let packet = MysqlPacket::parse_response_sync(&stream, data.len()).unwrap();

        assert!(packet.is_err());
        let err = packet.as_err().unwrap();
        assert_eq!(err.error_code, 1045);
    }

    #[test]
    fn test_parse_eof_packet() {
        let data = [
            0xFE, // EOF header
            0x00, 0x00, // warnings
            0x02, 0x00, // status_flags
        ];
        let stream = SliceStream::new(&data);

        let packet = MysqlPacket::parse_response_sync(&stream, 5).unwrap();

        assert!(packet.is_eof());
    }

    #[test]
    fn test_parse_result_set_start() {
        let data = [0x03]; // Column count = 3
        let stream = SliceStream::new(&data);

        let packet = MysqlPacket::parse_response_sync(&stream, 1).unwrap();

        match packet {
            MysqlPacket::ResultSetStart { column_count } => assert_eq!(column_count, 3),
            _ => panic!("Expected ResultSetStart"),
        }
    }

    #[test]
    fn test_into_result() {
        let ok = MysqlPacket::Ok(OkPacket {
            affected_rows: 1,
            last_insert_id: 0,
            status_flags: 0,
            warnings: 0,
            info: String::new(),
            session_state_changes: None,
        });
        assert!(ok.into_result().is_ok());

        let err = MysqlPacket::Err(ErrPacket::new(1045, "28000", "Access denied"));
        assert!(err.into_result().is_err());
    }
}
