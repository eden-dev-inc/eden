//! MySQL packet header parsing.
//!
//! Every MySQL packet has a 4-byte header:
//! - 3 bytes: payload length (little-endian, max 0xFFFFFF)
//! - 1 byte: sequence ID

use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// MySQL packet header size in bytes.
pub const HEADER_SIZE: usize = 4;

/// Maximum MySQL packet payload size (16MB - 1).
pub const MAX_PAYLOAD_SIZE: u32 = 0xFFFFFF;

/// MySQL packet header.
///
/// Every MySQL packet has a 4-byte header:
/// - 3 bytes: payload length (little-endian, max 0xFFFFFF)
/// - 1 byte: sequence ID
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MysqlPacketHeader {
    /// Payload length (not including header).
    pub payload_length: u32,
    /// Sequence ID for packet ordering.
    pub sequence_id: u8,
}

impl MysqlPacketHeader {
    /// Create a new packet header.
    pub fn new(payload_length: u32, sequence_id: u8) -> Self {
        Self {
            payload_length: payload_length.min(MAX_PAYLOAD_SIZE),
            sequence_id,
        }
    }

    /// Check if this is a complete packet (payload < max size).
    ///
    /// If payload_length == MAX_PAYLOAD_SIZE, more packets follow
    /// as part of the same logical message.
    #[inline]
    pub fn is_complete(&self) -> bool {
        self.payload_length < MAX_PAYLOAD_SIZE
    }

    /// Check if more packets follow (payload == max size).
    #[inline]
    pub fn has_more(&self) -> bool {
        self.payload_length == MAX_PAYLOAD_SIZE
    }

    /// Encode the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let len_bytes = self.payload_length.to_le_bytes();
        [len_bytes[0], len_bytes[1], len_bytes[2], self.sequence_id]
    }

    /// Decode a header from bytes.
    pub fn from_bytes(bytes: &[u8; HEADER_SIZE]) -> Self {
        let payload_length = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        Self { payload_length, sequence_id: bytes[3] }
    }
}

/// Error when parsing a MySQL packet header.
#[derive(Clone, Debug, thiserror::Error)]
pub enum PacketHeaderError {
    #[error("payload length {0} exceeds maximum {MAX_PAYLOAD_SIZE}")]
    PayloadTooLarge(u32),
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for MysqlPacketHeader {
    type ParseError = PacketHeaderError;
    type Value<'s>
        = MysqlPacketHeader
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let (payload_length, sequence_id) = stream.read_packet_header_sync().map_err(MysqlParseError::Stream)?;

        // Note: We don't error on MAX_PAYLOAD_SIZE since it's valid (indicates more packets)
        if payload_length > MAX_PAYLOAD_SIZE {
            return Err(MysqlParseError::Parse(PacketHeaderError::PayloadTooLarge(payload_length)));
        }

        Ok(MysqlPacketHeader { payload_length, sequence_id })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for MysqlPacketHeader {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // For SliceStream, we can use the sync version
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_header_parse() {
        // 4-byte header: payload_length (3 LE) + sequence_id (1)
        let data = [0x05, 0x00, 0x00, 0x00]; // length=5, seq=0
        let stream = SliceStream::new(&data);

        let header = MysqlPacketHeader::parse_sync(&stream).unwrap();
        assert_eq!(header.payload_length, 5);
        assert_eq!(header.sequence_id, 0);
        assert!(header.is_complete());
    }

    #[test]
    fn test_header_max_size() {
        let data = [0xFF, 0xFF, 0xFF, 0x01]; // length=0xFFFFFF (max), seq=1
        let stream = SliceStream::new(&data);

        let header = MysqlPacketHeader::parse_sync(&stream).unwrap();
        assert_eq!(header.payload_length, 0xFFFFFF);
        assert!(!header.is_complete()); // More packets follow
        assert!(header.has_more());
    }

    #[test]
    fn test_header_roundtrip() {
        let original = MysqlPacketHeader::new(1234, 5);
        let bytes = original.to_bytes();
        let parsed = MysqlPacketHeader::from_bytes(&bytes);

        assert_eq!(parsed.payload_length, 1234);
        assert_eq!(parsed.sequence_id, 5);
    }

    #[test]
    fn test_header_new_clamps() {
        // Creating with > MAX_PAYLOAD_SIZE should clamp
        let header = MysqlPacketHeader::new(0x1000000, 0);
        assert_eq!(header.payload_length, MAX_PAYLOAD_SIZE);
    }

    #[test]
    fn test_header_to_bytes() {
        let header = MysqlPacketHeader::new(0x123456, 7);
        let bytes = header.to_bytes();
        assert_eq!(bytes, [0x56, 0x34, 0x12, 0x07]);
    }
}
