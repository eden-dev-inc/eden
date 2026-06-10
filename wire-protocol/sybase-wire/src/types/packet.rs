//! TDS packet header and base types.

use crate::error::{SybaseWireError, packet_types, status_flags};
use crate::limits::HEADER_SIZE;
use crate::parse::{SybaseParse, SybaseParseError, SybaseParseSync};
use crate::sybase_ext::{SybaseRead, SybaseReadSync};
use wire_stream::{WireRead, WireReadSync};

/// TDS packet type enum.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    /// SQL batch / language command (TDS 4.2)
    Query = packet_types::QUERY,
    /// Login packet (TDS 4.2)
    Login = packet_types::LOGIN,
    /// Remote procedure call
    Rpc = packet_types::RPC,
    /// Server response packet
    Reply = packet_types::REPLY,
    /// Cancel / attention signal
    Cancel = packet_types::CANCEL,
    /// Bulk load data
    Bulk = packet_types::BULK,
    /// TDS 5.0 language command
    Query5 = packet_types::QUERY5,
    /// TDS 5.0 login packet
    Login5 = packet_types::LOGIN5,
    /// Pre-login packet (TDS 7.0+)
    PreLogin = packet_types::PRELOGIN,
    /// SSPI authentication
    Sspi = packet_types::SSPI,
    /// Transaction manager request
    TransMgr = packet_types::TRANS_MGR,
}

impl PacketType {
    /// Try to create a PacketType from a raw byte value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            packet_types::QUERY => Some(Self::Query),
            packet_types::LOGIN => Some(Self::Login),
            packet_types::RPC => Some(Self::Rpc),
            packet_types::REPLY => Some(Self::Reply),
            packet_types::CANCEL => Some(Self::Cancel),
            packet_types::BULK => Some(Self::Bulk),
            packet_types::QUERY5 => Some(Self::Query5),
            packet_types::LOGIN5 => Some(Self::Login5),
            packet_types::PRELOGIN => Some(Self::PreLogin),
            packet_types::SSPI => Some(Self::Sspi),
            packet_types::TRANS_MGR => Some(Self::TransMgr),
            _ => None,
        }
    }

    /// Get the raw byte value.
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }
}

/// TDS packet status flags.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct Status(pub u8);

impl Status {
    /// Create a new status with the given flags.
    pub fn new(flags: u8) -> Self {
        Self(flags)
    }

    /// Create a normal status (more packets to follow).
    pub fn normal() -> Self {
        Self(status_flags::NORMAL)
    }

    /// Create an end-of-message status.
    pub fn eom() -> Self {
        Self(status_flags::EOM)
    }

    /// Check if this is the last packet in the message.
    pub fn is_eom(&self) -> bool {
        self.0 & status_flags::EOM != 0
    }

    /// Check if this packet should be ignored.
    pub fn is_ignore(&self) -> bool {
        self.0 & status_flags::IGNORE != 0
    }

    /// Check if this is a reset connection request.
    pub fn is_reset_connection(&self) -> bool {
        self.0 & status_flags::RESET_CONNECTION != 0
    }

    /// Get the raw status byte.
    pub fn raw(&self) -> u8 {
        self.0
    }
}

/// TDS packet header (8 bytes).
///
/// The header format is:
/// - 1 byte: packet type
/// - 1 byte: status flags
/// - 2 bytes: total packet length (big-endian, includes header)
/// - 2 bytes: SPID/channel (big-endian)
/// - 1 byte: packet number (for multi-packet messages)
/// - 1 byte: window (unused, always 0)
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct TdsHeader {
    /// Packet type identifier.
    pub packet_type: PacketType,
    /// Status flags.
    pub status: u8,
    /// Total packet length including header.
    pub length: u16,
    /// SPID / channel identifier.
    pub spid: u16,
    /// Packet number for multi-packet messages.
    pub packet_number: u8,
    /// Window (unused, always 0).
    pub window: u8,
}

impl TdsHeader {
    /// Create a new header with the given packet type and data length.
    pub fn new(packet_type: PacketType, data_length: usize) -> Self {
        Self {
            packet_type,
            status: status_flags::EOM,
            length: (HEADER_SIZE + data_length) as u16,
            spid: 0,
            packet_number: 1,
            window: 0,
        }
    }

    /// Get the payload length (total length minus header).
    pub fn payload_length(&self) -> usize {
        self.length.saturating_sub(HEADER_SIZE as u16) as usize
    }

    /// Check if this is the last packet in the message.
    pub fn is_eom(&self) -> bool {
        self.status & status_flags::EOM != 0
    }

    /// Encode the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let length_bytes = self.length.to_be_bytes();
        let spid_bytes = self.spid.to_be_bytes();
        [
            self.packet_type.as_u8(),
            self.status,
            length_bytes[0],
            length_bytes[1],
            spid_bytes[0],
            spid_bytes[1],
            self.packet_number,
            self.window,
        ]
    }
}

impl<S: WireReadSync + ?Sized> SybaseParseSync<S> for TdsHeader {
    type ParseError = SybaseWireError;
    type Value<'s>
        = TdsHeader
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let packet_type_byte = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let length = stream.read_u16_be_sync().map_err(SybaseParseError::Stream)?;
        let spid = stream.read_u16_be_sync().map_err(SybaseParseError::Stream)?;
        let packet_number = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let window = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        let packet_type =
            PacketType::from_u8(packet_type_byte).ok_or(SybaseParseError::Parse(SybaseWireError::InvalidPacketType(packet_type_byte)))?;

        Ok(TdsHeader { packet_type, status, length, spid, packet_number, window })
    }
}

impl<S: WireRead + ?Sized> SybaseParse<S> for TdsHeader {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let packet_type_byte = stream.read_u8().await.map_err(SybaseParseError::Stream)?;
        let status = stream.read_u8().await.map_err(SybaseParseError::Stream)?;
        let length = stream.read_u16_be().await.map_err(SybaseParseError::Stream)?;
        let spid = stream.read_u16_be().await.map_err(SybaseParseError::Stream)?;
        let packet_number = stream.read_u8().await.map_err(SybaseParseError::Stream)?;
        let window = stream.read_u8().await.map_err(SybaseParseError::Stream)?;

        let packet_type =
            PacketType::from_u8(packet_type_byte).ok_or(SybaseParseError::Parse(SybaseWireError::InvalidPacketType(packet_type_byte)))?;

        Ok(TdsHeader { packet_type, status, length, spid, packet_number, window })
    }
}

/// A complete TDS packet with header and payload.
#[derive(Clone, Debug)]
pub struct TdsPacket {
    /// Packet header.
    pub header: TdsHeader,
    /// Packet payload (excluding header).
    pub payload: Vec<u8>,
}

impl TdsPacket {
    /// Create a new packet with the given header and payload.
    pub fn new(header: TdsHeader, payload: Vec<u8>) -> Self {
        Self { header, payload }
    }

    /// Get the packet type.
    pub fn packet_type(&self) -> PacketType {
        self.header.packet_type
    }

    /// Check if this is the last packet in the message.
    pub fn is_eom(&self) -> bool {
        self.header.is_eom()
    }

    /// Encode the packet to bytes.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(HEADER_SIZE + self.payload.len());
        bytes.extend_from_slice(&self.header.to_bytes());
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}

impl<S: WireReadSync + ?Sized> SybaseParseSync<S> for TdsPacket {
    type ParseError = SybaseWireError;
    type Value<'s>
        = TdsPacket
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let header = TdsHeader::parse_sync(stream)?;
        let payload_len = header.payload_length();

        let payload = if payload_len > 0 {
            let borrow = stream.peek(Some(payload_len)).map_err(SybaseParseError::Stream)?;
            let data = borrow[..payload_len].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            data
        } else {
            Vec::new()
        };

        Ok(TdsPacket { header, payload })
    }
}

impl<S: WireRead + ?Sized> SybaseParse<S> for TdsPacket {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let header = TdsHeader::parse(stream).await?;
        let payload_len = header.payload_length();

        let payload = if payload_len > 0 {
            let borrow = stream.peek_read(Some(payload_len)).await.map_err(SybaseParseError::Stream)?;
            let data = borrow[..payload_len].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            data
        } else {
            Vec::new()
        };

        Ok(TdsPacket { header, payload })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_packet_type_from_u8() {
        assert_eq!(PacketType::from_u8(0x01), Some(PacketType::Query));
        assert_eq!(PacketType::from_u8(0x02), Some(PacketType::Login));
        assert_eq!(PacketType::from_u8(0x04), Some(PacketType::Reply));
        assert_eq!(PacketType::from_u8(0xFF), None);
    }

    #[test]
    fn test_status_flags() {
        let normal = Status::normal();
        assert!(!normal.is_eom());

        let eom = Status::eom();
        assert!(eom.is_eom());
    }

    #[test]
    fn test_header_to_bytes() {
        let header = TdsHeader::new(PacketType::Query, 4);
        let bytes = header.to_bytes();

        assert_eq!(bytes[0], packet_types::QUERY);
        assert_eq!(bytes[1], status_flags::EOM);
        assert_eq!(&bytes[2..4], &[0x00, 0x0C]); // 12 = 8 + 4
    }

    #[test]
    fn test_header_parse() {
        let data = [
            packet_types::QUERY, // type
            status_flags::EOM,   // status
            0x00,
            0x10, // length = 16
            0x00,
            0x00, // spid = 0
            0x01, // packet number
            0x00, // window
        ];

        let stream = SliceStream::new(&data);
        let header = TdsHeader::parse_sync(&stream).unwrap();

        assert_eq!(header.packet_type, PacketType::Query);
        assert!(header.is_eom());
        assert_eq!(header.length, 16);
        assert_eq!(header.payload_length(), 8);
    }

    #[test]
    fn test_packet_parse() {
        let data = [
            packet_types::QUERY, // type
            status_flags::EOM,   // status
            0x00,
            0x0C, // length = 12
            0x00,
            0x00, // spid = 0
            0x01, // packet number
            0x00, // window
            b'T',
            b'E',
            b'S',
            b'T', // payload
        ];

        let stream = SliceStream::new(&data);
        let packet = TdsPacket::parse_sync(&stream).unwrap();

        assert_eq!(packet.packet_type(), PacketType::Query);
        assert!(packet.is_eom());
        assert_eq!(packet.payload, b"TEST");
    }
}
