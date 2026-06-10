//! TNS packet header and base types.

use crate::error::packet_types;
use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync};

/// TNS packet header size in bytes.
pub const HEADER_SIZE: usize = 8;

/// Maximum TNS packet size.
pub const MAX_PACKET_SIZE: usize = 32767;

/// TNS packet header.
///
/// The header is common across all TNS versions and consists of:
/// - 2 bytes: packet length (big-endian)
/// - 2 bytes: packet checksum
/// - 1 byte: packet type
/// - 1 byte: reserved/flags
/// - 2 bytes: header checksum
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TnsHeader {
    /// Total packet length including header.
    pub packet_length: u16,
    /// Packet data checksum (often 0).
    pub packet_checksum: u16,
    /// Packet type identifier.
    pub packet_type: PacketType,
    /// Reserved byte / flags.
    pub flags: u8,
    /// Header checksum (often 0).
    pub header_checksum: u16,
}

impl TnsHeader {
    /// Create a new TNS header.
    pub fn new(packet_type: PacketType, data_length: u16) -> Self {
        Self {
            packet_length: HEADER_SIZE as u16 + data_length,
            packet_checksum: 0,
            packet_type,
            flags: 0,
            header_checksum: 0,
        }
    }

    /// Returns the length of the packet data (excluding header).
    pub fn data_length(&self) -> u16 {
        self.packet_length.saturating_sub(HEADER_SIZE as u16)
    }

    /// Encode the header to bytes.
    pub fn to_bytes(&self) -> [u8; HEADER_SIZE] {
        let mut bytes = [0u8; HEADER_SIZE];
        bytes[0..2].copy_from_slice(&self.packet_length.to_be_bytes());
        bytes[2..4].copy_from_slice(&self.packet_checksum.to_be_bytes());
        bytes[4] = self.packet_type.as_u8();
        bytes[5] = self.flags;
        bytes[6..8].copy_from_slice(&self.header_checksum.to_be_bytes());
        bytes
    }
}

/// TNS packet type enumeration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum PacketType {
    /// Connect request packet.
    Connect = packet_types::CONNECT,
    /// Accept packet (connection accepted).
    Accept = packet_types::ACCEPT,
    /// Acknowledge packet.
    Ack = packet_types::ACK,
    /// Refuse packet (connection refused).
    Refuse = packet_types::REFUSE,
    /// Redirect packet.
    Redirect = packet_types::REDIRECT,
    /// Data packet.
    Data = packet_types::DATA,
    /// Null packet.
    Null = packet_types::NULL,
    /// Abort packet.
    Abort = packet_types::ABORT,
    /// Resend packet.
    Resend = packet_types::RESEND,
    /// Marker packet.
    Marker = packet_types::MARKER,
    /// Attention packet.
    Attention = packet_types::ATTENTION,
    /// Control packet.
    Control = packet_types::CONTROL,
    /// Data descriptor packet (TNS v12+).
    DataDescriptor = packet_types::DATA_DESCRIPTOR,
    /// Unknown packet type.
    Unknown(u8),
}

impl PacketType {
    /// Create a PacketType from a raw byte.
    pub fn from_u8(value: u8) -> Self {
        match value {
            packet_types::CONNECT => Self::Connect,
            packet_types::ACCEPT => Self::Accept,
            packet_types::ACK => Self::Ack,
            packet_types::REFUSE => Self::Refuse,
            packet_types::REDIRECT => Self::Redirect,
            packet_types::DATA => Self::Data,
            packet_types::NULL => Self::Null,
            packet_types::ABORT => Self::Abort,
            packet_types::RESEND => Self::Resend,
            packet_types::MARKER => Self::Marker,
            packet_types::ATTENTION => Self::Attention,
            packet_types::CONTROL => Self::Control,
            packet_types::DATA_DESCRIPTOR => Self::DataDescriptor,
            other => Self::Unknown(other),
        }
    }

    /// Convert to raw byte value.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::Connect => packet_types::CONNECT,
            Self::Accept => packet_types::ACCEPT,
            Self::Ack => packet_types::ACK,
            Self::Refuse => packet_types::REFUSE,
            Self::Redirect => packet_types::REDIRECT,
            Self::Data => packet_types::DATA,
            Self::Null => packet_types::NULL,
            Self::Abort => packet_types::ABORT,
            Self::Resend => packet_types::RESEND,
            Self::Marker => packet_types::MARKER,
            Self::Attention => packet_types::ATTENTION,
            Self::Control => packet_types::CONTROL,
            Self::DataDescriptor => packet_types::DATA_DESCRIPTOR,
            Self::Unknown(v) => *v,
        }
    }

    /// Returns the human-readable name of this packet type.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Connect => "Connect",
            Self::Accept => "Accept",
            Self::Ack => "Acknowledge",
            Self::Refuse => "Refuse",
            Self::Redirect => "Redirect",
            Self::Data => "Data",
            Self::Null => "Null",
            Self::Abort => "Abort",
            Self::Resend => "Resend",
            Self::Marker => "Marker",
            Self::Attention => "Attention",
            Self::Control => "Control",
            Self::DataDescriptor => "DataDescriptor",
            Self::Unknown(_) => "Unknown",
        }
    }

    /// Check if this packet type requires version 12 or higher.
    pub fn requires_v12(&self) -> bool {
        matches!(self, Self::DataDescriptor)
    }
}

/// Error when parsing a TNS header.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TnsHeaderError {
    #[error("invalid packet type: {0}")]
    InvalidPacketType(u8),
    #[error("packet length too small: {0} (minimum is {HEADER_SIZE})")]
    PacketTooSmall(u16),
    #[error("packet length exceeds maximum: {0} (maximum is {MAX_PACKET_SIZE})")]
    PacketTooLarge(u16),
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for TnsHeader {
    type ParseError = TnsHeaderError;
    type Value<'s>
        = TnsHeader
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let packet_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        if packet_length < HEADER_SIZE as u16 {
            return Err(OracleParseError::Parse(TnsHeaderError::PacketTooSmall(packet_length)));
        }

        if packet_length > MAX_PACKET_SIZE as u16 {
            return Err(OracleParseError::Parse(TnsHeaderError::PacketTooLarge(packet_length)));
        }

        let packet_checksum = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let packet_type_byte = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let flags = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let header_checksum = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        Ok(TnsHeader {
            packet_length,
            packet_checksum,
            packet_type: PacketType::from_u8(packet_type_byte),
            flags,
            header_checksum,
        })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for TnsHeader {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let packet_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        if packet_length < HEADER_SIZE as u16 {
            return Err(OracleParseError::Parse(TnsHeaderError::PacketTooSmall(packet_length)));
        }

        if packet_length > MAX_PACKET_SIZE as u16 {
            return Err(OracleParseError::Parse(TnsHeaderError::PacketTooLarge(packet_length)));
        }

        let packet_checksum = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let packet_type_byte = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let flags = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let header_checksum = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        Ok(TnsHeader {
            packet_length,
            packet_checksum,
            packet_type: PacketType::from_u8(packet_type_byte),
            flags,
            header_checksum,
        })
    }
}
