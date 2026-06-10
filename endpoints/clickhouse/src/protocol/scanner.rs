//! Zero-copy ClickHouse native protocol frame scanner.
//!
//! This module provides efficient packet boundary detection without allocating
//! or parsing packet contents. When full parsing is needed, it delegates to
//! the clickhouse-wire crate.

use clickhouse_wire::VARINT_MAX_BYTES;
use clickhouse_wire::native::packet::ServerPacketType;

/// Packet type detected during boundary scanning (no content parsing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PacketType {
    // Client packets
    ClientHello,
    ClientQuery,
    ClientData,
    ClientCancel,
    ClientPing,
    ClientTablesStatusRequest,
    ClientKeepAlive,
    ClientScalar,
    ClientIgnoredPartUUIDs,
    ClientReadTaskResponse,
    ClientMergeTreeReadTaskResponse,

    // Server packets
    ServerHello,
    ServerData,
    ServerException,
    ServerProgress,
    ServerPong,
    ServerEndOfStream,
    ServerProfileInfo,
    ServerTotals,
    ServerExtremes,
    ServerTablesStatusResponse,
    ServerLog,
    ServerTableColumns,
    ServerPartUUIDs,
    ServerReadTaskRequest,
    ServerProfileEvents,
    ServerMergeTreeAllRangesAnnouncement,
    ServerMergeTreeReadTaskRequest,
    ServerTimezoneUpdate,

    /// Unknown packet type
    Unknown(u64),
}

impl PacketType {
    /// Parse from a u64 value (client packet types 0-10).
    pub fn from_client_u64(value: u64) -> Self {
        match value {
            0 => Self::ClientHello,
            1 => Self::ClientQuery,
            2 => Self::ClientData,
            3 => Self::ClientCancel,
            4 => Self::ClientPing,
            5 => Self::ClientTablesStatusRequest,
            6 => Self::ClientKeepAlive,
            7 => Self::ClientScalar,
            8 => Self::ClientIgnoredPartUUIDs,
            9 => Self::ClientReadTaskResponse,
            10 => Self::ClientMergeTreeReadTaskResponse,
            _ => Self::Unknown(value),
        }
    }

    /// Parse from a u64 value (server packet types 0-17).
    pub fn from_server_u64(value: u64) -> Self {
        match ServerPacketType::from_u64(value) {
            Some(ServerPacketType::Hello) => Self::ServerHello,
            Some(ServerPacketType::Data) => Self::ServerData,
            Some(ServerPacketType::Exception) => Self::ServerException,
            Some(ServerPacketType::Progress) => Self::ServerProgress,
            Some(ServerPacketType::Pong) => Self::ServerPong,
            Some(ServerPacketType::EndOfStream) => Self::ServerEndOfStream,
            Some(ServerPacketType::ProfileInfo) => Self::ServerProfileInfo,
            Some(ServerPacketType::Totals) => Self::ServerTotals,
            Some(ServerPacketType::Extremes) => Self::ServerExtremes,
            Some(ServerPacketType::TablesStatusResponse) => Self::ServerTablesStatusResponse,
            Some(ServerPacketType::Log) => Self::ServerLog,
            Some(ServerPacketType::TableColumns) => Self::ServerTableColumns,
            Some(ServerPacketType::PartUUIDs) => Self::ServerPartUUIDs,
            Some(ServerPacketType::ReadTaskRequest) => Self::ServerReadTaskRequest,
            Some(ServerPacketType::ProfileEvents) => Self::ServerProfileEvents,
            Some(ServerPacketType::MergeTreeAllRangesAnnouncement) => Self::ServerMergeTreeAllRangesAnnouncement,
            Some(ServerPacketType::MergeTreeReadTaskRequest) => Self::ServerMergeTreeReadTaskRequest,
            Some(ServerPacketType::TimezoneUpdate) => Self::ServerTimezoneUpdate,
            None => Self::Unknown(value),
        }
    }

    /// Check if this is a server packet.
    pub fn is_server(&self) -> bool {
        matches!(
            self,
            Self::ServerHello
                | Self::ServerData
                | Self::ServerException
                | Self::ServerProgress
                | Self::ServerPong
                | Self::ServerEndOfStream
                | Self::ServerProfileInfo
                | Self::ServerTotals
                | Self::ServerExtremes
                | Self::ServerTablesStatusResponse
                | Self::ServerLog
                | Self::ServerTableColumns
                | Self::ServerPartUUIDs
                | Self::ServerReadTaskRequest
                | Self::ServerProfileEvents
                | Self::ServerMergeTreeAllRangesAnnouncement
                | Self::ServerMergeTreeReadTaskRequest
                | Self::ServerTimezoneUpdate
        )
    }

    /// Check if this is a client packet.
    pub fn is_client(&self) -> bool {
        matches!(
            self,
            Self::ClientHello
                | Self::ClientQuery
                | Self::ClientData
                | Self::ClientCancel
                | Self::ClientPing
                | Self::ClientTablesStatusRequest
                | Self::ClientKeepAlive
                | Self::ClientScalar
                | Self::ClientIgnoredPartUUIDs
                | Self::ClientReadTaskResponse
                | Self::ClientMergeTreeReadTaskResponse
        )
    }

    /// Check if this packet type indicates end of stream.
    pub fn is_end_of_stream(&self) -> bool {
        matches!(self, Self::ServerEndOfStream)
    }

    /// Check if this packet type indicates an error.
    pub fn is_exception(&self) -> bool {
        matches!(self, Self::ServerException)
    }
}

/// A zero-copy packet reference. Holds raw bytes without parsing content.
#[derive(Debug, Clone)]
pub struct RawPacket<'a> {
    /// The complete packet bytes including type prefix.
    pub bytes: &'a [u8],
    /// Detected packet type.
    pub packet_type: PacketType,
    /// Bytes consumed by packet type varint.
    pub type_len: usize,
}

impl<'a> RawPacket<'a> {
    /// Get the packet body (without type prefix).
    pub fn body(&self) -> &'a [u8] {
        &self.bytes[self.type_len..]
    }
}

/// Try to read a varint from the buffer and return (value, bytes_consumed).
/// Returns None if buffer is too short.
fn try_read_varint(buffer: &[u8]) -> Option<(u64, usize)> {
    if buffer.is_empty() {
        return None;
    }

    let mut result: u64 = 0;
    let mut shift = 0;

    for (i, &byte) in buffer.iter().take(VARINT_MAX_BYTES).enumerate() {
        let value = (byte & 0x7F) as u64;
        result |= value << shift;

        if byte & 0x80 == 0 {
            return Some((result, i + 1));
        }

        shift += 7;
    }

    None // Incomplete varint
}

/// Scan for packet boundary without parsing content.
///
/// This is a best-effort scan that tries to find packet boundaries.
/// For complex packets (Data, Query), it may not be able to determine
/// the exact boundary without full parsing.
///
/// Returns (packet_type, bytes_consumed) or None if incomplete.
pub fn scan_packet_boundary(buffer: &[u8], is_server: bool) -> Option<(PacketType, usize)> {
    if buffer.is_empty() {
        return None;
    }

    // Read packet type
    let (packet_type_value, type_len) = try_read_varint(buffer)?;

    let packet_type = if is_server {
        PacketType::from_server_u64(packet_type_value)
    } else {
        PacketType::from_client_u64(packet_type_value)
    };

    // For simple empty packets, we can return immediately
    match packet_type {
        PacketType::ClientPing
        | PacketType::ClientCancel
        | PacketType::ClientKeepAlive
        | PacketType::ServerPong
        | PacketType::ServerEndOfStream
        | PacketType::ServerReadTaskRequest
        | PacketType::ServerMergeTreeReadTaskRequest => {
            return Some((packet_type, type_len));
        }
        _ => {}
    }

    // For complex packets, we can't easily determine boundaries without full parsing
    // Return None to indicate that the caller should use full parsing
    // In a production proxy, you'd implement more sophisticated boundary detection
    None
}

/// Scan a client packet from buffer.
///
/// Returns (packet_type, bytes_consumed) or None if incomplete/unparseable.
pub fn scan_client_packet(buffer: &[u8]) -> Option<(PacketType, usize)> {
    scan_packet_boundary(buffer, false)
}

/// Scan a server packet from buffer.
///
/// Returns (packet_type, bytes_consumed) or None if incomplete/unparseable.
pub fn scan_server_packet(buffer: &[u8]) -> Option<(PacketType, usize)> {
    scan_packet_boundary(buffer, true)
}

/// Read just the packet type from buffer without advancing.
///
/// Returns (packet_type, type_bytes_len) or None if buffer is too short.
pub fn peek_packet_type(buffer: &[u8], is_server: bool) -> Option<(PacketType, usize)> {
    let (packet_type_value, type_len) = try_read_varint(buffer)?;

    let packet_type = if is_server {
        PacketType::from_server_u64(packet_type_value)
    } else {
        PacketType::from_client_u64(packet_type_value)
    };

    Some((packet_type, type_len))
}

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse_wire::native::write::ClickhouseWriteExt;

    #[test]
    fn test_try_read_varint() {
        // Single byte
        assert_eq!(try_read_varint(&[0x00]), Some((0, 1)));
        assert_eq!(try_read_varint(&[0x01]), Some((1, 1)));
        assert_eq!(try_read_varint(&[0x7F]), Some((127, 1)));

        // Two bytes
        assert_eq!(try_read_varint(&[0x80, 0x01]), Some((128, 2)));
        assert_eq!(try_read_varint(&[0xFF, 0x01]), Some((255, 2)));

        // Incomplete
        assert_eq!(try_read_varint(&[0x80]), None);
        assert_eq!(try_read_varint(&[]), None);
    }

    #[test]
    fn test_packet_type_from_client() {
        assert!(matches!(PacketType::from_client_u64(0), PacketType::ClientHello));
        assert!(matches!(PacketType::from_client_u64(1), PacketType::ClientQuery));
        assert!(matches!(PacketType::from_client_u64(4), PacketType::ClientPing));
        assert!(matches!(PacketType::from_client_u64(100), PacketType::Unknown(100)));
    }

    #[test]
    fn test_packet_type_from_server() {
        assert!(matches!(PacketType::from_server_u64(0), PacketType::ServerHello));
        assert!(matches!(PacketType::from_server_u64(2), PacketType::ServerException));
        assert!(matches!(PacketType::from_server_u64(5), PacketType::ServerEndOfStream));
    }

    #[test]
    fn test_scan_simple_packets() {
        // Ping packet (type 4, no body)
        let mut buf = Vec::new();
        buf.write_varuint(4).unwrap();
        let result = scan_client_packet(&buf);
        assert!(matches!(result, Some((PacketType::ClientPing, 1))));

        // Pong packet (type 4, no body)
        let mut buf = Vec::new();
        buf.write_varuint(4).unwrap();
        let result = scan_server_packet(&buf);
        assert!(matches!(result, Some((PacketType::ServerPong, 1))));

        // EndOfStream (type 5, no body)
        let mut buf = Vec::new();
        buf.write_varuint(5).unwrap();
        let result = scan_server_packet(&buf);
        assert!(matches!(result, Some((PacketType::ServerEndOfStream, 1))));
    }

    #[test]
    fn test_peek_packet_type() {
        let mut buf = Vec::new();
        buf.write_varuint(0).unwrap();
        buf.extend_from_slice(b"extra data");

        let result = peek_packet_type(&buf, true);
        assert!(matches!(result, Some((PacketType::ServerHello, 1))));
    }
}
