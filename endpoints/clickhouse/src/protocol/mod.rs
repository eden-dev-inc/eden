//! ClickHouse native protocol support for wire-level proxying.
//!
//! This module provides encoding, decoding, and frame scanning for
//! the ClickHouse native binary protocol (TCP port 9000).

pub mod scanner;

use bytes::Bytes;
use clickhouse_core::ClickhouseAsync;
use endpoint_types::request::EpWireRequest;
use ep_core::ReqType;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use scanner::PacketType;

/// Bytes that make up a ClickHouse native protocol input or output.
#[derive(Debug, Clone)]
pub struct ClickhouseBytes(Vec<u8>);

impl ClickhouseBytes {
    /// Create new ClickhouseBytes from a byte vector.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    /// Get the raw bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }

    /// Get the length of the bytes.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the bytes are empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Convert to owned bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Try to determine the packet type from the bytes.
    pub fn packet_type(&self) -> Option<PacketType> {
        scanner::peek_packet_type(&self.0, false).map(|(pt, _)| pt)
    }
}

impl From<Vec<u8>> for ClickhouseBytes {
    fn from(v: Vec<u8>) -> Self {
        Self(v)
    }
}

impl From<Bytes> for ClickhouseBytes {
    fn from(v: Bytes) -> Self {
        Self(v.to_vec())
    }
}

impl From<&[u8]> for ClickhouseBytes {
    fn from(v: &[u8]) -> Self {
        Self(v.to_vec())
    }
}

impl EpWireRequest<ClickhouseAsync> for ClickhouseBytes {
    fn kind(&self) -> EpKind {
        EpKind::Clickhouse
    }

    fn request_type(&self) -> ResultEP<ReqType> {
        // Parse the bytes to determine the packet type
        if let Some((packet_type, _)) = scanner::peek_packet_type(&self.0, false) {
            match packet_type {
                // Read operations
                PacketType::ClientPing | PacketType::ClientTablesStatusRequest | PacketType::ClientKeepAlive => Ok(ReqType::Read),

                // Write operations or operations that could modify state
                PacketType::ClientHello
                | PacketType::ClientQuery
                | PacketType::ClientData
                | PacketType::ClientCancel
                | PacketType::ClientScalar
                | PacketType::ClientIgnoredPartUUIDs
                | PacketType::ClientReadTaskResponse
                | PacketType::ClientMergeTreeReadTaskResponse => {
                    // For Query, we'd need to parse SQL to determine read vs write
                    // Default to Write for safety
                    Ok(ReqType::Write)
                }

                // Server packets shouldn't be sent by client
                _ => Ok(ReqType::Write),
            }
        } else {
            // Can't parse, default to Write for safety
            Ok(ReqType::Write)
        }
    }

    /// Note: ClickHouse HTTP API expects SQL queries, not native protocol bytes.
    /// For true wire protocol proxying, we'd need a native TCP client pool.
    /// This implementation returns an error until native TCP support is added.
    async fn send_raw_bytes(&self, _context: &ClickhouseAsync) -> ResultEP<(bytes::Bytes, u64)> {
        // The ClickHouse HTTP API doesn't support native protocol bytes directly.
        // For wire protocol proxying to work, we need native TCP protocol support.
        // Currently, wire protocol processing will fail with this error.
        Err(EpError::request(
            "ClickHouse wire protocol proxying requires native TCP support (not yet implemented)",
        ))
    }
}

/// ClickHouse native protocol handler.
#[derive(Debug)]
pub struct ClickhouseProtocol;

impl ClickhouseProtocol {
    /// Peek at the packet type without consuming bytes.
    ///
    /// Returns (packet_type, type_bytes_len) or None if buffer is too short.
    pub fn peek_packet_type(buffer: &[u8], is_server: bool) -> Option<(PacketType, usize)> {
        scanner::peek_packet_type(buffer, is_server)
    }

    /// Try to scan a packet boundary.
    ///
    /// For simple packets (Ping, Pong, EndOfStream), returns the boundary.
    /// For complex packets, returns None (caller should use full parsing).
    pub fn scan_packet_boundary(buffer: &[u8], is_server: bool) -> Option<(PacketType, usize)> {
        scanner::scan_packet_boundary(buffer, is_server)
    }

    /// Check if a packet type indicates end of response stream.
    pub fn is_end_of_stream(packet_type: &PacketType) -> bool {
        packet_type.is_end_of_stream()
    }

    /// Check if a packet type indicates an exception.
    pub fn is_exception(packet_type: &PacketType) -> bool {
        packet_type.is_exception()
    }

    /// Validate that buffer contains at least one complete packet.
    ///
    /// For ClickHouse, this is tricky because packet boundaries depend on content.
    /// This method returns the number of bytes that make up complete packets,
    /// or 0 if no complete packet is found.
    pub fn validate_buffer(buffer: &[u8], is_server: bool) -> usize {
        if buffer.is_empty() {
            return 0;
        }

        // Try to scan for packet boundary
        if let Some((_, consumed)) = scanner::scan_packet_boundary(buffer, is_server) {
            return consumed;
        }

        // For complex packets, we need full parsing
        // Return 0 to indicate caller should accumulate more data or use full parsing
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_clickhouse_bytes_new() {
        let bytes = ClickhouseBytes::new(vec![1, 2, 3]);
        assert_eq!(bytes.bytes(), &[1, 2, 3]);
        assert_eq!(bytes.len(), 3);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_clickhouse_bytes_from() {
        let bytes: ClickhouseBytes = vec![1, 2, 3].into();
        assert_eq!(bytes.bytes(), &[1, 2, 3]);

        let bytes: ClickhouseBytes = Bytes::from_static(&[4, 5, 6]).into();
        assert_eq!(bytes.bytes(), &[4, 5, 6]);
    }

    #[test]
    fn test_clickhouse_bytes_packet_type() {
        // Client Ping (type 4) - varint encoding
        let bytes = ClickhouseBytes::new(vec![4]);
        assert!(matches!(bytes.packet_type(), Some(PacketType::ClientPing)));
    }

    #[test]
    fn test_request_type() {
        // Ping is a read operation (type 4)
        let bytes = ClickhouseBytes::new(vec![4]);
        assert_eq!(bytes.request_type().unwrap(), ReqType::Read);

        // Query is a write operation (type 1)
        let bytes = ClickhouseBytes::new(vec![1]);
        assert_eq!(bytes.request_type().unwrap(), ReqType::Write);
    }

    #[test]
    fn test_protocol_peek_packet_type() {
        // Hello (type 0) - varint encoding
        let mut buf = vec![0];
        buf.extend_from_slice(b"extra");

        let result = ClickhouseProtocol::peek_packet_type(&buf, true);
        assert!(matches!(result, Some((PacketType::ServerHello, 1))));

        let result = ClickhouseProtocol::peek_packet_type(&buf, false);
        assert!(matches!(result, Some((PacketType::ClientHello, 1))));
    }
}
