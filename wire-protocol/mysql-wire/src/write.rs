//! MySQL wire protocol encoding helpers.
//!
//! This module provides functions and builders for encoding MySQL protocol
//! packets and data types.

use crate::error::commands;
use std::io::{self, Write};

// ============================================================================
// Primitive write helpers
// ============================================================================

/// Write a u16 in little-endian format.
#[inline]
pub fn write_u16_le(w: &mut impl Write, value: u16) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a u24 (3 bytes) in little-endian format.
#[inline]
pub fn write_u24_le(w: &mut impl Write, value: u32) -> io::Result<()> {
    let bytes = value.to_le_bytes();
    w.write_all(&bytes[0..3])
}

/// Write a u32 in little-endian format.
#[inline]
pub fn write_u32_le(w: &mut impl Write, value: u32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a u48 (6 bytes) in little-endian format.
#[inline]
pub fn write_u48_le(w: &mut impl Write, value: u64) -> io::Result<()> {
    let bytes = value.to_le_bytes();
    w.write_all(&bytes[0..6])
}

/// Write a u64 in little-endian format.
#[inline]
pub fn write_u64_le(w: &mut impl Write, value: u64) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a length-encoded integer.
///
/// MySQL's length-encoded integer format:
/// - 0x00-0xFA: 1-byte value
/// - 0xFC + 2 bytes: values up to 0xFFFF
/// - 0xFD + 3 bytes: values up to 0xFFFFFF
/// - 0xFE + 8 bytes: values up to u64::MAX
pub fn write_lenenc_int(w: &mut impl Write, value: u64) -> io::Result<()> {
    if value < 0xFB {
        w.write_all(&[value as u8])
    } else if value <= 0xFFFF {
        w.write_all(&[0xFC])?;
        write_u16_le(w, value as u16)
    } else if value <= 0xFFFFFF {
        w.write_all(&[0xFD])?;
        write_u24_le(w, value as u32)
    } else {
        w.write_all(&[0xFE])?;
        write_u64_le(w, value)
    }
}

/// Write a length-encoded string.
///
/// First writes the length as a length-encoded integer, then the bytes.
pub fn write_lenenc_string(w: &mut impl Write, s: &[u8]) -> io::Result<()> {
    write_lenenc_int(w, s.len() as u64)?;
    w.write_all(s)
}

/// Write a NUL-terminated string.
pub fn write_cstring(w: &mut impl Write, s: &[u8]) -> io::Result<()> {
    w.write_all(s)?;
    w.write_all(&[0])
}

/// Write a fixed-length string (padded with NUL if shorter, truncated if longer).
pub fn write_fixed_string(w: &mut impl Write, s: &[u8], len: usize) -> io::Result<()> {
    if s.len() >= len {
        w.write_all(&s[..len])
    } else {
        w.write_all(s)?;
        for _ in 0..(len - s.len()) {
            w.write_all(&[0])?;
        }
        Ok(())
    }
}

/// Write a MySQL packet header (3-byte length + 1-byte sequence).
#[inline]
pub fn write_packet_header(w: &mut impl Write, payload_length: u32, sequence_id: u8) -> io::Result<()> {
    write_u24_le(w, payload_length)?;
    w.write_all(&[sequence_id])
}

// ============================================================================
// Packet builder
// ============================================================================

/// Builder for constructing MySQL packets.
///
/// Automatically handles the 4-byte header when building.
#[derive(Debug)]
pub struct PacketBuilder {
    sequence_id: u8,
    data: Vec<u8>,
}

impl PacketBuilder {
    /// Create a new packet builder with the given sequence ID.
    pub fn new(sequence_id: u8) -> Self {
        Self { sequence_id, data: Vec::new() }
    }

    /// Create a new packet builder with capacity hint.
    pub fn with_capacity(sequence_id: u8, capacity: usize) -> Self {
        Self { sequence_id, data: Vec::with_capacity(capacity) }
    }

    /// Get the current payload length.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the payload is empty.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Write raw bytes to the packet.
    pub fn write_bytes(mut self, data: &[u8]) -> Self {
        self.data.extend_from_slice(data);
        self
    }

    /// Write a single byte.
    pub fn write_u8(mut self, value: u8) -> Self {
        self.data.push(value);
        self
    }

    /// Write a u16 (little-endian).
    pub fn write_u16(mut self, value: u16) -> Self {
        self.data.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Write a u24 (little-endian).
    pub fn write_u24(mut self, value: u32) -> Self {
        let bytes = value.to_le_bytes();
        self.data.extend_from_slice(&bytes[0..3]);
        self
    }

    /// Write a u32 (little-endian).
    pub fn write_u32(mut self, value: u32) -> Self {
        self.data.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Write a u64 (little-endian).
    pub fn write_u64(mut self, value: u64) -> Self {
        self.data.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Write a length-encoded integer.
    pub fn write_lenenc_int(mut self, value: u64) -> Self {
        write_lenenc_int(&mut self.data, value).expect("Vec write should not fail");
        self
    }

    /// Write a length-encoded string.
    pub fn write_lenenc_string(mut self, s: &[u8]) -> Self {
        write_lenenc_string(&mut self.data, s).expect("Vec write should not fail");
        self
    }

    /// Write a NUL-terminated string.
    pub fn write_cstring(mut self, s: &[u8]) -> Self {
        write_cstring(&mut self.data, s).expect("Vec write should not fail");
        self
    }

    /// Write a fixed-length string.
    pub fn write_fixed_string(mut self, s: &[u8], len: usize) -> Self {
        write_fixed_string(&mut self.data, s, len).expect("Vec write should not fail");
        self
    }

    /// Build the complete packet with header.
    ///
    /// Returns the full packet including the 4-byte header.
    pub fn build(self) -> Vec<u8> {
        let mut packet = Vec::with_capacity(4 + self.data.len());
        let len_bytes = (self.data.len() as u32).to_le_bytes();
        packet.extend_from_slice(&[len_bytes[0], len_bytes[1], len_bytes[2], self.sequence_id]);
        packet.extend_from_slice(&self.data);
        packet
    }

    /// Build and return only the payload (without header).
    pub fn build_payload(self) -> Vec<u8> {
        self.data
    }
}

// ============================================================================
// Command packet builders
// ============================================================================

/// Build a COM_QUERY packet.
pub fn build_query_packet(sequence_id: u8, query: &str) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_QUERY).write_bytes(query.as_bytes()).build()
}

/// Build a COM_PING packet.
pub fn build_ping_packet(sequence_id: u8) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_PING).build()
}

/// Build a COM_QUIT packet.
pub fn build_quit_packet(sequence_id: u8) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_QUIT).build()
}

/// Build a COM_INIT_DB packet.
pub fn build_init_db_packet(sequence_id: u8, database: &str) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_INIT_DB).write_bytes(database.as_bytes()).build()
}

/// Build a COM_STMT_PREPARE packet.
pub fn build_stmt_prepare_packet(sequence_id: u8, query: &str) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_STMT_PREPARE).write_bytes(query.as_bytes()).build()
}

/// Build a COM_STMT_CLOSE packet.
pub fn build_stmt_close_packet(sequence_id: u8, statement_id: u32) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_STMT_CLOSE).write_u32(statement_id).build()
}

/// Build a COM_STMT_RESET packet.
pub fn build_stmt_reset_packet(sequence_id: u8, statement_id: u32) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_STMT_RESET).write_u32(statement_id).build()
}

/// Build a COM_RESET_CONNECTION packet.
pub fn build_reset_connection_packet(sequence_id: u8) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_RESET_CONNECTION).build()
}

/// Build a COM_STATISTICS packet.
pub fn build_statistics_packet(sequence_id: u8) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_STATISTICS).build()
}

/// Build a COM_DEBUG packet.
pub fn build_debug_packet(sequence_id: u8) -> Vec<u8> {
    PacketBuilder::new(sequence_id).write_u8(commands::COM_DEBUG).build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_lenenc_int_small() {
        let mut buf = Vec::new();
        write_lenenc_int(&mut buf, 42).unwrap();
        assert_eq!(buf, vec![42]);
    }

    #[test]
    fn test_write_lenenc_int_2byte() {
        let mut buf = Vec::new();
        write_lenenc_int(&mut buf, 0x1234).unwrap();
        assert_eq!(buf, vec![0xFC, 0x34, 0x12]);
    }

    #[test]
    fn test_write_lenenc_int_3byte() {
        let mut buf = Vec::new();
        write_lenenc_int(&mut buf, 0x123456).unwrap();
        assert_eq!(buf, vec![0xFD, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_write_lenenc_int_8byte() {
        let mut buf = Vec::new();
        write_lenenc_int(&mut buf, 0x123456789ABCDEF0).unwrap();
        assert_eq!(buf, vec![0xFE, 0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_packet_builder() {
        let packet = PacketBuilder::new(0).write_u8(commands::COM_QUERY).write_bytes(b"SELECT 1").build();

        // Header: 9 bytes payload, sequence 0
        assert_eq!(&packet[0..4], &[0x09, 0x00, 0x00, 0x00]);
        // Payload: COM_QUERY + "SELECT 1"
        assert_eq!(packet[4], commands::COM_QUERY);
        assert_eq!(&packet[5..], b"SELECT 1");
    }

    #[test]
    fn test_build_query_packet() {
        let packet = build_query_packet(1, "SELECT * FROM users");
        assert_eq!(packet[3], 1); // sequence_id
        assert_eq!(packet[4], commands::COM_QUERY);
    }

    #[test]
    fn test_build_ping_packet() {
        let packet = build_ping_packet(5);
        assert_eq!(&packet[0..4], &[0x01, 0x00, 0x00, 0x05]); // 1 byte payload, seq 5
        assert_eq!(packet[4], commands::COM_PING);
    }

    #[test]
    fn test_write_cstring() {
        let mut buf = Vec::new();
        write_cstring(&mut buf, b"hello").unwrap();
        assert_eq!(buf, b"hello\0");
    }

    #[test]
    fn test_write_fixed_string_shorter() {
        let mut buf = Vec::new();
        write_fixed_string(&mut buf, b"hi", 5).unwrap();
        assert_eq!(buf, b"hi\0\0\0");
    }

    #[test]
    fn test_write_fixed_string_longer() {
        let mut buf = Vec::new();
        write_fixed_string(&mut buf, b"hello world", 5).unwrap();
        assert_eq!(buf, b"hello");
    }
}
