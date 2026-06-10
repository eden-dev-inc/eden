//! Oracle TNS packet encoding helpers.

use crate::types::packet::{HEADER_SIZE, PacketType, TnsHeader};
use std::io::{self, Write};

// ============================================================================
// OracleWrite trait
// ============================================================================

/// Trait for types that can be encoded to the Oracle wire format.
pub trait OracleWrite {
    /// Write this value to the given writer.
    fn write_to(&self, w: &mut impl Write) -> io::Result<()>;

    /// Encode this value to a new Vec<u8>.
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        self.write_to(&mut buf).expect("Vec<u8> write should not fail");
        buf
    }
}

// ============================================================================
// Primitive write helpers
// ============================================================================

/// Write a u16 in big-endian format to a buffer.
#[inline]
pub fn write_u16_be(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Write a u32 in big-endian format to a buffer.
#[inline]
pub fn write_u32_be(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Write a u64 in big-endian format to a buffer.
#[inline]
pub fn write_u64_be(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Builder for constructing TNS packets.
pub struct PacketBuilder {
    packet_type: PacketType,
    flags: u8,
    data: Vec<u8>,
}

impl PacketBuilder {
    /// Create a new packet builder.
    pub fn new(packet_type: PacketType) -> Self {
        Self { packet_type, flags: 0, data: Vec::new() }
    }

    /// Set the flags byte.
    pub fn flags(mut self, flags: u8) -> Self {
        self.flags = flags;
        self
    }

    /// Append raw bytes to the packet data.
    pub fn write_bytes(mut self, data: &[u8]) -> Self {
        self.data.extend_from_slice(data);
        self
    }

    /// Append a u8 to the packet data.
    pub fn write_u8(mut self, value: u8) -> Self {
        self.data.push(value);
        self
    }

    /// Append a u16 (big-endian) to the packet data.
    pub fn write_u16(mut self, value: u16) -> Self {
        write_u16_be(&mut self.data, value);
        self
    }

    /// Append a u32 (big-endian) to the packet data.
    pub fn write_u32(mut self, value: u32) -> Self {
        write_u32_be(&mut self.data, value);
        self
    }

    /// Append a u64 (big-endian) to the packet data.
    pub fn write_u64(mut self, value: u64) -> Self {
        write_u64_be(&mut self.data, value);
        self
    }

    /// Build the complete packet with header.
    pub fn build(self) -> Vec<u8> {
        let header = TnsHeader {
            packet_length: (HEADER_SIZE + self.data.len()) as u16,
            packet_checksum: 0,
            packet_type: self.packet_type,
            flags: self.flags,
            header_checksum: 0,
        };

        let mut packet = Vec::with_capacity(HEADER_SIZE + self.data.len());
        packet.extend_from_slice(&header.to_bytes());
        packet.extend_from_slice(&self.data);
        packet
    }

    /// Get the current data length.
    pub fn data_len(&self) -> usize {
        self.data.len()
    }
}

/// Build a Connect packet.
pub struct ConnectBuilder {
    version: u16,
    version_compatible: u16,
    sdu_size: u16,
    tdu_size: u16,
    connect_flags_1: u8,
    connect_flags_2: u8,
    connect_data: Vec<u8>,
}

impl ConnectBuilder {
    /// Create a new Connect packet builder.
    pub fn new(version: u16) -> Self {
        Self {
            version,
            version_compatible: version,
            sdu_size: 8192,
            tdu_size: 32767,
            connect_flags_1: 0,
            connect_flags_2: 0,
            connect_data: Vec::new(),
        }
    }

    /// Set the compatible version.
    pub fn version_compatible(mut self, version: u16) -> Self {
        self.version_compatible = version;
        self
    }

    /// Set the SDU size.
    pub fn sdu_size(mut self, size: u16) -> Self {
        self.sdu_size = size;
        self
    }

    /// Set the TDU size.
    pub fn tdu_size(mut self, size: u16) -> Self {
        self.tdu_size = size;
        self
    }

    /// Set connect flags.
    pub fn connect_flags(mut self, flags1: u8, flags2: u8) -> Self {
        self.connect_flags_1 = flags1;
        self.connect_flags_2 = flags2;
        self
    }

    /// Set the connect data string (TNS descriptor).
    pub fn connect_data(mut self, data: &[u8]) -> Self {
        self.connect_data = data.to_vec();
        self
    }

    /// Build the Connect packet.
    pub fn build(self) -> Vec<u8> {
        let connect_data_offset = 58u16; // Standard connect header size

        PacketBuilder::new(PacketType::Connect)
            .write_u16(self.version)
            .write_u16(self.version_compatible)
            .write_u16(0) // service_options
            .write_u16(self.sdu_size)
            .write_u16(self.tdu_size)
            .write_u16(0) // nt_proto_characteristics
            .write_u16(0) // line_turnaround
            .write_u16(1) // hardware_type (1 = little-endian)
            .write_u16(self.connect_data.len() as u16)
            .write_u16(connect_data_offset)
            .write_u32(0) // max_receivable_connect_data
            .write_u8(self.connect_flags_1)
            .write_u8(self.connect_flags_2)
            .write_u32(0) // trace_cross_facility_1
            .write_u32(0) // trace_cross_facility_2
            .write_u64(0) // trace_unique_conn_id
            .write_bytes(&self.connect_data)
            .build()
    }
}

/// Build a Data packet.
pub struct DataBuilder {
    flags: u16,
    payload: Vec<u8>,
}

impl DataBuilder {
    /// Create a new Data packet builder.
    pub fn new() -> Self {
        Self { flags: 0, payload: Vec::new() }
    }

    /// Set the data flags.
    pub fn flags(mut self, flags: u16) -> Self {
        self.flags = flags;
        self
    }

    /// Set the payload.
    pub fn payload(mut self, data: &[u8]) -> Self {
        self.payload = data.to_vec();
        self
    }

    /// Build the Data packet.
    pub fn build(self) -> Vec<u8> {
        PacketBuilder::new(PacketType::Data).write_u16(self.flags).write_bytes(&self.payload).build()
    }
}

impl Default for DataBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// OracleWrite implementations
// ============================================================================

impl OracleWrite for TnsHeader {
    fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.to_bytes())
    }
}

impl OracleWrite for crate::types::data::Data {
    fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.flags.raw().to_be_bytes())?;
        w.write_all(&self.payload)
    }
}

impl OracleWrite for crate::types::marker::Marker {
    fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&[self.marker_type, self.data])
    }
}

impl OracleWrite for crate::types::redirect::Redirect {
    fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.data_length.to_be_bytes())?;
        w.write_all(&self.redirect_data)
    }
}

impl OracleWrite for crate::types::tti::message::TtiMessage {
    fn write_to(&self, w: &mut impl Write) -> io::Result<()> {
        w.write_all(&self.to_bytes())
    }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_u16_be() {
        let mut buf = Vec::new();
        write_u16_be(&mut buf, 0x1234);
        assert_eq!(buf, &[0x12, 0x34]);
    }

    #[test]
    fn test_write_u32_be() {
        let mut buf = Vec::new();
        write_u32_be(&mut buf, 0x12345678);
        assert_eq!(buf, &[0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_packet_builder_data() {
        let packet = PacketBuilder::new(PacketType::Data)
            .write_u16(0x0000) // flags
            .write_bytes(b"test")
            .build();

        // Header (8 bytes) + flags (2) + data (4) = 14 bytes
        assert_eq!(packet.len(), 14);
        // Packet length field
        assert_eq!(&packet[0..2], &[0x00, 0x0E]); // 14 in big-endian
        // Packet type
        assert_eq!(packet[4], 0x06); // DATA
    }

    #[test]
    fn test_connect_builder() {
        let packet = ConnectBuilder::new(12) // TNS v12
            .sdu_size(8192)
            .connect_data(b"(DESCRIPTION=(ADDRESS=(PROTOCOL=tcp)))")
            .build();

        assert!(!packet.is_empty());
        // Should be a Connect packet
        assert_eq!(packet[4], 0x01); // CONNECT
    }

    #[test]
    fn test_data_builder() {
        let packet = DataBuilder::new()
            .flags(0x0040) // EOF
            .payload(b"hello")
            .build();

        // Header (8) + flags (2) + payload (5) = 15
        assert_eq!(packet.len(), 15);
        assert_eq!(packet[4], 0x06); // DATA
    }

    #[test]
    fn test_header_write() {
        let header = TnsHeader::new(PacketType::Data, 10);
        let bytes = header.to_bytes();

        assert_eq!(bytes.len(), HEADER_SIZE);
        assert_eq!(&bytes[0..2], &[0x00, 0x12]); // 18 = 8 + 10
        assert_eq!(bytes[4], 0x06); // DATA
    }
}
