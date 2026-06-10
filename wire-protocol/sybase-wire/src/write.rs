//! Sybase TDS packet encoding helpers.

use crate::limits::HEADER_SIZE;
use crate::types::packet::{PacketType, TdsHeader};
use std::io::{self, Write};

// ============================================================================
// SybaseWrite trait
// ============================================================================

/// Trait for types that can be encoded to the Sybase wire format.
pub trait SybaseWrite {
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

/// Write a u16 in little-endian format to a buffer.
#[inline]
pub fn write_u16_le(buf: &mut Vec<u8>, value: u16) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a u32 in big-endian format to a buffer.
#[inline]
pub fn write_u32_be(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Write a u32 in little-endian format to a buffer.
#[inline]
pub fn write_u32_le(buf: &mut Vec<u8>, value: u32) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a u64 in big-endian format to a buffer.
#[inline]
pub fn write_u64_be(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_be_bytes());
}

/// Write a u64 in little-endian format to a buffer.
#[inline]
pub fn write_u64_le(buf: &mut Vec<u8>, value: u64) {
    buf.extend_from_slice(&value.to_le_bytes());
}

/// Write a length-prefixed string (1-byte length prefix).
#[inline]
pub fn write_varchar(buf: &mut Vec<u8>, data: &[u8]) {
    debug_assert!(data.len() <= 255);
    buf.push(data.len() as u8);
    buf.extend_from_slice(data);
}

/// Write a length-prefixed string (2-byte length prefix, little-endian).
#[inline]
pub fn write_longvarchar(buf: &mut Vec<u8>, data: &[u8]) {
    debug_assert!(data.len() <= 65535);
    write_u16_le(buf, data.len() as u16);
    buf.extend_from_slice(data);
}

/// Write a fixed-length string, padding with zeros if necessary.
#[inline]
pub fn write_fixed_string(buf: &mut Vec<u8>, data: &[u8], len: usize) {
    let write_len = data.len().min(len);
    buf.extend_from_slice(&data[..write_len]);
    // Pad with zeros
    for _ in write_len..len {
        buf.push(0);
    }
}

// ============================================================================
// PacketBuilder
// ============================================================================

/// Builder for constructing TDS packets.
pub struct PacketBuilder {
    packet_type: PacketType,
    status: u8,
    spid: u16,
    packet_number: u8,
    window: u8,
    data: Vec<u8>,
}

impl PacketBuilder {
    /// Create a new packet builder.
    pub fn new(packet_type: PacketType) -> Self {
        Self {
            packet_type,
            status: 0x01, // EOM by default
            spid: 0,
            packet_number: 1,
            window: 0,
            data: Vec::new(),
        }
    }

    /// Set the status byte.
    pub fn status(mut self, status: u8) -> Self {
        self.status = status;
        self
    }

    /// Set the SPID (channel).
    pub fn spid(mut self, spid: u16) -> Self {
        self.spid = spid;
        self
    }

    /// Set the packet number.
    pub fn packet_number(mut self, packet_number: u8) -> Self {
        self.packet_number = packet_number;
        self
    }

    /// Set the window byte.
    pub fn window(mut self, window: u8) -> Self {
        self.window = window;
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
    pub fn write_u16_be(mut self, value: u16) -> Self {
        write_u16_be(&mut self.data, value);
        self
    }

    /// Append a u16 (little-endian) to the packet data.
    pub fn write_u16_le(mut self, value: u16) -> Self {
        write_u16_le(&mut self.data, value);
        self
    }

    /// Append a u32 (big-endian) to the packet data.
    pub fn write_u32_be(mut self, value: u32) -> Self {
        write_u32_be(&mut self.data, value);
        self
    }

    /// Append a u32 (little-endian) to the packet data.
    pub fn write_u32_le(mut self, value: u32) -> Self {
        write_u32_le(&mut self.data, value);
        self
    }

    /// Append a u64 (little-endian) to the packet data.
    pub fn write_u64_le(mut self, value: u64) -> Self {
        write_u64_le(&mut self.data, value);
        self
    }

    /// Append a varchar (1-byte length prefix) to the packet data.
    pub fn write_varchar(mut self, data: &[u8]) -> Self {
        write_varchar(&mut self.data, data);
        self
    }

    /// Append a fixed-length string to the packet data.
    pub fn write_fixed_string(mut self, data: &[u8], len: usize) -> Self {
        write_fixed_string(&mut self.data, data, len);
        self
    }

    /// Build the complete packet with header.
    pub fn build(self) -> Vec<u8> {
        let header = TdsHeader {
            packet_type: self.packet_type,
            status: self.status,
            length: (HEADER_SIZE + self.data.len()) as u16,
            spid: self.spid,
            packet_number: self.packet_number,
            window: self.window,
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

// ============================================================================
// LoginBuilder
// ============================================================================

/// Builder for TDS 4.2/5.0 login packets.
pub struct LoginBuilder {
    hostname: Vec<u8>,
    username: Vec<u8>,
    password: Vec<u8>,
    app_name: Vec<u8>,
    server_name: Vec<u8>,
    library_name: Vec<u8>,
    language: Vec<u8>,
    charset: Vec<u8>,
    packet_size: u32,
    tds_version: u32,
}

impl LoginBuilder {
    /// Create a new login builder with TDS 5.0 version.
    pub fn new() -> Self {
        Self {
            hostname: Vec::new(),
            username: Vec::new(),
            password: Vec::new(),
            app_name: Vec::new(),
            server_name: Vec::new(),
            library_name: b"sybase-wire".to_vec(),
            language: Vec::new(),
            charset: Vec::new(),
            packet_size: 512,
            tds_version: 0x05000000, // TDS 5.0
        }
    }

    /// Set the hostname.
    pub fn hostname(mut self, hostname: &[u8]) -> Self {
        self.hostname = hostname.to_vec();
        self
    }

    /// Set the username.
    pub fn username(mut self, username: &[u8]) -> Self {
        self.username = username.to_vec();
        self
    }

    /// Set the password.
    pub fn password(mut self, password: &[u8]) -> Self {
        self.password = password.to_vec();
        self
    }

    /// Set the application name.
    pub fn app_name(mut self, app_name: &[u8]) -> Self {
        self.app_name = app_name.to_vec();
        self
    }

    /// Set the server name.
    pub fn server_name(mut self, server_name: &[u8]) -> Self {
        self.server_name = server_name.to_vec();
        self
    }

    /// Set the library name.
    pub fn library_name(mut self, library_name: &[u8]) -> Self {
        self.library_name = library_name.to_vec();
        self
    }

    /// Set the language.
    pub fn language(mut self, language: &[u8]) -> Self {
        self.language = language.to_vec();
        self
    }

    /// Set the character set.
    pub fn charset(mut self, charset: &[u8]) -> Self {
        self.charset = charset.to_vec();
        self
    }

    /// Set the packet size.
    pub fn packet_size(mut self, packet_size: u32) -> Self {
        self.packet_size = packet_size;
        self
    }

    /// Set the TDS version.
    pub fn tds_version(mut self, version: u32) -> Self {
        self.tds_version = version;
        self
    }

    /// Build the login packet.
    ///
    /// TDS 5.0 login packet structure (simplified):
    /// - Fixed header fields
    /// - Variable-length strings with length prefixes
    pub fn build(self) -> Vec<u8> {
        let mut data = Vec::new();

        // Hostname (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.hostname, 30);
        data.push(self.hostname.len().min(30) as u8);

        // Username (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.username, 30);
        data.push(self.username.len().min(30) as u8);

        // Password (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.password, 30);
        data.push(self.password.len().min(30) as u8);

        // Host process ID (30 bytes max + 1 byte length) - use zeros
        write_fixed_string(&mut data, b"", 30);
        data.push(0);

        // Byte order: 2 = Intel (little-endian), 3 = big-endian
        data.push(0x02); // byte order
        data.push(0x01); // char type (ASCII)
        data.push(0x06); // float type (IEEE 754)
        data.push(0x0A); // date format (10 = YMD)
        data.push(0x09); // notify of use db
        data.push(0x01); // set lang
        data.push(0x01); // old secure login
        data.push(0x00); // encrypted password (not used in TDS 5.0)
        data.push(0x00); // spare bytes
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);

        // App name (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.app_name, 30);
        data.push(self.app_name.len().min(30) as u8);

        // Server name (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.server_name, 30);
        data.push(self.server_name.len().min(30) as u8);

        // Remote password (255 bytes max + 1 byte length) - not used
        data.push(0); // length of remote password
        write_fixed_string(&mut data, b"", 253);
        data.push(0); // remaining length

        // TDS version (4 bytes, big-endian)
        write_u32_be(&mut data, self.tds_version);

        // Library name (10 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.library_name, 10);
        data.push(self.library_name.len().min(10) as u8);

        // Program version (4 bytes)
        data.push(0x01); // major
        data.push(0x00); // minor
        data.push(0x00); // sub-minor
        data.push(0x00); // sub-sub-minor

        // Language (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.language, 30);
        data.push(self.language.len().min(30) as u8);

        // Notify of language change
        data.push(0x01);

        // Old secure login spare
        data.push(0x00);
        data.push(0x00);

        // Encrypted password placeholder (not used in TDS 5.0)
        data.push(0x00);

        // Charset (30 bytes max + 1 byte length)
        write_fixed_string(&mut data, &self.charset, 30);
        data.push(self.charset.len().min(30) as u8);

        // Set charset notify
        data.push(0x01);

        // Packet size as string (6 bytes + 1 byte length)
        let packet_size_str = format!("{}", self.packet_size);
        write_fixed_string(&mut data, packet_size_str.as_bytes(), 6);
        data.push(packet_size_str.len().min(6) as u8);

        // Spare bytes (4 bytes)
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);
        data.push(0x00);

        // Build packet with LOGIN type
        PacketBuilder::new(PacketType::Login).write_bytes(&data).build()
    }
}

impl Default for LoginBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// QueryBuilder
// ============================================================================

/// Builder for TDS query packets.
pub struct QueryBuilder {
    sql: Vec<u8>,
}

impl QueryBuilder {
    /// Create a new query builder.
    pub fn new() -> Self {
        Self { sql: Vec::new() }
    }

    /// Set the SQL query.
    pub fn sql(mut self, sql: &[u8]) -> Self {
        self.sql = sql.to_vec();
        self
    }

    /// Build the query packet.
    pub fn build(self) -> Vec<u8> {
        PacketBuilder::new(PacketType::Query).write_bytes(&self.sql).build()
    }
}

impl Default for QueryBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// CancelBuilder
// ============================================================================

/// Builder for TDS cancel/attention packets.
pub struct CancelBuilder;

impl CancelBuilder {
    /// Create a new cancel builder.
    pub fn new() -> Self {
        Self
    }

    /// Build the cancel packet.
    pub fn build(self) -> Vec<u8> {
        // Cancel packets are just a header with no data
        PacketBuilder::new(PacketType::Cancel).build()
    }
}

impl Default for CancelBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// SybaseWrite implementations
// ============================================================================

impl SybaseWrite for TdsHeader {
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
    use crate::error::packet_types;

    #[test]
    fn test_write_u16_be() {
        let mut buf = Vec::new();
        write_u16_be(&mut buf, 0x1234);
        assert_eq!(buf, &[0x12, 0x34]);
    }

    #[test]
    fn test_write_u16_le() {
        let mut buf = Vec::new();
        write_u16_le(&mut buf, 0x1234);
        assert_eq!(buf, &[0x34, 0x12]);
    }

    #[test]
    fn test_write_u32_be() {
        let mut buf = Vec::new();
        write_u32_be(&mut buf, 0x12345678);
        assert_eq!(buf, &[0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_write_u32_le() {
        let mut buf = Vec::new();
        write_u32_le(&mut buf, 0x12345678);
        assert_eq!(buf, &[0x78, 0x56, 0x34, 0x12]);
    }

    #[test]
    fn test_write_varchar() {
        let mut buf = Vec::new();
        write_varchar(&mut buf, b"test");
        assert_eq!(buf, &[4, b't', b'e', b's', b't']);
    }

    #[test]
    fn test_write_fixed_string() {
        let mut buf = Vec::new();
        write_fixed_string(&mut buf, b"hi", 5);
        assert_eq!(buf, &[b'h', b'i', 0, 0, 0]);
    }

    #[test]
    fn test_packet_builder_query() {
        let packet = PacketBuilder::new(PacketType::Query).write_bytes(b"SELECT 1").build();

        // Header (8 bytes) + data (8 bytes) = 16 bytes
        assert_eq!(packet.len(), 16);
        // Packet type
        assert_eq!(packet[0], packet_types::QUERY);
        // Status (EOM)
        assert_eq!(packet[1], 0x01);
        // Length (big-endian)
        assert_eq!(&packet[2..4], &[0x00, 0x10]); // 16
    }

    #[test]
    fn test_query_builder() {
        let packet = QueryBuilder::new().sql(b"SELECT * FROM users").build();

        assert!(!packet.is_empty());
        assert_eq!(packet[0], packet_types::QUERY);
    }

    #[test]
    fn test_cancel_builder() {
        let packet = CancelBuilder::new().build();

        // Header only, no data
        assert_eq!(packet.len(), HEADER_SIZE);
        assert_eq!(packet[0], packet_types::CANCEL);
    }

    #[test]
    fn test_login_builder() {
        let packet = LoginBuilder::new().hostname(b"client").username(b"sa").password(b"password").app_name(b"test-app").build();

        assert!(!packet.is_empty());
        assert_eq!(packet[0], packet_types::LOGIN);
    }
}
