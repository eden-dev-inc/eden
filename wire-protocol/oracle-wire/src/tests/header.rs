//! Tests for TNS header parsing and encoding.

use crate::types::packet::{HEADER_SIZE, PacketType, TnsHeader};
use crate::write::OracleWrite;

#[test]
fn test_header_roundtrip() {
    let original = TnsHeader {
        packet_length: 100,
        packet_checksum: 0,
        packet_type: PacketType::Data,
        flags: 0,
        header_checksum: 0,
    };

    let bytes = original.to_bytes();
    assert_eq!(bytes.len(), HEADER_SIZE);

    // Verify encoding
    assert_eq!(&bytes[0..2], &[0x00, 0x64]); // 100 in big-endian
    assert_eq!(bytes[4], 0x06); // DATA packet type
}

#[test]
fn test_header_oracle_write() {
    let header = TnsHeader::new(PacketType::Connect, 50);

    let mut buf = Vec::new();
    header.write_to(&mut buf).unwrap();

    assert_eq!(buf.len(), HEADER_SIZE);
    // Total length = header (8) + data (50) = 58
    assert_eq!(&buf[0..2], &[0x00, 0x3A]); // 58 in big-endian
    assert_eq!(buf[4], 0x01); // CONNECT packet type
}

#[test]
fn test_packet_types() {
    let test_cases = [
        (PacketType::Connect, 0x01),
        (PacketType::Accept, 0x02),
        (PacketType::Ack, 0x03),
        (PacketType::Refuse, 0x04),
        (PacketType::Redirect, 0x05),
        (PacketType::Data, 0x06),
        (PacketType::Null, 0x07),
        (PacketType::Abort, 0x09),
        (PacketType::Resend, 0x0B),
        (PacketType::Marker, 0x0C),
        (PacketType::Attention, 0x0D),
        (PacketType::Control, 0x0E),
        (PacketType::DataDescriptor, 0x0F),
    ];

    for (packet_type, expected_byte) in test_cases {
        assert_eq!(packet_type.as_u8(), expected_byte);
        assert_eq!(PacketType::from_u8(expected_byte), packet_type);
    }
}

#[test]
fn test_unknown_packet_type() {
    let unknown = PacketType::from_u8(0xFF);
    assert!(matches!(unknown, PacketType::Unknown(0xFF)));
    assert_eq!(unknown.as_u8(), 0xFF);
    assert_eq!(unknown.name(), "Unknown");
}

#[test]
fn test_data_length_calculation() {
    let header = TnsHeader {
        packet_length: 100,
        packet_checksum: 0,
        packet_type: PacketType::Data,
        flags: 0,
        header_checksum: 0,
    };

    // Data length = packet_length - header_size
    assert_eq!(header.data_length(), 100 - HEADER_SIZE as u16);
}

#[test]
fn test_header_new() {
    let header = TnsHeader::new(PacketType::Data, 50);

    assert_eq!(header.packet_length, HEADER_SIZE as u16 + 50);
    assert_eq!(header.packet_type, PacketType::Data);
    assert_eq!(header.flags, 0);
    assert_eq!(header.packet_checksum, 0);
    assert_eq!(header.header_checksum, 0);
}
