//! Integration tests for mysql-wire crate.

mod lib_tests {
    use crate::mysql_ext::MysqlReadSync;
    use wire_stream::SliceStream;

    #[test]
    fn test_read_u8() {
        let data = [0x42];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_u8_sync().unwrap(), 0x42);
    }

    #[test]
    fn test_read_u16_le() {
        let data = [0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_u16_le_sync().unwrap(), 0x1234);
    }

    #[test]
    fn test_read_u24_le() {
        let data = [0x56, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_u24_le_sync().unwrap(), 0x123456);
    }

    #[test]
    fn test_read_u32_le() {
        let data = [0x78, 0x56, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_u32_le_sync().unwrap(), 0x12345678);
    }

    #[test]
    fn test_read_u64_le() {
        let data = [0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_u64_le_sync().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_read_lenenc_int_1byte() {
        let data = [42];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_lenenc_int_sync().unwrap().unwrap(), 42);
    }

    #[test]
    fn test_read_lenenc_int_2byte() {
        let data = [0xFC, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_lenenc_int_sync().unwrap().unwrap(), 0x1234);
    }

    #[test]
    fn test_read_lenenc_int_3byte() {
        let data = [0xFD, 0x56, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_lenenc_int_sync().unwrap().unwrap(), 0x123456);
    }

    #[test]
    fn test_read_lenenc_int_8byte() {
        let data = [0xFE, 0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_lenenc_int_sync().unwrap().unwrap(), 0x123456789ABCDEF0);
    }

    #[test]
    fn test_read_lenenc_int_null() {
        let data = [0xFB];
        let stream = SliceStream::new(&data);
        assert_eq!(stream.read_lenenc_int_sync().unwrap().unwrap(), u64::MAX);
    }

    #[test]
    fn test_read_packet_header() {
        let data = [0x05, 0x00, 0x00, 0x03]; // length=5, seq=3
        let stream = SliceStream::new(&data);
        let (len, seq) = stream.read_packet_header_sync().unwrap();
        assert_eq!(len, 5);
        assert_eq!(seq, 3);
    }

    #[test]
    fn test_read_cstring() {
        let data = b"hello\0world";
        let stream = SliceStream::new(data);
        let s = stream.read_cstring_sync().unwrap();
        assert_eq!(s, b"hello");
    }
}

mod integration_tests {
    use crate::capabilities::CapabilityFlags;
    use crate::parse::MysqlParseSync;
    use crate::types::command::Command;
    use crate::types::dynamic::MysqlPacket;
    use crate::types::packet::MysqlPacketHeader;
    use crate::write::{PacketBuilder, build_query_packet};
    use wire_stream::SliceStream;

    #[test]
    fn test_full_query_flow() {
        // Build a query packet
        let packet = build_query_packet(0, "SELECT 1");

        // Parse the header
        let stream = SliceStream::new(&packet);
        let header = MysqlPacketHeader::parse_sync(&stream).unwrap();

        assert_eq!(header.sequence_id, 0);
        assert_eq!(header.payload_length as usize, packet.len() - 4);

        // Parse the command
        let cmd = Command::parse_sync(&stream).unwrap();
        match cmd {
            Command::Query { query } => assert_eq!(query, "SELECT 1"),
            _ => panic!("Expected Query command"),
        }
    }

    #[test]
    fn test_packet_builder_chain() {
        let packet = PacketBuilder::new(5)
            .write_u8(0x03) // COM_QUERY
            .write_bytes(b"SELECT * FROM users WHERE id = ")
            .write_lenenc_int(42)
            .build();

        // Verify header
        let stream = SliceStream::new(&packet);
        let header = MysqlPacketHeader::parse_sync(&stream).unwrap();
        assert_eq!(header.sequence_id, 5);
    }

    #[test]
    fn test_capabilities_negotiation() {
        let server = CapabilityFlags::client_default_8x() | CapabilityFlags::SSL;
        let client = CapabilityFlags::client_default_5x(); // No DEPRECATE_EOF

        let negotiated = CapabilityFlags::negotiate(client, server);

        // Both support PROTOCOL_41
        assert!(negotiated.supports_41());
        // Client doesn't support DEPRECATE_EOF
        assert!(!negotiated.deprecate_eof());
        // Client doesn't request SSL
        assert!(!negotiated.supports_ssl());
    }

    #[test]
    fn test_parse_simulated_ok_response() {
        // Simulate OK packet after query
        let data = [
            0x00, // OK header
            0x00, // affected_rows = 0
            0x00, // last_insert_id = 0
            0x02, 0x00, // status = AUTOCOMMIT
            0x00, 0x00, // warnings = 0
        ];

        let stream = SliceStream::new(&data);
        let packet = MysqlPacket::parse_response_sync(&stream, data.len()).unwrap();

        assert!(packet.is_ok());
        let ok = packet.as_ok().unwrap();
        assert!(ok.autocommit());
    }

    #[test]
    fn test_error_conversion() {
        let data = [
            0xFF, // ERR header
            0x15, 0x04, // error code 1045
            b'#', b'2', b'8', b'0', b'0', b'0', // SQL state
            b'A', b'c', b'c', b'e', b's', b's', b' ', b'd', b'e', b'n', b'i', b'e', b'd', // message
        ];

        let stream = SliceStream::new(&data);
        let packet = MysqlPacket::parse_response_sync(&stream, data.len()).unwrap();

        // Convert to Result
        let result = packet.into_result();
        assert!(result.is_err());

        let err = result.unwrap_err();
        assert_eq!(err.error_code, 1045);
    }
}
