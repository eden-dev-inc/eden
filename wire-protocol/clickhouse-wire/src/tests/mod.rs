//! Integration tests for clickhouse-wire.

#[cfg(test)]
mod integration {
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use crate::native::client::ClientHello;
    use crate::native::packet::{ClientPacketType, ServerPacketType};
    use crate::native::read::ClickhouseReadSyncExt;
    use wire_stream::SliceStream;

    #[test]
    fn test_full_client_hello_flow() {
        // Create a client hello
        let hello = ClientHello::new("testdb", "testuser", "testpass");

        // Encode it
        let mut buf = Vec::new();
        hello.encode(&mut buf).unwrap();

        // Verify packet type
        let stream = SliceStream::new(&buf);
        let packet_type = stream.read_varuint_sync().unwrap();
        assert_eq!(ClientPacketType::from_u64(packet_type), Some(ClientPacketType::Hello));

        // Parse the hello
        let decoded = ClientHello::parse_sync(&stream).unwrap();

        assert_eq!(decoded.database, "testdb");
        assert_eq!(decoded.user, "testuser");
        assert_eq!(decoded.password, "testpass");
        assert_eq!(decoded.protocol_version, DBMS_TCP_PROTOCOL_VERSION);
    }

    #[test]
    fn test_packet_type_identification() {
        use crate::native::client::{Cancel, Ping};

        // Test Ping
        let mut buf = Vec::new();
        Ping::new().encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let packet_type = stream.read_varuint_sync().unwrap();
        assert_eq!(ClientPacketType::from_u64(packet_type), Some(ClientPacketType::Ping));

        // Test Cancel
        let mut buf = Vec::new();
        Cancel::new().encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let packet_type = stream.read_varuint_sync().unwrap();
        assert_eq!(ClientPacketType::from_u64(packet_type), Some(ClientPacketType::Cancel));
    }

    #[test]
    fn test_server_packet_types() {
        assert!(ServerPacketType::Data.is_data());
        assert!(ServerPacketType::Totals.is_data());
        assert!(!ServerPacketType::Hello.is_data());

        assert!(ServerPacketType::Exception.is_error());
        assert!(!ServerPacketType::Data.is_error());

        assert!(ServerPacketType::EndOfStream.is_end());
        assert!(!ServerPacketType::Progress.is_end());
    }
}

#[cfg(all(test, feature = "lz4"))]
mod compression_tests {
    use crate::native::compression::{CompressedBlock, CompressionMethod};

    #[test]
    fn test_lz4_roundtrip() {
        let data = b"This is a test message that should be compressed and decompressed correctly.";

        let compressed = CompressedBlock::compress(data, CompressionMethod::Lz4).unwrap();
        assert!(compressed.method == CompressionMethod::Lz4);
        assert!(compressed.decompressed_size == data.len() as u32);

        let decompressed = compressed.decompress().unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_no_compression_roundtrip() {
        let data = b"Small data";

        let compressed = CompressedBlock::compress(data, CompressionMethod::None).unwrap();
        assert!(compressed.method == CompressionMethod::None);
        assert_eq!(compressed.data, data);

        let decompressed = compressed.decompress().unwrap();
        assert_eq!(decompressed, data);
    }
}

#[cfg(test)]
mod http_tests {
    use crate::http::{ClickhouseRequestHeaders, HttpProgress, QueryParams};

    #[test]
    fn test_headers_and_params_combined() {
        let headers = ClickhouseRequestHeaders::new().with_database("mydb").with_user("admin");

        let params = QueryParams::with_query("SELECT 1").format("JSON");

        // In real usage, database from headers would override params
        assert_eq!(headers.database, Some("mydb".to_string()));
        assert_eq!(params.format, Some("JSON".to_string()));
    }

    #[test]
    fn test_progress_tracking() {
        let mut total = HttpProgress::new();

        let update1 = HttpProgress {
            read_rows: 100,
            read_bytes: 5000,
            total_rows_to_read: 1000,
            ..Default::default()
        };
        total.accumulate(&update1);

        let update2 = HttpProgress { read_rows: 150, read_bytes: 7500, ..Default::default() };
        total.accumulate(&update2);

        assert_eq!(total.read_rows, 250);
        assert_eq!(total.read_bytes, 12500);
        assert_eq!(total.total_rows_to_read, 1000);

        // 250 out of 1000 = 25%
        let percent = total.completion_percent().unwrap();
        assert!((percent - 25.0).abs() < f64::EPSILON);
    }
}
