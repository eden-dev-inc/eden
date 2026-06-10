//! Unit tests for Sybase wire protocol.

use crate::error::packet_types;
use crate::limits::HEADER_SIZE;
use crate::parse::SybaseParseSync;
use crate::types::packet::{PacketType, TdsHeader, TdsPacket};
use crate::write::{LoginBuilder, PacketBuilder, QueryBuilder};
use wire_stream::SliceStream;

#[test]
fn test_packet_roundtrip() {
    // Build a query packet
    let packet = QueryBuilder::new().sql(b"SELECT 1").build();

    // Parse it back
    let stream = SliceStream::new(&packet);
    let parsed = TdsPacket::parse_sync(&stream).unwrap();

    assert_eq!(parsed.packet_type(), PacketType::Query);
    assert_eq!(parsed.payload, b"SELECT 1");
    assert!(parsed.is_eom());
}

#[test]
fn test_header_roundtrip() {
    let header = TdsHeader::new(PacketType::Query, 100);
    let bytes = header.to_bytes();

    let stream = SliceStream::new(&bytes);
    let parsed = TdsHeader::parse_sync(&stream).unwrap();

    assert_eq!(parsed.packet_type, header.packet_type);
    assert_eq!(parsed.length, header.length);
    assert_eq!(parsed.status, header.status);
}

#[test]
fn test_login_builder() {
    let packet = LoginBuilder::new()
        .hostname(b"testhost")
        .username(b"testuser")
        .password(b"testpass")
        .app_name(b"testapp")
        .server_name(b"testserver")
        .packet_size(512)
        .build();

    // Verify it's a valid TDS packet
    assert!(packet.len() > HEADER_SIZE);
    assert_eq!(packet[0], packet_types::LOGIN);

    // Parse the header
    let stream = SliceStream::new(&packet[..HEADER_SIZE]);
    let header = TdsHeader::parse_sync(&stream).unwrap();
    assert_eq!(header.packet_type, PacketType::Login);
}

#[test]
fn test_packet_builder_status() {
    // Test with EOM flag
    let packet = PacketBuilder::new(PacketType::Query).status(0x01).build();
    assert_eq!(packet[1], 0x01);

    // Test without EOM flag (more packets to come)
    let packet = PacketBuilder::new(PacketType::Query).status(0x00).build();
    assert_eq!(packet[1], 0x00);
}

#[test]
fn test_packet_builder_spid() {
    let packet = PacketBuilder::new(PacketType::Query).spid(0x1234).build();

    // SPID is bytes 4-5 (big-endian)
    assert_eq!(packet[4], 0x12);
    assert_eq!(packet[5], 0x34);
}

#[test]
fn test_packet_length_calculation() {
    let data = b"SELECT * FROM users WHERE id = 1";
    let packet = PacketBuilder::new(PacketType::Query).write_bytes(data).build();

    // Verify length field (bytes 2-3, big-endian)
    let expected_len = (HEADER_SIZE + data.len()) as u16;
    let actual_len = u16::from_be_bytes([packet[2], packet[3]]);
    assert_eq!(actual_len, expected_len);
}

#[test]
fn test_empty_payload() {
    let packet = PacketBuilder::new(PacketType::Cancel).build();

    assert_eq!(packet.len(), HEADER_SIZE);

    let stream = SliceStream::new(&packet);
    let parsed = TdsPacket::parse_sync(&stream).unwrap();

    assert_eq!(parsed.packet_type(), PacketType::Cancel);
    assert!(parsed.payload.is_empty());
}

// Token parsing tests
mod token_tests {
    use crate::error::token_types;
    use crate::types::done::Done;
    use crate::types::token::{Token, TokenStream};
    use wire_stream::SliceStream;

    #[test]
    fn test_done_token_parsing() {
        // DONE token: status=0x0010 (COUNT valid), curcmd=0x00C1, row_count=5
        let data = [
            0x10, 0x00, // status (COUNT flag set)
            0xC1, 0x00, // curcmd (SELECT)
            0x05, 0x00, 0x00, 0x00, // row count = 5
        ];

        let stream = SliceStream::new(&data);
        let done = Done::parse_after_token_sync(&stream).unwrap();

        assert_eq!(done.status, 0x0010);
        assert_eq!(done.row_count(), Some(5));
        assert!(!done.has_more());
        assert!(!done.has_error());
    }

    #[test]
    fn test_done_token_with_more_results() {
        // DONE token with DONE_MORE flag
        let data = [
            0x01, 0x00, // status (more results)
            0xC1, 0x00, // curcmd
            0x00, 0x00, 0x00, 0x00, // row count = 0
        ];

        let stream = SliceStream::new(&data);
        let done = Done::parse_after_token_sync(&stream).unwrap();

        assert!(done.has_more());
    }

    #[test]
    fn test_token_type_mapping() {
        // Test that Token enum correctly maps to token type bytes
        let done = Token::Done(Done { status: 0, cur_cmd: 0, done_row_count: 0 });
        assert_eq!(done.token_type(), token_types::DONE);

        let done_proc = Token::DoneProc(Done { status: 0, cur_cmd: 0, done_row_count: 0 });
        assert_eq!(done_proc.token_type(), token_types::DONEPROC);

        let done_in_proc = Token::DoneInProc(Done { status: 0, cur_cmd: 0, done_row_count: 0 });
        assert_eq!(done_in_proc.token_type(), token_types::DONEINPROC);
    }

    #[test]
    fn test_token_stream_new() {
        let stream = TokenStream::new();
        assert!(stream.current_columns.is_none());
    }

    #[test]
    fn test_return_status_token() {
        // Return status token is fixed length (4 bytes, little-endian i32)
        let data = [
            token_types::RETURNSTATUS,
            0x01,
            0x00,
            0x00,
            0x00, // status = 1
        ];

        let stream = SliceStream::new(&data);
        let mut token_stream = TokenStream::new();
        let token = token_stream.parse_next_sync(&stream).unwrap().unwrap();

        match token {
            Token::ReturnStatus(status) => assert_eq!(status, 1),
            _ => panic!("Expected ReturnStatus token"),
        }
    }
}

// Dynamic SQL tests
mod dynamic_tests {
    use crate::types::dynamic::{DynamicBuilder, DynamicOperation};

    #[test]
    fn test_dynamic_operation_values() {
        assert_eq!(DynamicOperation::Prepare as u8, 0x01);
        assert_eq!(DynamicOperation::Execute as u8, 0x02);
        assert_eq!(DynamicOperation::PrepExec as u8, 0x03);
        assert_eq!(DynamicOperation::Dealloc as u8, 0x04);
        assert_eq!(DynamicOperation::DescIn as u8, 0x08);
        assert_eq!(DynamicOperation::DescOut as u8, 0x10);
    }

    #[test]
    fn test_dynamic_operation_from_u8() {
        assert_eq!(DynamicOperation::from_u8(0x01), Some(DynamicOperation::Prepare));
        assert_eq!(DynamicOperation::from_u8(0x02), Some(DynamicOperation::Execute));
        assert_eq!(DynamicOperation::from_u8(0xFF), None);
    }

    #[test]
    fn test_dynamic_builder_prepare() {
        let packet = DynamicBuilder::prepare("stmt1", "SELECT * FROM users").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_dynamic_builder_execute() {
        let packet = DynamicBuilder::execute("stmt1").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_dynamic_builder_dealloc() {
        let packet = DynamicBuilder::dealloc("stmt1").build();
        assert!(!packet.is_empty());
    }
}

// Cursor tests
mod cursor_tests {
    use crate::types::cursor::CursorBuilder;

    #[test]
    fn test_cursor_declare() {
        let packet = CursorBuilder::declare("cur1", "SELECT * FROM users").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_cursor_open() {
        let packet = CursorBuilder::open("cur1").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_cursor_fetch() {
        let packet = CursorBuilder::fetch("cur1", 0x01).with_row_count(10).build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_cursor_close() {
        let packet = CursorBuilder::close("cur1").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_cursor_deallocate() {
        let packet = CursorBuilder::deallocate("cur1").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_cursor_with_options() {
        let packet = CursorBuilder::declare("cur1", "SELECT id FROM users")
            .with_options(0x0001) // SCROLLABLE
            .build();
        assert!(!packet.is_empty());
    }
}

// RPC tests
mod rpc_tests {
    use crate::types::rpc::RpcBuilder;

    #[test]
    fn test_rpc_builder() {
        let packet = RpcBuilder::new("sp_who").build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_rpc_with_int_param() {
        let packet = RpcBuilder::new("sp_adduser").add_int("userid", 100).build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_rpc_with_string_param() {
        let packet = RpcBuilder::new("sp_adduser").add_string("username", "testuser", 50).build();
        assert!(!packet.is_empty());
    }
}

// Capability tests
mod capability_tests {
    use crate::types::capability::CapabilityBuilder;

    #[test]
    fn test_capability_builder_request() {
        let packet = CapabilityBuilder::request().build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_capability_builder_response() {
        let packet = CapabilityBuilder::response().build();
        assert!(!packet.is_empty());
    }

    #[test]
    fn test_capability_with_features() {
        let packet = CapabilityBuilder::request().with_cursor().with_dynamic().with_rpc().build();
        assert!(!packet.is_empty());
    }
}
