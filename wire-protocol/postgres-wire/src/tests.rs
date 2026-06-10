//! Integration tests for the postgres-wire crate.

use crate::PgParseSync;
use crate::types::negotiate::NegotiateProtocolVersion;
use crate::types::unknown::UnknownMessage;
use crate::types::*;
use wire_stream::SliceStream;

#[test]
fn test_startup_flow() {
    // Simulate a startup message
    let startup = StartupMessage::new(vec![
        ("user".to_string(), "postgres".to_string()),
        ("database".to_string(), "mydb".to_string()),
        ("application_name".to_string(), "test".to_string()),
    ]);

    let encoded = startup.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = StartupMessage::parse_sync(&stream).expect("parse failed");

    assert_eq!(decoded.protocol_version, StartupMessage::PROTOCOL_VERSION_3_0);
    assert_eq!(decoded.user(), Some("postgres"));
    assert_eq!(decoded.database(), Some("mydb"));
    assert_eq!(decoded.get_parameter("application_name"), Some("test"));
}

#[test]
fn test_authentication_flow() {
    // AuthenticationOk
    let auth_ok = AuthenticationRequest::Ok;
    let encoded = auth_ok.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = AuthenticationRequest::parse_sync(&stream).expect("parse failed");
    assert!(decoded.is_ok());

    // AuthenticationMD5Password
    let salt = [0x01, 0x02, 0x03, 0x04];
    let auth_md5 = AuthenticationRequest::MD5Password { salt };
    let encoded = auth_md5.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = AuthenticationRequest::parse_sync(&stream).expect("parse failed");
    match decoded {
        AuthenticationRequest::MD5Password { salt: s } => assert_eq!(s, salt),
        _ => panic!("wrong auth type"),
    }
}

#[test]
fn test_simple_query_flow() {
    // Query
    let query = Query::new("SELECT id, name FROM users WHERE active = true");
    let encoded = query.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Query::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.query, "SELECT id, name FROM users WHERE active = true");

    // RowDescription
    let row_desc = RowDescription::new(vec![
        FieldDescription {
            name: "id".to_string(),
            table_oid: 16384,
            column_id: 1,
            type_oid: 23, // INT4
            type_size: 4,
            type_modifier: -1,
            format_code: 0,
        },
        FieldDescription {
            name: "name".to_string(),
            table_oid: 16384,
            column_id: 2,
            type_oid: 25, // TEXT
            type_size: -1,
            type_modifier: -1,
            format_code: 0,
        },
    ]);
    let encoded = row_desc.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = RowDescription::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.get_by_name("id").map(|f| f.type_oid), Some(23));

    // DataRow
    let data_row = DataRow::new(vec![ColumnValue::Value(b"42".to_vec()), ColumnValue::Value(b"Alice".to_vec())]);
    let encoded = data_row.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = DataRow::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.len(), 2);
    assert_eq!(decoded.columns[0].as_str(), Some("42"));
    assert_eq!(decoded.columns[1].as_str(), Some("Alice"));

    // CommandComplete
    let cmd = CommandComplete::select(100);
    let encoded = cmd.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = CommandComplete::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.command(), "SELECT");
    assert_eq!(decoded.row_count(), Some(100));

    // ReadyForQuery
    let ready = ReadyForQuery::idle();
    let encoded = ready.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = ReadyForQuery::parse_sync(&stream).expect("parse failed");
    assert!(decoded.status.is_idle());
}

#[test]
fn test_error_response() {
    let error = ErrorResponse::simple("ERROR", "42P01", "relation \"foo\" does not exist");
    let encoded = error.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = ErrorResponse::parse_sync(&stream).expect("parse failed");

    assert!(decoded.fields.is_error());
    assert_eq!(decoded.code(), Some("42P01"));
    assert_eq!(decoded.message(), Some("relation \"foo\" does not exist"));
}

#[test]
fn test_extended_query_protocol() {
    // Parse
    let parse = Parse::new("stmt1", "SELECT $1::int + $2::int", vec![23, 23]);
    let encoded = parse.encode();
    assert_eq!(encoded[0], b'P');

    // Bind
    let bind = Bind::new_text("", "stmt1", vec![Some(b"10".to_vec()), Some(b"20".to_vec())]);
    let encoded = bind.encode();
    assert_eq!(encoded[0], b'B');

    // Execute
    let exec = Execute::unnamed();
    let encoded = exec.encode();
    assert_eq!(encoded[0], b'E');

    // Sync
    let sync = Sync::encode();
    assert_eq!(sync[0], b'S');

    // ParseComplete
    let encoded = ParseComplete::encode();
    let stream = SliceStream::new(&encoded);
    let _decoded = ParseComplete::parse_sync(&stream).expect("parse failed");

    // BindComplete
    let encoded = BindComplete::encode();
    let stream = SliceStream::new(&encoded);
    let _decoded = BindComplete::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_copy_protocol() {
    // CopyInResponse
    let copy_in = CopyInResponse::new(FormatCode::Text, vec![FormatCode::Text, FormatCode::Text, FormatCode::Text]);
    let encoded = copy_in.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = CopyInResponse::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.format, FormatCode::Text);
    assert_eq!(decoded.column_formats.len(), 3);

    // CopyData
    let data = CopyData::new(b"1\tAlice\t30\n2\tBob\t25\n".to_vec());
    let encoded = data.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = CopyData::parse_sync(&stream).expect("parse failed");
    assert!(decoded.data.starts_with(b"1\tAlice"));

    // CopyDone
    let encoded = CopyDone::encode();
    let stream = SliceStream::new(&encoded);
    let _decoded = CopyDone::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_ssl_request() {
    let encoded = SSLRequest::encode();
    assert_eq!(encoded.len(), 8);

    let stream = SliceStream::new(&encoded);
    let _decoded = SSLRequest::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_cancel_request() {
    let cancel = CancelRequest::new(12345, 67890);
    let encoded = cancel.encode();
    assert_eq!(encoded.len(), 16);

    let stream = SliceStream::new(&encoded);
    let decoded = CancelRequest::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.process_id, 12345);
    assert_eq!(decoded.secret_key, 67890);
}

#[test]
fn test_parameter_status() {
    let params = [
        ("server_version", "14.5"),
        ("client_encoding", "UTF8"),
        ("DateStyle", "ISO, MDY"),
        ("TimeZone", "UTC"),
    ];

    for (name, value) in params {
        let status = ParameterStatus::new(name.to_string(), value.to_string());
        let encoded = status.encode();
        let stream = SliceStream::new(&encoded);
        let decoded = ParameterStatus::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.name, name);
        assert_eq!(decoded.value, value);
    }
}

#[test]
fn test_backend_key_data() {
    let key_data = BackendKeyData::new(1234, 5678);
    let encoded = key_data.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = BackendKeyData::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.process_id, 1234);
    assert_eq!(decoded.secret_key, 5678);
}

#[test]
fn test_null_handling_in_data_row() {
    let row = DataRow::new(vec![
        ColumnValue::Value(b"1".to_vec()),
        ColumnValue::Null,
        ColumnValue::Value(b"hello".to_vec()),
        ColumnValue::Null,
    ]);

    let encoded = row.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = DataRow::parse_sync(&stream).expect("parse failed");

    assert_eq!(decoded.len(), 4);
    assert!(!decoded.columns[0].is_null());
    assert!(decoded.columns[1].is_null());
    assert!(!decoded.columns[2].is_null());
    assert!(decoded.columns[3].is_null());
}

#[test]
fn test_terminate_message() {
    let encoded = Terminate::encode();
    assert_eq!(encoded[0], b'X');
    assert_eq!(encoded.len(), 5);

    let stream = SliceStream::new(&encoded);
    let _decoded = Terminate::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_empty_query_response() {
    let encoded = EmptyQueryResponse::encode();
    assert_eq!(encoded[0], b'I');

    let stream = SliceStream::new(&encoded);
    let _decoded = EmptyQueryResponse::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_notification_response() {
    let notification = NotificationResponse::new(9999, "events".to_string(), "{\"type\": \"update\"}".to_string());
    let encoded = notification.encode();
    assert_eq!(encoded[0], b'A');

    let stream = SliceStream::new(&encoded);
    let decoded = NotificationResponse::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.process_id, 9999);
    assert_eq!(decoded.channel, "events");
    assert_eq!(decoded.payload, "{\"type\": \"update\"}");
}

#[test]
fn test_gssenc_request() {
    let encoded = GSSEncRequest::encode();
    assert_eq!(encoded.len(), 8);

    let stream = SliceStream::new(&encoded);
    let _decoded = GSSEncRequest::parse_sync(&stream).expect("parse failed");
}

#[test]
fn test_negotiate_protocol_version() {
    // Test with no unrecognized options
    let msg = NegotiateProtocolVersion::new(0, vec![]);
    let encoded = msg.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = NegotiateProtocolVersion::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.newest_minor_version, 0);
    assert!(decoded.unrecognized_options.is_empty());

    // Test with unrecognized options (simulating PG 14+ protocol features)
    let msg = NegotiateProtocolVersion::new(1, vec!["_pq_.async_password".to_string()]);
    let encoded = msg.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = NegotiateProtocolVersion::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.newest_minor_version, 1);
    assert_eq!(decoded.unrecognized_options.len(), 1);
    assert_eq!(decoded.unrecognized_options[0], "_pq_.async_password");
}

#[test]
fn test_copy_both_response() {
    // CopyBothResponse is used for streaming replication (PG 9.0+)
    let copy_both = CopyBothResponse::new(FormatCode::Binary, vec![FormatCode::Binary]);
    let encoded = copy_both.encode();
    assert_eq!(encoded[0], b'W');

    let stream = SliceStream::new(&encoded);
    let decoded = CopyBothResponse::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.format, FormatCode::Binary);
    assert_eq!(decoded.column_formats.len(), 1);
}

#[test]
fn test_unknown_message_handling() {
    // Create a message with an unknown type byte
    let data = [
        b'?', // Unknown type
        0, 0, 0, 8, // Length = 8
        0xDE, 0xAD, 0xBE, 0xEF, // Payload
    ];

    let stream = SliceStream::new(&data);
    let msg = UnknownMessage::parse_sync(&stream).expect("parse failed");

    assert_eq!(msg.message_type, b'?');
    assert_eq!(msg.payload, vec![0xDE, 0xAD, 0xBE, 0xEF]);
}

#[test]
fn test_message_category() {
    use crate::types::MessageCategory;

    // Test known message types
    assert_eq!(MessageCategory::from_type_byte(b'R'), MessageCategory::Authentication);
    assert_eq!(MessageCategory::from_type_byte(b'E'), MessageCategory::Error);
    assert_eq!(MessageCategory::from_type_byte(b'Z'), MessageCategory::ReadyForQuery);
    assert_eq!(MessageCategory::from_type_byte(b'v'), MessageCategory::NegotiateProtocolVersion);
    assert_eq!(MessageCategory::from_type_byte(b'W'), MessageCategory::CopyBothResponse);

    // Test unknown type
    let unknown = MessageCategory::from_type_byte(b'?');
    assert!(unknown.is_unknown());

    // Test skippable messages (for lenient handling)
    assert!(MessageCategory::Notice.is_skippable());
    assert!(!MessageCategory::Error.is_skippable());
}

#[test]
fn test_frontend_message_roundtrip() {
    // Test Parse message roundtrip
    let parse = Parse::new("my_stmt", "SELECT $1, $2", vec![23, 25]);
    let encoded = parse.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Parse::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.name, "my_stmt");
    assert_eq!(decoded.query, "SELECT $1, $2");
    assert_eq!(decoded.param_types, vec![23, 25]);

    // Test Bind message roundtrip
    let bind = Bind::new_text("my_portal", "my_stmt", vec![Some(b"42".to_vec()), None, Some(b"hello".to_vec())]);
    let encoded = bind.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Bind::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.portal, "my_portal");
    assert_eq!(decoded.statement, "my_stmt");
    assert_eq!(decoded.param_values.len(), 3);
    assert_eq!(decoded.param_values[0], Some(b"42".to_vec()));
    assert_eq!(decoded.param_values[1], None);
    assert_eq!(decoded.param_values[2], Some(b"hello".to_vec()));

    // Test Describe message roundtrip
    let describe = Describe::statement("my_stmt");
    let encoded = describe.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Describe::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.kind, b'S');
    assert_eq!(decoded.name, "my_stmt");

    // Test Execute message roundtrip
    let execute = Execute::named("my_portal", 100);
    let encoded = execute.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Execute::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.portal, "my_portal");
    assert_eq!(decoded.max_rows, 100);

    // Test Close message roundtrip
    let close = Close::portal("my_portal");
    let encoded = close.encode();
    let stream = SliceStream::new(&encoded);
    let decoded = Close::parse_sync(&stream).expect("parse failed");
    assert_eq!(decoded.kind, b'P');
    assert_eq!(decoded.name, "my_portal");

    // Test Sync message roundtrip
    let encoded = Sync::encode();
    let stream = SliceStream::new(&encoded);
    let _decoded = Sync::parse_sync(&stream).expect("parse failed");

    // Test Flush message roundtrip
    let encoded = Flush::encode();
    let stream = SliceStream::new(&encoded);
    let _decoded = Flush::parse_sync(&stream).expect("parse failed");
}
