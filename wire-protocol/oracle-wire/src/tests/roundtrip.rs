//! Roundtrip tests for parsing and encoding.

use crate::types::data::{Data, DataFlags};
use crate::types::marker::Marker;
use crate::types::redirect::Redirect;
use crate::types::tti::message::{TtiMessage, TtiRequest};
use crate::write::OracleWrite;

#[test]
fn test_data_roundtrip() {
    let original = Data::with_flags(
        b"test payload data".to_vec(),
        DataFlags::from_raw(0x0040), // EOF flag
    );

    let bytes = original.to_bytes();

    // First 2 bytes are flags, rest is payload
    assert_eq!(&bytes[0..2], &[0x00, 0x40]);
    assert_eq!(&bytes[2..], b"test payload data");
}

#[test]
fn test_marker_roundtrip() {
    let break_marker = Marker::break_marker();
    let bytes = break_marker.to_bytes();
    assert_eq!(bytes, vec![0x01, 0x00]); // BREAK type, data=0

    let reset_marker = Marker::reset_marker();
    let bytes = reset_marker.to_bytes();
    assert_eq!(bytes, vec![0x02, 0x00]); // RESET type, data=0
}

#[test]
fn test_redirect_roundtrip() {
    let address = b"(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.1)(PORT=1521))";
    let redirect = Redirect::new(address.to_vec());

    let bytes = redirect.to_bytes();

    // First 2 bytes are length
    let len = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
    assert_eq!(len, address.len());

    // Rest is the address data
    assert_eq!(&bytes[2..], address);
}

#[test]
fn test_redirect_parse_address() {
    let redirect = Redirect::new(b"(ADDRESS=(PROTOCOL=tcp)(HOST=db.example.com)(PORT=1522))".to_vec());

    let (host, port) = redirect.parse_address().unwrap();
    assert_eq!(host, "db.example.com");
    assert_eq!(port, 1522);
}

#[test]
fn test_tti_message_roundtrip() {
    let original = TtiMessage::from_data_payload(&[0x04, 0x01, 0xAB, 0xCD]).unwrap();

    assert_eq!(original.function_code.as_u8(), 0x04); // Commit
    assert_eq!(original.sequence_number, 0x01);
    assert_eq!(original.payload, vec![0xAB, 0xCD]);

    let bytes = original.to_bytes();
    assert_eq!(bytes, vec![0x04, 0x01, 0xAB, 0xCD]);
}

#[test]
fn test_tti_request_builders() {
    // Commit
    let commit = TtiRequest::commit();
    assert!(commit.is_request());
    assert!(!commit.is_response());

    // Rollback
    let rollback = TtiRequest::rollback();
    assert!(rollback.is_request());

    // Version
    let version = TtiRequest::version();
    assert!(version.is_request());

    // Protocol negotiation
    let proto = TtiRequest::protocol_negotiation(12);
    let bytes = proto.to_bytes();
    // Function code + sequence + version (2 bytes)
    assert!(bytes.len() >= 4);
}

#[test]
fn test_data_flags() {
    let flags = DataFlags::from_raw(0x0041); // SEND_TOKEN | EOF

    assert!(flags.send_token());
    assert!(flags.eof());
    assert!(!flags.more_data());
    assert!(!flags.reset());
    assert!(!flags.request_to_send());

    assert_eq!(flags.raw(), 0x0041);
}

#[test]
fn test_data_is_final() {
    // EOF flag set
    let data_eof = Data::with_flags(vec![], DataFlags::from_raw(0x0040));
    assert!(data_eof.is_final());

    // MORE_DATA flag NOT set (so it's final)
    let data_no_more = Data::with_flags(vec![], DataFlags::from_raw(0x0000));
    assert!(data_no_more.is_final());

    // MORE_DATA flag set (not final)
    let data_more = Data::with_flags(vec![], DataFlags::from_raw(0x0020));
    assert!(!data_more.is_final());
}
