//! Fuzz target for Dynamic RESP parser.
//!
//! Tests the Dynamic parser which handles all RESP2 and RESP3 types.
//! This is the primary fuzz target as it exercises the full protocol.
//!
//! Run with memory limit to handle large RESP length fields:
//!   cargo +nightly fuzz run fuzz_dynamic -- -rss_limit_mb=512
//!
//! Looking for:
//! - Panics from malformed input
//! - Integer overflow in length calculations
//! - Stack overflow from deeply nested structures

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::types::dynamic::Dynamic;
use resp_wire::RespParseSync;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    if let Ok(value) = Dynamic::parse_sync(&stream) {
        // Exercise Debug formatting to ensure no panics in display logic
        let _ = format!("{:?}", value);

        // Verify we can match on all variants without panic
        match &value {
            Dynamic::SimpleString { value } => {
                let _ = value.len();
            }
            Dynamic::SimpleError { value } => {
                let _ = value.len();
            }
            Dynamic::Integer { value } => {
                let _ = *value;
            }
            Dynamic::NullBulkString => {}
            Dynamic::BulkString { value } => {
                let _ = value.len();
            }
            Dynamic::NullArray => {}
            Dynamic::Array { elements } => {
                let _ = elements.len();
            }
            Dynamic::Null => {}
            Dynamic::Boolean { value } => {
                let _ = *value;
            }
            Dynamic::Double { value } => {
                let _ = *value;
            }
            Dynamic::Bignum { value } => {
                let _ = value.len();
            }
            Dynamic::BulkError { value } => {
                let _ = value.len();
            }
            Dynamic::VerbatimString { encoding, value } => {
                let _ = encoding;
                let _ = value.len();
            }
            Dynamic::Map { entries } => {
                let _ = entries.len();
            }
            Dynamic::Attributes { entries } => {
                let _ = entries.len();
            }
            Dynamic::Set { items } => {
                let _ = items.len();
            }
            Dynamic::Push { elements } => {
                let _ = elements.len();
            }
        }
    }
    // Errors are expected for malformed input - no panic means success
});
