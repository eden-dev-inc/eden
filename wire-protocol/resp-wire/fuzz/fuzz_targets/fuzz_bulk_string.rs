//! Fuzz target for RESP Bulk String parsing.
//!
//! Tests bulk string parsing with arbitrary byte sequences to find:
//! - Length parsing overflow
//! - Null bulk string handling
//! - CRLF termination validation
//!
//! Run with memory limit to handle large length fields:
//!   cargo +nightly fuzz run fuzz_bulk_string -- -rss_limit_mb=512

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::types::bulk_string::{BulkString, BulkStringValue};
use resp_wire::RespParseSync;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    match BulkString::parse_sync(&stream) {
        Ok(BulkStringValue::Null) => {
            // Null bulk string parsed successfully
        }
        Ok(BulkStringValue::String(mut reader)) => {
            // Exercise the reader
            let _ = reader.remaining();

            // Read all chunks
            let mut total_len = 0;
            while let Ok(Some(chunk)) = reader.next_sync() {
                total_len += chunk.len();
                // Limit to prevent memory issues
                if total_len > 1024 * 1024 {
                    break;
                }
            }
        }
        Err(_) => {
            // Errors are expected for malformed input
        }
    }
});
