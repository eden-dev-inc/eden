//! Fuzz target for OP_MSG parsing.
//!
//! Tests the OP_MSG parser with arbitrary byte sequences to find:
//! - Panics from unchecked array accesses
//! - Integer overflows/underflows
//! - Infinite loops
//! - Memory exhaustion from large allocations

#![no_main]

use libfuzzer_sys::fuzz_target;
use mongo_wire::{MessageHeader, OpMsg};
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    // Need at least a header to parse
    if data.len() < 16 {
        return;
    }

    let stream = SliceStream::new(data);

    // Try to parse header first
    if let Ok(header) = MessageHeader::parse_sync(&stream) {
        // Check if this looks like an OP_MSG
        if header.op_code == 2013 {
            if let Ok(body_len) = header.body_length() {
                // Limit body_len to prevent OOM in fuzzer
                if body_len <= 1024 * 1024 {
                    let _ = OpMsg::parse_sync(&stream, body_len);
                }
            }
        }
    }

    // Also try parse_with_checksum for complete coverage
    let _ = OpMsg::parse_with_checksum(data);
});
