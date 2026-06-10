//! Fuzz target for OP_COMPRESSED parsing with CVE-2025-14847 validation.
//!
//! Tests the compressed message parser with arbitrary byte sequences to find:
//! - Bypasses of CVE-2025-14847 validation
//! - Integer overflows in size calculations
//! - Panics from malformed compression headers
//! - Memory exhaustion from decompression bombs

#![no_main]

use libfuzzer_sys::fuzz_target;
use mongo_wire::{MessageHeader, OpCompressed};
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    // Need at least header + compressed message header
    if data.len() < 16 + 9 {
        return;
    }

    let stream = SliceStream::new(data);

    // Try to parse header first
    if let Ok(header) = MessageHeader::parse_sync(&stream) {
        // Check if this looks like an OP_COMPRESSED
        if header.op_code == 2012 {
            if let Ok(body_len) = header.body_length() {
                // Limit body_len to prevent OOM in fuzzer
                if body_len <= 1024 * 1024 {
                    let _ = OpCompressed::parse_sync(&stream, &header, body_len);
                }
            }
        }
    }
});
