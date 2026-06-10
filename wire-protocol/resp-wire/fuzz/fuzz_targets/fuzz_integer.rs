//! Fuzz target for RESP Integer parsing.
//!
//! Tests integer parsing with arbitrary byte sequences to find:
//! - Integer overflow/underflow
//! - Sign handling edge cases
//! - Invalid format handling
//! - Boundary conditions for i128

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::types::integer::Integer;
use resp_wire::RespParseSync;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    if let Ok(value) = Integer::parse_sync(&stream) {
        // Verify the value is usable
        let _ = value.abs();
        let _ = value.is_positive();
        let _ = value.is_negative();
        let _ = format!("{}", value);

        // Test arithmetic doesn't panic (using checked ops)
        let _ = value.checked_add(1);
        let _ = value.checked_sub(1);
        let _ = value.checked_mul(2);
    }
    // Errors are expected for malformed input
});
