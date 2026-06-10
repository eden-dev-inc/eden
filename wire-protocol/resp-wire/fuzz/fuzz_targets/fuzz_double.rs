//! Fuzz target for RESP Double parsing.
//!
//! Tests double parsing with arbitrary byte sequences to find:
//! - UTF-8 encoding edge cases (doubles are parsed via from_utf8)
//! - Special float values (inf, -inf, nan)
//! - Overflow/underflow to infinity
//! - Precision and rounding edge cases
//! - Invalid format handling

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::types::double::Double;
use resp_wire::RespParseSync;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    if let Ok(value) = Double::parse_sync(&stream) {
        // Verify the value is usable
        let _ = value.is_finite();
        let _ = value.is_nan();
        let _ = value.is_infinite();
        let _ = value.is_sign_positive();
        let _ = value.is_sign_negative();
        let _ = format!("{}", value);
        let _ = format!("{:?}", value);

        // Test arithmetic doesn't panic
        let _ = value + 1.0;
        let _ = value - 1.0;
        let _ = value * 2.0;
        let _ = value / 2.0;

        // Test conversions
        let _ = value as i64;
        let _ = value as f32;
    }
    // Errors are expected for malformed input
});
