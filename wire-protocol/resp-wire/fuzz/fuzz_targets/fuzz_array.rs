//! Fuzz target for RESP Array parsing.
//!
//! Tests array parsing with arbitrary byte sequences to find:
//! - Stack overflow from deeply nested arrays
//! - Length parsing overflow
//! - Null array handling
//!
//! Run with memory limit to handle large length fields:
//!   cargo +nightly fuzz run fuzz_array -- -rss_limit_mb=512

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::types::array::Array;
use resp_wire::types::dynamic::Dynamic;
use resp_wire::RespParseSync;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    match Array::parse_sync(&stream) {
        Ok(mut reader) => {
            // Exercise the reader
            let _ = reader.remaining();

            // Try to read elements (parse as Dynamic for full coverage)
            let mut count = 0;
            while let Ok(Some(element_reader)) = reader.next_sync() {
                // Parse each element as Dynamic
                let _ = element_reader.parse_sync::<Dynamic>();
                count += 1;
                // Limit iterations to prevent timeout
                if count > 10000 {
                    break;
                }
            }
        }
        Err(_) => {
            // Errors are expected for malformed input
        }
    }
});
