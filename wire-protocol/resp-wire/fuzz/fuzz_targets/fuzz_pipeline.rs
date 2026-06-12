//! Fuzz target for zero-allocation Pipeline parser.
//!
//! Tests the Pipeline which provides zero-copy RESP value extraction.
//! This exercises recursive skipping and length parsing.
//!
//! Looking for:
//! - Stack overflow from deeply nested structures
//! - Integer overflow in length calculations
//! - Invalid CRLF handling
//! - Panics from boundary conditions

#![no_main]

use libfuzzer_sys::fuzz_target;
use resp_wire::PipelineExt;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);
    let mut pipeline = stream.pipeline();

    // Test skip() - recursive value skipping
    let mut skip_count = 0;
    while let Ok(true) = pipeline.skip() {
        skip_count += 1;
        // Limit iterations to prevent timeout on pathological input
        if skip_count > 10000 {
            break;
        }
    }

    // Reset and test next_raw()
    let stream = SliceStream::new(data);
    let mut pipeline = stream.pipeline();

    let mut raw_count = 0;
    while let Ok(Some(raw)) = pipeline.next_raw() {
        // Exercise the borrowed slice
        let _ = raw.len();
        if !raw.is_empty() {
            let _ = raw[0];
        }
        raw_count += 1;
        if raw_count > 10000 {
            break;
        }
    }

    // Reset and test next_tagged()
    let stream = SliceStream::new(data);
    let mut pipeline = stream.pipeline();

    let mut tagged_count = 0;
    while let Ok(Some(slice)) = pipeline.next_tagged() {
        // Exercise RespSlice accessors
        let _ = slice.tag;
        let _ = slice.raw.len();
        let _ = slice.payload().len();
        let _ = slice.is_aggregate();
        tagged_count += 1;
        if tagged_count > 10000 {
            break;
        }
    }

    // Reset and test count()
    let stream = SliceStream::new(data);
    let mut pipeline = stream.pipeline();
    let _ = pipeline.count();
});
