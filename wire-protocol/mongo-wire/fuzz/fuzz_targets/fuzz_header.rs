//! Fuzz target for MongoDB wire protocol header parsing.
//!
//! Tests header parsing with arbitrary byte sequences to find:
//! - Panics from short buffers
//! - Integer overflow in length calculations
//! - Invalid opcode handling

#![no_main]

use libfuzzer_sys::fuzz_target;
use mongo_wire::MessageHeader;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    if let Ok(header) = MessageHeader::parse_sync(&stream) {
        // Exercise all accessors
        let _ = header.body_length();
        let _ = header.op_code();

        // Verify invariants
        assert!(header.message_length >= 0 || header.body_length().is_err());
    }
});
