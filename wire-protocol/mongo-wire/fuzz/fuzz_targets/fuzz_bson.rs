//! Fuzz target for BSON parsing helpers.
//!
//! Tests BSON reading functions with arbitrary byte sequences to find:
//! - Panics from malformed length fields
//! - Null terminator handling issues
//! - Integer overflow in size calculations
//! - Memory exhaustion from large allocations

#![no_main]

use libfuzzer_sys::fuzz_target;
use mongo_wire::MongoReadSyncExt;
use wire_stream::SliceStream;

fuzz_target!(|data: &[u8]| {
    let stream = SliceStream::new(data);

    // Test document parsing
    let _ = stream.read_bson_document_sync();

    // Reset and test string parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_string_sync();

    // Reset and test binary parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_binary_sync();

    // Reset and test ObjectId parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_object_id_sync();

    // Reset and test boolean parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_boolean_sync();

    // Reset and test double parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_double_sync();

    // Reset and test datetime parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_datetime_sync();

    // Reset and test decimal128 parsing
    let stream = SliceStream::new(data);
    let _ = stream.read_bson_decimal128_sync();

    // Test element skipping with various type tags
    if !data.is_empty() {
        let element_type = data[0];
        let stream = SliceStream::new(&data[1..]);
        let _ = stream.skip_bson_element_sync(element_type);
    }
});
