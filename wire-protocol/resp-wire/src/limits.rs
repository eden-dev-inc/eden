//! Configurable size limits for RESP parsing to prevent DoS attacks.
//!
//! These limits prevent malicious inputs from causing excessive memory allocation
//! or CPU consumption.

/// Maximum number of elements in arrays, sets, or push types.
/// Default: 1000
pub const MAX_ELEMENTS: usize = 1_000;

/// Maximum number of entries in maps or attributes.
/// Default: 1000
pub const MAX_MAP_ENTRIES: usize = 1_000;

/// Maximum size in bytes for bulk strings, bulk errors, or verbatim strings.
/// Default: 1MB
pub const MAX_STRING_BYTES: usize = 1_048_576;

/// Maximum pre-allocation size for vectors.
/// We cap pre-allocation to avoid OOM from large declared sizes,
/// while still allowing the actual limit checks to properly reject oversized inputs.
/// Default: 8KB elements/bytes
pub const MAX_PREALLOC: usize = 8_192;

/// Maximum nesting depth for recursive structures (arrays, maps, sets, etc).
/// Prevents stack overflow from deeply nested inputs like `[[[[...]]]]`.
/// Default: 64
pub const MAX_DEPTH: usize = 64;
