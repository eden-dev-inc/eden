//! Endpoint analytics compatibility types.
//!
//! The open-source build keeps only lightweight protocol labels here.

/// Redis protocol identifier used by lightweight callers that need a stable label.
pub const PROTOCOL_REDIS: u8 = 0;
/// PostgreSQL protocol identifier used by lightweight callers that need a stable label.
pub const PROTOCOL_POSTGRES: u8 = 1;
/// MongoDB protocol identifier used by lightweight callers that need a stable label.
pub const PROTOCOL_MONGODB: u8 = 2;
/// LLM protocol identifier used by lightweight callers that need a stable label.
pub const PROTOCOL_LLM: u8 = 3;
