//! Server EndOfStream packet for ClickHouse native protocol.

/// Server EndOfStream packet (type 5).
///
/// Indicates the query has completed and all data has been sent.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EndOfStream;

impl EndOfStream {
    /// Create a new EndOfStream packet.
    pub const fn new() -> Self {
        Self
    }

    // EndOfStream has no body, only the packet type identifier is sent.
}
