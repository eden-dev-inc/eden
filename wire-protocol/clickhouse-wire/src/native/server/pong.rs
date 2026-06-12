//! Server Pong packet for ClickHouse native protocol.

/// Server Pong packet (type 4).
///
/// Response to a client Ping packet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Pong;

impl Pong {
    /// Create a new Pong packet.
    pub const fn new() -> Self {
        Self
    }

    // Pong has no body, only the packet type identifier is sent.
}
