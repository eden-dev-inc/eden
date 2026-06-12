//! Client Ping packet for ClickHouse native protocol.

use crate::native::packet::ClientPacketType;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};

/// Client Ping packet (type 4).
///
/// A simple ping to check if the server is alive.
/// The server responds with a Pong packet.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Ping;

impl Ping {
    /// Create a new Ping packet.
    pub const fn new() -> Self {
        Self
    }

    /// Encode the Ping packet.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Ping.as_u64())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ping_encode() {
        let ping = Ping::new();
        let mut buf = Vec::new();
        ping.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x04]); // Ping packet type = 4
    }
}
