//! Client KeepAlive packet for ClickHouse native protocol.

use crate::native::packet::ClientPacketType;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};

/// Client KeepAlive packet (type 6).
///
/// Keeps the connection alive without executing any query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct KeepAlive;

impl KeepAlive {
    /// Create a new KeepAlive packet.
    pub const fn new() -> Self {
        Self
    }

    /// Encode the KeepAlive packet.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::KeepAlive.as_u64())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_keep_alive_encode() {
        let keep_alive = KeepAlive::new();
        let mut buf = Vec::new();
        keep_alive.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x06]); // KeepAlive packet type = 6
    }
}
