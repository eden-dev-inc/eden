//! Client Cancel packet for ClickHouse native protocol.

use crate::native::packet::ClientPacketType;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};

/// Client Cancel packet (type 3).
///
/// Sent to cancel the currently executing query.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cancel;

impl Cancel {
    /// Create a new Cancel packet.
    pub const fn new() -> Self {
        Self
    }

    /// Encode the Cancel packet.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Cancel.as_u64())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cancel_encode() {
        let cancel = Cancel::new();
        let mut buf = Vec::new();
        cancel.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x03]); // Cancel packet type = 3
    }
}
