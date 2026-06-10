//! Server PartUUIDs packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server PartUUIDs packet (type 12).
///
/// Contains UUIDs of parts for distributed queries.
/// Used in distributed table queries to track which parts have been processed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartUUIDs {
    /// List of part UUIDs (as 128-bit values).
    pub uuids: Vec<u128>,
}

impl PartUUIDs {
    /// Create a new PartUUIDs packet.
    pub fn new(uuids: Vec<u128>) -> Self {
        Self { uuids }
    }

    /// Create an empty PartUUIDs packet.
    pub fn empty() -> Self {
        Self { uuids: Vec::new() }
    }

    /// Check if this packet is empty.
    pub fn is_empty(&self) -> bool {
        self.uuids.is_empty()
    }

    /// Parse a PartUUIDs packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let count = stream.read_varuint_sync()? as usize;
        let mut uuids = Vec::with_capacity(count.min(10000));

        for _ in 0..count {
            let uuid = stream.read_u128_le_ch_sync()?;
            uuids.push(uuid);
        }

        Ok(Self { uuids })
    }

    /// Parse a PartUUIDs packet asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let count = stream.read_varuint().await? as usize;
        let mut uuids = Vec::with_capacity(count.min(10000));

        for _ in 0..count {
            let uuid = stream.read_u128_le_ch().await?;
            uuids.push(uuid);
        }

        Ok(Self { uuids })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::PartUUIDs.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the PartUUIDs packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.uuids.len() as u64)?;
        for uuid in &self.uuids {
            w.write_all(&uuid.to_le_bytes())?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_part_uuids_empty() {
        let part_uuids = PartUUIDs::empty();
        assert!(part_uuids.is_empty());

        let mut buf = Vec::new();
        part_uuids.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = PartUUIDs::parse_sync(&stream).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_part_uuids_roundtrip() {
        let uuids = vec![0x0123456789abcdef0123456789abcdefu128, 0xfedcba9876543210fedcba9876543210u128];
        let part_uuids = PartUUIDs::new(uuids.clone());

        let mut buf = Vec::new();
        part_uuids.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = PartUUIDs::parse_sync(&stream).unwrap();

        assert_eq!(decoded.uuids, uuids);
    }
}
