//! Server ProfileEvents packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server ProfileEvents packet (type 14).
///
/// Contains profile events as a block. These are performance counters
/// and metrics collected during query execution.
///
/// The block typically has columns: host_name, current_time, thread_id,
/// type, name, value.
#[derive(Clone, Debug, PartialEq)]
pub struct ProfileEvents {
    /// Data block containing profile events.
    pub block: Block,
}

impl ProfileEvents {
    /// Create a new ProfileEvents packet with a block.
    pub fn new(block: Block) -> Self {
        Self { block }
    }

    /// Create an empty ProfileEvents packet.
    pub fn empty() -> Self {
        Self { block: Block::empty() }
    }

    /// Check if this packet is empty.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// Parse a ProfileEvents packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        // ProfileEvents packets have an empty table name prefix
        let _table_name = stream.read_ch_string_utf8_sync()?;
        let block = Block::parse_sync(stream, protocol_version)?;

        Ok(Self { block })
    }

    /// Parse a ProfileEvents packet asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        // ProfileEvents packets have an empty table name prefix
        let _table_name = stream.read_ch_string_utf8().await?;
        let block = Block::parse(stream, protocol_version).await?;

        Ok(Self { block })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::ProfileEvents.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the ProfileEvents packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Write empty table name
        w.write_ch_string_utf8("")?;
        self.block.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_profile_events_empty() {
        let events = ProfileEvents::empty();
        assert!(events.is_empty());
    }

    #[test]
    fn test_profile_events_roundtrip() {
        let events = ProfileEvents::new(Block::empty());

        let mut buf = Vec::new();
        events.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = ProfileEvents::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert!(decoded.is_empty());
    }
}
