//! Server Totals packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server Totals packet (type 7).
///
/// Contains totals row for aggregation queries (WITH TOTALS modifier).
/// Has the same format as ServerData.
#[derive(Clone, Debug, PartialEq)]
pub struct Totals {
    /// Table name (may be empty).
    pub table_name: String,
    /// Data block containing totals row.
    pub block: Block,
}

impl Totals {
    /// Create a new Totals packet with a block.
    pub fn new(block: Block) -> Self {
        Self { table_name: String::new(), block }
    }

    /// Create a Totals packet with a table name.
    pub fn with_table_name(table_name: impl Into<String>, block: Block) -> Self {
        Self { table_name: table_name.into(), block }
    }

    /// Check if this is an empty totals packet.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// Parse a Totals packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let table_name = stream.read_ch_string_utf8_sync()?;
        let block = Block::parse_sync(stream, protocol_version)?;

        Ok(Self { table_name, block })
    }

    /// Parse a Totals packet asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let table_name = stream.read_ch_string_utf8().await?;
        let block = Block::parse(stream, protocol_version).await?;

        Ok(Self { table_name, block })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::Totals.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the Totals packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.table_name)?;
        self.block.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_totals_empty() {
        let totals = Totals::new(Block::empty());
        assert!(totals.is_empty());
    }

    #[test]
    fn test_totals_roundtrip() {
        let totals = Totals::with_table_name("test_table", Block::empty());

        let mut buf = Vec::new();
        totals.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = Totals::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.table_name, totals.table_name);
        assert!(decoded.is_empty());
    }
}
