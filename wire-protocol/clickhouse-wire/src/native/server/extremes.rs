//! Server Extremes packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server Extremes packet (type 8).
///
/// Contains min/max values for columns (WITH EXTREMES modifier).
/// Has the same format as ServerData.
#[derive(Clone, Debug, PartialEq)]
pub struct Extremes {
    /// Table name (may be empty).
    pub table_name: String,
    /// Data block containing extremes (min row and max row).
    pub block: Block,
}

impl Extremes {
    /// Create a new Extremes packet with a block.
    pub fn new(block: Block) -> Self {
        Self { table_name: String::new(), block }
    }

    /// Create an Extremes packet with a table name.
    pub fn with_table_name(table_name: impl Into<String>, block: Block) -> Self {
        Self { table_name: table_name.into(), block }
    }

    /// Check if this is an empty extremes packet.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// Parse an Extremes packet from a synchronous stream.
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

    /// Parse an Extremes packet asynchronously.
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
        w.write_varuint(ServerPacketType::Extremes.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the Extremes packet body (without packet type).
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
    fn test_extremes_empty() {
        let extremes = Extremes::new(Block::empty());
        assert!(extremes.is_empty());
    }

    #[test]
    fn test_extremes_roundtrip() {
        let extremes = Extremes::with_table_name("test_table", Block::empty());

        let mut buf = Vec::new();
        extremes.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = Extremes::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.table_name, extremes.table_name);
        assert!(decoded.is_empty());
    }
}
