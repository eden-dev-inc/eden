//! Client Data packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Client Data packet (type 2).
///
/// Sends a data block to the server (e.g., for INSERT operations).
#[derive(Clone, Debug, PartialEq)]
pub struct ClientData {
    /// Table name (empty for query results).
    pub table_name: String,
    /// Data block.
    pub block: Block,
}

impl ClientData {
    /// Create a new ClientData with a block.
    pub fn new(block: Block) -> Self {
        Self { table_name: String::new(), block }
    }

    /// Create a ClientData with a table name.
    pub fn with_table_name(table_name: impl Into<String>, block: Block) -> Self {
        Self { table_name: table_name.into(), block }
    }

    /// Create an empty data packet (used as end marker).
    pub fn empty() -> Self {
        Self { table_name: String::new(), block: Block::empty() }
    }

    /// Check if this is an empty data packet.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// Parse a ClientData packet from a synchronous stream.
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

    /// Parse a ClientData packet asynchronously.
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

    /// Encode the ClientData packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Data.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the ClientData packet body (without packet type).
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
    fn test_empty_client_data() {
        let data = ClientData::empty();
        assert!(data.is_empty());
    }

    #[test]
    fn test_client_data_roundtrip() {
        let data = ClientData::empty();

        let mut buf = Vec::new();
        data.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = ClientData::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert!(decoded.is_empty());
        assert_eq!(decoded.table_name, data.table_name);
    }
}
