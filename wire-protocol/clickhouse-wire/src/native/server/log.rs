//! Server Log packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server Log packet (type 10).
///
/// Contains server log messages as a block.
/// The block typically has columns: time, time_microseconds, host_name,
/// query_id, thread_id, priority, source, text.
#[derive(Clone, Debug, PartialEq)]
pub struct Log {
    /// Data block containing log entries.
    pub block: Block,
}

impl Log {
    /// Create a new Log packet with a block.
    pub fn new(block: Block) -> Self {
        Self { block }
    }

    /// Create an empty Log packet.
    pub fn empty() -> Self {
        Self { block: Block::empty() }
    }

    /// Check if this log packet is empty.
    pub fn is_empty(&self) -> bool {
        self.block.is_empty()
    }

    /// Parse a Log packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        // Log packets have an empty table name prefix
        let _table_name = stream.read_ch_string_utf8_sync()?;
        let block = Block::parse_sync(stream, protocol_version)?;

        Ok(Self { block })
    }

    /// Parse a Log packet asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        // Log packets have an empty table name prefix
        let _table_name = stream.read_ch_string_utf8().await?;
        let block = Block::parse(stream, protocol_version).await?;

        Ok(Self { block })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::Log.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the Log packet body (without packet type).
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
    fn test_log_empty() {
        let log = Log::empty();
        assert!(log.is_empty());
    }

    #[test]
    fn test_log_roundtrip() {
        let log = Log::new(Block::empty());

        let mut buf = Vec::new();
        log.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = Log::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert!(decoded.is_empty());
    }
}
