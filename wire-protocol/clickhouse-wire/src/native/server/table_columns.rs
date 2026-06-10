//! Server TableColumns packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server TableColumns packet (type 11).
///
/// Contains column information for a table, used during DESCRIBE queries
/// and when the server needs to communicate table structure.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableColumns {
    /// External table name.
    pub table_name: String,
    /// Column descriptions as a string (serialized column info).
    pub columns: String,
}

impl TableColumns {
    /// Create a new TableColumns packet.
    pub fn new(table_name: impl Into<String>, columns: impl Into<String>) -> Self {
        Self { table_name: table_name.into(), columns: columns.into() }
    }

    /// Create an empty TableColumns packet.
    pub fn empty() -> Self {
        Self { table_name: String::new(), columns: String::new() }
    }

    /// Check if this packet is empty.
    pub fn is_empty(&self) -> bool {
        self.table_name.is_empty() && self.columns.is_empty()
    }

    /// Parse a TableColumns packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let table_name = stream.read_ch_string_utf8_sync()?;
        let columns = stream.read_ch_string_utf8_sync()?;

        Ok(Self { table_name, columns })
    }

    /// Parse a TableColumns packet asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let table_name = stream.read_ch_string_utf8().await?;
        let columns = stream.read_ch_string_utf8().await?;

        Ok(Self { table_name, columns })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::TableColumns.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the TableColumns packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.table_name)?;
        w.write_ch_string_utf8(&self.columns)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_table_columns_empty() {
        let tc = TableColumns::empty();
        assert!(tc.is_empty());
    }

    #[test]
    fn test_table_columns_roundtrip() {
        let tc = TableColumns::new("test_table", "id UInt64, name String");

        let mut buf = Vec::new();
        tc.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = TableColumns::parse_sync(&stream).unwrap();

        assert_eq!(decoded.table_name, "test_table");
        assert_eq!(decoded.columns, "id UInt64, name String");
    }
}
