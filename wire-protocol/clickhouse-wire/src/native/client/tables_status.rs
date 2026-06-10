//! Client TablesStatusRequest packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// A table identifier (database + table name).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableIdentifier {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
}

impl TableIdentifier {
    /// Create a new table identifier.
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self { database: database.into(), table: table.into() }
    }

    /// Parse from a synchronous stream.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let database = stream.read_ch_string_utf8_sync()?;
        let table = stream.read_ch_string_utf8_sync()?;
        Ok(Self { database, table })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let database = stream.read_ch_string_utf8().await?;
        let table = stream.read_ch_string_utf8().await?;
        Ok(Self { database, table })
    }

    /// Encode to a writer.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.database)?;
        w.write_ch_string_utf8(&self.table)?;
        Ok(())
    }
}

/// Client TablesStatusRequest packet (type 5).
///
/// Requests the status of specified tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablesStatusRequest {
    /// Tables to check status for.
    pub tables: Vec<TableIdentifier>,
}

impl TablesStatusRequest {
    /// Create a new TablesStatusRequest.
    pub fn new(tables: Vec<TableIdentifier>) -> Self {
        Self { tables }
    }

    /// Create an empty request.
    pub fn empty() -> Self {
        Self { tables: Vec::new() }
    }

    /// Parse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let count = stream.read_varuint_sync()? as usize;
        let mut tables = Vec::with_capacity(count);
        for _ in 0..count {
            tables.push(TableIdentifier::parse_sync(stream)?);
        }
        Ok(Self { tables })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let count = stream.read_varuint().await? as usize;
        let mut tables = Vec::with_capacity(count);
        for _ in 0..count {
            tables.push(TableIdentifier::parse(stream).await?);
        }
        Ok(Self { tables })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::TablesStatusRequest.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.tables.len() as u64)?;
        for table in &self.tables {
            table.encode(w)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_tables_status_request_roundtrip() {
        let request = TablesStatusRequest::new(vec![
            TableIdentifier::new("default", "test_table"),
            TableIdentifier::new("system", "numbers"),
        ]);

        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = TablesStatusRequest::parse_sync(&stream).unwrap();

        assert_eq!(decoded.tables.len(), 2);
        assert_eq!(decoded.tables[0].database, "default");
        assert_eq!(decoded.tables[0].table, "test_table");
        assert_eq!(decoded.tables[1].database, "system");
        assert_eq!(decoded.tables[1].table, "numbers");
    }

    #[test]
    fn test_empty_tables_status_request() {
        let request = TablesStatusRequest::empty();

        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = TablesStatusRequest::parse_sync(&stream).unwrap();

        assert!(decoded.tables.is_empty());
    }
}
