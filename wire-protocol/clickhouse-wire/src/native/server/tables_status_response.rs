//! Server TablesStatusResponse packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Status information for a single table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableStatus {
    /// Database name.
    pub database: String,
    /// Table name.
    pub table: String,
    /// Whether the table is the absolute leader.
    pub is_absolute_delay_leader: bool,
    /// Absolute delay value.
    pub absolute_delay: u64,
    /// Number of parts.
    pub parts: u64,
}

impl TableStatus {
    /// Create a new TableStatus.
    pub fn new(database: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
            is_absolute_delay_leader: false,
            absolute_delay: 0,
            parts: 0,
        }
    }

    /// Parse a TableStatus from a synchronous stream.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let database = stream.read_ch_string_utf8_sync()?;
        let table = stream.read_ch_string_utf8_sync()?;
        let is_absolute_delay_leader = stream.read_u8_ch_sync()? != 0;
        let absolute_delay = stream.read_varuint_sync()?;
        let parts = stream.read_varuint_sync()?;

        Ok(Self {
            database,
            table,
            is_absolute_delay_leader,
            absolute_delay,
            parts,
        })
    }

    /// Parse a TableStatus asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let database = stream.read_ch_string_utf8().await?;
        let table = stream.read_ch_string_utf8().await?;
        let is_absolute_delay_leader = stream.read_u8_ch().await? != 0;
        let absolute_delay = stream.read_varuint().await?;
        let parts = stream.read_varuint().await?;

        Ok(Self {
            database,
            table,
            is_absolute_delay_leader,
            absolute_delay,
            parts,
        })
    }

    /// Encode the TableStatus.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.database)?;
        w.write_ch_string_utf8(&self.table)?;
        w.write_all(&[if self.is_absolute_delay_leader { 1 } else { 0 }])?;
        w.write_varuint(self.absolute_delay)?;
        w.write_varuint(self.parts)?;
        Ok(())
    }
}

/// Server TablesStatusResponse packet (type 9).
///
/// Response to TablesStatusRequest with information about requested tables.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TablesStatusResponse {
    /// Status for each requested table.
    pub tables: Vec<TableStatus>,
}

impl TablesStatusResponse {
    /// Create a new empty TablesStatusResponse.
    pub fn new() -> Self {
        Self { tables: Vec::new() }
    }

    /// Create a TablesStatusResponse with table statuses.
    pub fn with_tables(tables: Vec<TableStatus>) -> Self {
        Self { tables }
    }

    /// Add a table status.
    pub fn add_table(&mut self, status: TableStatus) {
        self.tables.push(status);
    }

    /// Check if this response is empty.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Parse a TablesStatusResponse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let count = stream.read_varuint_sync()? as usize;
        let mut tables = Vec::with_capacity(count.min(1024));

        for _ in 0..count {
            tables.push(TableStatus::parse_sync(stream)?);
        }

        Ok(Self { tables })
    }

    /// Parse a TablesStatusResponse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let count = stream.read_varuint().await? as usize;
        let mut tables = Vec::with_capacity(count.min(1024));

        for _ in 0..count {
            tables.push(TableStatus::parse(stream).await?);
        }

        Ok(Self { tables })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::TablesStatusResponse.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the TablesStatusResponse body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.tables.len() as u64)?;
        for table in &self.tables {
            table.encode(w)?;
        }
        Ok(())
    }
}

impl Default for TablesStatusResponse {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_tables_status_response_empty() {
        let response = TablesStatusResponse::new();
        assert!(response.is_empty());

        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = TablesStatusResponse::parse_sync(&stream).unwrap();

        assert!(decoded.is_empty());
    }

    #[test]
    fn test_tables_status_response_with_tables() {
        let mut response = TablesStatusResponse::new();
        response.add_table(TableStatus {
            database: "default".into(),
            table: "test_table".into(),
            is_absolute_delay_leader: true,
            absolute_delay: 100,
            parts: 5,
        });

        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = TablesStatusResponse::parse_sync(&stream).unwrap();

        assert_eq!(decoded.tables.len(), 1);
        assert_eq!(decoded.tables[0].database, "default");
        assert_eq!(decoded.tables[0].table, "test_table");
        assert!(decoded.tables[0].is_absolute_delay_leader);
        assert_eq!(decoded.tables[0].absolute_delay, 100);
        assert_eq!(decoded.tables[0].parts, 5);
    }
}
