//! Bulk Copy Protocol (BCP) support.
//!
//! BCP is used for high-performance bulk data transfer between
//! client and server. It supports both bulk insert and bulk extract.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::{PacketBuilder, write_u16_le, write_u32_le, write_varchar};
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// BCP direction.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum BcpDirection {
    /// Bulk insert (client to server).
    In = 1,
    /// Bulk extract (server to client).
    Out = 2,
}

impl BcpDirection {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            1 => Some(BcpDirection::In),
            2 => Some(BcpDirection::Out),
            _ => None,
        }
    }
}

/// BCP hint flags.
pub mod bcp_hints {
    /// Keep identity values from the data file.
    pub const KEEP_IDENTITY: u32 = 0x0001;
    /// Keep null values (don't apply defaults).
    pub const KEEP_NULLS: u32 = 0x0002;
    /// Check constraints during insert.
    pub const CHECK_CONSTRAINTS: u32 = 0x0004;
    /// Fire triggers during insert.
    pub const FIRE_TRIGGERS: u32 = 0x0008;
    /// Table lock hint for better performance.
    pub const TABLOCK: u32 = 0x0010;
}

/// Column metadata for BCP operations.
#[derive(Clone, Debug)]
pub struct BcpColumn {
    /// Column name.
    pub name: String,
    /// Data type.
    pub data_type: u8,
    /// User type.
    pub user_type: u32,
    /// Maximum length.
    pub max_length: u32,
    /// Precision (for decimal/numeric).
    pub precision: u8,
    /// Scale (for decimal/numeric).
    pub scale: u8,
    /// Column flags.
    pub flags: u16,
}

impl BcpColumn {
    /// Parse a BCP column descriptor.
    pub fn parse_sync<'s>(stream: &'s SliceStream<'s>) -> Result<BcpColumn, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Column name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        // Data type
        let data_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // User type
        let user_type = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Max length
        let max_length = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Precision
        let precision = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Scale
        let scale = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Flags
        let flags = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(BcpColumn {
            name,
            data_type,
            user_type,
            max_length,
            precision,
            scale,
            flags,
        })
    }
}

/// BCP row data.
#[derive(Clone, Debug)]
pub struct BcpRow {
    /// Column values (raw bytes, None for NULL).
    pub values: Vec<Option<Vec<u8>>>,
}

impl BcpRow {
    /// Create a new BCP row.
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Add a column value.
    pub fn add_value(mut self, value: Option<Vec<u8>>) -> Self {
        self.values.push(value);
        self
    }

    /// Add a NULL value.
    pub fn add_null(mut self) -> Self {
        self.values.push(None);
        self
    }

    /// Add a string value.
    pub fn add_string(self, value: impl AsRef<str>) -> Self {
        self.add_value(Some(value.as_ref().as_bytes().to_vec()))
    }

    /// Add an i32 value.
    pub fn add_i32(self, value: i32) -> Self {
        self.add_value(Some(value.to_le_bytes().to_vec()))
    }

    /// Add an i64 value.
    pub fn add_i64(self, value: i64) -> Self {
        self.add_value(Some(value.to_le_bytes().to_vec()))
    }

    /// Serialize the row for BCP.
    pub fn serialize(&self) -> Vec<u8> {
        let mut data = Vec::new();

        for value in &self.values {
            match value {
                Some(bytes) => {
                    // Length prefix (4 bytes for BCP)
                    write_u32_le(&mut data, bytes.len() as u32);
                    data.extend_from_slice(bytes);
                }
                None => {
                    // NULL marker (length = -1)
                    data.extend_from_slice(&(-1i32).to_le_bytes());
                }
            }
        }

        data
    }
}

impl Default for BcpRow {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for bulk insert operations.
pub struct BulkInsertBuilder {
    table_name: String,
    columns: Vec<BcpColumn>,
    rows: Vec<BcpRow>,
    hints: u32,
    batch_size: u32,
}

impl BulkInsertBuilder {
    /// Create a new bulk insert builder.
    pub fn new(table_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            columns: Vec::new(),
            rows: Vec::new(),
            hints: 0,
            batch_size: 1000,
        }
    }

    /// Add a column definition.
    pub fn add_column(mut self, column: BcpColumn) -> Self {
        self.columns.push(column);
        self
    }

    /// Add a simple column by name and type.
    pub fn column(mut self, name: impl Into<String>, data_type: u8, max_length: u32) -> Self {
        self.columns.push(BcpColumn {
            name: name.into(),
            data_type,
            user_type: 0,
            max_length,
            precision: 0,
            scale: 0,
            flags: 0,
        });
        self
    }

    /// Add a row of data.
    pub fn add_row(mut self, row: BcpRow) -> Self {
        self.rows.push(row);
        self
    }

    /// Set BCP hints.
    pub fn hints(mut self, hints: u32) -> Self {
        self.hints = hints;
        self
    }

    /// Keep identity values from the data.
    pub fn keep_identity(mut self) -> Self {
        self.hints |= bcp_hints::KEEP_IDENTITY;
        self
    }

    /// Keep null values (don't apply defaults).
    pub fn keep_nulls(mut self) -> Self {
        self.hints |= bcp_hints::KEEP_NULLS;
        self
    }

    /// Check constraints during insert.
    pub fn check_constraints(mut self) -> Self {
        self.hints |= bcp_hints::CHECK_CONSTRAINTS;
        self
    }

    /// Fire triggers during insert.
    pub fn fire_triggers(mut self) -> Self {
        self.hints |= bcp_hints::FIRE_TRIGGERS;
        self
    }

    /// Use table lock for better performance.
    pub fn tablock(mut self) -> Self {
        self.hints |= bcp_hints::TABLOCK;
        self
    }

    /// Set the batch size.
    pub fn batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Build the initial BCP request packet.
    ///
    /// This sends the table name and column metadata.
    /// Follow with row data packets and then done packet.
    pub fn build_init_packet(&self) -> Vec<u8> {
        let mut payload = Vec::new();

        // Direction (1 = IN)
        payload.push(BcpDirection::In as u8);

        // Table name
        write_varchar(&mut payload, self.table_name.as_bytes());

        // Hints
        write_u32_le(&mut payload, self.hints);

        // Column count
        write_u16_le(&mut payload, self.columns.len() as u16);

        // Column metadata
        for col in &self.columns {
            write_varchar(&mut payload, col.name.as_bytes());
            payload.push(col.data_type);
            write_u32_le(&mut payload, col.user_type);
            write_u32_le(&mut payload, col.max_length);
            payload.push(col.precision);
            payload.push(col.scale);
            write_u16_le(&mut payload, col.flags);
        }

        PacketBuilder::new(PacketType::Bulk).write_bytes(&payload).build()
    }

    /// Build a row data packet.
    ///
    /// This creates a packet containing the specified rows.
    pub fn build_row_packet(&self, start: usize, count: usize) -> Vec<u8> {
        let mut payload = Vec::new();

        let end = (start + count).min(self.rows.len());
        for row in &self.rows[start..end] {
            payload.extend_from_slice(&row.serialize());
        }

        PacketBuilder::new(PacketType::Bulk).write_bytes(&payload).build()
    }

    /// Build the done packet to signal end of bulk insert.
    pub fn build_done_packet(&self, row_count: u64) -> Vec<u8> {
        let mut payload = Vec::new();

        // Done token
        payload.push(0xFD); // DONE token

        // Status (0x0010 = DONE_COUNT)
        write_u16_le(&mut payload, 0x0010);

        // Current command
        write_u16_le(&mut payload, 0x00C6); // BULK_INSERT command

        // Row count (8 bytes in TDS 7.2+, 4 bytes in older)
        write_u32_le(&mut payload, row_count as u32);
        write_u32_le(&mut payload, (row_count >> 32) as u32);

        PacketBuilder::new(PacketType::Bulk).write_bytes(&payload).build()
    }

    /// Get the row count.
    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    /// Get the batch size.
    pub fn get_batch_size(&self) -> u32 {
        self.batch_size
    }
}

/// BCP format file parser.
///
/// Parses native BCP format files (.fmt) for column mappings.
#[derive(Clone, Debug)]
pub struct BcpFormatFile {
    /// Version string.
    pub version: String,
    /// Number of columns.
    pub column_count: u16,
    /// Column definitions.
    pub columns: Vec<BcpFormatColumn>,
}

/// Column definition from a format file.
#[derive(Clone, Debug)]
pub struct BcpFormatColumn {
    /// Host file column order.
    pub host_order: u16,
    /// Host file data type.
    pub host_type: String,
    /// Prefix length.
    pub prefix_length: u8,
    /// Host file data length.
    pub host_length: u32,
    /// Field terminator.
    pub terminator: String,
    /// Server column order.
    pub server_order: u16,
    /// Server column name.
    pub server_name: String,
    /// Collation.
    pub collation: String,
}

impl BcpFormatFile {
    /// Parse a BCP format file from text content.
    pub fn parse(content: &str) -> Result<Self, SybaseWireError> {
        let mut lines = content.lines().filter(|l| !l.trim().is_empty());

        // First line: version
        let version = lines.next().ok_or(SybaseWireError::InvalidPacketType(0))?.trim().to_string();

        // Second line: column count
        let column_count: u16 = lines
            .next()
            .ok_or(SybaseWireError::InvalidPacketType(0))?
            .trim()
            .parse()
            .map_err(|_| SybaseWireError::InvalidPacketType(0))?;

        // Remaining lines: column definitions
        let mut columns = Vec::with_capacity(column_count as usize);
        for line in lines {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 7 {
                columns.push(BcpFormatColumn {
                    host_order: parts[0].parse().unwrap_or(0),
                    host_type: parts[1].to_string(),
                    prefix_length: parts[2].parse().unwrap_or(0),
                    host_length: parts[3].parse().unwrap_or(0),
                    terminator: parts[4].trim_matches('"').to_string(),
                    server_order: parts[5].parse().unwrap_or(0),
                    server_name: parts[6].to_string(),
                    collation: parts.get(7).map(|s| s.to_string()).unwrap_or_default(),
                });
            }
        }

        Ok(BcpFormatFile { version, column_count, columns })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bcp_row() {
        let row = BcpRow::new().add_i32(42).add_string("hello").add_null();

        let data = row.serialize();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_bulk_insert_builder() {
        let builder = BulkInsertBuilder::new("test_table")
            .column("id", 0x38, 4) // INT
            .column("name", 0xA7, 100) // VARCHAR
            .keep_identity()
            .tablock()
            .add_row(BcpRow::new().add_i32(1).add_string("Alice"))
            .add_row(BcpRow::new().add_i32(2).add_string("Bob"));

        let init_packet = builder.build_init_packet();
        assert!(!init_packet.is_empty());

        let row_packet = builder.build_row_packet(0, 2);
        assert!(!row_packet.is_empty());

        let done_packet = builder.build_done_packet(2);
        assert!(!done_packet.is_empty());
    }

    #[test]
    fn test_bcp_direction() {
        assert_eq!(BcpDirection::from_u8(1), Some(BcpDirection::In));
        assert_eq!(BcpDirection::from_u8(2), Some(BcpDirection::Out));
        assert_eq!(BcpDirection::from_u8(3), None);
    }

    #[test]
    fn test_format_file_parse() {
        let content = r#"
14.0
2
1 SQLCHAR 0 50 "\t" 1 col1 ""
2 SQLINT 0 4 "\n" 2 col2 ""
"#;
        let fmt = BcpFormatFile::parse(content).unwrap();
        assert_eq!(fmt.version, "14.0");
        assert_eq!(fmt.column_count, 2);
        assert_eq!(fmt.columns.len(), 2);
        assert_eq!(fmt.columns[0].server_name, "col1");
        assert_eq!(fmt.columns[1].server_name, "col2");
    }
}
