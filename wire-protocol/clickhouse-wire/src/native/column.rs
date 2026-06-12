//! Column type handling for ClickHouse native protocol.
//!
//! Columns contain the actual data in data blocks.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// A column in a data block.
#[derive(Clone, Debug, PartialEq)]
pub struct Column {
    /// Column name.
    pub name: String,
    /// Column type (e.g., "UInt64", "String", "Nullable(Int32)").
    pub type_name: String,
    /// Raw column data (unparsed).
    pub data: Vec<u8>,
}

impl Column {
    /// Create a new column with data.
    pub fn new(name: impl Into<String>, type_name: impl Into<String>, data: Vec<u8>) -> Self {
        Self { name: name.into(), type_name: type_name.into(), data }
    }

    /// Create an empty column.
    pub fn empty(name: impl Into<String>, type_name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            type_name: type_name.into(),
            data: Vec::new(),
        }
    }

    /// Parse a column from a synchronous stream.
    ///
    /// Note: This only reads the column header (name and type) for now.
    /// Full data parsing requires knowledge of row count and type-specific
    /// deserialization.
    pub fn parse_sync<S>(stream: &S, num_rows: usize, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let name = stream.read_ch_string_utf8_sync()?;
        let type_name = stream.read_ch_string_utf8_sync()?;

        // For now, we don't parse the actual column data.
        // Full implementation would need type-specific deserialization.
        // The data size depends on the type and row count.
        let data = if num_rows > 0 {
            // This is a placeholder - actual implementation needs
            // type-aware parsing
            Self::read_column_data_sync(stream, &type_name, num_rows)?
        } else {
            Vec::new()
        };

        Ok(Self { name, type_name, data })
    }

    /// Parse a column asynchronously.
    pub async fn parse<S>(stream: &S, num_rows: usize, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let name = stream.read_ch_string_utf8().await?;
        let type_name = stream.read_ch_string_utf8().await?;

        let data = if num_rows > 0 {
            Self::read_column_data(stream, &type_name, num_rows).await?
        } else {
            Vec::new()
        };

        Ok(Self { name, type_name, data })
    }

    /// Read column data based on type (sync).
    fn read_column_data_sync<S>(stream: &S, type_name: &str, num_rows: usize) -> Result<Vec<u8>, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let size = Self::calculate_fixed_column_size(type_name, num_rows);

        if let Some(size) = size {
            // Fixed-size type
            stream.read_bytes_ch_sync(size)
        } else if type_name == "String" || type_name.starts_with("FixedString") {
            // For String type, we need to read each string's length + data
            Self::read_string_column_sync(stream, type_name, num_rows)
        } else {
            // For complex types, just return empty for now
            // Full implementation would handle Nullable, Array, etc.
            Ok(Vec::new())
        }
    }

    /// Read column data based on type (async).
    async fn read_column_data<S>(stream: &S, type_name: &str, num_rows: usize) -> Result<Vec<u8>, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let size = Self::calculate_fixed_column_size(type_name, num_rows);

        if let Some(size) = size {
            let data = stream.peek_read(Some(size)).await.map_err(Into::into)?;
            let result = data.to_vec();
            stream.accept(&data, None).map_err(Into::into)?;
            Ok(result)
        } else if type_name == "String" || type_name.starts_with("FixedString") {
            Self::read_string_column(stream, type_name, num_rows).await
        } else {
            Ok(Vec::new())
        }
    }

    /// Calculate the size of a fixed-size column.
    fn calculate_fixed_column_size(type_name: &str, num_rows: usize) -> Option<usize> {
        let element_size = match type_name {
            "UInt8" | "Int8" | "Bool" => Some(1),
            "UInt16" | "Int16" | "Date" => Some(2),
            "UInt32" | "Int32" | "Float32" | "Date32" => Some(4),
            "UInt64" | "Int64" | "Float64" | "DateTime" => Some(8),
            "UInt128" | "Int128" | "UUID" => Some(16),
            "UInt256" | "Int256" => Some(32),
            "IPv4" => Some(4),
            "IPv6" => Some(16),
            _ if type_name.starts_with("DateTime64") => Some(8),
            _ if type_name.starts_with("Decimal32") => Some(4),
            _ if type_name.starts_with("Decimal64") => Some(8),
            _ if type_name.starts_with("Decimal128") => Some(16),
            _ if type_name.starts_with("Decimal256") => Some(32),
            _ if type_name.starts_with("FixedString(") => {
                // Parse FixedString(N)
                type_name.strip_prefix("FixedString(").and_then(|s| s.strip_suffix(')')).and_then(|s| s.parse::<usize>().ok())
            }
            _ if type_name.starts_with("Enum8") => Some(1),
            _ if type_name.starts_with("Enum16") => Some(2),
            _ => None,
        };

        element_size.map(|s| s * num_rows)
    }

    /// Read a String column (sync).
    fn read_string_column_sync<S>(stream: &S, type_name: &str, num_rows: usize) -> Result<Vec<u8>, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        if type_name.starts_with("FixedString(") {
            // Fixed string - just read the fixed size
            if let Some(size) = Self::calculate_fixed_column_size(type_name, num_rows) {
                return stream.read_bytes_ch_sync(size);
            }
        }

        // Variable-length strings
        let mut data = Vec::new();
        for _ in 0..num_rows {
            let s = stream.read_ch_string_sync()?;
            // Re-encode for storage
            data.extend_from_slice(&(s.len() as u64).to_le_bytes());
            data.extend_from_slice(&s);
        }
        Ok(data)
    }

    /// Read a String column (async).
    async fn read_string_column<S>(stream: &S, type_name: &str, num_rows: usize) -> Result<Vec<u8>, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        if type_name.starts_with("FixedString(")
            && let Some(size) = Self::calculate_fixed_column_size(type_name, num_rows)
        {
            let data = stream.peek_read(Some(size)).await.map_err(Into::into)?;
            let result = data.to_vec();
            stream.accept(&data, None).map_err(Into::into)?;
            return Ok(result);
        }

        let mut data = Vec::new();
        for _ in 0..num_rows {
            let s = stream.read_ch_string().await?;
            data.extend_from_slice(&(s.len() as u64).to_le_bytes());
            data.extend_from_slice(&s);
        }
        Ok(data)
    }

    /// Encode a column to a writer.
    pub fn encode<W: Write>(&self, w: &mut W, _num_rows: usize) -> io::Result<()> {
        w.write_ch_string_utf8(&self.name)?;
        w.write_ch_string_utf8(&self.type_name)?;
        w.write_all(&self.data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_empty() {
        let col = Column::empty("id", "UInt64");
        assert_eq!(col.name, "id");
        assert_eq!(col.type_name, "UInt64");
        assert!(col.data.is_empty());
    }

    #[test]
    fn test_fixed_size_calculation() {
        assert_eq!(Column::calculate_fixed_column_size("UInt8", 10), Some(10));
        assert_eq!(Column::calculate_fixed_column_size("UInt64", 10), Some(80));
        assert_eq!(Column::calculate_fixed_column_size("UUID", 5), Some(80));
        assert_eq!(Column::calculate_fixed_column_size("FixedString(32)", 10), Some(320));
        assert_eq!(Column::calculate_fixed_column_size("String", 10), None);
    }
}
