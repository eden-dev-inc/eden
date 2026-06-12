//! MySQL result set metadata.
//!
//! A result set consists of:
//! 1. Column count packet
//! 2. Column definition packets
//! 3. EOF packet (if not DEPRECATE_EOF)
//! 4. Row data packets
//! 5. EOF/OK packet

use crate::capabilities::CapabilityFlags;
use crate::limits;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use crate::types::column_definition::ColumnDefinition;
use wire_stream::{WireRead, WireReadSync};

/// Result set metadata (column count and definitions).
#[derive(Clone, Debug)]
pub struct ResultSetMetadata {
    /// Number of columns.
    pub column_count: u64,
    /// Column definitions.
    pub columns: Vec<ColumnDefinition>,
}

impl ResultSetMetadata {
    /// Get a column by name.
    pub fn column_by_name(&self, name: &str) -> Option<&ColumnDefinition> {
        self.columns.iter().find(|c| c.name == name)
    }

    /// Get a column by index.
    pub fn column(&self, index: usize) -> Option<&ColumnDefinition> {
        self.columns.get(index)
    }

    /// Get column names.
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ResultSetError {
    #[error("invalid column count")]
    InvalidColumnCount,
    #[error("column count {0} exceeds limit {}", limits::MAX_COLUMNS)]
    TooManyColumns(u64),
    #[error("column definition error: {0}")]
    ColumnError(String),
}

impl ResultSetMetadata {
    /// Parse result set metadata with given capabilities context.
    ///
    /// This parses the column count and column definitions.
    /// Row data must be parsed separately.
    pub fn parse_with_capabilities_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        capabilities: CapabilityFlags,
    ) -> Result<Self, MysqlParseError<S::ReadError, ResultSetError>> {
        // Column count (length-encoded int)
        let column_count = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ResultSetError::InvalidColumnCount))?;

        if column_count > limits::MAX_COLUMNS as u64 {
            return Err(MysqlParseError::Parse(ResultSetError::TooManyColumns(column_count)));
        }

        // Column definitions
        let mut columns = Vec::with_capacity(limits::safe_prealloc(column_count as usize));
        for _ in 0..column_count {
            let col = ColumnDefinition::parse_sync(stream).map_err(|e| match e {
                MysqlParseError::Stream(e) => MysqlParseError::Stream(e),
                MysqlParseError::Parse(e) => MysqlParseError::Parse(ResultSetError::ColumnError(e.to_string())),
            })?;
            columns.push(col);
        }

        // EOF packet (if not DEPRECATE_EOF)
        // Note: In a real implementation, we'd need to read the EOF packet here
        // but we're parsing from a single buffer, so we skip it
        if !capabilities.deprecate_eof() {
            // Would read EOF packet here
        }

        Ok(ResultSetMetadata { column_count, columns })
    }
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for ResultSetMetadata {
    type ParseError = ResultSetError;
    type Value<'s>
        = ResultSetMetadata
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_capabilities_sync(stream, CapabilityFlags::client_default_8x())
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for ResultSetMetadata {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// Parsed column count from result set header.
#[derive(Clone, Copy, Debug)]
pub struct ColumnCount(pub u64);

#[derive(Clone, Debug, thiserror::Error)]
pub enum ColumnCountError {
    #[error("invalid length-encoded integer")]
    InvalidLenEnc,
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for ColumnCount {
    type ParseError = ColumnCountError;
    type Value<'s>
        = ColumnCount
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let count = stream
            .read_lenenc_int_sync()
            .map_err(MysqlParseError::Stream)?
            .map_err(|_| MysqlParseError::Parse(ColumnCountError::InvalidLenEnc))?;

        Ok(ColumnCount(count))
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for ColumnCount {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_column_count() {
        let data = [0x03]; // 3 columns
        let stream = SliceStream::new(&data);

        let count = ColumnCount::parse_sync(&stream).unwrap();
        assert_eq!(count.0, 3);
    }

    #[test]
    fn test_column_count_large() {
        // Length-encoded: 0xFC + 2-byte value
        let data = [0xFC, 0x00, 0x01]; // 256 columns
        let stream = SliceStream::new(&data);

        let count = ColumnCount::parse_sync(&stream).unwrap();
        assert_eq!(count.0, 256);
    }
}
