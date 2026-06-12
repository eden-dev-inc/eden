//! Block structure for ClickHouse native protocol.
//!
//! Blocks are the primary unit of data transfer in the native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block_info::BlockInfo;
use crate::native::column::Column;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// A data block containing columnar data.
///
/// Blocks are the primary unit of data transfer in ClickHouse.
/// They contain metadata (BlockInfo), dimensions (rows/columns),
/// and the actual column data.
#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    /// Block metadata (overflow status, bucket number).
    pub info: BlockInfo,
    /// Number of columns.
    pub num_columns: u64,
    /// Number of rows.
    pub num_rows: u64,
    /// Column data.
    pub columns: Vec<Column>,
}

impl Block {
    /// Create an empty block.
    pub fn empty() -> Self {
        Self {
            info: BlockInfo::new(),
            num_columns: 0,
            num_rows: 0,
            columns: Vec::new(),
        }
    }

    /// Create a block with columns.
    pub fn with_columns(columns: Vec<Column>, num_rows: u64) -> Self {
        Self {
            info: BlockInfo::new(),
            num_columns: columns.len() as u64,
            num_rows,
            columns,
        }
    }

    /// Check if this is an empty block (used as end marker).
    pub fn is_empty(&self) -> bool {
        self.num_columns == 0 && self.num_rows == 0
    }

    /// Parse a Block from a synchronous stream.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let info = BlockInfo::parse_sync(stream, protocol_version)?;
        let num_columns = stream.read_varuint_sync()?;
        let num_rows = stream.read_varuint_sync()?;

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let column = Column::parse_sync(stream, num_rows as usize, protocol_version)?;
            columns.push(column);
        }

        Ok(Self { info, num_columns, num_rows, columns })
    }

    /// Parse a Block asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let info = BlockInfo::parse(stream, protocol_version).await?;
        let num_columns = stream.read_varuint().await?;
        let num_rows = stream.read_varuint().await?;

        let mut columns = Vec::with_capacity(num_columns as usize);
        for _ in 0..num_columns {
            let column = Column::parse(stream, num_rows as usize, protocol_version).await?;
            columns.push(column);
        }

        Ok(Self { info, num_columns, num_rows, columns })
    }

    /// Encode a Block to a writer.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.info.encode(w)?;
        w.write_varuint(self.num_columns)?;
        w.write_varuint(self.num_rows)?;

        for column in &self.columns {
            column.encode(w, self.num_rows as usize)?;
        }

        Ok(())
    }
}

impl Default for Block {
    fn default() -> Self {
        Self::empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_empty_block() {
        let block = Block::empty();
        assert!(block.is_empty());
        assert_eq!(block.num_columns, 0);
        assert_eq!(block.num_rows, 0);
    }

    #[test]
    fn test_empty_block_roundtrip() {
        let block = Block::empty();

        let mut buf = Vec::new();
        block.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = Block::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert!(decoded.is_empty());
        assert_eq!(decoded.num_columns, block.num_columns);
        assert_eq!(decoded.num_rows, block.num_rows);
    }

    #[test]
    fn test_block_with_columns() {
        let columns = vec![Column::empty("id", "UInt64"), Column::empty("name", "String")];
        let block = Block::with_columns(columns, 0);

        assert!(!block.is_empty()); // Has columns, even if no rows
        assert_eq!(block.num_columns, 2);
        assert_eq!(block.num_rows, 0);
    }
}
