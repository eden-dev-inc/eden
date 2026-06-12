//! BlockInfo structure for ClickHouse native protocol.
//!
//! Contains metadata about a data block.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Block info field identifiers.
mod field_num {
    pub const IS_OVERFLOWS: u64 = 1;
    pub const BUCKET_NUM: u64 = 2;
}

/// Default bucket number (not assigned to bucket).
pub const DEFAULT_BUCKET_NUM: i32 = -1;

/// BlockInfo metadata for data blocks.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlockInfo {
    /// Whether this block is overflow data (for aggregation with TOTALS).
    pub is_overflows: bool,
    /// Bucket number for distributed processing (-1 if not assigned).
    pub bucket_num: i32,
}

impl BlockInfo {
    /// Create new BlockInfo with default values.
    pub fn new() -> Self {
        Self { is_overflows: false, bucket_num: DEFAULT_BUCKET_NUM }
    }

    /// Parse BlockInfo from a synchronous stream.
    ///
    /// Block info is encoded as a series of (field_num, value) pairs,
    /// terminated by field_num 0.
    pub fn parse_sync<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let mut info = Self::new();

        loop {
            let field_num = stream.read_varuint_sync()?;

            match field_num {
                0 => break,
                field_num::IS_OVERFLOWS => {
                    info.is_overflows = stream.read_u8_ch_sync()? != 0;
                }
                field_num::BUCKET_NUM => {
                    info.bucket_num = stream.read_i32_le_ch_sync()?;
                }
                _ => {
                    // Unknown field - skip it
                    // We don't know the size, so this is an error
                    return Err(ClickhouseWireError::InvalidBlock(format!("unknown block info field: {}", field_num)));
                }
            }
        }

        Ok(info)
    }

    /// Parse BlockInfo asynchronously.
    pub async fn parse<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let mut info = Self::new();

        loop {
            let field_num = stream.read_varuint().await?;

            match field_num {
                0 => break,
                field_num::IS_OVERFLOWS => {
                    info.is_overflows = stream.read_u8_ch().await? != 0;
                }
                field_num::BUCKET_NUM => {
                    info.bucket_num = stream.read_i32_le_ch().await?;
                }
                _ => {
                    return Err(ClickhouseWireError::InvalidBlock(format!("unknown block info field: {}", field_num)));
                }
            }
        }

        Ok(info)
    }

    /// Encode BlockInfo to a writer.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        // Write is_overflows if not default
        if self.is_overflows {
            w.write_varuint(field_num::IS_OVERFLOWS)?;
            w.write_u8_ch(1)?;
        }

        // Write bucket_num if not default
        if self.bucket_num != DEFAULT_BUCKET_NUM {
            w.write_varuint(field_num::BUCKET_NUM)?;
            w.write_i32_le_ch(self.bucket_num)?;
        }

        // Terminate with 0
        w.write_varuint(0)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_block_info_default() {
        let info = BlockInfo::new();
        assert!(!info.is_overflows);
        assert_eq!(info.bucket_num, DEFAULT_BUCKET_NUM);
    }

    #[test]
    fn test_block_info_roundtrip_default() {
        let info = BlockInfo::new();

        let mut buf = Vec::new();
        info.encode(&mut buf).unwrap();

        // Default info should just be a terminating 0
        assert_eq!(buf, vec![0x00]);

        let stream = SliceStream::new(&buf);
        let decoded = BlockInfo::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.is_overflows, info.is_overflows);
        assert_eq!(decoded.bucket_num, info.bucket_num);
    }

    #[test]
    fn test_block_info_roundtrip_with_values() {
        let info = BlockInfo { is_overflows: true, bucket_num: 42 };

        let mut buf = Vec::new();
        info.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = BlockInfo::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.is_overflows, info.is_overflows);
        assert_eq!(decoded.bucket_num, info.bucket_num);
    }
}
