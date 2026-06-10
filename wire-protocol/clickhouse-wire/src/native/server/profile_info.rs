//! Server ProfileInfo packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server ProfileInfo packet (type 6).
///
/// Contains query execution profile information.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProfileInfo {
    /// Number of rows processed.
    pub rows: u64,
    /// Number of blocks processed.
    pub blocks: u64,
    /// Number of bytes processed.
    pub bytes: u64,
    /// Whether limit was applied.
    pub applied_limit: bool,
    /// Rows before limit.
    pub rows_before_limit: u64,
    /// Whether limit is approximate.
    pub calculated_rows_before_limit: bool,
}

impl ProfileInfo {
    /// Create a new ProfileInfo with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a ProfileInfo packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let rows = stream.read_varuint_sync()?;
        let blocks = stream.read_varuint_sync()?;
        let bytes = stream.read_varuint_sync()?;
        let applied_limit = stream.read_bool_ch_sync()?;
        let rows_before_limit = stream.read_varuint_sync()?;
        let calculated_rows_before_limit = stream.read_bool_ch_sync()?;

        Ok(Self {
            rows,
            blocks,
            bytes,
            applied_limit,
            rows_before_limit,
            calculated_rows_before_limit,
        })
    }

    /// Parse a ProfileInfo packet asynchronously.
    pub async fn parse<S>(stream: &S, _protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let rows = stream.read_varuint().await?;
        let blocks = stream.read_varuint().await?;
        let bytes = stream.read_varuint().await?;
        let applied_limit = stream.read_bool_ch().await?;
        let rows_before_limit = stream.read_varuint().await?;
        let calculated_rows_before_limit = stream.read_bool_ch().await?;

        Ok(Self {
            rows,
            blocks,
            bytes,
            applied_limit,
            rows_before_limit,
            calculated_rows_before_limit,
        })
    }

    /// Encode the ProfileInfo packet.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.rows)?;
        w.write_varuint(self.blocks)?;
        w.write_varuint(self.bytes)?;
        w.write_bool_ch(self.applied_limit)?;
        w.write_varuint(self.rows_before_limit)?;
        w.write_bool_ch(self.calculated_rows_before_limit)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_profile_info_roundtrip() {
        let info = ProfileInfo {
            rows: 1000,
            blocks: 10,
            bytes: 50000,
            applied_limit: true,
            rows_before_limit: 5000,
            calculated_rows_before_limit: false,
        };

        let mut buf = Vec::new();
        info.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = ProfileInfo::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.rows, info.rows);
        assert_eq!(decoded.blocks, info.blocks);
        assert_eq!(decoded.bytes, info.bytes);
        assert_eq!(decoded.applied_limit, info.applied_limit);
        assert_eq!(decoded.rows_before_limit, info.rows_before_limit);
    }
}
