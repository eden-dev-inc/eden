//! Server Progress packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Protocol revision thresholds for Progress fields.
pub mod revisions {
    /// Minimum revision with total_rows_to_read.
    pub const TOTAL_ROWS: u64 = 54060;
    /// Minimum revision with written rows/bytes.
    pub const WRITTEN: u64 = 54310;
    /// Minimum revision with elapsed_ns.
    pub const ELAPSED: u64 = 54460;
}

/// Server Progress packet (type 3).
///
/// Reports query execution progress.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Progress {
    /// Number of rows read.
    pub rows: u64,
    /// Number of bytes read.
    pub bytes: u64,
    /// Total rows to read (estimate, if known).
    pub total_rows_to_read: u64,
    /// Number of rows written.
    pub written_rows: u64,
    /// Number of bytes written.
    pub written_bytes: u64,
    /// Elapsed time in nanoseconds.
    pub elapsed_ns: u64,
}

impl Progress {
    /// Create a new Progress with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parse a Progress packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let rows = stream.read_varuint_sync()?;
        let bytes = stream.read_varuint_sync()?;

        let total_rows_to_read = if protocol_version >= revisions::TOTAL_ROWS {
            stream.read_varuint_sync()?
        } else {
            0
        };

        let (written_rows, written_bytes) = if protocol_version >= revisions::WRITTEN {
            (stream.read_varuint_sync()?, stream.read_varuint_sync()?)
        } else {
            (0, 0)
        };

        let elapsed_ns = if protocol_version >= revisions::ELAPSED {
            stream.read_varuint_sync()?
        } else {
            0
        };

        Ok(Self {
            rows,
            bytes,
            total_rows_to_read,
            written_rows,
            written_bytes,
            elapsed_ns,
        })
    }

    /// Parse a Progress packet asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let rows = stream.read_varuint().await?;
        let bytes = stream.read_varuint().await?;

        let total_rows_to_read = if protocol_version >= revisions::TOTAL_ROWS {
            stream.read_varuint().await?
        } else {
            0
        };

        let (written_rows, written_bytes) = if protocol_version >= revisions::WRITTEN {
            (stream.read_varuint().await?, stream.read_varuint().await?)
        } else {
            (0, 0)
        };

        let elapsed_ns = if protocol_version >= revisions::ELAPSED {
            stream.read_varuint().await?
        } else {
            0
        };

        Ok(Self {
            rows,
            bytes,
            total_rows_to_read,
            written_rows,
            written_bytes,
            elapsed_ns,
        })
    }

    /// Encode the Progress packet.
    pub fn encode<W: Write>(&self, w: &mut W, protocol_version: u64) -> io::Result<()> {
        w.write_varuint(self.rows)?;
        w.write_varuint(self.bytes)?;

        if protocol_version >= revisions::TOTAL_ROWS {
            w.write_varuint(self.total_rows_to_read)?;
        }

        if protocol_version >= revisions::WRITTEN {
            w.write_varuint(self.written_rows)?;
            w.write_varuint(self.written_bytes)?;
        }

        if protocol_version >= revisions::ELAPSED {
            w.write_varuint(self.elapsed_ns)?;
        }

        Ok(())
    }

    /// Get completion percentage (if total_rows is known).
    pub fn completion_percent(&self) -> Option<f64> {
        if self.total_rows_to_read > 0 {
            Some((self.rows as f64 / self.total_rows_to_read as f64) * 100.0)
        } else {
            None
        }
    }

    /// Get elapsed time in seconds.
    pub fn elapsed_seconds(&self) -> f64 {
        self.elapsed_ns as f64 / 1_000_000_000.0
    }

    /// Accumulate another progress update.
    pub fn accumulate(&mut self, other: &Progress) {
        self.rows += other.rows;
        self.bytes += other.bytes;
        self.written_rows += other.written_rows;
        self.written_bytes += other.written_bytes;
        // total_rows_to_read and elapsed_ns are replaced, not accumulated
        if other.total_rows_to_read > 0 {
            self.total_rows_to_read = other.total_rows_to_read;
        }
        if other.elapsed_ns > 0 {
            self.elapsed_ns = other.elapsed_ns;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_progress_roundtrip() {
        // Use a protocol version that supports all fields
        let protocol_version = revisions::ELAPSED + 1;

        let progress = Progress {
            rows: 1000,
            bytes: 50000,
            total_rows_to_read: 10000,
            written_rows: 500,
            written_bytes: 25000,
            elapsed_ns: 1_000_000_000,
        };

        let mut buf = Vec::new();
        progress.encode(&mut buf, protocol_version).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = Progress::parse_sync(&stream, protocol_version).unwrap();

        assert_eq!(decoded.rows, progress.rows);
        assert_eq!(decoded.bytes, progress.bytes);
        assert_eq!(decoded.total_rows_to_read, progress.total_rows_to_read);
        assert_eq!(decoded.written_rows, progress.written_rows);
        assert_eq!(decoded.written_bytes, progress.written_bytes);
        assert_eq!(decoded.elapsed_ns, progress.elapsed_ns);
    }

    #[test]
    fn test_completion_percent() {
        let progress = Progress { rows: 50, total_rows_to_read: 100, ..Default::default() };
        assert!((progress.completion_percent().unwrap() - 50.0).abs() < f64::EPSILON);

        let no_total = Progress::default();
        assert!(no_total.completion_percent().is_none());
    }

    #[test]
    fn test_elapsed_seconds() {
        let progress = Progress { elapsed_ns: 2_500_000_000, ..Default::default() };
        assert!((progress.elapsed_seconds() - 2.5).abs() < 0.001);
    }

    #[test]
    fn test_accumulate() {
        let mut total = Progress::default();
        let update = Progress {
            rows: 100,
            bytes: 5000,
            total_rows_to_read: 1000,
            ..Default::default()
        };

        total.accumulate(&update);
        assert_eq!(total.rows, 100);
        assert_eq!(total.bytes, 5000);
        assert_eq!(total.total_rows_to_read, 1000);

        total.accumulate(&update);
        assert_eq!(total.rows, 200);
        assert_eq!(total.bytes, 10000);
    }
}
