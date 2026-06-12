//! Server MergeTreeAllRangesAnnouncement packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Information about a mark range within a part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MarkRange {
    /// Starting mark index.
    pub begin: u64,
    /// Ending mark index (exclusive).
    pub end: u64,
}

impl MarkRange {
    /// Create a new MarkRange.
    pub fn new(begin: u64, end: u64) -> Self {
        Self { begin, end }
    }

    /// Parse a MarkRange from a synchronous stream.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let begin = stream.read_varuint_sync()?;
        let end = stream.read_varuint_sync()?;
        Ok(Self { begin, end })
    }

    /// Parse a MarkRange asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let begin = stream.read_varuint().await?;
        let end = stream.read_varuint().await?;
        Ok(Self { begin, end })
    }

    /// Encode the MarkRange.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.begin)?;
        w.write_varuint(self.end)?;
        Ok(())
    }
}

/// Information about ranges in a data part.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PartRanges {
    /// Part name.
    pub part_name: String,
    /// Part UUID (as string).
    pub part_uuid: String,
    /// Mark ranges within this part.
    pub ranges: Vec<MarkRange>,
}

impl PartRanges {
    /// Create new PartRanges.
    pub fn new(part_name: impl Into<String>, part_uuid: impl Into<String>) -> Self {
        Self {
            part_name: part_name.into(),
            part_uuid: part_uuid.into(),
            ranges: Vec::new(),
        }
    }

    /// Add a mark range.
    pub fn add_range(&mut self, range: MarkRange) {
        self.ranges.push(range);
    }

    /// Parse PartRanges from a synchronous stream.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let part_name = stream.read_ch_string_utf8_sync()?;
        let part_uuid = stream.read_ch_string_utf8_sync()?;
        let range_count = stream.read_varuint_sync()? as usize;

        let mut ranges = Vec::with_capacity(range_count.min(10000));
        for _ in 0..range_count {
            ranges.push(MarkRange::parse_sync(stream)?);
        }

        Ok(Self { part_name, part_uuid, ranges })
    }

    /// Parse PartRanges asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let part_name = stream.read_ch_string_utf8().await?;
        let part_uuid = stream.read_ch_string_utf8().await?;
        let range_count = stream.read_varuint().await? as usize;

        let mut ranges = Vec::with_capacity(range_count.min(10000));
        for _ in 0..range_count {
            ranges.push(MarkRange::parse(stream).await?);
        }

        Ok(Self { part_name, part_uuid, ranges })
    }

    /// Encode PartRanges.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.part_name)?;
        w.write_ch_string_utf8(&self.part_uuid)?;
        w.write_varuint(self.ranges.len() as u64)?;
        for range in &self.ranges {
            range.encode(w)?;
        }
        Ok(())
    }
}

/// Server MergeTreeAllRangesAnnouncement packet (type 15).
///
/// Announces all ranges available for reading in a distributed MergeTree query.
/// This allows the coordinator to distribute read tasks across nodes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeTreeAllRangesAnnouncement {
    /// Replica number that sent this announcement.
    pub replica_num: u64,
    /// Total number of replicas.
    pub replica_count: u64,
    /// Parts with their ranges.
    pub parts: Vec<PartRanges>,
}

impl MergeTreeAllRangesAnnouncement {
    /// Create a new MergeTreeAllRangesAnnouncement.
    pub fn new(replica_num: u64, replica_count: u64) -> Self {
        Self { replica_num, replica_count, parts: Vec::new() }
    }

    /// Add a part with ranges.
    pub fn add_part(&mut self, part: PartRanges) {
        self.parts.push(part);
    }

    /// Check if this announcement is empty.
    pub fn is_empty(&self) -> bool {
        self.parts.is_empty()
    }

    /// Parse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let replica_num = stream.read_varuint_sync()?;
        let replica_count = stream.read_varuint_sync()?;
        let parts_count = stream.read_varuint_sync()? as usize;

        let mut parts = Vec::with_capacity(parts_count.min(10000));
        for _ in 0..parts_count {
            parts.push(PartRanges::parse_sync(stream)?);
        }

        Ok(Self { replica_num, replica_count, parts })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let replica_num = stream.read_varuint().await?;
        let replica_count = stream.read_varuint().await?;
        let parts_count = stream.read_varuint().await? as usize;

        let mut parts = Vec::with_capacity(parts_count.min(10000));
        for _ in 0..parts_count {
            parts.push(PartRanges::parse(stream).await?);
        }

        Ok(Self { replica_num, replica_count, parts })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::MergeTreeAllRangesAnnouncement.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.replica_num)?;
        w.write_varuint(self.replica_count)?;
        w.write_varuint(self.parts.len() as u64)?;
        for part in &self.parts {
            part.encode(w)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_merge_tree_announcement_empty() {
        let announcement = MergeTreeAllRangesAnnouncement::new(0, 3);
        assert!(announcement.is_empty());

        let mut buf = Vec::new();
        announcement.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = MergeTreeAllRangesAnnouncement::parse_sync(&stream).unwrap();

        assert_eq!(decoded.replica_num, 0);
        assert_eq!(decoded.replica_count, 3);
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_merge_tree_announcement_with_parts() {
        let mut announcement = MergeTreeAllRangesAnnouncement::new(1, 3);

        let mut part = PartRanges::new("part_001", "uuid-1234");
        part.add_range(MarkRange::new(0, 100));
        part.add_range(MarkRange::new(100, 200));
        announcement.add_part(part);

        let mut buf = Vec::new();
        announcement.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = MergeTreeAllRangesAnnouncement::parse_sync(&stream).unwrap();

        assert_eq!(decoded.replica_num, 1);
        assert_eq!(decoded.replica_count, 3);
        assert_eq!(decoded.parts.len(), 1);
        assert_eq!(decoded.parts[0].part_name, "part_001");
        assert_eq!(decoded.parts[0].ranges.len(), 2);
        assert_eq!(decoded.parts[0].ranges[0].begin, 0);
        assert_eq!(decoded.parts[0].ranges[0].end, 100);
    }
}
