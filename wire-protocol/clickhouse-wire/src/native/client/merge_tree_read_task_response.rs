//! Client MergeTreeReadTaskResponse packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Client MergeTreeReadTaskResponse packet (type 10).
///
/// Response to server's MergeTreeReadTaskRequest for distributed MergeTree queries.
/// Contains information about the data part and ranges to read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MergeTreeReadTaskResponse {
    /// Description of the task (may contain part name and ranges).
    pub task_description: String,
}

impl MergeTreeReadTaskResponse {
    /// Create a new MergeTreeReadTaskResponse.
    pub fn new(task_description: impl Into<String>) -> Self {
        Self { task_description: task_description.into() }
    }

    /// Create an empty response (no more tasks).
    pub fn empty() -> Self {
        Self { task_description: String::new() }
    }

    /// Check if this is an empty response.
    pub fn is_empty(&self) -> bool {
        self.task_description.is_empty()
    }

    /// Parse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let task_description = stream.read_ch_string_utf8_sync()?;
        Ok(Self { task_description })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let task_description = stream.read_ch_string_utf8().await?;
        Ok(Self { task_description })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::MergeTreeReadTaskResponse.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.task_description)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_merge_tree_read_task_response_roundtrip() {
        let response = MergeTreeReadTaskResponse::new("part_001_range_0_100");

        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = MergeTreeReadTaskResponse::parse_sync(&stream).unwrap();

        assert_eq!(decoded.task_description, "part_001_range_0_100");
    }

    #[test]
    fn test_empty_merge_tree_read_task_response() {
        let response = MergeTreeReadTaskResponse::empty();
        assert!(response.is_empty());

        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = MergeTreeReadTaskResponse::parse_sync(&stream).unwrap();

        assert!(decoded.is_empty());
    }
}
