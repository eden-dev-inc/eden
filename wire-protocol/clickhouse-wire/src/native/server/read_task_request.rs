//! Server ReadTaskRequest packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server ReadTaskRequest packet (type 13).
///
/// Sent by server to request the client to process a read task
/// in a distributed query. The client should respond with ReadTaskResponse.
///
/// This is an empty packet - just the packet type indicates the request.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct ReadTaskRequest;

impl ReadTaskRequest {
    /// Create a new ReadTaskRequest.
    pub fn new() -> Self {
        Self
    }

    /// Parse a ReadTaskRequest from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    /// This packet has no body.
    pub fn parse_sync<S>(_stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        Ok(Self)
    }

    /// Parse a ReadTaskRequest asynchronously.
    pub async fn parse<S>(_stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        Ok(Self)
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::ReadTaskRequest.as_u64())?;
        Ok(())
    }

    /// Encode the ReadTaskRequest body (without packet type).
    /// This packet has no body.
    pub fn encode_body<W: Write>(&self, _w: &mut W) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_read_task_request_roundtrip() {
        let request = ReadTaskRequest::new();

        let mut buf = Vec::new();
        request.encode(&mut buf).unwrap();

        // Verify packet type is correct
        assert_eq!(buf[0], ServerPacketType::ReadTaskRequest.as_u64() as u8);

        // Parse (empty body)
        let stream = SliceStream::new(&buf[1..]);
        let decoded = ReadTaskRequest::parse_sync(&stream).unwrap();

        assert_eq!(decoded, request);
    }
}
