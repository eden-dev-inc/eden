//! Client Scalar packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::block::Block;
use crate::native::packet::ClientPacketType;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Client Scalar packet (type 7).
///
/// Sends a single scalar value to the server.
/// Used for scalar subqueries and parameters.
#[derive(Clone, Debug, PartialEq)]
pub struct Scalar {
    /// The scalar data block (should contain a single value).
    pub block: Block,
}

impl Scalar {
    /// Create a new Scalar packet with a block.
    pub fn new(block: Block) -> Self {
        Self { block }
    }

    /// Create an empty scalar.
    pub fn empty() -> Self {
        Self { block: Block::empty() }
    }

    /// Parse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let block = Block::parse_sync(stream, protocol_version)?;
        Ok(Self { block })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S, protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let block = Block::parse(stream, protocol_version).await?;
        Ok(Self { block })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Scalar.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.block.encode(w)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_scalar_roundtrip() {
        let scalar = Scalar::empty();

        let mut buf = Vec::new();
        scalar.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = Scalar::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert!(decoded.block.is_empty());
    }
}
