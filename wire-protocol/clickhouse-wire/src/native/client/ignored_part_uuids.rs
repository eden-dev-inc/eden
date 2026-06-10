//! Client IgnoredPartUUIDs packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Client IgnoredPartUUIDs packet (type 8).
///
/// Sends a list of part UUIDs that should be ignored during distributed queries.
/// This is used to avoid reading duplicate data in distributed queries.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IgnoredPartUUIDs {
    /// List of UUIDs to ignore (stored as strings for simplicity).
    pub uuids: Vec<String>,
}

impl IgnoredPartUUIDs {
    /// Create a new IgnoredPartUUIDs packet.
    pub fn new(uuids: Vec<String>) -> Self {
        Self { uuids }
    }

    /// Create an empty packet.
    pub fn empty() -> Self {
        Self { uuids: Vec::new() }
    }

    /// Parse from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let count = stream.read_varuint_sync()? as usize;
        let mut uuids = Vec::with_capacity(count);
        for _ in 0..count {
            // UUIDs are transmitted as 16 bytes, read as hex string
            let uuid_bytes = stream.read_ch_string_sync()?;
            let uuid_str = if uuid_bytes.len() == 16 {
                // Format as UUID string
                format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    uuid_bytes[0],
                    uuid_bytes[1],
                    uuid_bytes[2],
                    uuid_bytes[3],
                    uuid_bytes[4],
                    uuid_bytes[5],
                    uuid_bytes[6],
                    uuid_bytes[7],
                    uuid_bytes[8],
                    uuid_bytes[9],
                    uuid_bytes[10],
                    uuid_bytes[11],
                    uuid_bytes[12],
                    uuid_bytes[13],
                    uuid_bytes[14],
                    uuid_bytes[15]
                )
            } else {
                String::from_utf8_lossy(&uuid_bytes).to_string()
            };
            uuids.push(uuid_str);
        }
        Ok(Self { uuids })
    }

    /// Parse asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let count = stream.read_varuint().await? as usize;
        let mut uuids = Vec::with_capacity(count);
        for _ in 0..count {
            let uuid_bytes = stream.read_ch_string().await?;
            let uuid_str = if uuid_bytes.len() == 16 {
                format!(
                    "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                    uuid_bytes[0],
                    uuid_bytes[1],
                    uuid_bytes[2],
                    uuid_bytes[3],
                    uuid_bytes[4],
                    uuid_bytes[5],
                    uuid_bytes[6],
                    uuid_bytes[7],
                    uuid_bytes[8],
                    uuid_bytes[9],
                    uuid_bytes[10],
                    uuid_bytes[11],
                    uuid_bytes[12],
                    uuid_bytes[13],
                    uuid_bytes[14],
                    uuid_bytes[15]
                )
            } else {
                String::from_utf8_lossy(&uuid_bytes).to_string()
            };
            uuids.push(uuid_str);
        }
        Ok(Self { uuids })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::IgnoredPartUUIDs.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(self.uuids.len() as u64)?;
        for uuid in &self.uuids {
            // Write UUID as 16 bytes
            let bytes = parse_uuid_to_bytes(uuid);
            w.write_ch_string(&bytes)?;
        }
        Ok(())
    }
}

/// Parse a UUID string to 16 bytes.
fn parse_uuid_to_bytes(uuid: &str) -> Vec<u8> {
    let hex: String = uuid.chars().filter(|c| c.is_ascii_hexdigit()).collect();
    if hex.len() == 32 {
        (0..16).map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or(0)).collect()
    } else {
        uuid.as_bytes().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_ignored_part_uuids_empty() {
        let packet = IgnoredPartUUIDs::empty();

        let mut buf = Vec::new();
        packet.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf[1..]);
        let decoded = IgnoredPartUUIDs::parse_sync(&stream).unwrap();

        assert!(decoded.uuids.is_empty());
    }

    #[test]
    fn test_parse_uuid_to_bytes() {
        let uuid = "550e8400-e29b-41d4-a716-446655440000";
        let bytes = parse_uuid_to_bytes(uuid);
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[0], 0x55);
        assert_eq!(bytes[1], 0x0e);
    }
}
