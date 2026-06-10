//! Server TimezoneUpdate packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::packet::ServerPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server TimezoneUpdate packet (type 17).
///
/// Sent by server to update the client's timezone setting.
/// This can happen when the server's timezone changes or when
/// a session parameter update affects the timezone.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimezoneUpdate {
    /// The new timezone name (e.g., "UTC", "America/New_York").
    pub timezone: String,
}

impl TimezoneUpdate {
    /// Create a new TimezoneUpdate packet.
    pub fn new(timezone: impl Into<String>) -> Self {
        Self { timezone: timezone.into() }
    }

    /// Create a UTC timezone update.
    pub fn utc() -> Self {
        Self::new("UTC")
    }

    /// Check if this is UTC timezone.
    pub fn is_utc(&self) -> bool {
        self.timezone == "UTC"
    }

    /// Parse a TimezoneUpdate from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let timezone = stream.read_ch_string_utf8_sync()?;
        Ok(Self { timezone })
    }

    /// Parse a TimezoneUpdate asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;
        let timezone = stream.read_ch_string_utf8().await?;
        Ok(Self { timezone })
    }

    /// Encode the packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ServerPacketType::TimezoneUpdate.as_u64())?;
        self.encode_body(w)
    }

    /// Encode the TimezoneUpdate body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.timezone)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_timezone_update_utc() {
        let update = TimezoneUpdate::utc();
        assert!(update.is_utc());
    }

    #[test]
    fn test_timezone_update_roundtrip() {
        let update = TimezoneUpdate::new("America/New_York");

        let mut buf = Vec::new();
        update.encode(&mut buf).unwrap();

        // Skip packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = TimezoneUpdate::parse_sync(&stream).unwrap();

        assert_eq!(decoded.timezone, "America/New_York");
        assert!(!decoded.is_utc());
    }
}
