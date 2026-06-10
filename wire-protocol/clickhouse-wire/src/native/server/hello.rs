//! Server Hello packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Protocol revision thresholds.
pub mod revisions {
    /// Minimum revision with server timezone support.
    pub const SERVER_TIMEZONE: u64 = 54423;
    /// Minimum revision with server display name.
    pub const DISPLAY_NAME: u64 = 54372;
    /// Minimum revision with server version patch.
    pub const VERSION_PATCH: u64 = 54401;
}

/// Server Hello packet (type 0).
///
/// Sent by the server in response to a ClientHello.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerHello {
    /// Server name (e.g., "ClickHouse").
    pub server_name: String,
    /// Server version major number.
    pub version_major: u64,
    /// Server version minor number.
    pub version_minor: u64,
    /// Protocol revision.
    pub protocol_version: u64,
    /// Server timezone (if supported).
    pub timezone: Option<String>,
    /// Server display name (if supported).
    pub display_name: Option<String>,
    /// Server version patch number (if supported).
    pub version_patch: Option<u64>,
}

impl ServerHello {
    /// Parse a ServerHello packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S, client_protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let server_name = stream.read_ch_string_utf8_sync()?;
        let version_major = stream.read_varuint_sync()?;
        let version_minor = stream.read_varuint_sync()?;
        let protocol_version = stream.read_varuint_sync()?;

        let timezone = if client_protocol_version >= revisions::SERVER_TIMEZONE {
            Some(stream.read_ch_string_utf8_sync()?)
        } else {
            None
        };

        let display_name = if client_protocol_version >= revisions::DISPLAY_NAME {
            Some(stream.read_ch_string_utf8_sync()?)
        } else {
            None
        };

        let version_patch = if client_protocol_version >= revisions::VERSION_PATCH {
            Some(stream.read_varuint_sync()?)
        } else {
            None
        };

        Ok(Self {
            server_name,
            version_major,
            version_minor,
            protocol_version,
            timezone,
            display_name,
            version_patch,
        })
    }

    /// Parse a ServerHello packet asynchronously.
    pub async fn parse<S>(stream: &S, client_protocol_version: u64) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let server_name = stream.read_ch_string_utf8().await?;
        let version_major = stream.read_varuint().await?;
        let version_minor = stream.read_varuint().await?;
        let protocol_version = stream.read_varuint().await?;

        let timezone = if client_protocol_version >= revisions::SERVER_TIMEZONE {
            Some(stream.read_ch_string_utf8().await?)
        } else {
            None
        };

        let display_name = if client_protocol_version >= revisions::DISPLAY_NAME {
            Some(stream.read_ch_string_utf8().await?)
        } else {
            None
        };

        let version_patch = if client_protocol_version >= revisions::VERSION_PATCH {
            Some(stream.read_varuint().await?)
        } else {
            None
        };

        Ok(Self {
            server_name,
            version_major,
            version_minor,
            protocol_version,
            timezone,
            display_name,
            version_patch,
        })
    }

    /// Encode the ServerHello packet body (without packet type).
    pub fn encode<W: Write>(&self, w: &mut W, client_protocol_version: u64) -> io::Result<()> {
        w.write_ch_string_utf8(&self.server_name)?;
        w.write_varuint(self.version_major)?;
        w.write_varuint(self.version_minor)?;
        w.write_varuint(self.protocol_version)?;

        if client_protocol_version >= revisions::SERVER_TIMEZONE {
            w.write_ch_string_utf8(self.timezone.as_deref().unwrap_or(""))?;
        }

        if client_protocol_version >= revisions::DISPLAY_NAME {
            w.write_ch_string_utf8(self.display_name.as_deref().unwrap_or(""))?;
        }

        if client_protocol_version >= revisions::VERSION_PATCH {
            w.write_varuint(self.version_patch.unwrap_or(self.protocol_version))?;
        }

        Ok(())
    }

    /// Get full server version string.
    pub fn version_string(&self) -> String {
        match self.version_patch {
            Some(patch) => format!("{}.{}.{}", self.version_major, self.version_minor, patch),
            None => format!("{}.{}", self.version_major, self.version_minor),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::DBMS_TCP_PROTOCOL_VERSION;
    use wire_stream::SliceStream;

    #[test]
    fn test_server_hello_roundtrip() {
        let hello = ServerHello {
            server_name: "ClickHouse".to_string(),
            version_major: 21,
            version_minor: 8,
            protocol_version: 54448,
            timezone: Some("UTC".to_string()),
            display_name: Some("clickhouse-server".to_string()),
            version_patch: Some(54448),
        };

        let mut buf = Vec::new();
        hello.encode(&mut buf, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = ServerHello::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION).unwrap();

        assert_eq!(decoded.server_name, hello.server_name);
        assert_eq!(decoded.version_major, hello.version_major);
        assert_eq!(decoded.version_minor, hello.version_minor);
        assert_eq!(decoded.protocol_version, hello.protocol_version);
        assert_eq!(decoded.timezone, hello.timezone);
        assert_eq!(decoded.display_name, hello.display_name);
    }

    #[test]
    fn test_version_string() {
        let hello = ServerHello {
            server_name: "ClickHouse".to_string(),
            version_major: 21,
            version_minor: 8,
            protocol_version: 54448,
            timezone: None,
            display_name: None,
            version_patch: Some(123),
        };

        assert_eq!(hello.version_string(), "21.8.123");
    }
}
