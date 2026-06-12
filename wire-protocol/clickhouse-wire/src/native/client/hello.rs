//! Client Hello packet for ClickHouse native protocol.
//!
//! The first packet sent by the client to initiate a connection.

use crate::error::ClickhouseWireError;
use crate::native::packet::ClientPacketType;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Current ClickHouse protocol version.
pub const DBMS_TCP_PROTOCOL_VERSION: u64 = 54448;

/// Default client name.
pub const DEFAULT_CLIENT_NAME: &str = "ClickHouse Rust Client";

/// Client Hello packet (type 0).
///
/// Sent by the client as the first packet to initiate a connection.
/// Contains client identification and authentication credentials.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientHello {
    /// Client name (e.g., "ClickHouse Rust Client").
    pub client_name: String,
    /// Client version major number.
    pub version_major: u64,
    /// Client version minor number.
    pub version_minor: u64,
    /// Protocol version (e.g., 54448).
    pub protocol_version: u64,
    /// Database to connect to.
    pub database: String,
    /// Username for authentication.
    pub user: String,
    /// Password for authentication.
    pub password: String,
}

impl ClientHello {
    /// Create a new ClientHello with default client version.
    pub fn new(database: impl Into<String>, user: impl Into<String>, password: impl Into<String>) -> Self {
        Self {
            client_name: DEFAULT_CLIENT_NAME.to_string(),
            version_major: 21,
            version_minor: 0,
            protocol_version: DBMS_TCP_PROTOCOL_VERSION,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        }
    }

    /// Create a ClientHello with custom client name and version.
    pub fn with_client_info(
        client_name: impl Into<String>,
        version_major: u64,
        version_minor: u64,
        protocol_version: u64,
        database: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            client_name: client_name.into(),
            version_major,
            version_minor,
            protocol_version,
            database: database.into(),
            user: user.into(),
            password: password.into(),
        }
    }

    /// Parse a ClientHello packet from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte - the caller should
    /// have already read and verified it.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let client_name = stream.read_ch_string_utf8_sync()?;
        let version_major = stream.read_varuint_sync()?;
        let version_minor = stream.read_varuint_sync()?;
        let protocol_version = stream.read_varuint_sync()?;
        let database = stream.read_ch_string_utf8_sync()?;
        let user = stream.read_ch_string_utf8_sync()?;
        let password = stream.read_ch_string_utf8_sync()?;

        Ok(Self {
            client_name,
            version_major,
            version_minor,
            protocol_version,
            database,
            user,
            password,
        })
    }

    /// Parse a ClientHello packet asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let client_name = stream.read_ch_string_utf8().await?;
        let version_major = stream.read_varuint().await?;
        let version_minor = stream.read_varuint().await?;
        let protocol_version = stream.read_varuint().await?;
        let database = stream.read_ch_string_utf8().await?;
        let user = stream.read_ch_string_utf8().await?;
        let password = stream.read_ch_string_utf8().await?;

        Ok(Self {
            client_name,
            version_major,
            version_minor,
            protocol_version,
            database,
            user,
            password,
        })
    }

    /// Encode the ClientHello packet (including packet type).
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_varuint(ClientPacketType::Hello.as_u64())?;
        w.write_ch_string_utf8(&self.client_name)?;
        w.write_varuint(self.version_major)?;
        w.write_varuint(self.version_minor)?;
        w.write_varuint(self.protocol_version)?;
        w.write_ch_string_utf8(&self.database)?;
        w.write_ch_string_utf8(&self.user)?;
        w.write_ch_string_utf8(&self.password)?;
        Ok(())
    }

    /// Encode the ClientHello packet body (without packet type).
    pub fn encode_body<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_ch_string_utf8(&self.client_name)?;
        w.write_varuint(self.version_major)?;
        w.write_varuint(self.version_minor)?;
        w.write_varuint(self.protocol_version)?;
        w.write_ch_string_utf8(&self.database)?;
        w.write_ch_string_utf8(&self.user)?;
        w.write_ch_string_utf8(&self.password)?;
        Ok(())
    }
}

impl Default for ClientHello {
    fn default() -> Self {
        Self::new("default", "default", "")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_client_hello_roundtrip() {
        let hello = ClientHello::new("mydb", "myuser", "mypassword");

        let mut buf = Vec::new();
        hello.encode(&mut buf).unwrap();

        // Skip the packet type byte
        let stream = SliceStream::new(&buf[1..]);
        let decoded = ClientHello::parse_sync(&stream).unwrap();

        assert_eq!(decoded.client_name, hello.client_name);
        assert_eq!(decoded.version_major, hello.version_major);
        assert_eq!(decoded.version_minor, hello.version_minor);
        assert_eq!(decoded.protocol_version, hello.protocol_version);
        assert_eq!(decoded.database, hello.database);
        assert_eq!(decoded.user, hello.user);
        assert_eq!(decoded.password, hello.password);
    }

    #[test]
    fn test_client_hello_default() {
        let hello = ClientHello::default();
        assert_eq!(hello.database, "default");
        assert_eq!(hello.user, "default");
        assert_eq!(hello.password, "");
    }

    #[test]
    fn test_client_hello_with_client_info() {
        let hello = ClientHello::with_client_info("My Client", 1, 2, 54448, "testdb", "testuser", "testpass");
        assert_eq!(hello.client_name, "My Client");
        assert_eq!(hello.version_major, 1);
        assert_eq!(hello.version_minor, 2);
    }
}
