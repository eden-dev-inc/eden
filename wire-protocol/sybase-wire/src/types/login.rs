//! TDS login packet types.

use crate::error::SybaseWireError;
use crate::parse::{SybaseParse, SybaseParseError, SybaseParseSync};
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{WireRead, WireReadSync};

/// TDS 4.2 login packet.
#[derive(Clone, Debug)]
pub struct Login {
    /// Client hostname.
    pub hostname: Vec<u8>,
    /// Username.
    pub username: Vec<u8>,
    /// Password (encrypted or plain depending on version).
    pub password: Vec<u8>,
    /// Application name.
    pub app_name: Vec<u8>,
    /// Server name.
    pub server_name: Vec<u8>,
    /// Library name.
    pub library_name: Vec<u8>,
    /// Language.
    pub language: Vec<u8>,
    /// Character set.
    pub charset: Vec<u8>,
    /// Requested packet size.
    pub packet_size: u32,
    /// TDS version.
    pub tds_version: u32,
}

impl Login {
    /// Parse a fixed-length string field with length byte at the end.
    fn parse_fixed_string<S: WireReadSync + ?Sized>(stream: &S, max_len: usize) -> Result<Vec<u8>, S::ReadError> {
        // Read fixed bytes
        let borrow = stream.peek(Some(max_len))?;
        let data = borrow[..max_len].to_vec();
        stream.accept(&borrow, None)?;

        // Read length byte
        let len = stream.read_u8_sync()? as usize;
        let actual_len = len.min(max_len);

        Ok(data[..actual_len].to_vec())
    }
}

impl<S: WireReadSync + ?Sized> SybaseParseSync<S> for Login {
    type ParseError = SybaseWireError;
    type Value<'s>
        = Login
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Hostname (30 bytes + 1 length)
        let hostname = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Username (30 bytes + 1 length)
        let username = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Password (30 bytes + 1 length)
        let password = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Host process ID (30 bytes + 1 length) - skip
        let _ = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Skip byte order, char type, float type, date format, etc. (18 bytes)
        let borrow = stream.peek(Some(18)).map_err(SybaseParseError::Stream)?;
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        // App name (30 bytes + 1 length)
        let app_name = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Server name (30 bytes + 1 length)
        let server_name = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Remote password (skip: 1 length + 253 bytes + 1 remaining)
        let borrow = stream.peek(Some(255)).map_err(SybaseParseError::Stream)?;
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        // TDS version (4 bytes, big-endian)
        let tds_version = stream.read_u32_be_sync().map_err(SybaseParseError::Stream)?;

        // Library name (10 bytes + 1 length)
        let library_name = Self::parse_fixed_string(stream, 10).map_err(SybaseParseError::Stream)?;

        // Program version (4 bytes) - skip
        let borrow = stream.peek(Some(4)).map_err(SybaseParseError::Stream)?;
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        // Language (30 bytes + 1 length)
        let language = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Skip notify, old secure login, encrypted password (4 bytes)
        let borrow = stream.peek(Some(4)).map_err(SybaseParseError::Stream)?;
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        // Charset (30 bytes + 1 length)
        let charset = Self::parse_fixed_string(stream, 30).map_err(SybaseParseError::Stream)?;

        // Skip set charset notify (1 byte)
        let _ = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Packet size as string (6 bytes + 1 length)
        let packet_size_str = Self::parse_fixed_string(stream, 6).map_err(SybaseParseError::Stream)?;
        let packet_size = std::str::from_utf8(&packet_size_str).ok().and_then(|s| s.trim_end_matches('\0').parse().ok()).unwrap_or(512);

        Ok(Login {
            hostname,
            username,
            password,
            app_name,
            server_name,
            library_name,
            language,
            charset,
            packet_size,
            tds_version,
        })
    }
}

impl<S: WireRead + ?Sized> SybaseParse<S> for Login {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // For now, use the sync version since login parsing is typically done on complete buffers
        Self::parse_sync(stream)
    }
}

/// TDS 5.0 login packet.
///
/// Similar to Login but with additional capability negotiation.
#[derive(Clone, Debug)]
pub struct Login5 {
    /// Base login information.
    pub base: Login,
    /// Capability tokens (if present).
    pub capabilities: Option<Vec<u8>>,
}

impl<S: WireReadSync + ?Sized> SybaseParseSync<S> for Login5 {
    type ParseError = SybaseWireError;
    type Value<'s>
        = Login5
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let base = Login::parse_sync(stream)?;

        // TDS 5.0 may have additional capability data
        // For now, we don't parse it
        Ok(Login5 { base, capabilities: None })
    }
}

impl<S: WireRead + ?Sized> SybaseParse<S> for Login5 {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}
