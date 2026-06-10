//! MySQL authentication switch request packet.
//!
//! The server sends this when it wants the client to use a different
//! authentication plugin than initially requested.

use crate::error::packet_types;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use wire_stream::{WireRead, WireReadSync};

/// Authentication switch request from server.
///
/// Sent when the server requires the client to switch to a different
/// authentication plugin during the handshake.
#[derive(Clone, Debug)]
pub struct AuthSwitchRequest {
    /// The name of the authentication plugin to use.
    pub plugin_name: String,
    /// Plugin-specific data (e.g., new challenge/nonce).
    pub plugin_data: Vec<u8>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthSwitchError {
    #[error("invalid auth switch header: expected 0xFE, got {0:#04x}")]
    InvalidHeader(u8),
    #[error("invalid plugin name encoding")]
    InvalidPluginName,
}

impl AuthSwitchRequest {
    /// Create a new auth switch request.
    pub fn new(plugin_name: impl Into<String>, plugin_data: Vec<u8>) -> Self {
        Self { plugin_name: plugin_name.into(), plugin_data }
    }

    /// Parse an auth switch request from a stream.
    ///
    /// The packet format is:
    /// - 1 byte: 0xFE (auth switch indicator)
    /// - NUL-terminated: plugin name
    /// - Rest: plugin data (may or may not be NUL-terminated)
    pub fn parse_from_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MysqlParseError<S::ReadError, AuthSwitchError>> {
        // Check header byte
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != packet_types::EOF {
            // Note: 0xFE is used for both EOF and auth switch
            return Err(MysqlParseError::Parse(AuthSwitchError::InvalidHeader(header)));
        }

        // Read plugin name (NUL-terminated)
        let plugin_name_bytes = stream.read_cstring_sync().map_err(MysqlParseError::Stream)?;
        let plugin_name = String::from_utf8(plugin_name_bytes).map_err(|_| MysqlParseError::Parse(AuthSwitchError::InvalidPluginName))?;

        // Read remaining data as plugin data
        let mut plugin_data = Vec::new();
        while let Ok(b) = stream.read_u8_sync() {
            plugin_data.push(b);
        }

        // Remove trailing NUL if present
        if plugin_data.last() == Some(&0) {
            plugin_data.pop();
        }

        Ok(Self { plugin_name, plugin_data })
    }
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for AuthSwitchRequest {
    type ParseError = AuthSwitchError;
    type Value<'s>
        = AuthSwitchRequest
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_from_sync(stream)
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for AuthSwitchRequest {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// Authentication "more data" packet.
///
/// Used during multi-round authentication (e.g., caching_sha2_password).
/// The server may send additional data requests with this packet type.
#[derive(Clone, Debug)]
pub struct AuthMoreData {
    /// Additional data from the server.
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum AuthMoreDataError {
    #[error("invalid auth more data header: expected 0x01, got {0:#04x}")]
    InvalidHeader(u8),
}

impl AuthMoreData {
    /// Create a new auth more data packet.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Parse from a stream.
    ///
    /// Format:
    /// - 1 byte: 0x01 (auth more data indicator)
    /// - Rest: data
    pub fn parse_from_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MysqlParseError<S::ReadError, AuthMoreDataError>> {
        let header = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if header != 0x01 {
            return Err(MysqlParseError::Parse(AuthMoreDataError::InvalidHeader(header)));
        }

        let mut data = Vec::new();
        while let Ok(b) = stream.read_u8_sync() {
            data.push(b);
        }

        Ok(Self { data })
    }

    /// Check if this is a fast auth success (single byte 0x03).
    pub fn is_fast_auth_success(&self) -> bool {
        self.data.len() == 1 && self.data[0] == 0x03
    }

    /// Check if this is a request for full authentication (single byte 0x04).
    pub fn is_full_auth_required(&self) -> bool {
        self.data.len() == 1 && self.data[0] == 0x04
    }
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for AuthMoreData {
    type ParseError = AuthMoreDataError;
    type Value<'s>
        = AuthMoreData
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_from_sync(stream)
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for AuthMoreData {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// Client authentication response (auth switch response).
///
/// Sent by the client in response to an AuthSwitchRequest or AuthMoreData.
#[derive(Clone, Debug)]
pub struct AuthSwitchResponse {
    /// Authentication data computed by the client.
    pub auth_data: Vec<u8>,
}

impl AuthSwitchResponse {
    /// Create a new auth switch response.
    pub fn new(auth_data: Vec<u8>) -> Self {
        Self { auth_data }
    }

    /// Create an empty response (for plugins that don't need data).
    pub fn empty() -> Self {
        Self { auth_data: Vec::new() }
    }

    /// Encode to bytes (just the raw auth data, no header).
    pub fn encode(&self) -> Vec<u8> {
        self.auth_data.clone()
    }

    /// Build a complete packet with header.
    pub fn build_packet(&self, sequence_id: u8) -> Vec<u8> {
        let payload = self.encode();
        let mut packet = Vec::with_capacity(4 + payload.len());

        // Header
        let len = payload.len() as u32;
        packet.push(len as u8);
        packet.push((len >> 8) as u8);
        packet.push((len >> 16) as u8);
        packet.push(sequence_id);

        // Payload
        packet.extend_from_slice(&payload);

        packet
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_auth_switch_request() {
        let mut data = Vec::new();
        data.push(0xFE); // Auth switch marker
        data.extend_from_slice(b"mysql_native_password\0");
        data.extend_from_slice(&[1, 2, 3, 4, 5, 6, 7, 8]); // Challenge

        let stream = SliceStream::new(&data);
        let request = AuthSwitchRequest::parse_sync(&stream).unwrap();

        assert_eq!(request.plugin_name, "mysql_native_password");
        assert_eq!(request.plugin_data, vec![1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn test_auth_switch_caching_sha2() {
        let mut data = Vec::new();
        data.push(0xFE);
        data.extend_from_slice(b"caching_sha2_password\0");
        data.extend_from_slice(&[0x12, 0x34, 0x56, 0x78]);

        let stream = SliceStream::new(&data);
        let request = AuthSwitchRequest::parse_sync(&stream).unwrap();

        assert_eq!(request.plugin_name, "caching_sha2_password");
        assert_eq!(request.plugin_data, vec![0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_auth_more_data_fast_auth() {
        let data = [0x01, 0x03]; // Fast auth success
        let stream = SliceStream::new(&data);
        let more = AuthMoreData::parse_sync(&stream).unwrap();

        assert!(more.is_fast_auth_success());
        assert!(!more.is_full_auth_required());
    }

    #[test]
    fn test_auth_more_data_full_auth() {
        let data = [0x01, 0x04]; // Full auth required
        let stream = SliceStream::new(&data);
        let more = AuthMoreData::parse_sync(&stream).unwrap();

        assert!(!more.is_fast_auth_success());
        assert!(more.is_full_auth_required());
    }

    #[test]
    fn test_auth_switch_response() {
        let response = AuthSwitchResponse::new(vec![0xAB, 0xCD, 0xEF]);
        let packet = response.build_packet(3);

        assert_eq!(packet[0], 3); // length low byte
        assert_eq!(packet[1], 0);
        assert_eq!(packet[2], 0);
        assert_eq!(packet[3], 3); // sequence ID
        assert_eq!(&packet[4..], &[0xAB, 0xCD, 0xEF]);
    }
}
