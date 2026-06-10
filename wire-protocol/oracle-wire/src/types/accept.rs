//! TNS Accept packet type.
//!
//! The Accept packet is sent by the server to accept a connection request.

use crate::error::versions;
use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Accept packet.
///
/// # Version Differences
///
/// - **TNS v8-v10**: Basic accept structure
/// - **TNS v11+**: Adds reconnect address for DRCP
/// - **TNS v12+**: Extended accept data for multitenant
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Accept {
    /// Accepted TNS version.
    pub version: u16,
    /// Service options agreed upon.
    pub service_options: u16,
    /// Session data unit size.
    pub sdu_size: u16,
    /// Maximum transmission data unit size.
    pub tdu_size: u16,
    /// Hardware type (byte order indicator).
    pub hardware_type: u16,
    /// Accept data length.
    pub accept_data_length: u16,
    /// Accept data offset from start of packet.
    pub accept_data_offset: u16,
    /// Connect flags byte 1.
    pub connect_flags_1: u8,
    /// Connect flags byte 2.
    pub connect_flags_2: u8,
    /// Reconnect address (TNS v11+ for DRCP).
    pub reconnect_address: Option<Vec<u8>>,
    /// Accept data.
    pub accept_data: Vec<u8>,
}

/// Error when parsing an Accept packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AcceptError {
    #[error("accept data offset beyond packet: offset {offset}, packet length {packet_length}")]
    InvalidOffset { offset: u16, packet_length: u16 },
    #[error("accept data extends beyond packet")]
    DataBeyondPacket,
}

impl Accept {
    /// Check if this is a DRCP reconnect accept (TNS v11+).
    pub fn is_drcp_reconnect(&self) -> bool {
        self.reconnect_address.is_some()
    }

    /// Returns the accept data as a UTF-8 string if valid.
    pub fn accept_data_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.accept_data).ok()
    }

    /// Returns the reconnect address as a UTF-8 string if valid.
    pub fn reconnect_address_str(&self) -> Option<&str> {
        self.reconnect_address.as_ref().and_then(|a| std::str::from_utf8(a).ok())
    }

    /// Parse host and port from reconnect address.
    pub fn parse_reconnect_address(&self) -> Option<(String, u16)> {
        let addr_str = self.reconnect_address_str()?;
        // Format: (ADDRESS=(PROTOCOL=TCP)(HOST=host)(PORT=port))
        let host_start = addr_str.find("(HOST=")? + 6;
        let host_end = addr_str[host_start..].find(')')? + host_start;
        let host = addr_str[host_start..host_end].to_string();

        let port_start = addr_str.find("(PORT=")? + 6;
        let port_end = addr_str[port_start..].find(')')? + port_start;
        let port: u16 = addr_str[port_start..port_end].parse().ok()?;

        Some((host, port))
    }
}

/// Parse reconnect address from accept data for v11+ DRCP.
fn parse_reconnect_address(data: &[u8], version: u16) -> Option<Vec<u8>> {
    if version < versions::TNS_V11 || data.is_empty() {
        return None;
    }

    // Look for DRCP reconnect marker in accept data
    // Format varies, but typically starts with a length byte followed by address
    let data_str = std::str::from_utf8(data).ok()?;

    // Check if this contains a reconnect address (indicated by ADDRESS pattern)
    if data_str.contains("(ADDRESS=") && data_str.contains("RECONNECT") {
        // Extract the address portion
        let start = data_str.find("(ADDRESS=")?;
        let mut depth = 0;
        let mut end = start;
        for (i, c) in data_str[start..].char_indices() {
            match c {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        end = start + i + 1;
                        break;
                    }
                }
                _ => {}
            }
        }
        if end > start {
            return Some(data_str.as_bytes()[start..end].to_vec());
        }
    }

    None
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Accept {
    type ParseError = AcceptError;
    type Value<'s>
        = Accept
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let version = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let service_options = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let sdu_size = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let tdu_size = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let hardware_type = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let accept_data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let accept_data_offset = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
        let connect_flags_1 = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let connect_flags_2 = stream.read_u8_sync().map_err(OracleParseError::Stream)?;

        // Read accept data
        let accept_data = stream.read_bytes_sync(accept_data_length as usize).map_err(OracleParseError::Stream)?.to_vec();

        // Parse reconnect address for DRCP (v11+)
        let reconnect_address = parse_reconnect_address(&accept_data, version);

        Ok(Accept {
            version,
            service_options,
            sdu_size,
            tdu_size,
            hardware_type,
            accept_data_length,
            accept_data_offset,
            connect_flags_1,
            connect_flags_2,
            reconnect_address,
            accept_data,
        })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Accept {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let version = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let service_options = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let sdu_size = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let tdu_size = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let hardware_type = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let accept_data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let accept_data_offset = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
        let connect_flags_1 = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let connect_flags_2 = stream.read_u8().await.map_err(OracleParseError::Stream)?;

        // Read accept data
        let accept_data = stream.read_bytes(accept_data_length as usize).await.map_err(OracleParseError::Stream)?.to_vec();

        // Parse reconnect address for DRCP (v11+)
        let reconnect_address = parse_reconnect_address(&accept_data, version);

        Ok(Accept {
            version,
            service_options,
            sdu_size,
            tdu_size,
            hardware_type,
            accept_data_length,
            accept_data_offset,
            connect_flags_1,
            connect_flags_2,
            reconnect_address,
            accept_data,
        })
    }
}
