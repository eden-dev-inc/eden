//! TNS Redirect packet type.
//!
//! Redirect packets are sent by the server to direct the client to a different
//! address or port, commonly used in Oracle RAC (Real Application Clusters)
//! and load balancing scenarios.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// TNS Redirect packet.
///
/// The server sends this packet to redirect the client to a different listener
/// or service. This is commonly used in:
/// - Oracle RAC for load balancing
/// - Service relocation
/// - Failover scenarios
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Redirect {
    /// Length of redirect data.
    pub data_length: u16,
    /// Redirect data containing the new connection address.
    /// Format is typically: "(ADDRESS=(PROTOCOL=tcp)(HOST=...)(PORT=...))"
    pub redirect_data: Vec<u8>,
}

/// Error when parsing a Redirect packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum RedirectError {
    #[error("redirect data length mismatch: declared {declared}, actual {actual}")]
    LengthMismatch { declared: u16, actual: usize },
    #[error("redirect data is not valid UTF-8")]
    InvalidUtf8,
}

impl Redirect {
    /// Create a new redirect packet.
    pub fn new(redirect_data: Vec<u8>) -> Self {
        Self { data_length: redirect_data.len() as u16, redirect_data }
    }

    /// Get the redirect data as a string (if valid UTF-8).
    pub fn redirect_address(&self) -> Result<&str, std::str::Utf8Error> {
        std::str::from_utf8(&self.redirect_data)
    }

    /// Parse the redirect data to extract host and port.
    ///
    /// Returns `(host, port)` if the address can be parsed.
    pub fn parse_address(&self) -> Option<(String, u16)> {
        let data = self.redirect_address().ok()?;

        // Parse TNS address format: (ADDRESS=(PROTOCOL=tcp)(HOST=...)(PORT=...))
        let host_start = data.find("HOST=")? + 5;
        let host_end = data[host_start..].find(')')? + host_start;
        let host = data[host_start..host_end].to_string();

        let port_start = data.find("PORT=")? + 5;
        let port_end = data[port_start..].find(')')? + port_start;
        let port: u16 = data[port_start..port_end].parse().ok()?;

        Some((host, port))
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Redirect {
    type ParseError = RedirectError;
    type Value<'s>
        = Redirect
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        let redirect_data = stream.read_bytes_sync(data_length as usize).map_err(OracleParseError::Stream)?.to_vec();

        Ok(Redirect { data_length, redirect_data })
    }
}

impl Redirect {
    /// Parse with a known remaining length (from header).
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        remaining_length: usize,
    ) -> Result<Redirect, OracleParseError<S::ReadError, RedirectError>> {
        let data_length = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        // Use either declared length or remaining packet length (minus 2 for length field)
        let actual_length = (remaining_length.saturating_sub(2)).min(data_length as usize);

        let redirect_data = stream.read_bytes_sync(actual_length).map_err(OracleParseError::Stream)?.to_vec();

        Ok(Redirect { data_length, redirect_data })
    }

    /// Parse with a known remaining length (from header) - async version.
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        remaining_length: usize,
    ) -> Result<Redirect, OracleParseError<S::ReadError, RedirectError>> {
        let data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        // Use either declared length or remaining packet length (minus 2 for length field)
        let actual_length = (remaining_length.saturating_sub(2)).min(data_length as usize);

        let redirect_data = stream.read_bytes(actual_length).await.map_err(OracleParseError::Stream)?.to_vec();

        Ok(Redirect { data_length, redirect_data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Redirect {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let data_length = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        let redirect_data = stream.read_bytes(data_length as usize).await.map_err(OracleParseError::Stream)?.to_vec();

        Ok(Redirect { data_length, redirect_data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_address() {
        let data = b"(ADDRESS=(PROTOCOL=tcp)(HOST=192.168.1.100)(PORT=1521))".to_vec();
        let redirect = Redirect::new(data);

        let (host, port) = redirect.parse_address().unwrap();
        assert_eq!(host, "192.168.1.100");
        assert_eq!(port, 1521);
    }

    #[test]
    fn test_redirect_address() {
        let data = b"(ADDRESS=(PROTOCOL=tcp)(HOST=dbserver)(PORT=1522))".to_vec();
        let redirect = Redirect::new(data);

        assert!(redirect.redirect_address().is_ok());
        assert!(redirect.redirect_address().unwrap().contains("dbserver"));
    }
}
