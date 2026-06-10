//! TDS login acknowledgment token.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Login acknowledgment interface values.
pub mod interface_types {
    /// SQL Server interface.
    pub const SQL: u8 = 0;
    /// ODBC interface.
    pub const ODBC: u8 = 1;
}

/// Login acknowledgment token.
///
/// Sent by the server to acknowledge a successful login.
#[derive(Clone, Debug)]
pub struct LoginAck {
    /// Token length.
    pub length: u16,
    /// Interface type (SQL or ODBC).
    pub interface: u8,
    /// TDS version negotiated.
    pub tds_version: u32,
    /// Server program name.
    pub prog_name: String,
    /// Server program version (major.minor.build.subbuild).
    pub prog_version: (u8, u8, u8, u8),
}

impl LoginAck {
    /// Parse a LOGINACK token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<LoginAck, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Interface (1 byte)
        let interface = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // TDS version (4 bytes, big-endian for Sybase)
        let tds_version = stream.read_u32_be_sync().map_err(SybaseParseError::Stream)?;

        // Program name length and text
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let prog_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let name = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            name
        } else {
            String::new()
        };

        // Program version (4 bytes)
        let major = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let minor = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let build_hi = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;
        let build_lo = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        Ok(LoginAck {
            length,
            interface,
            tds_version,
            prog_name,
            prog_version: (major, minor, build_hi, build_lo),
        })
    }

    /// Get a human-readable TDS version string.
    pub fn tds_version_string(&self) -> String {
        let major = (self.tds_version >> 24) & 0xFF;
        let minor = (self.tds_version >> 16) & 0xFF;
        format!("{}.{}", major, minor)
    }

    /// Check if this is a Sybase server (TDS 4.x or 5.x).
    pub fn is_sybase(&self) -> bool {
        let major = (self.tds_version >> 24) & 0xFF;
        major <= 5
    }

    /// Get the server version as a string.
    pub fn server_version_string(&self) -> String {
        format!("{}.{}.{}.{}", self.prog_version.0, self.prog_version.1, self.prog_version.2, self.prog_version.3)
    }
}
