//! TDS EED (Extended Error Data) token.
//!
//! The EED token provides extended error and message information in TDS 5.0,
//! including additional diagnostic details beyond the basic ERROR/INFO tokens.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// EED status flags.
pub mod eed_status {
    /// Error has extended information.
    pub const TDS_EED_HASARGS: u8 = 0x01;
    /// Error is followed by transaction state.
    pub const TDS_EED_TRANSTATE: u8 = 0x02;
}

/// EED (Extended Error Data) token.
///
/// Provides extended error information in TDS 5.0.
#[derive(Clone, Debug)]
pub struct Eed {
    /// Token length.
    pub length: u16,
    /// Message number.
    pub msg_number: u32,
    /// Message state.
    pub state: u8,
    /// Severity class.
    pub severity: u8,
    /// SQL state (5 bytes SQLSTATE).
    pub sql_state: [u8; 5],
    /// Status flags.
    pub status: u8,
    /// Transaction state (if TDS_EED_TRANSTATE is set).
    pub tran_state: u16,
    /// Message text.
    pub message: String,
    /// Server name.
    pub server_name: String,
    /// Procedure name (if in stored procedure).
    pub proc_name: String,
    /// Line number.
    pub line_number: u16,
}

impl Eed {
    /// Parse an EED token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Eed, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Message number (4 bytes)
        let msg_number = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // State (1 byte)
        let state = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Severity (1 byte)
        let severity = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // SQL state (5 bytes)
        let borrow = stream.peek(Some(5)).map_err(SybaseParseError::Stream)?;
        let mut sql_state = [0u8; 5];
        sql_state.copy_from_slice(&borrow[..5]);
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        // Status (1 byte)
        let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Transaction state (2 bytes) - only present if TDS_EED_TRANSTATE is set
        let tran_state = if status & eed_status::TDS_EED_TRANSTATE != 0 {
            stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?
        } else {
            0
        };

        // Message text length (2 bytes) + message
        let msg_len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
        let message = if msg_len > 0 {
            let borrow = stream.peek(Some(msg_len)).map_err(SybaseParseError::Stream)?;
            let m = String::from_utf8_lossy(&borrow[..msg_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            m
        } else {
            String::new()
        };

        // Server name length (1 byte) + server name
        let server_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let server_name = if server_len > 0 {
            let borrow = stream.peek(Some(server_len)).map_err(SybaseParseError::Stream)?;
            let s = String::from_utf8_lossy(&borrow[..server_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            s
        } else {
            String::new()
        };

        // Procedure name length (1 byte) + procedure name
        let proc_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let proc_name = if proc_len > 0 {
            let borrow = stream.peek(Some(proc_len)).map_err(SybaseParseError::Stream)?;
            let p = String::from_utf8_lossy(&borrow[..proc_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            p
        } else {
            String::new()
        };

        // Line number (2 bytes)
        let line_number = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(Eed {
            length,
            msg_number,
            state,
            severity,
            sql_state,
            status,
            tran_state,
            message,
            server_name,
            proc_name,
            line_number,
        })
    }

    /// Get the SQLSTATE as a string.
    pub fn sql_state_string(&self) -> String {
        String::from_utf8_lossy(&self.sql_state).into_owned()
    }

    /// Check if this is an error (severity >= 11).
    pub fn is_error(&self) -> bool {
        self.severity >= 11
    }

    /// Check if this has extended arguments.
    pub fn has_args(&self) -> bool {
        self.status & eed_status::TDS_EED_HASARGS != 0
    }
}
