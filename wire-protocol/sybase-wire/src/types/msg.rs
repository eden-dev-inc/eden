//! TDS MSG token.
//!
//! Server message token for TDS 5.0.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// MSG status flags.
pub mod msg_status {
    /// Message has arguments.
    pub const MSG_HASARGS: u8 = 0x01;
}

/// MSG token.
///
/// Contains a server message.
#[derive(Clone, Debug)]
pub struct Msg {
    /// Token length.
    pub length: u16,
    /// Message number.
    pub msg_number: u32,
    /// Message state.
    pub state: u8,
    /// Severity.
    pub severity: u8,
    /// Message text.
    pub message: String,
    /// Server name.
    pub server_name: String,
    /// Procedure name.
    pub proc_name: String,
    /// Line number.
    pub line_number: u16,
}

impl Msg {
    /// Parse a MSG token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Msg, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Message number (4 bytes)
        let msg_number = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // State (1 byte)
        let state = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Severity (1 byte)
        let severity = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

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

        Ok(Msg {
            length,
            msg_number,
            state,
            severity,
            message,
            server_name,
            proc_name,
            line_number,
        })
    }
}
