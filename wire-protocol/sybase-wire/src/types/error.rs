//! TDS error and info message token types.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Error message from the server.
#[derive(Clone, Debug)]
pub struct ErrorInfo {
    /// Token length (for skipping).
    pub length: u16,
    /// Error number.
    pub number: i32,
    /// State.
    pub state: u8,
    /// Severity class.
    pub class: u8,
    /// Error message text.
    pub message: String,
    /// Server name.
    pub server_name: String,
    /// Procedure name (if in a procedure).
    pub proc_name: String,
    /// Line number in the SQL batch.
    pub line_number: u16,
}

impl ErrorInfo {
    /// Parse an ERROR token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<ErrorInfo, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Error number (4 bytes)
        let number = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)? as i32;

        // State (1 byte)
        let state = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Severity class (1 byte)
        let class = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Message length and text
        let msg_len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
        let message = if msg_len > 0 {
            let borrow = stream.peek(Some(msg_len)).map_err(SybaseParseError::Stream)?;
            let msg = String::from_utf8_lossy(&borrow[..msg_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            msg
        } else {
            String::new()
        };

        // Server name length and text
        let srv_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let server_name = if srv_len > 0 {
            let borrow = stream.peek(Some(srv_len)).map_err(SybaseParseError::Stream)?;
            let name = String::from_utf8_lossy(&borrow[..srv_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            name
        } else {
            String::new()
        };

        // Procedure name length and text
        let proc_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let proc_name = if proc_len > 0 {
            let borrow = stream.peek(Some(proc_len)).map_err(SybaseParseError::Stream)?;
            let name = String::from_utf8_lossy(&borrow[..proc_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            name
        } else {
            String::new()
        };

        // Line number (2 bytes)
        let line_number = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(ErrorInfo {
            length,
            number,
            state,
            class,
            message,
            server_name,
            proc_name,
            line_number,
        })
    }

    /// Check if this is a fatal error (severity >= 20).
    pub fn is_fatal(&self) -> bool {
        self.class >= 20
    }

    /// Check if this requires user action (severity 11-16).
    pub fn requires_action(&self) -> bool {
        (11..=16).contains(&self.class)
    }
}

/// Informational message from the server.
///
/// Same structure as ErrorInfo but used for non-error messages.
#[derive(Clone, Debug)]
pub struct InfoMessage {
    /// Token length (for skipping).
    pub length: u16,
    /// Message number.
    pub number: i32,
    /// State.
    pub state: u8,
    /// Severity class (usually 0-10 for info).
    pub class: u8,
    /// Message text.
    pub message: String,
    /// Server name.
    pub server_name: String,
    /// Procedure name (if in a procedure).
    pub proc_name: String,
    /// Line number in the SQL batch.
    pub line_number: u16,
}

impl InfoMessage {
    /// Parse an INFO token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(
        stream: &'s SliceStream<'s>,
    ) -> Result<InfoMessage, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Same structure as ErrorInfo
        let error = ErrorInfo::parse_after_token_sync(stream)?;

        Ok(InfoMessage {
            length: error.length,
            number: error.number,
            state: error.state,
            class: error.class,
            message: error.message,
            server_name: error.server_name,
            proc_name: error.proc_name,
            line_number: error.line_number,
        })
    }
}
