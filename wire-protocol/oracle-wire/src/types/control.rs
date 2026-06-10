//! TNS Control packet type.
//!
//! Control packets are used for protocol-level flow control and management.
//! They handle operations like connection pooling control and session management.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// Control command types.
pub mod control_commands {
    /// Test connection (ping).
    pub const PING: u16 = 0x0001;
    /// Set option.
    pub const SET_OPTION: u16 = 0x0002;
    /// Get option.
    pub const GET_OPTION: u16 = 0x0003;
    /// Flush buffers.
    pub const FLUSH: u16 = 0x0004;
    /// Enable flow control.
    pub const ENABLE_FLOW_CONTROL: u16 = 0x0010;
    /// Disable flow control.
    pub const DISABLE_FLOW_CONTROL: u16 = 0x0011;
    /// Session control (DRCP).
    pub const SESSION_CONTROL: u16 = 0x0020;
}

/// TNS Control packet.
///
/// Control packets are used for protocol-level management operations
/// outside of normal data transfer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Control {
    /// Control command.
    pub command: u16,
    /// Command-specific data.
    pub data: Vec<u8>,
}

/// Error when parsing a Control packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ControlError {
    #[error("control packet too short")]
    TooShort,
    #[error("unknown control command: {0}")]
    UnknownCommand(u16),
}

impl Control {
    /// Create a new control packet.
    pub fn new(command: u16, data: Vec<u8>) -> Self {
        Self { command, data }
    }

    /// Create a ping control packet.
    pub fn ping() -> Self {
        Self { command: control_commands::PING, data: Vec::new() }
    }

    /// Create a flush control packet.
    pub fn flush() -> Self {
        Self { command: control_commands::FLUSH, data: Vec::new() }
    }

    /// Check if this is a ping command.
    pub fn is_ping(&self) -> bool {
        self.command == control_commands::PING
    }

    /// Check if this is a session control command.
    pub fn is_session_control(&self) -> bool {
        self.command == control_commands::SESSION_CONTROL
    }

    /// Get command name.
    pub fn command_name(&self) -> &'static str {
        match self.command {
            control_commands::PING => "Ping",
            control_commands::SET_OPTION => "SetOption",
            control_commands::GET_OPTION => "GetOption",
            control_commands::FLUSH => "Flush",
            control_commands::ENABLE_FLOW_CONTROL => "EnableFlowControl",
            control_commands::DISABLE_FLOW_CONTROL => "DisableFlowControl",
            control_commands::SESSION_CONTROL => "SessionControl",
            _ => "Unknown",
        }
    }

    /// Encode to wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.data.len());
        bytes.extend_from_slice(&self.command.to_be_bytes());
        bytes.extend_from_slice(&self.data);
        bytes
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Control {
    type ParseError = ControlError;
    type Value<'s>
        = Control
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let command = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        Ok(Control { command, data: Vec::new() })
    }
}

impl Control {
    /// Parse with a known length.
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Control, OracleParseError<S::ReadError, ControlError>> {
        if length < 2 {
            return Err(OracleParseError::Parse(ControlError::TooShort));
        }

        let command = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

        let data = if length > 2 {
            stream.read_bytes_sync(length - 2).map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(Control { command, data })
    }

    /// Parse with a known length (async).
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Control, OracleParseError<S::ReadError, ControlError>> {
        if length < 2 {
            return Err(OracleParseError::Parse(ControlError::TooShort));
        }

        let command = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        let data = if length > 2 {
            stream.read_bytes(length - 2).await.map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(Control { command, data })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Control {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let command = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

        Ok(Control { command, data: Vec::new() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_ping() {
        let ctrl = Control::ping();
        assert!(ctrl.is_ping());
        assert_eq!(ctrl.command_name(), "Ping");
    }

    #[test]
    fn test_control_to_bytes() {
        let ctrl = Control::new(0x0020, vec![1, 2, 3]);
        let bytes = ctrl.to_bytes();
        assert_eq!(bytes, vec![0x00, 0x20, 1, 2, 3]);
    }

    #[test]
    fn test_control_command_names() {
        assert_eq!(Control::ping().command_name(), "Ping");
        assert_eq!(Control::flush().command_name(), "Flush");
        assert_eq!(Control::new(control_commands::SESSION_CONTROL, vec![]).command_name(), "SessionControl");
    }
}
