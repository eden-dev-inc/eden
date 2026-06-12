//! Unknown message handling for backwards compatibility.
//!
//! This module provides types and utilities for gracefully handling
//! unknown or unrecognized message types, which is essential for
//! backwards compatibility with older PostgreSQL versions and forward
//! compatibility with newer versions that may introduce new message types.

use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use wire_stream::{WireRead, WireReadSync};

/// An unknown or unrecognized message.
///
/// This type captures messages that are not recognized by the parser,
/// allowing the connection to continue gracefully rather than failing.
/// This is important for:
/// - Forward compatibility with newer PostgreSQL versions
/// - Backwards compatibility with older versions that may send deprecated messages
/// - Handling custom or extension messages
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnknownMessage {
    /// The message type byte.
    pub message_type: u8,
    /// The raw message payload (excluding type and length).
    pub payload: Vec<u8>,
}

impl UnknownMessage {
    /// Create a new UnknownMessage.
    pub fn new(message_type: u8, payload: Vec<u8>) -> Self {
        Self { message_type, payload }
    }

    /// Returns the message type as a char (for debugging).
    pub fn type_char(&self) -> char {
        self.message_type as char
    }

    /// Returns true if this is a backend message type based on common patterns.
    pub fn is_likely_backend_message(&self) -> bool {
        // Backend messages are typically uppercase letters or digits
        matches!(self.message_type, b'A'..=b'Z' | b'1'..=b'9' | b'n' | b's' | b't' | b'v' | b'c' | b'd')
    }

    /// Returns true if this is a frontend message type based on common patterns.
    pub fn is_likely_frontend_message(&self) -> bool {
        // Frontend messages are typically uppercase letters or lowercase
        matches!(
            self.message_type,
            b'B' | b'C' | b'D' | b'E' | b'F' | b'H' | b'P' | b'Q' | b'S' | b'X' | b'p' | b'c' | b'd' | b'f'
        )
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum UnknownMessageError {
    #[error("message too large: {size} bytes")]
    MessageTooLarge { size: usize },
    #[error("negative length: {0}")]
    NegativeLength(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for UnknownMessage {
    type ParseError = UnknownMessageError;
    type Value<'s>
        = UnknownMessage
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let message_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        if length < 4 {
            return Err(PgParseError::Parse(UnknownMessageError::NegativeLength(length)));
        }

        let payload_length = (length - 4) as usize;

        // Limit maximum message size for safety
        const MAX_UNKNOWN_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB
        if payload_length > MAX_UNKNOWN_MESSAGE_SIZE {
            return Err(PgParseError::Parse(UnknownMessageError::MessageTooLarge { size: payload_length }));
        }

        let payload = stream.read_bytes_sync(payload_length).map_err(PgParseError::Stream)?;

        Ok(UnknownMessage { message_type, payload })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for UnknownMessage {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let message_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        if length < 4 {
            return Err(PgParseError::Parse(UnknownMessageError::NegativeLength(length)));
        }

        let payload_length = (length - 4) as usize;

        // Limit maximum message size for safety
        const MAX_UNKNOWN_MESSAGE_SIZE: usize = 1024 * 1024; // 1MB
        if payload_length > MAX_UNKNOWN_MESSAGE_SIZE {
            return Err(PgParseError::Parse(UnknownMessageError::MessageTooLarge { size: payload_length }));
        }

        let payload = stream.read_bytes(payload_length).await.map_err(PgParseError::Stream)?;

        Ok(UnknownMessage { message_type, payload })
    }
}

/// Result of peeking at a message type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MessageCategory {
    /// Authentication-related message ('R')
    Authentication,
    /// Error response ('E')
    Error,
    /// Notice response ('N')
    Notice,
    /// Parameter status ('S')
    ParameterStatus,
    /// Backend key data ('K')
    BackendKeyData,
    /// Ready for query ('Z')
    ReadyForQuery,
    /// Row description ('T')
    RowDescription,
    /// Data row ('D')
    DataRow,
    /// Command complete ('C')
    CommandComplete,
    /// Empty query response ('I')
    EmptyQueryResponse,
    /// Parse complete ('1')
    ParseComplete,
    /// Bind complete ('2')
    BindComplete,
    /// Close complete ('3')
    CloseComplete,
    /// No data ('n')
    NoData,
    /// Parameter description ('t')
    ParameterDescription,
    /// Portal suspended ('s')
    PortalSuspended,
    /// Copy in response ('G')
    CopyInResponse,
    /// Copy out response ('H')
    CopyOutResponse,
    /// Copy both response ('W')
    CopyBothResponse,
    /// Copy data ('d')
    CopyData,
    /// Copy done ('c')
    CopyDone,
    /// Notification response ('A')
    NotificationResponse,
    /// Negotiate protocol version ('v')
    NegotiateProtocolVersion,
    /// Function call response ('V') - deprecated
    FunctionCallResponse,
    /// Unknown message type
    Unknown(u8),
}

impl MessageCategory {
    /// Categorize a message type byte.
    pub fn from_type_byte(type_byte: u8) -> Self {
        use crate::error::backend;

        match type_byte {
            backend::AUTHENTICATION => MessageCategory::Authentication,
            backend::ERROR_RESPONSE => MessageCategory::Error,
            backend::NOTICE_RESPONSE => MessageCategory::Notice,
            backend::PARAMETER_STATUS => MessageCategory::ParameterStatus,
            backend::BACKEND_KEY_DATA => MessageCategory::BackendKeyData,
            backend::READY_FOR_QUERY => MessageCategory::ReadyForQuery,
            backend::ROW_DESCRIPTION => MessageCategory::RowDescription,
            backend::DATA_ROW => MessageCategory::DataRow,
            backend::COMMAND_COMPLETE => MessageCategory::CommandComplete,
            backend::EMPTY_QUERY_RESPONSE => MessageCategory::EmptyQueryResponse,
            backend::PARSE_COMPLETE => MessageCategory::ParseComplete,
            backend::BIND_COMPLETE => MessageCategory::BindComplete,
            backend::CLOSE_COMPLETE => MessageCategory::CloseComplete,
            backend::NO_DATA => MessageCategory::NoData,
            backend::PARAMETER_DESCRIPTION => MessageCategory::ParameterDescription,
            backend::PORTAL_SUSPENDED => MessageCategory::PortalSuspended,
            backend::COPY_IN_RESPONSE => MessageCategory::CopyInResponse,
            backend::COPY_OUT_RESPONSE => MessageCategory::CopyOutResponse,
            backend::COPY_BOTH_RESPONSE => MessageCategory::CopyBothResponse,
            backend::COPY_DATA => MessageCategory::CopyData,
            backend::COPY_DONE => MessageCategory::CopyDone,
            backend::NOTIFICATION_RESPONSE => MessageCategory::NotificationResponse,
            backend::NEGOTIATE_PROTOCOL_VERSION => MessageCategory::NegotiateProtocolVersion,
            backend::FUNCTION_CALL_RESPONSE => MessageCategory::FunctionCallResponse,
            other => MessageCategory::Unknown(other),
        }
    }

    /// Returns true if this category represents an error.
    pub fn is_error(&self) -> bool {
        matches!(self, MessageCategory::Error)
    }

    /// Returns true if this category represents an unknown message.
    pub fn is_unknown(&self) -> bool {
        matches!(self, MessageCategory::Unknown(_))
    }

    /// Returns true if this is a message that can safely be skipped.
    ///
    /// Some messages like notices are informational and can be skipped
    /// without affecting the protocol flow.
    pub fn is_skippable(&self) -> bool {
        matches!(
            self,
            MessageCategory::Notice | MessageCategory::NotificationResponse | MessageCategory::ParameterStatus
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_unknown_message_parse() {
        // Create a fake message with type 'Z' (unknown in this context)
        let data = [
            b'?', // Unknown type
            0, 0, 0, 8, // Length = 8 (4 for length + 4 for payload)
            0x01, 0x02, 0x03, 0x04, // Payload
        ];

        let stream = SliceStream::new(&data);
        let msg = UnknownMessage::parse_sync(&stream).expect("parse failed");

        assert_eq!(msg.message_type, b'?');
        assert_eq!(msg.payload, vec![0x01, 0x02, 0x03, 0x04]);
        assert_eq!(msg.type_char(), '?');
    }

    #[test]
    fn test_unknown_message_empty_payload() {
        let data = [
            b'?', // Unknown type
            0, 0, 0, 4, // Length = 4 (just the length field, no payload)
        ];

        let stream = SliceStream::new(&data);
        let msg = UnknownMessage::parse_sync(&stream).expect("parse failed");

        assert_eq!(msg.message_type, b'?');
        assert!(msg.payload.is_empty());
    }

    #[test]
    fn test_message_category() {
        assert_eq!(MessageCategory::from_type_byte(b'R'), MessageCategory::Authentication);
        assert_eq!(MessageCategory::from_type_byte(b'E'), MessageCategory::Error);
        assert_eq!(MessageCategory::from_type_byte(b'Z'), MessageCategory::ReadyForQuery);
        assert_eq!(MessageCategory::from_type_byte(b'v'), MessageCategory::NegotiateProtocolVersion);
        assert!(MessageCategory::from_type_byte(b'?').is_unknown());
    }

    #[test]
    fn test_is_skippable() {
        assert!(MessageCategory::Notice.is_skippable());
        assert!(MessageCategory::NotificationResponse.is_skippable());
        assert!(MessageCategory::ParameterStatus.is_skippable());
        assert!(!MessageCategory::Error.is_skippable());
        assert!(!MessageCategory::DataRow.is_skippable());
    }
}
