//! TTI message parsing.
//!
//! This module provides parsing for TTI messages that are contained
//! within TNS Data packets.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use crate::types::tti::function_codes::FunctionCode;
use wire_stream::{WireRead, WireReadSync, WireReadSyncExt};

/// A TTI message parsed from a Data packet payload.
///
/// TTI (Two-Task Interface) messages encapsulate database operations
/// within TNS Data packets.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TtiMessage {
    /// The function code identifying the operation.
    pub function_code: FunctionCode,
    /// Sequence number for request/response correlation.
    pub sequence_number: u8,
    /// The message payload (function-specific data).
    pub payload: Vec<u8>,
}

/// Error when parsing a TTI message.
#[derive(Clone, Debug, thiserror::Error)]
pub enum TtiMessageError {
    #[error("message too short: expected at least {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("unknown function code: {0}")]
    UnknownFunction(u8),
}

impl TtiMessage {
    /// Create a new TTI message.
    pub fn new(function_code: FunctionCode, sequence_number: u8, payload: Vec<u8>) -> Self {
        Self { function_code, sequence_number, payload }
    }

    /// Create from a Data packet payload.
    ///
    /// The first byte is the function code, followed by the sequence number
    /// and function-specific data.
    pub fn from_data_payload(payload: &[u8]) -> Result<Self, TtiMessageError> {
        if payload.is_empty() {
            return Err(TtiMessageError::TooShort { expected: 1, actual: 0 });
        }

        let function_code = FunctionCode::from_u8(payload[0]);

        // Sequence number is typically the second byte for requests
        let sequence_number = if payload.len() > 1 { payload[1] } else { 0 };

        let data_start = if payload.len() > 2 { 2 } else { payload.len() };
        let data = payload[data_start..].to_vec();

        Ok(Self { function_code, sequence_number, payload: data })
    }

    /// Check if this is a request message.
    pub fn is_request(&self) -> bool {
        self.function_code.is_request()
    }

    /// Check if this is a response message.
    pub fn is_response(&self) -> bool {
        self.function_code.is_response()
    }

    /// Check if this is a LOB operation.
    pub fn is_lob_operation(&self) -> bool {
        self.function_code.is_lob_operation()
    }

    /// Get the message as bytes (for sending).
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(2 + self.payload.len());
        bytes.push(self.function_code.as_u8());
        bytes.push(self.sequence_number);
        bytes.extend_from_slice(&self.payload);
        bytes
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for TtiMessage {
    type ParseError = TtiMessageError;
    type Value<'s>
        = TtiMessage
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_length_sync(stream, None)
    }
}

impl TtiMessage {
    /// Parse with a known payload length.
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        length: Option<usize>,
    ) -> Result<TtiMessage, OracleParseError<S::ReadError, TtiMessageError>> {
        let function_byte = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        let function_code = FunctionCode::from_u8(function_byte);

        let sequence_number = stream.read_u8_sync().map_err(OracleParseError::Stream)?;

        // Read remaining bytes as payload
        let payload = if let Some(len) = length {
            // Subtract 2 bytes for function code and sequence number
            let payload_len = len.saturating_sub(2);
            stream.read_bytes_sync(payload_len).map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(TtiMessage { function_code, sequence_number, payload })
    }

    /// Parse with a known payload length (async version).
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        length: Option<usize>,
    ) -> Result<TtiMessage, OracleParseError<S::ReadError, TtiMessageError>> {
        let function_byte = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        let function_code = FunctionCode::from_u8(function_byte);

        let sequence_number = stream.read_u8().await.map_err(OracleParseError::Stream)?;

        // Read remaining bytes as payload
        let payload = if let Some(len) = length {
            // Subtract 2 bytes for function code and sequence number
            let payload_len = len.saturating_sub(2);
            stream.read_bytes(payload_len).await.map_err(OracleParseError::Stream)?.to_vec()
        } else {
            Vec::new()
        };

        Ok(TtiMessage { function_code, sequence_number, payload })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for TtiMessage {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_with_length(stream, None).await
    }
}

/// Fluent builder for constructing TTI messages.
///
/// Provides a flexible builder pattern for creating any TTI message,
/// with support for bind variables, LOB operations, and other features.
///
/// # Example
///
/// ```ignore
/// use oracle_wire::types::tti::message::TtiMessageBuilder;
/// use oracle_wire::types::tti::FunctionCode;
///
/// // Build a parse request
/// let msg = TtiMessageBuilder::new(FunctionCode::Parse)
///     .sequence(1)
///     .cursor_id(0)
///     .sql("SELECT * FROM employees WHERE id = :1")
///     .build();
///
/// // Build an execute request with binds
/// let msg = TtiMessageBuilder::new(FunctionCode::Execute)
///     .sequence(2)
///     .cursor_id(1)
///     .options_u32(0x0001) // auto_commit
///     .iterations(1)
///     .build();
/// ```
#[derive(Clone, Debug)]
pub struct TtiMessageBuilder {
    function_code: FunctionCode,
    sequence_number: u8,
    payload: Vec<u8>,
}

impl TtiMessageBuilder {
    /// Create a new builder for the given function code.
    pub fn new(function_code: FunctionCode) -> Self {
        Self { function_code, sequence_number: 0, payload: Vec::new() }
    }

    /// Set the sequence number.
    pub fn sequence(mut self, seq: u8) -> Self {
        self.sequence_number = seq;
        self
    }

    /// Append raw bytes to the payload.
    pub fn bytes(mut self, data: &[u8]) -> Self {
        self.payload.extend_from_slice(data);
        self
    }

    /// Append a u8 to the payload.
    pub fn u8(mut self, value: u8) -> Self {
        self.payload.push(value);
        self
    }

    /// Append a u16 (big-endian) to the payload.
    pub fn u16(mut self, value: u16) -> Self {
        self.payload.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Append a u32 (big-endian) to the payload.
    pub fn u32(mut self, value: u32) -> Self {
        self.payload.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Append a u64 (big-endian) to the payload.
    pub fn u64(mut self, value: u64) -> Self {
        self.payload.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Add a cursor ID (u32).
    pub fn cursor_id(self, cursor_id: u32) -> Self {
        self.u32(cursor_id)
    }

    /// Add SQL text with length prefix.
    pub fn sql(mut self, sql: &str) -> Self {
        let bytes = sql.as_bytes();
        self.payload.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        self.payload.extend_from_slice(bytes);
        self
    }

    /// Add a length-prefixed string (u8 length prefix).
    pub fn string_u8(mut self, s: &str) -> Self {
        let bytes = s.as_bytes();
        self.payload.push(bytes.len() as u8);
        self.payload.extend_from_slice(bytes);
        self
    }

    /// Add a length-prefixed string (u16 length prefix).
    pub fn string_u16(mut self, s: &str) -> Self {
        let bytes = s.as_bytes();
        self.payload.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        self.payload.extend_from_slice(bytes);
        self
    }

    /// Add option flags (u32).
    pub fn options_u32(self, flags: u32) -> Self {
        self.u32(flags)
    }

    /// Add iteration count (for array DML).
    pub fn iterations(self, count: u32) -> Self {
        self.u32(count)
    }

    /// Add fetch size.
    pub fn fetch_size(self, size: u32) -> Self {
        self.u32(size)
    }

    /// Add LOB locator bytes.
    pub fn lob_locator(mut self, locator: &[u8]) -> Self {
        self.payload.extend_from_slice(&(locator.len() as u16).to_be_bytes());
        self.payload.extend_from_slice(locator);
        self
    }

    /// Add LOB offset (for read/write operations).
    pub fn lob_offset(self, offset: u64) -> Self {
        self.u64(offset)
    }

    /// Add LOB amount (bytes to read/write).
    pub fn lob_amount(self, amount: u64) -> Self {
        self.u64(amount)
    }

    /// Build the final TtiMessage.
    pub fn build(self) -> TtiMessage {
        TtiMessage {
            function_code: self.function_code,
            sequence_number: self.sequence_number,
            payload: self.payload,
        }
    }

    /// Build directly to bytes (including function code and sequence).
    pub fn build_bytes(self) -> Vec<u8> {
        self.build().to_bytes()
    }
}

// Convenience constructors for common message types
impl TtiMessageBuilder {
    /// Create a parse request builder.
    pub fn parse_request() -> Self {
        Self::new(FunctionCode::Parse)
    }

    /// Create an execute request builder.
    pub fn execute_request() -> Self {
        Self::new(FunctionCode::Execute)
    }

    /// Create a fetch request builder.
    pub fn fetch_request() -> Self {
        Self::new(FunctionCode::Fetch)
    }

    /// Create a close cursor request builder.
    pub fn close_cursor_request() -> Self {
        Self::new(FunctionCode::CloseCursor)
    }

    /// Create a commit request builder.
    pub fn commit_request() -> Self {
        Self::new(FunctionCode::Commit)
    }

    /// Create a rollback request builder.
    pub fn rollback_request() -> Self {
        Self::new(FunctionCode::Rollback)
    }

    /// Create a LOB read request builder.
    pub fn lob_read_request() -> Self {
        Self::new(FunctionCode::LobRead)
    }

    /// Create a LOB write request builder.
    pub fn lob_write_request() -> Self {
        Self::new(FunctionCode::LobWrite)
    }

    /// Create a LOB create temp request builder.
    pub fn lob_create_temp_request() -> Self {
        Self::new(FunctionCode::LobCreateTemp)
    }

    /// Create a LOB free temp request builder.
    pub fn lob_free_temp_request() -> Self {
        Self::new(FunctionCode::LobFreeTemp)
    }

    /// Create a LOB get length request builder.
    pub fn lob_get_length_request() -> Self {
        Self::new(FunctionCode::LobGetLength)
    }

    /// Create a describe request builder.
    pub fn describe_request() -> Self {
        Self::new(FunctionCode::Describe)
    }

    /// Create a protocol negotiation request builder.
    pub fn protocol_negotiation_request() -> Self {
        Self::new(FunctionCode::ProtocolNegotiation)
    }
}

/// Helper for creating TTI request messages.
pub struct TtiRequest;

impl TtiRequest {
    /// Create a protocol negotiation request.
    pub fn protocol_negotiation(version: u16) -> TtiMessage {
        let mut payload = Vec::new();
        payload.extend_from_slice(&version.to_be_bytes());
        TtiMessage::new(FunctionCode::ProtocolNegotiation, 0, payload)
    }

    /// Create a version exchange request.
    pub fn version() -> TtiMessage {
        TtiMessage::new(FunctionCode::Version, 0, Vec::new())
    }

    /// Create a commit request.
    pub fn commit() -> TtiMessage {
        TtiMessage::new(FunctionCode::Commit, 0, Vec::new())
    }

    /// Create a rollback request.
    pub fn rollback() -> TtiMessage {
        TtiMessage::new(FunctionCode::Rollback, 0, Vec::new())
    }

    /// Create a parse request.
    pub fn parse(cursor_id: u16, sql: &str) -> TtiMessage {
        let mut payload = Vec::new();
        payload.extend_from_slice(&cursor_id.to_be_bytes());
        payload.extend_from_slice(&(sql.len() as u32).to_be_bytes());
        payload.extend_from_slice(sql.as_bytes());
        TtiMessage::new(FunctionCode::Parse, 0, payload)
    }

    /// Create an execute request.
    pub fn execute(cursor_id: u16) -> TtiMessage {
        let mut payload = Vec::new();
        payload.extend_from_slice(&cursor_id.to_be_bytes());
        TtiMessage::new(FunctionCode::Execute, 0, payload)
    }

    /// Create a fetch request.
    pub fn fetch(cursor_id: u16, row_count: u32) -> TtiMessage {
        let mut payload = Vec::new();
        payload.extend_from_slice(&cursor_id.to_be_bytes());
        payload.extend_from_slice(&row_count.to_be_bytes());
        TtiMessage::new(FunctionCode::Fetch, 0, payload)
    }

    /// Create a close cursor request.
    pub fn close_cursor(cursor_id: u16) -> TtiMessage {
        let mut payload = Vec::new();
        payload.extend_from_slice(&cursor_id.to_be_bytes());
        TtiMessage::new(FunctionCode::CloseCursor, 0, payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_data_payload() {
        let payload = vec![0x0E, 0x01, 0x00, 0x01]; // Execute with sequence 1
        let msg = TtiMessage::from_data_payload(&payload).unwrap();

        assert_eq!(msg.function_code, FunctionCode::Execute);
        assert_eq!(msg.sequence_number, 1);
        assert_eq!(msg.payload, vec![0x00, 0x01]);
    }

    #[test]
    fn test_to_bytes_roundtrip() {
        let original = TtiMessage::new(FunctionCode::Commit, 5, vec![0xAB, 0xCD]);
        let bytes = original.to_bytes();

        assert_eq!(bytes[0], FunctionCode::Commit.as_u8());
        assert_eq!(bytes[1], 5);
        assert_eq!(&bytes[2..], &[0xAB, 0xCD]);
    }

    #[test]
    fn test_request_helpers() {
        let commit = TtiRequest::commit();
        assert_eq!(commit.function_code, FunctionCode::Commit);
        assert!(commit.is_request());

        let rollback = TtiRequest::rollback();
        assert_eq!(rollback.function_code, FunctionCode::Rollback);
    }

    #[test]
    fn test_message_builder_basic() {
        let msg = TtiMessageBuilder::new(FunctionCode::Execute)
            .sequence(5)
            .u32(123) // cursor_id
            .build();

        assert_eq!(msg.function_code, FunctionCode::Execute);
        assert_eq!(msg.sequence_number, 5);
        assert_eq!(msg.payload, vec![0, 0, 0, 123]);
    }

    #[test]
    fn test_message_builder_parse_request() {
        let msg = TtiMessageBuilder::parse_request().sequence(1).cursor_id(0).sql("SELECT 1").build();

        assert_eq!(msg.function_code, FunctionCode::Parse);
        // cursor_id (4) + sql_len (4) + sql (8) = 16 bytes
        assert_eq!(msg.payload.len(), 16);
    }

    #[test]
    fn test_message_builder_fetch() {
        let msg = TtiMessageBuilder::fetch_request().cursor_id(42).fetch_size(100).build();

        assert_eq!(msg.function_code, FunctionCode::Fetch);
        // cursor_id (4) + fetch_size (4) = 8 bytes
        assert_eq!(msg.payload.len(), 8);
    }

    #[test]
    fn test_message_builder_lob_read() {
        let locator = vec![0x01, 0x02, 0x03, 0x04];
        let msg = TtiMessageBuilder::lob_read_request().lob_locator(&locator).lob_offset(1000).lob_amount(4096).build();

        assert_eq!(msg.function_code, FunctionCode::LobRead);
        // locator_len (2) + locator (4) + offset (8) + amount (8) = 22 bytes
        assert_eq!(msg.payload.len(), 22);
    }

    #[test]
    fn test_message_builder_string_prefixes() {
        let msg = TtiMessageBuilder::new(FunctionCode::Unknown(0xFF)).string_u8("hi").string_u16("hello").build();

        // u8 len (1) + "hi" (2) + u16 len (2) + "hello" (5) = 10 bytes
        assert_eq!(msg.payload.len(), 10);
        assert_eq!(msg.payload[0], 2); // length of "hi"
        assert_eq!(&msg.payload[1..3], b"hi");
    }

    #[test]
    fn test_message_builder_to_bytes() {
        let bytes = TtiMessageBuilder::commit_request().sequence(10).build_bytes();

        assert_eq!(bytes[0], FunctionCode::Commit.as_u8());
        assert_eq!(bytes[1], 10); // sequence
        assert_eq!(bytes.len(), 2); // no payload
    }
}
