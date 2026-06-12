//! ErrorResponse and NoticeResponse messages.

use crate::error::{backend, error_field};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// ErrorResponse or NoticeResponse fields.
///
/// Both message types share the same structure.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResponseFields {
    /// Severity (localized).
    pub severity_localized: Option<String>,
    /// Severity (always English: ERROR, FATAL, PANIC, WARNING, NOTICE, DEBUG, INFO, LOG).
    pub severity: Option<String>,
    /// SQLSTATE code (5 characters).
    pub code: Option<String>,
    /// Primary message.
    pub message: Option<String>,
    /// Detail message.
    pub detail: Option<String>,
    /// Hint message.
    pub hint: Option<String>,
    /// Position (1-based character offset in the query).
    pub position: Option<i32>,
    /// Internal position.
    pub internal_position: Option<i32>,
    /// Internal query.
    pub internal_query: Option<String>,
    /// Where (call stack).
    pub where_: Option<String>,
    /// Schema name.
    pub schema: Option<String>,
    /// Table name.
    pub table: Option<String>,
    /// Column name.
    pub column: Option<String>,
    /// Data type name.
    pub data_type: Option<String>,
    /// Constraint name.
    pub constraint: Option<String>,
    /// Source file name.
    pub file: Option<String>,
    /// Source line number.
    pub line: Option<i32>,
    /// Source routine name.
    pub routine: Option<String>,
}

impl ResponseFields {
    /// Get the severity, preferring the English version.
    pub fn severity(&self) -> Option<&str> {
        self.severity.as_deref().or(self.severity_localized.as_deref())
    }

    /// Check if this is an error (as opposed to a notice/warning).
    pub fn is_error(&self) -> bool {
        matches!(self.severity().map(|s| s.to_uppercase()).as_deref(), Some("ERROR" | "FATAL" | "PANIC"))
    }

    /// Check if this is fatal.
    pub fn is_fatal(&self) -> bool {
        matches!(self.severity().map(|s| s.to_uppercase()).as_deref(), Some("FATAL" | "PANIC"))
    }
}

/// ErrorResponse message from the server.
///
/// Indicates that an error has occurred.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ErrorResponse {
    /// The error fields.
    pub fields: ResponseFields,
}

impl ErrorResponse {
    /// Create a new error response.
    pub fn new(fields: ResponseFields) -> Self {
        Self { fields }
    }

    /// Create a simple error response.
    pub fn simple(severity: &str, code: &str, message: &str) -> Self {
        Self {
            fields: ResponseFields {
                severity: Some(severity.to_string()),
                code: Some(code.to_string()),
                message: Some(message.to_string()),
                ..Default::default()
            },
        }
    }

    /// Get the SQLSTATE code.
    pub fn code(&self) -> Option<&str> {
        self.fields.code.as_deref()
    }

    /// Get the error message.
    pub fn message(&self) -> Option<&str> {
        self.fields.message.as_deref()
    }

    /// Encode the error response message.
    pub fn encode(&self) -> Vec<u8> {
        encode_response(backend::ERROR_RESPONSE, &self.fields)
    }
}

/// NoticeResponse message from the server.
///
/// Indicates a warning or informational message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NoticeResponse {
    /// The notice fields.
    pub fields: ResponseFields,
}

impl NoticeResponse {
    /// Create a new notice response.
    pub fn new(fields: ResponseFields) -> Self {
        Self { fields }
    }

    /// Create a simple notice response.
    pub fn simple(severity: &str, code: &str, message: &str) -> Self {
        Self {
            fields: ResponseFields {
                severity: Some(severity.to_string()),
                code: Some(code.to_string()),
                message: Some(message.to_string()),
                ..Default::default()
            },
        }
    }

    /// Get the notice message.
    pub fn message(&self) -> Option<&str> {
        self.fields.message.as_deref()
    }

    /// Encode the notice response message.
    pub fn encode(&self) -> Vec<u8> {
        encode_response(backend::NOTICE_RESPONSE, &self.fields)
    }
}

fn encode_response(msg_type: u8, fields: &ResponseFields) -> Vec<u8> {
    let mut builder = MessageBuilder::new();
    builder.begin(msg_type);

    if let Some(ref v) = fields.severity_localized {
        builder.write_u8(error_field::SEVERITY_LOCALIZED);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.severity {
        builder.write_u8(error_field::SEVERITY);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.code {
        builder.write_u8(error_field::CODE);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.message {
        builder.write_u8(error_field::MESSAGE);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.detail {
        builder.write_u8(error_field::DETAIL);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.hint {
        builder.write_u8(error_field::HINT);
        builder.write_cstring_str(v);
    }
    if let Some(v) = fields.position {
        builder.write_u8(error_field::POSITION);
        builder.write_cstring_str(&v.to_string());
    }
    if let Some(v) = fields.internal_position {
        builder.write_u8(error_field::INTERNAL_POSITION);
        builder.write_cstring_str(&v.to_string());
    }
    if let Some(ref v) = fields.internal_query {
        builder.write_u8(error_field::INTERNAL_QUERY);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.where_ {
        builder.write_u8(error_field::WHERE);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.schema {
        builder.write_u8(error_field::SCHEMA);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.table {
        builder.write_u8(error_field::TABLE);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.column {
        builder.write_u8(error_field::COLUMN);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.data_type {
        builder.write_u8(error_field::DATATYPE);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.constraint {
        builder.write_u8(error_field::CONSTRAINT);
        builder.write_cstring_str(v);
    }
    if let Some(ref v) = fields.file {
        builder.write_u8(error_field::FILE);
        builder.write_cstring_str(v);
    }
    if let Some(v) = fields.line {
        builder.write_u8(error_field::LINE);
        builder.write_cstring_str(&v.to_string());
    }
    if let Some(ref v) = fields.routine {
        builder.write_u8(error_field::ROUTINE);
        builder.write_cstring_str(v);
    }

    // Terminator
    builder.write_u8(0);

    builder.finish_owned()
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ErrorResponseError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'E' or 'N', got '{0}'")]
    UnexpectedMessageType(char),
}

fn parse_response_fields<S: WireReadSync + ?Sized>(stream: &S) -> Result<ResponseFields, PgParseError<S::ReadError, ErrorResponseError>> {
    let mut fields = ResponseFields::default();

    loop {
        let field_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if field_type == 0 {
            // Terminator
            break;
        }

        let value_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(ErrorResponseError::InvalidEncoding))?;

        match field_type {
            error_field::SEVERITY_LOCALIZED => fields.severity_localized = Some(value),
            error_field::SEVERITY => fields.severity = Some(value),
            error_field::CODE => fields.code = Some(value),
            error_field::MESSAGE => fields.message = Some(value),
            error_field::DETAIL => fields.detail = Some(value),
            error_field::HINT => fields.hint = Some(value),
            error_field::POSITION => fields.position = value.parse().ok(),
            error_field::INTERNAL_POSITION => fields.internal_position = value.parse().ok(),
            error_field::INTERNAL_QUERY => fields.internal_query = Some(value),
            error_field::WHERE => fields.where_ = Some(value),
            error_field::SCHEMA => fields.schema = Some(value),
            error_field::TABLE => fields.table = Some(value),
            error_field::COLUMN => fields.column = Some(value),
            error_field::DATATYPE => fields.data_type = Some(value),
            error_field::CONSTRAINT => fields.constraint = Some(value),
            error_field::FILE => fields.file = Some(value),
            error_field::LINE => fields.line = value.parse().ok(),
            error_field::ROUTINE => fields.routine = Some(value),
            _ => {} // Unknown field, ignore
        }
    }

    Ok(fields)
}

async fn parse_response_fields_async<S: WireRead + ?Sized>(
    stream: &S,
) -> Result<ResponseFields, PgParseError<S::ReadError, ErrorResponseError>> {
    let mut fields = ResponseFields::default();

    loop {
        let field_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if field_type == 0 {
            break;
        }

        let value_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let value = String::from_utf8(value_bytes).map_err(|_| PgParseError::Parse(ErrorResponseError::InvalidEncoding))?;

        match field_type {
            error_field::SEVERITY_LOCALIZED => fields.severity_localized = Some(value),
            error_field::SEVERITY => fields.severity = Some(value),
            error_field::CODE => fields.code = Some(value),
            error_field::MESSAGE => fields.message = Some(value),
            error_field::DETAIL => fields.detail = Some(value),
            error_field::HINT => fields.hint = Some(value),
            error_field::POSITION => fields.position = value.parse().ok(),
            error_field::INTERNAL_POSITION => fields.internal_position = value.parse().ok(),
            error_field::INTERNAL_QUERY => fields.internal_query = Some(value),
            error_field::WHERE => fields.where_ = Some(value),
            error_field::SCHEMA => fields.schema = Some(value),
            error_field::TABLE => fields.table = Some(value),
            error_field::COLUMN => fields.column = Some(value),
            error_field::DATATYPE => fields.data_type = Some(value),
            error_field::CONSTRAINT => fields.constraint = Some(value),
            error_field::FILE => fields.file = Some(value),
            error_field::LINE => fields.line = value.parse().ok(),
            error_field::ROUTINE => fields.routine = Some(value),
            _ => {}
        }
    }

    Ok(fields)
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for ErrorResponse {
    type ParseError = ErrorResponseError;
    type Value<'s>
        = ErrorResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::ERROR_RESPONSE {
            return Err(PgParseError::Parse(ErrorResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let fields = parse_response_fields(stream)?;

        Ok(ErrorResponse { fields })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for ErrorResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::ERROR_RESPONSE {
            return Err(PgParseError::Parse(ErrorResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let fields = parse_response_fields_async(stream).await?;

        Ok(ErrorResponse { fields })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for NoticeResponse {
    type ParseError = ErrorResponseError;
    type Value<'s>
        = NoticeResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::NOTICE_RESPONSE {
            return Err(PgParseError::Parse(ErrorResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let fields = parse_response_fields(stream)?;

        Ok(NoticeResponse { fields })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for NoticeResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::NOTICE_RESPONSE {
            return Err(PgParseError::Parse(ErrorResponseError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let fields = parse_response_fields_async(stream).await?;

        Ok(NoticeResponse { fields })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_error_response() {
        let error = ErrorResponse::simple("ERROR", "42P01", "relation \"foo\" does not exist");

        let encoded = error.encode();
        assert_eq!(encoded[0], b'E');

        let stream = SliceStream::new(&encoded);
        let decoded = ErrorResponse::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.fields.severity(), Some("ERROR"));
        assert_eq!(decoded.code(), Some("42P01"));
        assert_eq!(decoded.message(), Some("relation \"foo\" does not exist"));
        assert!(decoded.fields.is_error());
    }

    #[test]
    fn test_notice_response() {
        let notice = NoticeResponse::simple("WARNING", "01000", "deprecated feature used");

        let encoded = notice.encode();
        assert_eq!(encoded[0], b'N');

        let stream = SliceStream::new(&encoded);
        let decoded = NoticeResponse::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.fields.severity(), Some("WARNING"));
        assert_eq!(decoded.message(), Some("deprecated feature used"));
        assert!(!decoded.fields.is_error());
    }
}
