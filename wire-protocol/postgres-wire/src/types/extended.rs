//! Extended query protocol messages.
//!
//! The extended query protocol separates parsing and execution,
//! allowing for prepared statements and parameterized queries.

use crate::error::{backend, frontend};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Parse message (frontend).
///
/// Creates a prepared statement from a SQL query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Parse {
    /// The prepared statement name (empty for unnamed).
    pub name: String,
    /// The SQL query.
    pub query: String,
    /// Parameter type OIDs (0 means infer).
    pub param_types: Vec<i32>,
}

impl Parse {
    /// Create a new Parse message.
    pub fn new(name: impl Into<String>, query: impl Into<String>, param_types: Vec<i32>) -> Self {
        Self { name: name.into(), query: query.into(), param_types }
    }

    /// Create an unnamed Parse message.
    pub fn unnamed(query: impl Into<String>) -> Self {
        Self::new("", query, vec![])
    }

    /// Encode the Parse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(frontend::PARSE)
            .write_cstring_str(&self.name)
            .write_cstring_str(&self.query)
            .write_i16_be(self.param_types.len() as i16);

        for &oid in &self.param_types {
            builder.write_i32_be(oid);
        }

        builder.finish_owned()
    }
}

/// ParseComplete message (backend).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ParseComplete;

impl ParseComplete {
    /// Encode the ParseComplete message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::PARSE_COMPLETE);
        builder.finish_owned()
    }
}

/// Bind message (frontend).
///
/// Binds parameter values to a prepared statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bind {
    /// The portal name (empty for unnamed).
    pub portal: String,
    /// The prepared statement name (empty for unnamed).
    pub statement: String,
    /// Parameter format codes (0 = text, 1 = binary).
    pub param_formats: Vec<i16>,
    /// Parameter values (None = NULL).
    pub param_values: Vec<Option<Vec<u8>>>,
    /// Result column format codes.
    pub result_formats: Vec<i16>,
}

impl Bind {
    /// Create a new Bind message with all text formats.
    pub fn new_text(portal: impl Into<String>, statement: impl Into<String>, values: Vec<Option<Vec<u8>>>) -> Self {
        Self {
            portal: portal.into(),
            statement: statement.into(),
            param_formats: vec![0], // All text
            param_values: values,
            result_formats: vec![0], // All text
        }
    }

    /// Encode the Bind message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::BIND).write_cstring_str(&self.portal).write_cstring_str(&self.statement);

        // Parameter format codes
        builder.write_i16_be(self.param_formats.len() as i16);
        for &fmt in &self.param_formats {
            builder.write_i16_be(fmt);
        }

        // Parameter values
        builder.write_i16_be(self.param_values.len() as i16);
        for value in &self.param_values {
            match value {
                None => builder.write_i32_be(-1),
                Some(v) => builder.write_i32_be(v.len() as i32).write_bytes(v),
            };
        }

        // Result format codes
        builder.write_i16_be(self.result_formats.len() as i16);
        for &fmt in &self.result_formats {
            builder.write_i16_be(fmt);
        }

        builder.finish_owned()
    }
}

/// BindComplete message (backend).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BindComplete;

impl BindComplete {
    /// Encode the BindComplete message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::BIND_COMPLETE);
        builder.finish_owned()
    }
}

/// Describe message (frontend).
///
/// Requests a description of a prepared statement or portal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Describe {
    /// 'S' for statement, 'P' for portal.
    pub kind: u8,
    /// The name (empty for unnamed).
    pub name: String,
}

impl Describe {
    /// Describe a prepared statement.
    pub fn statement(name: impl Into<String>) -> Self {
        Self { kind: b'S', name: name.into() }
    }

    /// Describe a portal.
    pub fn portal(name: impl Into<String>) -> Self {
        Self { kind: b'P', name: name.into() }
    }

    /// Encode the Describe message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::DESCRIBE).write_u8(self.kind).write_cstring_str(&self.name);
        builder.finish_owned()
    }
}

/// Execute message (frontend).
///
/// Executes a bound portal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Execute {
    /// The portal name (empty for unnamed).
    pub portal: String,
    /// Maximum rows to return (0 = unlimited).
    pub max_rows: i32,
}

impl Execute {
    /// Execute the unnamed portal with no row limit.
    pub fn unnamed() -> Self {
        Self { portal: String::new(), max_rows: 0 }
    }

    /// Execute a named portal.
    pub fn named(portal: impl Into<String>, max_rows: i32) -> Self {
        Self { portal: portal.into(), max_rows }
    }

    /// Encode the Execute message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::EXECUTE).write_cstring_str(&self.portal).write_i32_be(self.max_rows);
        builder.finish_owned()
    }
}

/// Sync message (frontend).
///
/// Signals the end of an extended query sequence.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sync;

impl Sync {
    /// Encode the Sync message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::SYNC);
        builder.finish_owned()
    }
}

/// Flush message (frontend).
///
/// Requests the server to send any pending output.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Flush;

impl Flush {
    /// Encode the Flush message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::FLUSH);
        builder.finish_owned()
    }
}

/// Close message (frontend).
///
/// Closes a prepared statement or portal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Close {
    /// 'S' for statement, 'P' for portal.
    pub kind: u8,
    /// The name (empty for unnamed).
    pub name: String,
}

impl Close {
    /// Close a prepared statement.
    pub fn statement(name: impl Into<String>) -> Self {
        Self { kind: b'S', name: name.into() }
    }

    /// Close a portal.
    pub fn portal(name: impl Into<String>) -> Self {
        Self { kind: b'P', name: name.into() }
    }

    /// Encode the Close message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::CLOSE).write_u8(self.kind).write_cstring_str(&self.name);
        builder.finish_owned()
    }
}

/// CloseComplete message (backend).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CloseComplete;

impl CloseComplete {
    /// Encode the CloseComplete message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::CLOSE_COMPLETE);
        builder.finish_owned()
    }
}

/// NoData message (backend).
///
/// Indicates that a statement will not return rows.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NoData;

impl NoData {
    /// Encode the NoData message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::NO_DATA);
        builder.finish_owned()
    }
}

/// PortalSuspended message (backend).
///
/// Indicates that a portal was suspended (max_rows limit reached).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PortalSuspended;

impl PortalSuspended {
    /// Encode the PortalSuspended message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::PORTAL_SUSPENDED);
        builder.finish_owned()
    }
}

/// ParameterDescription message (backend).
///
/// Describes the parameters of a prepared statement.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ParameterDescription {
    /// Parameter type OIDs.
    pub param_types: Vec<i32>,
}

impl ParameterDescription {
    /// Create a new ParameterDescription.
    pub fn new(param_types: Vec<i32>) -> Self {
        Self { param_types }
    }

    /// Encode the ParameterDescription message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::PARAMETER_DESCRIPTION).write_i16_be(self.param_types.len() as i16);

        for &oid in &self.param_types {
            builder.write_i32_be(oid);
        }

        builder.finish_owned()
    }
}

// Simple parse implementations for backend messages

#[derive(Clone, Debug, thiserror::Error)]
pub enum ExtendedError {
    #[error("unexpected message type: expected '{expected}', got '{encountered}'")]
    UnexpectedMessageType { expected: char, encountered: char },
    #[error("invalid encoding")]
    InvalidEncoding,
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for ParseComplete {
    type ParseError = ExtendedError;
    type Value<'s>
        = ParseComplete
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::PARSE_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PARSE_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(ParseComplete)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for ParseComplete {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::PARSE_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PARSE_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(ParseComplete)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for BindComplete {
    type ParseError = ExtendedError;
    type Value<'s>
        = BindComplete
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::BIND_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::BIND_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(BindComplete)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for BindComplete {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::BIND_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::BIND_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(BindComplete)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CloseComplete {
    type ParseError = ExtendedError;
    type Value<'s>
        = CloseComplete
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::CLOSE_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::CLOSE_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(CloseComplete)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CloseComplete {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::CLOSE_COMPLETE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::CLOSE_COMPLETE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(CloseComplete)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for NoData {
    type ParseError = ExtendedError;
    type Value<'s>
        = NoData
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::NO_DATA {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::NO_DATA as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(NoData)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for NoData {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::NO_DATA {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::NO_DATA as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(NoData)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for PortalSuspended {
    type ParseError = ExtendedError;
    type Value<'s>
        = PortalSuspended
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::PORTAL_SUSPENDED {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PORTAL_SUSPENDED as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(PortalSuspended)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for PortalSuspended {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::PORTAL_SUSPENDED {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PORTAL_SUSPENDED as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(PortalSuspended)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for ParameterDescription {
    type ParseError = ExtendedError;
    type Value<'s>
        = ParameterDescription
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::PARAMETER_DESCRIPTION {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PARAMETER_DESCRIPTION as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        // Validate count to prevent allocation DOS
        let count = count.max(0) as usize;
        let mut param_types = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            param_types.push(stream.read_i32_be_sync().map_err(PgParseError::Stream)?);
        }

        Ok(ParameterDescription { param_types })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for ParameterDescription {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::PARAMETER_DESCRIPTION {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: backend::PARAMETER_DESCRIPTION as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        // Validate count to prevent allocation DOS
        let count = count.max(0) as usize;
        let mut param_types = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            param_types.push(stream.read_i32_be().await.map_err(PgParseError::Stream)?);
        }

        Ok(ParameterDescription { param_types })
    }
}

// Parse implementations for frontend messages (for server-side parsing)

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Parse {
    type ParseError = ExtendedError;
    type Value<'s>
        = Parse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::PARSE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::PARSE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let query_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let count = count.max(0) as usize;
        let mut param_types = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            param_types.push(stream.read_i32_be_sync().map_err(PgParseError::Stream)?);
        }

        Ok(Parse { name, query, param_types })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Parse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::PARSE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::PARSE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let name_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let query_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let count = count.max(0) as usize;
        let mut param_types = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            param_types.push(stream.read_i32_be().await.map_err(PgParseError::Stream)?);
        }

        Ok(Parse { name, query, param_types })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Describe {
    type ParseError = ExtendedError;
    type Value<'s>
        = Describe
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::DESCRIBE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::DESCRIBE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let kind = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        Ok(Describe { kind, name })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Describe {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::DESCRIBE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::DESCRIBE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let kind = stream.read_u8().await.map_err(PgParseError::Stream)?;
        let name_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        Ok(Describe { kind, name })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Execute {
    type ParseError = ExtendedError;
    type Value<'s>
        = Execute
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::EXECUTE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::EXECUTE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let portal_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let max_rows = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        Ok(Execute { portal, max_rows })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Execute {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::EXECUTE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::EXECUTE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let portal_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let max_rows = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        Ok(Execute { portal, max_rows })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Close {
    type ParseError = ExtendedError;
    type Value<'s>
        = Close
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::CLOSE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::CLOSE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let kind = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let name_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        Ok(Close { kind, name })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Close {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::CLOSE {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::CLOSE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let kind = stream.read_u8().await.map_err(PgParseError::Stream)?;
        let name_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let name = String::from_utf8(name_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        Ok(Close { kind, name })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Bind {
    type ParseError = ExtendedError;
    type Value<'s>
        = Bind
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::BIND {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::BIND as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let portal_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let statement_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let statement = String::from_utf8(statement_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        // Parameter format codes
        let format_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let format_count = format_count.max(0) as usize;
        let mut param_formats = Vec::with_capacity(format_count.min(1024));
        for _ in 0..format_count {
            param_formats.push(stream.read_i16_be_sync().map_err(PgParseError::Stream)?);
        }

        // Parameter values
        let value_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let value_count = value_count.max(0) as usize;
        let mut param_values = Vec::with_capacity(value_count.min(1024));
        for _ in 0..value_count {
            let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            if len == -1 {
                param_values.push(None);
            } else if len >= 0 {
                let data = stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?;
                param_values.push(Some(data));
            } else {
                // Invalid length
                param_values.push(None);
            }
        }

        // Result format codes
        let result_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let result_count = result_count.max(0) as usize;
        let mut result_formats = Vec::with_capacity(result_count.min(1024));
        for _ in 0..result_count {
            result_formats.push(stream.read_i16_be_sync().map_err(PgParseError::Stream)?);
        }

        Ok(Bind {
            portal,
            statement,
            param_formats,
            param_values,
            result_formats,
        })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Bind {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::BIND {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::BIND as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let portal_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let portal = String::from_utf8(portal_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        let statement_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let statement = String::from_utf8(statement_bytes).map_err(|_| PgParseError::Parse(ExtendedError::InvalidEncoding))?;

        // Parameter format codes
        let format_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let format_count = format_count.max(0) as usize;
        let mut param_formats = Vec::with_capacity(format_count.min(1024));
        for _ in 0..format_count {
            param_formats.push(stream.read_i16_be().await.map_err(PgParseError::Stream)?);
        }

        // Parameter values
        let value_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let value_count = value_count.max(0) as usize;
        let mut param_values = Vec::with_capacity(value_count.min(1024));
        for _ in 0..value_count {
            let len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            if len == -1 {
                param_values.push(None);
            } else if len >= 0 {
                let data = stream.read_bytes(len as usize).await.map_err(PgParseError::Stream)?;
                param_values.push(Some(data));
            } else {
                param_values.push(None);
            }
        }

        // Result format codes
        let result_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let result_count = result_count.max(0) as usize;
        let mut result_formats = Vec::with_capacity(result_count.min(1024));
        for _ in 0..result_count {
            result_formats.push(stream.read_i16_be().await.map_err(PgParseError::Stream)?);
        }

        Ok(Bind {
            portal,
            statement,
            param_formats,
            param_values,
            result_formats,
        })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Sync {
    type ParseError = ExtendedError;
    type Value<'s>
        = Sync
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::SYNC {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::SYNC as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(Sync)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Sync {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::SYNC {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::SYNC as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(Sync)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Flush {
    type ParseError = ExtendedError;
    type Value<'s>
        = Flush
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::FLUSH {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::FLUSH as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(Flush)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Flush {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::FLUSH {
            return Err(PgParseError::Parse(ExtendedError::UnexpectedMessageType {
                expected: frontend::FLUSH as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(Flush)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parse_message() {
        let parse = Parse::unnamed("SELECT $1::int");
        let encoded = parse.encode();
        assert_eq!(encoded[0], b'P');
    }

    #[test]
    fn test_bind_message() {
        let bind = Bind::new_text("", "", vec![Some(b"42".to_vec())]);
        let encoded = bind.encode();
        assert_eq!(encoded[0], b'B');
    }

    #[test]
    fn test_execute_message() {
        let exec = Execute::unnamed();
        let encoded = exec.encode();
        assert_eq!(encoded[0], b'E');
    }

    #[test]
    fn test_sync_message() {
        let encoded = Sync::encode();
        assert_eq!(encoded[0], b'S');
        assert_eq!(encoded.len(), 5); // type + length
    }

    #[test]
    fn test_parse_complete() {
        let encoded = ParseComplete::encode();
        let stream = SliceStream::new(&encoded);
        let _decoded = ParseComplete::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_bind_complete() {
        let encoded = BindComplete::encode();
        let stream = SliceStream::new(&encoded);
        let _decoded = BindComplete::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_parameter_description() {
        let desc = ParameterDescription::new(vec![23, 25, 1043]); // int4, text, varchar
        let encoded = desc.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = ParameterDescription::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.param_types, vec![23, 25, 1043]);
    }
}
