//! TDS query packet types.

use crate::error::SybaseWireError;
use crate::parse::{SybaseParse, SybaseParseError, SybaseParseSync};
use wire_stream::{WireRead, WireReadSync};

/// A SQL query packet.
#[derive(Clone, Debug)]
pub struct Query {
    /// The SQL statement.
    pub sql: Vec<u8>,
}

impl Query {
    /// Create a new query.
    pub fn new(sql: impl Into<Vec<u8>>) -> Self {
        Self { sql: sql.into() }
    }

    /// Get the SQL as a string slice (if valid UTF-8).
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.sql).ok()
    }
}

impl<S: WireReadSync + ?Sized> SybaseParseSync<S> for Query {
    type ParseError = SybaseWireError;
    type Value<'s>
        = Query
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Read all remaining bytes as the SQL statement
        let borrow = stream.peek(None).map_err(SybaseParseError::Stream)?;
        let sql = borrow.to_vec();
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        Ok(Query { sql })
    }
}

impl<S: WireRead + ?Sized> SybaseParse<S> for Query {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, SybaseParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let borrow = stream.peek_read(None).await.map_err(SybaseParseError::Stream)?;
        let sql = borrow.to_vec();
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;

        Ok(Query { sql })
    }
}
