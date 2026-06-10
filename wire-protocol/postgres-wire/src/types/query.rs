//! Simple query protocol messages.

use crate::error::frontend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Query message (simple query protocol).
///
/// Contains a SQL query string. The server will process the entire query
/// and return all results before sending ReadyForQuery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Query {
    /// The SQL query string.
    pub query: String,
}

impl Query {
    /// Create a new query message.
    pub fn new(query: impl Into<String>) -> Self {
        Self { query: query.into() }
    }

    /// Get the query string.
    pub fn as_str(&self) -> &str {
        &self.query
    }

    /// Encode the query message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::QUERY).write_cstring_str(&self.query);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum QueryError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'Q', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for Query {
    type ParseError = QueryError;
    type Value<'s>
        = Query
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::QUERY {
            return Err(PgParseError::Parse(QueryError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let query_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(QueryError::InvalidEncoding))?;

        Ok(Query { query })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for Query {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::QUERY {
            return Err(PgParseError::Parse(QueryError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let query_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let query = String::from_utf8(query_bytes).map_err(|_| PgParseError::Parse(QueryError::InvalidEncoding))?;

        Ok(Query { query })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_query_encode_decode() {
        let query = Query::new("SELECT * FROM users WHERE id = 1");
        let encoded = query.encode();

        assert_eq!(encoded[0], b'Q');

        let stream = SliceStream::new(&encoded);
        let decoded = Query::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.query, "SELECT * FROM users WHERE id = 1");
    }

    #[test]
    fn test_query_simple() {
        let query = Query::new("SELECT 1");
        let encoded = query.encode();

        // Type 'Q'
        assert_eq!(encoded[0], b'Q');
        // Length (4 bytes)
        let length = i32::from_be_bytes([encoded[1], encoded[2], encoded[3], encoded[4]]);
        // Length = 4 (itself) + 8 ("SELECT 1") + 1 (NUL) = 13
        assert_eq!(length, 13);
    }
}
