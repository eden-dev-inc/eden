//! CommandComplete message.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// CommandComplete message from the server.
///
/// Indicates that a command has finished. The tag describes what was done.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandComplete {
    /// The command tag (e.g., "SELECT 100", "INSERT 0 1", "UPDATE 5").
    pub tag: String,
}

impl CommandComplete {
    /// Create a new command complete message.
    pub fn new(tag: impl Into<String>) -> Self {
        Self { tag: tag.into() }
    }

    /// Create a SELECT command complete.
    pub fn select(rows: u64) -> Self {
        Self::new(format!("SELECT {}", rows))
    }

    /// Create an INSERT command complete.
    pub fn insert(oid: u32, rows: u64) -> Self {
        Self::new(format!("INSERT {} {}", oid, rows))
    }

    /// Create an UPDATE command complete.
    pub fn update(rows: u64) -> Self {
        Self::new(format!("UPDATE {}", rows))
    }

    /// Create a DELETE command complete.
    pub fn delete(rows: u64) -> Self {
        Self::new(format!("DELETE {}", rows))
    }

    /// Get the command name (first word of the tag).
    pub fn command(&self) -> &str {
        self.tag.split_whitespace().next().unwrap_or("")
    }

    /// Get the row count from the tag, if present.
    pub fn row_count(&self) -> Option<u64> {
        let parts: Vec<&str> = self.tag.split_whitespace().collect();
        match parts.as_slice() {
            // SELECT n
            ["SELECT", n] => n.parse().ok(),
            // INSERT oid n
            ["INSERT", _, n] => n.parse().ok(),
            // UPDATE n, DELETE n, MOVE n, FETCH n, COPY n
            [_, n] => n.parse().ok(),
            _ => None,
        }
    }

    /// Encode the command complete message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::COMMAND_COMPLETE).write_cstring_str(&self.tag);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CommandCompleteError {
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("unexpected message type: expected 'C', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CommandComplete {
    type ParseError = CommandCompleteError;
    type Value<'s>
        = CommandComplete
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COMMAND_COMPLETE {
            return Err(PgParseError::Parse(CommandCompleteError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let tag_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let tag = String::from_utf8(tag_bytes).map_err(|_| PgParseError::Parse(CommandCompleteError::InvalidEncoding))?;

        Ok(CommandComplete { tag })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CommandComplete {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COMMAND_COMPLETE {
            return Err(PgParseError::Parse(CommandCompleteError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let tag_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let tag = String::from_utf8(tag_bytes).map_err(|_| PgParseError::Parse(CommandCompleteError::InvalidEncoding))?;

        Ok(CommandComplete { tag })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_command_complete_select() {
        let cmd = CommandComplete::select(100);
        assert_eq!(cmd.tag, "SELECT 100");
        assert_eq!(cmd.command(), "SELECT");
        assert_eq!(cmd.row_count(), Some(100));

        let encoded = cmd.encode();
        assert_eq!(encoded[0], b'C');

        let stream = SliceStream::new(&encoded);
        let decoded = CommandComplete::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.tag, "SELECT 100");
    }

    #[test]
    fn test_command_complete_insert() {
        let cmd = CommandComplete::insert(0, 5);
        assert_eq!(cmd.tag, "INSERT 0 5");
        assert_eq!(cmd.command(), "INSERT");
        assert_eq!(cmd.row_count(), Some(5));
    }

    #[test]
    fn test_command_complete_update() {
        let cmd = CommandComplete::update(10);
        assert_eq!(cmd.tag, "UPDATE 10");
        assert_eq!(cmd.command(), "UPDATE");
        assert_eq!(cmd.row_count(), Some(10));
    }

    #[test]
    fn test_command_complete_delete() {
        let cmd = CommandComplete::delete(3);
        assert_eq!(cmd.tag, "DELETE 3");
        assert_eq!(cmd.command(), "DELETE");
        assert_eq!(cmd.row_count(), Some(3));
    }

    #[test]
    fn test_command_complete_custom() {
        let cmd = CommandComplete::new("CREATE TABLE");
        assert_eq!(cmd.command(), "CREATE");
        assert_eq!(cmd.row_count(), None);
    }
}
