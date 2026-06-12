//! ReadyForQuery message.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Transaction status indicator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TransactionStatus {
    /// Idle (not in a transaction block).
    Idle,
    /// In a transaction block.
    InTransaction,
    /// In a failed transaction block.
    Failed,
}

impl TransactionStatus {
    /// Convert from the wire protocol byte.
    pub fn from_byte(b: u8) -> Option<Self> {
        match b {
            b'I' => Some(TransactionStatus::Idle),
            b'T' => Some(TransactionStatus::InTransaction),
            b'E' => Some(TransactionStatus::Failed),
            _ => None,
        }
    }

    /// Convert to the wire protocol byte.
    pub fn to_byte(self) -> u8 {
        match self {
            TransactionStatus::Idle => b'I',
            TransactionStatus::InTransaction => b'T',
            TransactionStatus::Failed => b'E',
        }
    }

    /// Check if idle (not in a transaction).
    pub fn is_idle(self) -> bool {
        matches!(self, TransactionStatus::Idle)
    }

    /// Check if in a transaction block.
    pub fn is_in_transaction(self) -> bool {
        matches!(self, TransactionStatus::InTransaction)
    }

    /// Check if in a failed transaction block.
    pub fn is_failed(self) -> bool {
        matches!(self, TransactionStatus::Failed)
    }
}

/// ReadyForQuery message from the server.
///
/// Indicates that the server is ready to receive a new query.
/// Sent after startup and after each query completes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReadyForQuery {
    /// Current transaction status.
    pub status: TransactionStatus,
}

impl ReadyForQuery {
    /// Create a new ReadyForQuery with Idle status.
    pub fn idle() -> Self {
        Self { status: TransactionStatus::Idle }
    }

    /// Create a new ReadyForQuery with InTransaction status.
    pub fn in_transaction() -> Self {
        Self { status: TransactionStatus::InTransaction }
    }

    /// Create a new ReadyForQuery with Failed status.
    pub fn failed() -> Self {
        Self { status: TransactionStatus::Failed }
    }

    /// Encode the ReadyForQuery message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::READY_FOR_QUERY).write_u8(self.status.to_byte());
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum ReadyForQueryError {
    #[error("invalid transaction status: '{0}' ({0:#04X})")]
    InvalidStatus(u8),
    #[error("unexpected message type: expected 'Z', got '{0}'")]
    UnexpectedMessageType(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for ReadyForQuery {
    type ParseError = ReadyForQueryError;
    type Value<'s>
        = ReadyForQuery
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::READY_FOR_QUERY {
            return Err(PgParseError::Parse(ReadyForQueryError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let status_byte = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        let status =
            TransactionStatus::from_byte(status_byte).ok_or(PgParseError::Parse(ReadyForQueryError::InvalidStatus(status_byte)))?;

        Ok(ReadyForQuery { status })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for ReadyForQuery {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::READY_FOR_QUERY {
            return Err(PgParseError::Parse(ReadyForQueryError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let status_byte = stream.read_u8().await.map_err(PgParseError::Stream)?;
        let status =
            TransactionStatus::from_byte(status_byte).ok_or(PgParseError::Parse(ReadyForQueryError::InvalidStatus(status_byte)))?;

        Ok(ReadyForQuery { status })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_ready_for_query_idle() {
        let msg = ReadyForQuery::idle();
        let encoded = msg.encode();

        assert_eq!(encoded[0], b'Z');

        let stream = SliceStream::new(&encoded);
        let decoded = ReadyForQuery::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.status, TransactionStatus::Idle);
        assert!(decoded.status.is_idle());
    }

    #[test]
    fn test_ready_for_query_in_transaction() {
        let msg = ReadyForQuery::in_transaction();
        let encoded = msg.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = ReadyForQuery::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.status, TransactionStatus::InTransaction);
        assert!(decoded.status.is_in_transaction());
    }

    #[test]
    fn test_ready_for_query_failed() {
        let msg = ReadyForQuery::failed();
        let encoded = msg.encode();

        let stream = SliceStream::new(&encoded);
        let decoded = ReadyForQuery::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.status, TransactionStatus::Failed);
        assert!(decoded.status.is_failed());
    }
}
