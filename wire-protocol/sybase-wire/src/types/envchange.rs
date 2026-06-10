//! TDS environment change token.

use crate::error::{SybaseWireError, env_change_types};
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Environment change notification.
///
/// Sent by the server when an environment setting changes.
#[derive(Clone, Debug)]
pub enum EnvChange {
    /// Database changed.
    Database { new: String, old: String },
    /// Language changed.
    Language { new: String, old: String },
    /// Character set changed.
    Charset { new: String, old: String },
    /// Packet size changed.
    PacketSize { new: String, old: String },
    /// Collation changed.
    Collation { new: Vec<u8>, old: Vec<u8> },
    /// Transaction began.
    BeginTransaction { descriptor: Vec<u8> },
    /// Transaction committed.
    CommitTransaction { old_descriptor: Vec<u8> },
    /// Transaction rolled back.
    RollbackTransaction { old_descriptor: Vec<u8> },
    /// Unknown environment change.
    Unknown {
        change_type: u8,
        new_value: Vec<u8>,
        old_value: Vec<u8>,
    },
}

impl EnvChange {
    /// Parse an ENVCHANGE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<EnvChange, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let _length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Change type (1 byte)
        let change_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // New value length and data
        let new_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let new_value = if new_len > 0 {
            let borrow = stream.peek(Some(new_len)).map_err(SybaseParseError::Stream)?;
            let data = borrow[..new_len].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            data
        } else {
            Vec::new()
        };

        // Old value length and data
        let old_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let old_value = if old_len > 0 {
            let borrow = stream.peek(Some(old_len)).map_err(SybaseParseError::Stream)?;
            let data = borrow[..old_len].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            data
        } else {
            Vec::new()
        };

        // Convert based on change type
        match change_type {
            env_change_types::DATABASE => Ok(EnvChange::Database {
                new: String::from_utf8_lossy(&new_value).into_owned(),
                old: String::from_utf8_lossy(&old_value).into_owned(),
            }),

            env_change_types::LANGUAGE => Ok(EnvChange::Language {
                new: String::from_utf8_lossy(&new_value).into_owned(),
                old: String::from_utf8_lossy(&old_value).into_owned(),
            }),

            env_change_types::CHARSET => Ok(EnvChange::Charset {
                new: String::from_utf8_lossy(&new_value).into_owned(),
                old: String::from_utf8_lossy(&old_value).into_owned(),
            }),

            env_change_types::PACKET_SIZE => Ok(EnvChange::PacketSize {
                new: String::from_utf8_lossy(&new_value).into_owned(),
                old: String::from_utf8_lossy(&old_value).into_owned(),
            }),

            env_change_types::COLLATION => Ok(EnvChange::Collation { new: new_value, old: old_value }),

            env_change_types::BEGIN_TRAN => Ok(EnvChange::BeginTransaction { descriptor: new_value }),

            env_change_types::COMMIT_TRAN => Ok(EnvChange::CommitTransaction { old_descriptor: old_value }),

            env_change_types::ROLLBACK_TRAN => Ok(EnvChange::RollbackTransaction { old_descriptor: old_value }),

            _ => Ok(EnvChange::Unknown { change_type, new_value, old_value }),
        }
    }

    /// Get the change type byte.
    pub fn change_type(&self) -> u8 {
        match self {
            EnvChange::Database { .. } => env_change_types::DATABASE,
            EnvChange::Language { .. } => env_change_types::LANGUAGE,
            EnvChange::Charset { .. } => env_change_types::CHARSET,
            EnvChange::PacketSize { .. } => env_change_types::PACKET_SIZE,
            EnvChange::Collation { .. } => env_change_types::COLLATION,
            EnvChange::BeginTransaction { .. } => env_change_types::BEGIN_TRAN,
            EnvChange::CommitTransaction { .. } => env_change_types::COMMIT_TRAN,
            EnvChange::RollbackTransaction { .. } => env_change_types::ROLLBACK_TRAN,
            EnvChange::Unknown { change_type, .. } => *change_type,
        }
    }
}
