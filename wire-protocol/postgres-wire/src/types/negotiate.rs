//! Protocol negotiation messages.
//!
//! These messages handle protocol version negotiation for backwards compatibility.

use crate::error::backend;
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// NegotiateProtocolVersion message (backend).
///
/// Sent by the server when it does not support the minor protocol version
/// requested by the client, but does support an earlier version of the protocol.
/// This message is also used if the client requested unsupported protocol options.
///
/// This message was introduced in PostgreSQL 9.3.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NegotiateProtocolVersion {
    /// The newest minor protocol version supported by the server
    /// for the major protocol version requested by the client.
    pub newest_minor_version: i32,
    /// List of protocol options not recognized by the server.
    pub unrecognized_options: Vec<String>,
}

impl NegotiateProtocolVersion {
    /// Create a new NegotiateProtocolVersion message.
    pub fn new(newest_minor_version: i32, unrecognized_options: Vec<String>) -> Self {
        Self { newest_minor_version, unrecognized_options }
    }

    /// Encode the NegotiateProtocolVersion message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::NEGOTIATE_PROTOCOL_VERSION)
            .write_i32_be(self.newest_minor_version)
            .write_i32_be(self.unrecognized_options.len() as i32);

        for option in &self.unrecognized_options {
            builder.write_cstring_str(option);
        }

        builder.finish_owned()
    }

    /// Returns true if there are unrecognized options.
    pub fn has_unrecognized_options(&self) -> bool {
        !self.unrecognized_options.is_empty()
    }

    /// Get the number of unrecognized options.
    pub fn unrecognized_count(&self) -> usize {
        self.unrecognized_options.len()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum NegotiateProtocolVersionError {
    #[error("unexpected message type: expected 'v', got '{0}'")]
    UnexpectedMessageType(char),
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("invalid option count: {0}")]
    InvalidOptionCount(i32),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for NegotiateProtocolVersion {
    type ParseError = NegotiateProtocolVersionError;
    type Value<'s>
        = NegotiateProtocolVersion
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::NEGOTIATE_PROTOCOL_VERSION {
            return Err(PgParseError::Parse(NegotiateProtocolVersionError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let newest_minor_version = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let option_count = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        if option_count < 0 {
            return Err(PgParseError::Parse(NegotiateProtocolVersionError::InvalidOptionCount(option_count)));
        }

        // Limit allocation to prevent DoS
        let option_count = option_count.clamp(0, 1024) as usize;
        let mut unrecognized_options = Vec::with_capacity(option_count);

        for _ in 0..option_count {
            let option_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
            let option =
                String::from_utf8(option_bytes).map_err(|_| PgParseError::Parse(NegotiateProtocolVersionError::InvalidEncoding))?;
            unrecognized_options.push(option);
        }

        Ok(NegotiateProtocolVersion { newest_minor_version, unrecognized_options })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for NegotiateProtocolVersion {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::NEGOTIATE_PROTOCOL_VERSION {
            return Err(PgParseError::Parse(NegotiateProtocolVersionError::UnexpectedMessageType(msg_type as char)));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let newest_minor_version = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let option_count = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        if option_count < 0 {
            return Err(PgParseError::Parse(NegotiateProtocolVersionError::InvalidOptionCount(option_count)));
        }

        // Limit allocation to prevent DoS
        let option_count = option_count.clamp(0, 1024) as usize;
        let mut unrecognized_options = Vec::with_capacity(option_count);

        for _ in 0..option_count {
            let option_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
            let option =
                String::from_utf8(option_bytes).map_err(|_| PgParseError::Parse(NegotiateProtocolVersionError::InvalidEncoding))?;
            unrecognized_options.push(option);
        }

        Ok(NegotiateProtocolVersion { newest_minor_version, unrecognized_options })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_negotiate_protocol_version_no_options() {
        let msg = NegotiateProtocolVersion::new(0, vec![]);
        let encoded = msg.encode();
        assert_eq!(encoded[0], b'v');

        let stream = SliceStream::new(&encoded);
        let decoded = NegotiateProtocolVersion::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.newest_minor_version, 0);
        assert!(decoded.unrecognized_options.is_empty());
    }

    #[test]
    fn test_negotiate_protocol_version_with_options() {
        let msg = NegotiateProtocolVersion::new(1, vec!["_pq_.async_password".to_string(), "_pq_.some_feature".to_string()]);
        let encoded = msg.encode();
        assert_eq!(encoded[0], b'v');

        let stream = SliceStream::new(&encoded);
        let decoded = NegotiateProtocolVersion::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.newest_minor_version, 1);
        assert_eq!(decoded.unrecognized_options.len(), 2);
        assert_eq!(decoded.unrecognized_options[0], "_pq_.async_password");
        assert_eq!(decoded.unrecognized_options[1], "_pq_.some_feature");
    }

    #[test]
    fn test_has_unrecognized_options() {
        let msg = NegotiateProtocolVersion::new(0, vec![]);
        assert!(!msg.has_unrecognized_options());

        let msg = NegotiateProtocolVersion::new(0, vec!["option".to_string()]);
        assert!(msg.has_unrecognized_options());
    }
}
