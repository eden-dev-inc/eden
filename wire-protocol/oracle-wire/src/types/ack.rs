//! TNS Acknowledge (ACK) packet type.
//!
//! ACK packets are used for flow control and acknowledgment in the TNS protocol.
//! They are typically sent in response to certain operations to confirm receipt.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync};

/// TNS Acknowledge packet.
///
/// ACK packets provide flow control and acknowledgment mechanisms.
/// They are minimal packets, often with no payload or just a sequence number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub struct Ack {
    /// Sequence number being acknowledged (if present).
    pub sequence: Option<u8>,
}

/// Error when parsing an ACK packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum AckError {
    #[error("invalid ACK packet")]
    Invalid,
}

impl Ack {
    /// Create a new ACK packet.
    pub fn new() -> Self {
        Self { sequence: None }
    }

    /// Create an ACK with a sequence number.
    pub fn with_sequence(sequence: u8) -> Self {
        Self { sequence: Some(sequence) }
    }

    /// Encode to wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        match self.sequence {
            Some(seq) => vec![seq],
            None => Vec::new(),
        }
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Ack {
    type ParseError = AckError;
    type Value<'s>
        = Ack
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // ACK packets may have 0 or 1 byte payload
        // Try to read a sequence byte, but it's optional
        match stream.read_u8_sync() {
            Ok(seq) => Ok(Ack { sequence: Some(seq) }),
            Err(_) => Ok(Ack { sequence: None }),
        }
    }
}

impl Ack {
    /// Parse with a known length.
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Ack, OracleParseError<S::ReadError, AckError>> {
        if length == 0 {
            return Ok(Ack { sequence: None });
        }

        let seq = stream.read_u8_sync().map_err(OracleParseError::Stream)?;
        Ok(Ack { sequence: Some(seq) })
    }

    /// Parse with a known length (async).
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Ack, OracleParseError<S::ReadError, AckError>> {
        if length == 0 {
            return Ok(Ack { sequence: None });
        }

        let seq = stream.read_u8().await.map_err(OracleParseError::Stream)?;
        Ok(Ack { sequence: Some(seq) })
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Ack {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        match stream.read_u8().await {
            Ok(seq) => Ok(Ack { sequence: Some(seq) }),
            Err(_) => Ok(Ack { sequence: None }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ack_new() {
        let ack = Ack::new();
        assert_eq!(ack.sequence, None);
    }

    #[test]
    fn test_ack_with_sequence() {
        let ack = Ack::with_sequence(42);
        assert_eq!(ack.sequence, Some(42));
    }

    #[test]
    fn test_ack_to_bytes() {
        let ack = Ack::new();
        assert!(ack.to_bytes().is_empty());

        let ack = Ack::with_sequence(5);
        assert_eq!(ack.to_bytes(), vec![5]);
    }
}
