//! TNS Resend packet type.
//!
//! Resend packets are used to request retransmission of data that was
//! lost or corrupted during transmission.

use crate::oracle_ext::{OracleRead, OracleReadSync};
use crate::parse::{OracleParse, OracleParseError, OracleParseSync};
use wire_stream::{WireRead, WireReadSync};

/// TNS Resend packet.
///
/// Requests retransmission of lost or corrupted packets.
/// The packet typically contains information about which data needs
/// to be resent.
#[derive(Clone, Debug, PartialEq, Eq, Default)]
pub struct Resend {
    /// Sequence number to resend from (if applicable).
    pub from_sequence: Option<u16>,
    /// Number of packets to resend.
    pub count: Option<u16>,
}

/// Error when parsing a Resend packet.
#[derive(Clone, Debug, thiserror::Error)]
pub enum ResendError {
    #[error("resend packet too short")]
    TooShort,
}

impl Resend {
    /// Create a new resend request.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a resend request for a specific sequence.
    pub fn from_sequence(from: u16) -> Self {
        Self { from_sequence: Some(from), count: Some(1) }
    }

    /// Create a resend request for a range.
    pub fn range(from: u16, count: u16) -> Self {
        Self { from_sequence: Some(from), count: Some(count) }
    }

    /// Encode to wire format.
    pub fn to_bytes(&self) -> Vec<u8> {
        match (self.from_sequence, self.count) {
            (Some(from), Some(count)) => {
                let mut bytes = Vec::with_capacity(4);
                bytes.extend_from_slice(&from.to_be_bytes());
                bytes.extend_from_slice(&count.to_be_bytes());
                bytes
            }
            (Some(from), None) => from.to_be_bytes().to_vec(),
            _ => Vec::new(),
        }
    }
}

impl<S: WireReadSync + ?Sized> OracleParseSync<S> for Resend {
    type ParseError = ResendError;
    type Value<'s>
        = Resend
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Try to read sequence and count, but they may not be present
        match stream.read_u16_be_sync() {
            Ok(from) => match stream.read_u16_be_sync() {
                Ok(count) => Ok(Resend { from_sequence: Some(from), count: Some(count) }),
                Err(_) => Ok(Resend { from_sequence: Some(from), count: None }),
            },
            Err(_) => Ok(Resend::new()),
        }
    }
}

impl Resend {
    /// Parse with a known length.
    pub fn parse_with_length_sync<S: WireReadSync + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Resend, OracleParseError<S::ReadError, ResendError>> {
        if length == 0 {
            return Ok(Resend::new());
        }

        if length >= 2 {
            let from = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;

            if length >= 4 {
                let count = stream.read_u16_be_sync().map_err(OracleParseError::Stream)?;
                return Ok(Resend { from_sequence: Some(from), count: Some(count) });
            }

            return Ok(Resend { from_sequence: Some(from), count: None });
        }

        Ok(Resend::new())
    }

    /// Parse with a known length (async).
    pub async fn parse_with_length<S: WireRead + ?Sized>(
        stream: &S,
        length: usize,
    ) -> Result<Resend, OracleParseError<S::ReadError, ResendError>> {
        if length == 0 {
            return Ok(Resend::new());
        }

        if length >= 2 {
            let from = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;

            if length >= 4 {
                let count = stream.read_u16_be().await.map_err(OracleParseError::Stream)?;
                return Ok(Resend { from_sequence: Some(from), count: Some(count) });
            }

            return Ok(Resend { from_sequence: Some(from), count: None });
        }

        Ok(Resend::new())
    }
}

impl<S: WireRead + ?Sized> OracleParse<S> for Resend {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, OracleParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        match stream.read_u16_be().await {
            Ok(from) => match stream.read_u16_be().await {
                Ok(count) => Ok(Resend { from_sequence: Some(from), count: Some(count) }),
                Err(_) => Ok(Resend { from_sequence: Some(from), count: None }),
            },
            Err(_) => Ok(Resend::new()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resend_new() {
        let resend = Resend::new();
        assert_eq!(resend.from_sequence, None);
        assert_eq!(resend.count, None);
    }

    #[test]
    fn test_resend_from_sequence() {
        let resend = Resend::from_sequence(100);
        assert_eq!(resend.from_sequence, Some(100));
        assert_eq!(resend.count, Some(1));
    }

    #[test]
    fn test_resend_range() {
        let resend = Resend::range(50, 10);
        assert_eq!(resend.from_sequence, Some(50));
        assert_eq!(resend.count, Some(10));
    }

    #[test]
    fn test_resend_to_bytes() {
        let resend = Resend::new();
        assert!(resend.to_bytes().is_empty());

        let resend = Resend::range(0x0102, 0x0304);
        assert_eq!(resend.to_bytes(), vec![0x01, 0x02, 0x03, 0x04]);
    }
}
