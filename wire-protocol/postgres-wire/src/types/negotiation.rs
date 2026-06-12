//! SSL/GSS negotiation response types.
//!
//! After sending an SSLRequest or GSSEncRequest, the server responds with
//! a single byte indicating whether it supports the requested feature.

use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use wire_stream::{WireRead, WireReadSync};

/// SSL negotiation response from the server.
///
/// After sending an SSLRequest, the server responds with:
/// - 'S' if SSL is supported and the connection should be upgraded
/// - 'N' if SSL is not supported
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SSLResponse {
    /// SSL is supported - upgrade the connection.
    Supported,
    /// SSL is not supported - continue without encryption.
    NotSupported,
}

impl SSLResponse {
    /// Check if SSL is supported.
    pub fn is_supported(&self) -> bool {
        matches!(self, SSLResponse::Supported)
    }

    /// Encode the SSL response (for server implementations).
    pub fn encode(&self) -> [u8; 1] {
        match self {
            SSLResponse::Supported => [b'S'],
            SSLResponse::NotSupported => [b'N'],
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum SSLResponseError {
    #[error("unexpected SSL response: expected 'S' or 'N', got '{0}'")]
    UnexpectedResponse(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for SSLResponse {
    type ParseError = SSLResponseError;
    type Value<'s>
        = SSLResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let response = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        match response {
            b'S' => Ok(SSLResponse::Supported),
            b'N' => Ok(SSLResponse::NotSupported),
            _ => Err(PgParseError::Parse(SSLResponseError::UnexpectedResponse(response as char))),
        }
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for SSLResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let response = stream.read_u8().await.map_err(PgParseError::Stream)?;
        match response {
            b'S' => Ok(SSLResponse::Supported),
            b'N' => Ok(SSLResponse::NotSupported),
            _ => Err(PgParseError::Parse(SSLResponseError::UnexpectedResponse(response as char))),
        }
    }
}

/// GSS encryption negotiation response from the server.
///
/// After sending a GSSEncRequest, the server responds with:
/// - 'G' if GSSAPI encryption is supported
/// - 'N' if GSSAPI encryption is not supported
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GSSResponse {
    /// GSSAPI encryption is supported.
    Supported,
    /// GSSAPI encryption is not supported.
    NotSupported,
}

impl GSSResponse {
    /// Check if GSS encryption is supported.
    pub fn is_supported(&self) -> bool {
        matches!(self, GSSResponse::Supported)
    }

    /// Encode the GSS response (for server implementations).
    pub fn encode(&self) -> [u8; 1] {
        match self {
            GSSResponse::Supported => [b'G'],
            GSSResponse::NotSupported => [b'N'],
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum GSSResponseError {
    #[error("unexpected GSS response: expected 'G' or 'N', got '{0}'")]
    UnexpectedResponse(char),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for GSSResponse {
    type ParseError = GSSResponseError;
    type Value<'s>
        = GSSResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let response = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        match response {
            b'G' => Ok(GSSResponse::Supported),
            b'N' => Ok(GSSResponse::NotSupported),
            _ => Err(PgParseError::Parse(GSSResponseError::UnexpectedResponse(response as char))),
        }
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for GSSResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let response = stream.read_u8().await.map_err(PgParseError::Stream)?;
        match response {
            b'G' => Ok(GSSResponse::Supported),
            b'N' => Ok(GSSResponse::NotSupported),
            _ => Err(PgParseError::Parse(GSSResponseError::UnexpectedResponse(response as char))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_ssl_response_supported() {
        let data = [b'S'];
        let stream = SliceStream::new(&data);
        let response = SSLResponse::parse_sync(&stream).expect("parse failed");
        assert!(response.is_supported());
    }

    #[test]
    fn test_ssl_response_not_supported() {
        let data = [b'N'];
        let stream = SliceStream::new(&data);
        let response = SSLResponse::parse_sync(&stream).expect("parse failed");
        assert!(!response.is_supported());
    }

    #[test]
    fn test_ssl_response_encode() {
        assert_eq!(SSLResponse::Supported.encode(), [b'S']);
        assert_eq!(SSLResponse::NotSupported.encode(), [b'N']);
    }

    #[test]
    fn test_gss_response_supported() {
        let data = [b'G'];
        let stream = SliceStream::new(&data);
        let response = GSSResponse::parse_sync(&stream).expect("parse failed");
        assert!(response.is_supported());
    }

    #[test]
    fn test_gss_response_not_supported() {
        let data = [b'N'];
        let stream = SliceStream::new(&data);
        let response = GSSResponse::parse_sync(&stream).expect("parse failed");
        assert!(!response.is_supported());
    }

    #[test]
    fn test_gss_response_encode() {
        assert_eq!(GSSResponse::Supported.encode(), [b'G']);
        assert_eq!(GSSResponse::NotSupported.encode(), [b'N']);
    }
}
