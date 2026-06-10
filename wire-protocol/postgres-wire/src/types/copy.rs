//! COPY protocol messages.
//!
//! Used for bulk data transfer (COPY TO/FROM).

use crate::error::{backend, frontend};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// Format code for COPY columns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FormatCode {
    /// Text format.
    Text,
    /// Binary format.
    Binary,
}

impl FormatCode {
    /// Convert from the wire protocol value.
    pub fn from_i16(value: i16) -> Self {
        if value == 1 { FormatCode::Binary } else { FormatCode::Text }
    }

    /// Convert to the wire protocol value.
    pub fn to_i16(self) -> i16 {
        match self {
            FormatCode::Text => 0,
            FormatCode::Binary => 1,
        }
    }
}

/// CopyInResponse message (backend).
///
/// Indicates that the server is ready to receive COPY data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyInResponse {
    /// Overall format (0 = text, 1 = binary).
    pub format: FormatCode,
    /// Per-column format codes.
    pub column_formats: Vec<FormatCode>,
}

impl CopyInResponse {
    /// Create a new CopyInResponse.
    pub fn new(format: FormatCode, column_formats: Vec<FormatCode>) -> Self {
        Self { format, column_formats }
    }

    /// Encode the CopyInResponse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::COPY_IN_RESPONSE)
            .write_u8(self.format.to_i16() as u8)
            .write_i16_be(self.column_formats.len() as i16);

        for &fmt in &self.column_formats {
            builder.write_i16_be(fmt.to_i16());
        }

        builder.finish_owned()
    }
}

/// CopyOutResponse message (backend).
///
/// Indicates that the server will send COPY data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyOutResponse {
    /// Overall format (0 = text, 1 = binary).
    pub format: FormatCode,
    /// Per-column format codes.
    pub column_formats: Vec<FormatCode>,
}

impl CopyOutResponse {
    /// Create a new CopyOutResponse.
    pub fn new(format: FormatCode, column_formats: Vec<FormatCode>) -> Self {
        Self { format, column_formats }
    }

    /// Encode the CopyOutResponse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::COPY_OUT_RESPONSE)
            .write_u8(self.format.to_i16() as u8)
            .write_i16_be(self.column_formats.len() as i16);

        for &fmt in &self.column_formats {
            builder.write_i16_be(fmt.to_i16());
        }

        builder.finish_owned()
    }
}

/// CopyBothResponse message (backend).
///
/// Indicates that the server is ready for bidirectional COPY data transfer.
/// This is used primarily for streaming replication (PostgreSQL 9.0+).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyBothResponse {
    /// Overall format (0 = text, 1 = binary).
    pub format: FormatCode,
    /// Per-column format codes.
    pub column_formats: Vec<FormatCode>,
}

impl CopyBothResponse {
    /// Create a new CopyBothResponse.
    pub fn new(format: FormatCode, column_formats: Vec<FormatCode>) -> Self {
        Self { format, column_formats }
    }

    /// Encode the CopyBothResponse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder
            .begin(backend::COPY_BOTH_RESPONSE)
            .write_u8(self.format.to_i16() as u8)
            .write_i16_be(self.column_formats.len() as i16);

        for &fmt in &self.column_formats {
            builder.write_i16_be(fmt.to_i16());
        }

        builder.finish_owned()
    }
}

/// CopyData message (both directions).
///
/// Contains a chunk of COPY data.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyData {
    /// The data bytes.
    pub data: Vec<u8>,
}

impl CopyData {
    /// Create a new CopyData message.
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    /// Encode the CopyData message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::COPY_DATA).write_bytes(&self.data);
        builder.finish_owned()
    }
}

/// CopyDone message (both directions).
///
/// Indicates the end of COPY data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CopyDone;

impl CopyDone {
    /// Encode the CopyDone message.
    pub fn encode() -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::COPY_DONE);
        builder.finish_owned()
    }
}

/// CopyFail message (frontend).
///
/// Indicates that the client is aborting a COPY operation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CopyFail {
    /// Error message.
    pub message: String,
}

impl CopyFail {
    /// Create a new CopyFail message.
    pub fn new(message: impl Into<String>) -> Self {
        Self { message: message.into() }
    }

    /// Encode the CopyFail message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::COPY_FAIL).write_cstring_str(&self.message);
        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CopyError {
    #[error("unexpected message type: expected '{expected}', got '{encountered}'")]
    UnexpectedMessageType { expected: char, encountered: char },
    #[error("invalid encoding")]
    InvalidEncoding,
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyInResponse {
    type ParseError = CopyError;
    type Value<'s>
        = CopyInResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_IN_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_IN_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8_sync().map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

        let mut column_formats = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be_sync().map_err(PgParseError::Stream)?));
        }

        Ok(CopyInResponse { format, column_formats })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyInResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_IN_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_IN_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8().await.map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;

        let mut column_formats = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be().await.map_err(PgParseError::Stream)?));
        }

        Ok(CopyInResponse { format, column_formats })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyOutResponse {
    type ParseError = CopyError;
    type Value<'s>
        = CopyOutResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_OUT_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_OUT_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8_sync().map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

        let mut column_formats = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be_sync().map_err(PgParseError::Stream)?));
        }

        Ok(CopyOutResponse { format, column_formats })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyOutResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_OUT_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_OUT_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8().await.map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;

        let mut column_formats = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be().await.map_err(PgParseError::Stream)?));
        }

        Ok(CopyOutResponse { format, column_formats })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyBothResponse {
    type ParseError = CopyError;
    type Value<'s>
        = CopyBothResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_BOTH_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_BOTH_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8_sync().map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

        // Limit allocation to prevent DoS
        let column_count = column_count.clamp(0, 1024) as usize;
        let mut column_formats = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be_sync().map_err(PgParseError::Stream)?));
        }

        Ok(CopyBothResponse { format, column_formats })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyBothResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_BOTH_RESPONSE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_BOTH_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let format = FormatCode::from_i16(stream.read_u8().await.map_err(PgParseError::Stream)? as i16);
        let column_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;

        // Limit allocation to prevent DoS
        let column_count = column_count.clamp(0, 1024) as usize;
        let mut column_formats = Vec::with_capacity(column_count);
        for _ in 0..column_count {
            column_formats.push(FormatCode::from_i16(stream.read_i16_be().await.map_err(PgParseError::Stream)?));
        }

        Ok(CopyBothResponse { format, column_formats })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyData {
    type ParseError = CopyError;
    type Value<'s>
        = CopyData
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_DATA {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_DATA as char,
                encountered: msg_type as char,
            }));
        }

        let length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let data_length = (length - 4) as usize;
        let data = stream.read_bytes_sync(data_length).map_err(PgParseError::Stream)?;

        Ok(CopyData { data })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyData {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_DATA {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_DATA as char,
                encountered: msg_type as char,
            }));
        }

        let length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let data_length = (length - 4) as usize;
        let data = stream.read_bytes(data_length).await.map_err(PgParseError::Stream)?;

        Ok(CopyData { data })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyDone {
    type ParseError = CopyError;
    type Value<'s>
        = CopyDone
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_DONE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_DONE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        Ok(CopyDone)
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyDone {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::COPY_DONE {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: backend::COPY_DONE as char,
                encountered: msg_type as char,
            }));
        }
        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        Ok(CopyDone)
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for CopyFail {
    type ParseError = CopyError;
    type Value<'s>
        = CopyFail
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::COPY_FAIL {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: frontend::COPY_FAIL as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let message_bytes = stream.read_cstring_sync().map_err(PgParseError::Stream)?;
        let message = String::from_utf8(message_bytes).map_err(|_| PgParseError::Parse(CopyError::InvalidEncoding))?;

        Ok(CopyFail { message })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for CopyFail {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::COPY_FAIL {
            return Err(PgParseError::Parse(CopyError::UnexpectedMessageType {
                expected: frontend::COPY_FAIL as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let message_bytes = stream.read_cstring().await.map_err(PgParseError::Stream)?;
        let message = String::from_utf8(message_bytes).map_err(|_| PgParseError::Parse(CopyError::InvalidEncoding))?;

        Ok(CopyFail { message })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_copy_in_response() {
        let response = CopyInResponse::new(FormatCode::Text, vec![FormatCode::Text, FormatCode::Text]);
        let encoded = response.encode();
        assert_eq!(encoded[0], b'G');

        let stream = SliceStream::new(&encoded);
        let decoded = CopyInResponse::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.format, FormatCode::Text);
        assert_eq!(decoded.column_formats.len(), 2);
    }

    #[test]
    fn test_copy_out_response() {
        let response = CopyOutResponse::new(FormatCode::Binary, vec![FormatCode::Binary]);
        let encoded = response.encode();
        assert_eq!(encoded[0], b'H');

        let stream = SliceStream::new(&encoded);
        let decoded = CopyOutResponse::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.format, FormatCode::Binary);
    }

    #[test]
    fn test_copy_both_response() {
        let response = CopyBothResponse::new(FormatCode::Binary, vec![FormatCode::Binary, FormatCode::Binary]);
        let encoded = response.encode();
        assert_eq!(encoded[0], b'W');

        let stream = SliceStream::new(&encoded);
        let decoded = CopyBothResponse::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.format, FormatCode::Binary);
        assert_eq!(decoded.column_formats.len(), 2);
    }

    #[test]
    fn test_copy_data() {
        let data = CopyData::new(b"1\tAlice\n2\tBob\n".to_vec());
        let encoded = data.encode();
        assert_eq!(encoded[0], b'd');

        let stream = SliceStream::new(&encoded);
        let decoded = CopyData::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.data, b"1\tAlice\n2\tBob\n");
    }

    #[test]
    fn test_copy_done() {
        let encoded = CopyDone::encode();
        assert_eq!(encoded[0], b'c');

        let stream = SliceStream::new(&encoded);
        let _decoded = CopyDone::parse_sync(&stream).expect("parse failed");
    }

    #[test]
    fn test_copy_fail() {
        let fail = CopyFail::new("out of disk space");
        let encoded = fail.encode();
        assert_eq!(encoded[0], b'f');

        let stream = SliceStream::new(&encoded);
        let decoded = CopyFail::parse_sync(&stream).expect("parse failed");
        assert_eq!(decoded.message, "out of disk space");
    }
}
