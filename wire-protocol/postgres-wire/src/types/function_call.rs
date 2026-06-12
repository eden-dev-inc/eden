//! Function call protocol messages (DEPRECATED).
//!
//! These messages implement the legacy function call sub-protocol.
//! This is maintained for backwards compatibility but should not be used
//! in new code. The documentation states it's "probably best avoided."
//!
//! Supported since PostgreSQL 7.4, deprecated in favor of using
//! SELECT function_name(...) with the regular query protocol.

use crate::error::{backend, frontend};
use crate::parse::{PgParse, PgParseError, PgParseSync};
use crate::pg_ext::{PgRead, PgReadSync};
use crate::write::MessageBuilder;
use wire_stream::{WireRead, WireReadSync};

/// FunctionCall message (frontend, DEPRECATED).
///
/// Requests execution of a server-side function by OID.
/// This is a legacy protocol that should be avoided in new code.
/// Use `SELECT function_name(...)` instead.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionCall {
    /// The OID of the function to call.
    pub function_oid: i32,
    /// Format codes for arguments (0=text, 1=binary).
    pub arg_format_codes: Vec<i16>,
    /// The argument values (None = NULL).
    pub arguments: Vec<Option<Vec<u8>>>,
    /// Format code for the result (0=text, 1=binary).
    pub result_format_code: i16,
}

impl FunctionCall {
    /// Create a new FunctionCall message.
    #[deprecated(note = "Use regular query protocol with SELECT instead")]
    pub fn new(function_oid: i32, arg_format_codes: Vec<i16>, arguments: Vec<Option<Vec<u8>>>, result_format_code: i16) -> Self {
        Self {
            function_oid,
            arg_format_codes,
            arguments,
            result_format_code,
        }
    }

    /// Encode the FunctionCall message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(frontend::FUNCTION_CALL).write_i32_be(self.function_oid).write_i16_be(self.arg_format_codes.len() as i16);

        for &code in &self.arg_format_codes {
            builder.write_i16_be(code);
        }

        builder.write_i16_be(self.arguments.len() as i16);

        for arg in &self.arguments {
            match arg {
                Some(data) => {
                    builder.write_i32_be(data.len() as i32);
                    builder.write_bytes(data);
                }
                None => {
                    builder.write_i32_be(-1); // NULL
                }
            }
        }

        builder.write_i16_be(self.result_format_code);
        builder.finish_owned()
    }
}

/// FunctionCallResponse message (backend, DEPRECATED).
///
/// Returns the result of a function call.
/// This is a legacy protocol that should be avoided in new code.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FunctionCallResponse {
    /// The result value (None = NULL).
    pub result: Option<Vec<u8>>,
}

impl FunctionCallResponse {
    /// Create a new FunctionCallResponse with a value.
    pub fn with_value(value: Vec<u8>) -> Self {
        Self { result: Some(value) }
    }

    /// Create a new FunctionCallResponse with NULL.
    pub fn null() -> Self {
        Self { result: None }
    }

    /// Returns true if the result is NULL.
    pub fn is_null(&self) -> bool {
        self.result.is_none()
    }

    /// Encode the FunctionCallResponse message.
    pub fn encode(&self) -> Vec<u8> {
        let mut builder = MessageBuilder::new();
        builder.begin(backend::FUNCTION_CALL_RESPONSE);

        match &self.result {
            Some(data) => {
                builder.write_i32_be(data.len() as i32);
                builder.write_bytes(data);
            }
            None => {
                builder.write_i32_be(-1); // NULL
            }
        }

        builder.finish_owned()
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum FunctionCallError {
    #[error("unexpected message type: expected '{expected}', got '{encountered}'")]
    UnexpectedMessageType { expected: char, encountered: char },
    #[error("invalid encoding")]
    InvalidEncoding,
    #[error("invalid argument count: {0}")]
    InvalidArgumentCount(i16),
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for FunctionCall {
    type ParseError = FunctionCallError;
    type Value<'s>
        = FunctionCall
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != frontend::FUNCTION_CALL {
            return Err(PgParseError::Parse(FunctionCallError::UnexpectedMessageType {
                expected: frontend::FUNCTION_CALL as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let function_oid = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        // Read argument format codes
        let format_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let format_count = format_count.clamp(0, 1024) as usize;
        let mut arg_format_codes = Vec::with_capacity(format_count);
        for _ in 0..format_count {
            arg_format_codes.push(stream.read_i16_be_sync().map_err(PgParseError::Stream)?);
        }

        // Read arguments
        let arg_count = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;
        let arg_count = arg_count.clamp(0, 1024) as usize;
        let mut arguments = Vec::with_capacity(arg_count);
        for _ in 0..arg_count {
            let len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
            if len == -1 {
                arguments.push(None);
            } else if len >= 0 {
                let data = stream.read_bytes_sync(len as usize).map_err(PgParseError::Stream)?;
                arguments.push(Some(data));
            } else {
                arguments.push(None);
            }
        }

        let result_format_code = stream.read_i16_be_sync().map_err(PgParseError::Stream)?;

        Ok(FunctionCall {
            function_oid,
            arg_format_codes,
            arguments,
            result_format_code,
        })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for FunctionCall {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != frontend::FUNCTION_CALL {
            return Err(PgParseError::Parse(FunctionCallError::UnexpectedMessageType {
                expected: frontend::FUNCTION_CALL as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let function_oid = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        // Read argument format codes
        let format_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let format_count = format_count.clamp(0, 1024) as usize;
        let mut arg_format_codes = Vec::with_capacity(format_count);
        for _ in 0..format_count {
            arg_format_codes.push(stream.read_i16_be().await.map_err(PgParseError::Stream)?);
        }

        // Read arguments
        let arg_count = stream.read_i16_be().await.map_err(PgParseError::Stream)?;
        let arg_count = arg_count.clamp(0, 1024) as usize;
        let mut arguments = Vec::with_capacity(arg_count);
        for _ in 0..arg_count {
            let len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
            if len == -1 {
                arguments.push(None);
            } else if len >= 0 {
                let data = stream.read_bytes(len as usize).await.map_err(PgParseError::Stream)?;
                arguments.push(Some(data));
            } else {
                arguments.push(None);
            }
        }

        let result_format_code = stream.read_i16_be().await.map_err(PgParseError::Stream)?;

        Ok(FunctionCall {
            function_oid,
            arg_format_codes,
            arguments,
            result_format_code,
        })
    }
}

impl<S: WireReadSync + ?Sized> PgParseSync<S> for FunctionCallResponse {
    type ParseError = FunctionCallError;
    type Value<'s>
        = FunctionCallResponse
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8_sync().map_err(PgParseError::Stream)?;
        if msg_type != backend::FUNCTION_CALL_RESPONSE {
            return Err(PgParseError::Parse(FunctionCallError::UnexpectedMessageType {
                expected: backend::FUNCTION_CALL_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;
        let result_len = stream.read_i32_be_sync().map_err(PgParseError::Stream)?;

        let result = if result_len == -1 {
            None
        } else if result_len >= 0 {
            Some(stream.read_bytes_sync(result_len as usize).map_err(PgParseError::Stream)?)
        } else {
            None
        };

        Ok(FunctionCallResponse { result })
    }
}

impl<S: WireRead + ?Sized> PgParse<S> for FunctionCallResponse {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, PgParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        let msg_type = stream.read_u8().await.map_err(PgParseError::Stream)?;
        if msg_type != backend::FUNCTION_CALL_RESPONSE {
            return Err(PgParseError::Parse(FunctionCallError::UnexpectedMessageType {
                expected: backend::FUNCTION_CALL_RESPONSE as char,
                encountered: msg_type as char,
            }));
        }

        let _length = stream.read_i32_be().await.map_err(PgParseError::Stream)?;
        let result_len = stream.read_i32_be().await.map_err(PgParseError::Stream)?;

        let result = if result_len == -1 {
            None
        } else if result_len >= 0 {
            Some(stream.read_bytes(result_len as usize).await.map_err(PgParseError::Stream)?)
        } else {
            None
        };

        Ok(FunctionCallResponse { result })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    #[allow(deprecated)]
    fn test_function_call() {
        let call = FunctionCall::new(
            12345,      // function OID
            vec![0, 1], // format codes (text, binary)
            vec![Some(b"arg1".to_vec()), None, Some(b"arg3".to_vec())],
            1, // result format (binary)
        );

        let encoded = call.encode();
        assert_eq!(encoded[0], b'F');

        let stream = SliceStream::new(&encoded);
        let decoded = FunctionCall::parse_sync(&stream).expect("parse failed");

        assert_eq!(decoded.function_oid, 12345);
        assert_eq!(decoded.arg_format_codes, vec![0, 1]);
        assert_eq!(decoded.arguments.len(), 3);
        assert_eq!(decoded.arguments[0], Some(b"arg1".to_vec()));
        assert_eq!(decoded.arguments[1], None);
        assert_eq!(decoded.arguments[2], Some(b"arg3".to_vec()));
        assert_eq!(decoded.result_format_code, 1);
    }

    #[test]
    fn test_function_call_response_with_value() {
        let response = FunctionCallResponse::with_value(b"result".to_vec());

        let encoded = response.encode();
        assert_eq!(encoded[0], b'V');

        let stream = SliceStream::new(&encoded);
        let decoded = FunctionCallResponse::parse_sync(&stream).expect("parse failed");

        assert!(!decoded.is_null());
        assert_eq!(decoded.result, Some(b"result".to_vec()));
    }

    #[test]
    fn test_function_call_response_null() {
        let response = FunctionCallResponse::null();

        let encoded = response.encode();
        assert_eq!(encoded[0], b'V');

        let stream = SliceStream::new(&encoded);
        let decoded = FunctionCallResponse::parse_sync(&stream).expect("parse failed");

        assert!(decoded.is_null());
        assert_eq!(decoded.result, None);
    }
}
