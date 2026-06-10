//! Server Exception packet for ClickHouse native protocol.

use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Server Exception packet (type 2).
///
/// Indicates an error occurred during query processing.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerException {
    /// Error code.
    pub code: i32,
    /// Exception name/class.
    pub name: String,
    /// Error message.
    pub message: String,
    /// Stack trace (may be empty).
    pub stack_trace: String,
    /// Nested exception (if any).
    pub nested: Option<Box<ServerException>>,
}

impl ServerException {
    /// Create a new ServerException.
    pub fn new(code: i32, name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code,
            name: name.into(),
            message: message.into(),
            stack_trace: String::new(),
            nested: None,
        }
    }

    /// Create with stack trace.
    pub fn with_stack_trace(code: i32, name: impl Into<String>, message: impl Into<String>, stack_trace: impl Into<String>) -> Self {
        Self {
            code,
            name: name.into(),
            message: message.into(),
            stack_trace: stack_trace.into(),
            nested: None,
        }
    }

    /// Parse a ServerException from a synchronous stream.
    ///
    /// Note: This does NOT read the packet type byte.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        let code = stream.read_i32_le_ch_sync()?;
        let name = stream.read_ch_string_utf8_sync()?;
        let message = stream.read_ch_string_utf8_sync()?;
        let stack_trace = stream.read_ch_string_utf8_sync()?;

        let has_nested = stream.read_bool_ch_sync()?;
        let nested = if has_nested {
            Some(Box::new(Self::parse_sync(stream)?))
        } else {
            None
        };

        Ok(Self { code, name, message, stack_trace, nested })
    }

    /// Parse a ServerException asynchronously.
    ///
    /// Note: For nested exceptions, this uses an iterative approach to avoid
    /// issues with recursive async functions.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        // Parse all exceptions iteratively
        let mut exceptions = Vec::new();

        loop {
            let code = stream.read_i32_le_ch().await?;
            let name = stream.read_ch_string_utf8().await?;
            let message = stream.read_ch_string_utf8().await?;
            let stack_trace = stream.read_ch_string_utf8().await?;
            let has_nested = stream.read_bool_ch().await?;

            exceptions.push((code, name, message, stack_trace));

            if !has_nested {
                break;
            }
        }

        // Build the exception chain from innermost to outermost
        let mut result: Option<ServerException> = None;
        for (code, name, message, stack_trace) in exceptions.into_iter().rev() {
            result = Some(ServerException {
                code,
                name,
                message,
                stack_trace,
                nested: result.map(Box::new),
            });
        }

        result.ok_or_else(|| ClickhouseWireError::InvalidBlock("empty exception".to_string()))
    }

    /// Encode the ServerException.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_i32_le_ch(self.code)?;
        w.write_ch_string_utf8(&self.name)?;
        w.write_ch_string_utf8(&self.message)?;
        w.write_ch_string_utf8(&self.stack_trace)?;
        w.write_bool_ch(self.nested.is_some())?;

        if let Some(nested) = &self.nested {
            nested.encode(w)?;
        }

        Ok(())
    }

    /// Convert to a ClickhouseWireError.
    pub fn into_error(self) -> ClickhouseWireError {
        ClickhouseWireError::ServerException {
            code: self.code,
            name: self.name,
            message: self.message,
            stack_trace: self.stack_trace,
            nested: self.nested.map(|n| Box::new(n.into_error())),
        }
    }

    /// Get a formatted error message.
    pub fn display_message(&self) -> String {
        if self.name.is_empty() {
            format!("Code: {}. {}", self.code, self.message)
        } else {
            format!("Code: {}. {}: {}", self.code, self.name, self.message)
        }
    }
}

impl std::fmt::Display for ServerException {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_message())?;
        if !self.stack_trace.is_empty() {
            write!(f, "\n{}", self.stack_trace)?;
        }
        if let Some(nested) = &self.nested {
            write!(f, "\nCaused by: {}", nested)?;
        }
        Ok(())
    }
}

impl std::error::Error for ServerException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.nested.as_ref().map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_server_exception_simple() {
        let exc = ServerException::new(62, "DB::Exception", "Table not found");

        let mut buf = Vec::new();
        exc.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = ServerException::parse_sync(&stream).unwrap();

        assert_eq!(decoded.code, exc.code);
        assert_eq!(decoded.name, exc.name);
        assert_eq!(decoded.message, exc.message);
        assert!(decoded.nested.is_none());
    }

    #[test]
    fn test_server_exception_nested() {
        let inner = ServerException::new(1, "Inner", "Inner error");
        let outer = ServerException {
            code: 2,
            name: "Outer".to_string(),
            message: "Outer error".to_string(),
            stack_trace: "at line 1".to_string(),
            nested: Some(Box::new(inner)),
        };

        let mut buf = Vec::new();
        outer.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let decoded = ServerException::parse_sync(&stream).unwrap();

        assert_eq!(decoded.code, 2);
        assert!(decoded.nested.is_some());
        let nested = decoded.nested.unwrap();
        assert_eq!(nested.code, 1);
        assert_eq!(nested.name, "Inner");
    }

    #[test]
    fn test_display_message() {
        let exc = ServerException::new(62, "DB::Exception", "Table not found");
        assert_eq!(exc.display_message(), "Code: 62. DB::Exception: Table not found");

        let exc2 = ServerException::new(1, "", "Unknown error");
        assert_eq!(exc2.display_message(), "Code: 1. Unknown error");
    }
}
