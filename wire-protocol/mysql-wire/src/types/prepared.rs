//! MySQL prepared statement packets.
//!
//! Prepared statements use the binary protocol for efficient execution.

use crate::mysql_ext::MysqlReadSync;
use crate::parse::{MysqlParse, MysqlParseError, MysqlParseSync};
use crate::types::column_definition::ColumnDefinition;
use wire_stream::{WireRead, WireReadSync};

/// COM_STMT_PREPARE response (OK).
///
/// Sent by the server after a successful COM_STMT_PREPARE.
#[derive(Clone, Debug)]
pub struct StmtPrepareOk {
    /// Statement ID (used in subsequent COM_STMT_EXECUTE).
    pub statement_id: u32,
    /// Number of columns in the result set.
    pub num_columns: u16,
    /// Number of parameters.
    pub num_params: u16,
    /// Number of warnings.
    pub warnings: u16,
    /// Parameter definitions (if num_params > 0).
    pub params: Vec<ColumnDefinition>,
    /// Column definitions (if num_columns > 0).
    pub columns: Vec<ColumnDefinition>,
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum StmtPrepareOkError {
    #[error("invalid statement prepare OK header")]
    InvalidHeader,
    #[error("parameter definition error: {0}")]
    ParamError(String),
    #[error("column definition error: {0}")]
    ColumnError(String),
}

impl<S: WireReadSync + ?Sized> MysqlParseSync<S> for StmtPrepareOk {
    type ParseError = StmtPrepareOkError;
    type Value<'s>
        = StmtPrepareOk
    where
        S: 's;

    fn parse_sync<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        // Status (1 byte, always 0x00 for OK)
        let status = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        if status != 0x00 {
            return Err(MysqlParseError::Parse(StmtPrepareOkError::InvalidHeader));
        }

        // Statement ID (4 bytes)
        let statement_id = stream.read_u32_le_sync().map_err(MysqlParseError::Stream)?;

        // Number of columns (2 bytes)
        let num_columns = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Number of parameters (2 bytes)
        let num_params = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Reserved (1 byte)
        let _ = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;

        // Warnings (2 bytes)
        let warnings = stream.read_u16_le_sync().map_err(MysqlParseError::Stream)?;

        // Parameter definitions would follow (num_params packets)
        // Column definitions would follow (num_columns packets)
        // For now, we don't parse these as they require multiple packet reads
        let params = Vec::new();
        let columns = Vec::new();

        Ok(StmtPrepareOk {
            statement_id,
            num_columns,
            num_params,
            warnings,
            params,
            columns,
        })
    }
}

impl<S: WireRead + ?Sized> MysqlParse<S> for StmtPrepareOk {
    async fn parse<'s>(stream: &'s S) -> Result<Self::Value<'s>, MysqlParseError<S::ReadError, Self::ParseError>>
    where
        S: 's,
    {
        Self::parse_sync(stream)
    }
}

/// COM_STMT_EXECUTE packet builder.
///
/// Used to execute a prepared statement.
#[derive(Clone, Debug)]
pub struct StmtExecute {
    /// Statement ID from COM_STMT_PREPARE_OK.
    pub statement_id: u32,
    /// Cursor flags.
    pub flags: StmtExecuteFlags,
    /// Iteration count (always 1).
    pub iteration_count: u32,
    /// Parameter values.
    pub params: Vec<StmtParam>,
}

/// Cursor flags for COM_STMT_EXECUTE.
#[derive(Clone, Copy, Debug, Default)]
pub struct StmtExecuteFlags(pub u8);

impl StmtExecuteFlags {
    /// No cursor.
    pub const CURSOR_TYPE_NO_CURSOR: u8 = 0x00;
    /// Read-only cursor.
    pub const CURSOR_TYPE_READ_ONLY: u8 = 0x01;
    /// Cursor for update.
    pub const CURSOR_TYPE_FOR_UPDATE: u8 = 0x02;
    /// Scrollable cursor.
    pub const CURSOR_TYPE_SCROLLABLE: u8 = 0x04;

    pub fn new() -> Self {
        Self(Self::CURSOR_TYPE_NO_CURSOR)
    }

    pub fn read_only() -> Self {
        Self(Self::CURSOR_TYPE_READ_ONLY)
    }
}

/// Parameter value for prepared statement execution.
#[derive(Clone, Debug)]
pub struct StmtParam {
    /// Parameter type.
    pub param_type: u8,
    /// Parameter value (None for NULL).
    pub value: Option<Vec<u8>>,
    /// Is unsigned.
    pub unsigned: bool,
}

impl StmtParam {
    /// Create a NULL parameter.
    pub fn null(param_type: u8) -> Self {
        Self { param_type, value: None, unsigned: false }
    }

    /// Create a string parameter.
    pub fn string(value: &str) -> Self {
        Self {
            param_type: crate::error::column_types::MYSQL_TYPE_VAR_STRING,
            value: Some(value.as_bytes().to_vec()),
            unsigned: false,
        }
    }

    /// Create an i64 parameter.
    pub fn i64(value: i64) -> Self {
        Self {
            param_type: crate::error::column_types::MYSQL_TYPE_LONGLONG,
            value: Some(value.to_le_bytes().to_vec()),
            unsigned: false,
        }
    }

    /// Create a u64 parameter.
    pub fn u64(value: u64) -> Self {
        Self {
            param_type: crate::error::column_types::MYSQL_TYPE_LONGLONG,
            value: Some(value.to_le_bytes().to_vec()),
            unsigned: true,
        }
    }

    /// Create an f64 parameter.
    pub fn f64(value: f64) -> Self {
        Self {
            param_type: crate::error::column_types::MYSQL_TYPE_DOUBLE,
            value: Some(value.to_le_bytes().to_vec()),
            unsigned: false,
        }
    }
}

impl StmtExecute {
    /// Create a new COM_STMT_EXECUTE.
    pub fn new(statement_id: u32) -> Self {
        Self {
            statement_id,
            flags: StmtExecuteFlags::new(),
            iteration_count: 1,
            params: Vec::new(),
        }
    }

    /// Set cursor flags.
    pub fn with_flags(mut self, flags: StmtExecuteFlags) -> Self {
        self.flags = flags;
        self
    }

    /// Add a parameter.
    pub fn with_param(mut self, param: StmtParam) -> Self {
        self.params.push(param);
        self
    }

    /// Encode to bytes (payload only, no packet header).
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Command type
        buf.push(crate::error::commands::COM_STMT_EXECUTE);

        // Statement ID
        buf.extend_from_slice(&self.statement_id.to_le_bytes());

        // Flags
        buf.push(self.flags.0);

        // Iteration count (always 1)
        buf.extend_from_slice(&self.iteration_count.to_le_bytes());

        if !self.params.is_empty() {
            // NULL bitmap
            let null_bitmap_len = self.params.len().div_ceil(8);
            let mut null_bitmap = vec![0u8; null_bitmap_len];
            for (i, param) in self.params.iter().enumerate() {
                if param.value.is_none() {
                    null_bitmap[i / 8] |= 1 << (i % 8);
                }
            }
            buf.extend_from_slice(&null_bitmap);

            // New params bound flag (1 = send types)
            buf.push(0x01);

            // Parameter types
            for param in &self.params {
                buf.push(param.param_type);
                buf.push(if param.unsigned { 0x80 } else { 0x00 });
            }

            // Parameter values
            for param in &self.params {
                if let Some(ref value) = param.value {
                    // For strings, need length prefix
                    if param.param_type == crate::error::column_types::MYSQL_TYPE_VAR_STRING
                        || param.param_type == crate::error::column_types::MYSQL_TYPE_STRING
                        || param.param_type == crate::error::column_types::MYSQL_TYPE_BLOB
                    {
                        crate::write::write_lenenc_string(&mut buf, value).expect("Vec write should not fail");
                    } else {
                        buf.extend_from_slice(value);
                    }
                }
            }
        }

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_stmt_prepare_ok() {
        let data = [
            0x00, // Status OK
            0x01, 0x00, 0x00, 0x00, // Statement ID = 1
            0x02, 0x00, // 2 columns
            0x01, 0x00, // 1 parameter
            0x00, // Reserved
            0x00, 0x00, // 0 warnings
        ];
        let stream = SliceStream::new(&data);

        let ok = StmtPrepareOk::parse_sync(&stream).unwrap();

        assert_eq!(ok.statement_id, 1);
        assert_eq!(ok.num_columns, 2);
        assert_eq!(ok.num_params, 1);
        assert_eq!(ok.warnings, 0);
    }

    #[test]
    fn test_stmt_execute_encode() {
        let execute = StmtExecute::new(1).with_param(StmtParam::string("hello")).with_param(StmtParam::i64(42));

        let encoded = execute.encode();

        // Should start with COM_STMT_EXECUTE
        assert_eq!(encoded[0], crate::error::commands::COM_STMT_EXECUTE);

        // Statement ID
        assert_eq!(&encoded[1..5], &1u32.to_le_bytes());
    }

    #[test]
    fn test_stmt_param_types() {
        let null = StmtParam::null(0);
        assert!(null.value.is_none());

        let s = StmtParam::string("test");
        assert_eq!(s.value, Some(b"test".to_vec()));

        let i = StmtParam::i64(-42);
        assert_eq!(i.value, Some((-42i64).to_le_bytes().to_vec()));

        let u = StmtParam::u64(42);
        assert!(u.unsigned);
    }
}
