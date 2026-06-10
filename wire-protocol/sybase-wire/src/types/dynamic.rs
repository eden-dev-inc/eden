//! Dynamic SQL types for TDS 5.0.
//!
//! TDS 5.0 supports dynamic (prepared) SQL statements.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::{PacketBuilder, write_u16_le, write_varchar};
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Dynamic SQL operation types.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum DynamicOperation {
    /// Prepare a statement.
    Prepare = 0x01,
    /// Execute a prepared statement.
    Execute = 0x02,
    /// Deallocate a prepared statement.
    Dealloc = 0x04,
    /// Prepare and execute in one step.
    PrepExec = 0x03,
    /// Describe input parameters.
    DescIn = 0x08,
    /// Describe output columns.
    DescOut = 0x10,
}

impl DynamicOperation {
    /// Try to create from a raw byte value.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x01 => Some(Self::Prepare),
            0x02 => Some(Self::Execute),
            0x04 => Some(Self::Dealloc),
            0x03 => Some(Self::PrepExec),
            0x08 => Some(Self::DescIn),
            0x10 => Some(Self::DescOut),
            _ => None,
        }
    }
}

/// Dynamic SQL statement.
#[derive(Clone, Debug)]
pub struct Dynamic {
    /// Operation type.
    pub operation: u8,
    /// Status flags.
    pub status: u8,
    /// Statement ID (for prepared statements).
    pub id: String,
    /// SQL statement text (for prepare operations).
    pub statement: Option<String>,
}

impl Dynamic {
    /// Parse a DYNAMIC token.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Dynamic, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let _length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Operation (1 byte)
        let operation = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Status (1 byte)
        let status = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Statement ID length and value
        let id_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let id = if id_len > 0 {
            let borrow = stream.peek(Some(id_len)).map_err(SybaseParseError::Stream)?;
            let s = String::from_utf8_lossy(&borrow[..id_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            s
        } else {
            String::new()
        };

        // Statement text (if prepare operation)
        let statement = if operation & 0x01 != 0 {
            // Has prepare component
            let stmt_len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
            if stmt_len > 0 {
                let borrow = stream.peek(Some(stmt_len)).map_err(SybaseParseError::Stream)?;
                let s = String::from_utf8_lossy(&borrow[..stmt_len]).into_owned();
                stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
                Some(s)
            } else {
                None
            }
        } else {
            None
        };

        Ok(Dynamic { operation, status, id, statement })
    }

    /// Get the operation type as an enum.
    pub fn operation_type(&self) -> Option<DynamicOperation> {
        DynamicOperation::from_u8(self.operation)
    }
}

/// Builder for dynamic SQL packets.
pub struct DynamicBuilder {
    operation: u8,
    status: u8,
    id: String,
    statement: Option<String>,
}

impl DynamicBuilder {
    /// Create a prepare request.
    pub fn prepare(id: impl Into<String>, statement: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::Prepare as u8,
            status: 0,
            id: id.into(),
            statement: Some(statement.into()),
        }
    }

    /// Create an execute request.
    pub fn execute(id: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::Execute as u8,
            status: 0,
            id: id.into(),
            statement: None,
        }
    }

    /// Create a prepare-and-execute request.
    pub fn prep_exec(id: impl Into<String>, statement: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::PrepExec as u8,
            status: 0,
            id: id.into(),
            statement: Some(statement.into()),
        }
    }

    /// Create a deallocate request.
    pub fn dealloc(id: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::Dealloc as u8,
            status: 0,
            id: id.into(),
            statement: None,
        }
    }

    /// Create a describe-input request.
    pub fn describe_in(id: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::DescIn as u8,
            status: 0,
            id: id.into(),
            statement: None,
        }
    }

    /// Create a describe-output request.
    pub fn describe_out(id: impl Into<String>) -> Self {
        Self {
            operation: DynamicOperation::DescOut as u8,
            status: 0,
            id: id.into(),
            statement: None,
        }
    }

    /// Build the dynamic SQL packet.
    pub fn build(self) -> Vec<u8> {
        let mut data = Vec::new();

        // Operation
        data.push(self.operation);

        // Status
        data.push(self.status);

        // Statement ID
        write_varchar(&mut data, self.id.as_bytes());

        // Statement text (if present)
        if let Some(stmt) = self.statement {
            let stmt_bytes = stmt.as_bytes();
            write_u16_le(&mut data, stmt_bytes.len() as u16);
            data.extend_from_slice(stmt_bytes);
        }

        PacketBuilder::new(PacketType::Query5).write_bytes(&data).build()
    }
}
