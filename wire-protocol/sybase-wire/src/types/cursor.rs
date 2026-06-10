//! TDS cursor tokens.
//!
//! These tokens are used for server-side cursor operations in TDS 5.0.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::{PacketBuilder, write_u16_le, write_u32_le, write_varchar};
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Cursor command types.
pub mod cursor_cmd {
    /// Declare a cursor.
    pub const CURSOR_DECLARE: u8 = 0x01;
    /// Open a cursor.
    pub const CURSOR_OPEN: u8 = 0x02;
    /// Fetch rows.
    pub const CURSOR_FETCH: u8 = 0x03;
    /// Close a cursor.
    pub const CURSOR_CLOSE: u8 = 0x04;
    /// Deallocate a cursor.
    pub const CURSOR_DEALLOC: u8 = 0x05;
    /// Update current row.
    pub const CURSOR_UPDATE: u8 = 0x06;
    /// Delete current row.
    pub const CURSOR_DELETE: u8 = 0x07;
    /// Get cursor info.
    pub const CURSOR_INFO: u8 = 0x08;
}

/// Cursor option flags.
pub mod cursor_options {
    /// Cursor is read-only.
    pub const CUR_READONLY: u16 = 0x0001;
    /// Cursor is updatable.
    pub const CUR_UPDATABLE: u16 = 0x0002;
    /// Cursor is sensitive to changes.
    pub const CUR_SENSITIVE: u16 = 0x0004;
    /// Cursor is insensitive to changes.
    pub const CUR_INSENSITIVE: u16 = 0x0008;
    /// Cursor supports scrolling.
    pub const CUR_SCROLL: u16 = 0x0010;
    /// Cursor is dynamic.
    pub const CUR_DYNAMIC: u16 = 0x0020;
    /// Implicit cursor.
    pub const CUR_IMPLICIT: u16 = 0x0040;
}

/// Cursor fetch types.
pub mod fetch_type {
    /// Fetch next row.
    pub const FETCH_NEXT: u8 = 0x01;
    /// Fetch previous row.
    pub const FETCH_PREV: u8 = 0x02;
    /// Fetch first row.
    pub const FETCH_FIRST: u8 = 0x03;
    /// Fetch last row.
    pub const FETCH_LAST: u8 = 0x04;
    /// Fetch absolute position.
    pub const FETCH_ABSOLUTE: u8 = 0x05;
    /// Fetch relative position.
    pub const FETCH_RELATIVE: u8 = 0x06;
}

/// Cursor status flags (returned in CURINFO).
pub mod cursor_status {
    /// Cursor is declared.
    pub const CUR_DECLARED: u16 = 0x0001;
    /// Cursor is open.
    pub const CUR_OPEN: u16 = 0x0002;
    /// Cursor is closed.
    pub const CUR_CLOSED: u16 = 0x0004;
    /// Row fetch succeeded.
    pub const CUR_ROWFETCH: u16 = 0x0008;
    /// At end of result set.
    pub const CUR_ATEND: u16 = 0x0010;
    /// At beginning of result set.
    pub const CUR_ATSTART: u16 = 0x0020;
}

/// CURDECLARE token - declare a server-side cursor.
#[derive(Clone, Debug)]
pub struct CurDeclare {
    /// Token length.
    pub length: u16,
    /// Cursor name.
    pub cursor_name: String,
    /// Cursor options.
    pub options: u16,
    /// SQL statement.
    pub statement: String,
}

impl CurDeclare {
    /// Create a new cursor declaration.
    pub fn new(name: impl Into<String>, statement: impl Into<String>) -> Self {
        Self {
            length: 0,
            cursor_name: name.into(),
            options: 0,
            statement: statement.into(),
        }
    }

    /// Set cursor options.
    pub fn with_options(mut self, options: u16) -> Self {
        self.options = options;
        self
    }

    /// Parse a CURDECLARE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(
        stream: &'s SliceStream<'s>,
    ) -> Result<CurDeclare, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        // Options (2 bytes)
        let options = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Statement length (2 bytes)
        let stmt_len = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)? as usize;
        let statement = if stmt_len > 0 {
            let borrow = stream.peek(Some(stmt_len)).map_err(SybaseParseError::Stream)?;
            let s = String::from_utf8_lossy(&borrow[..stmt_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            s
        } else {
            String::new()
        };

        Ok(CurDeclare { length, cursor_name, options, statement })
    }
}

/// CURINFO token - cursor status information.
#[derive(Clone, Debug)]
pub struct CurInfo {
    /// Token length.
    pub length: u16,
    /// Cursor ID.
    pub cursor_id: u32,
    /// Cursor name.
    pub cursor_name: String,
    /// Cursor status.
    pub status: u16,
    /// Number of columns.
    pub column_count: u16,
    /// Total row count (if known).
    pub row_count: u32,
}

impl CurInfo {
    /// Parse a CURINFO token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<CurInfo, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor ID (4 bytes)
        let cursor_id = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        // Status (2 bytes)
        let status = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Column count (2 bytes)
        let column_count = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Row count (4 bytes)
        let row_count = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(CurInfo {
            length,
            cursor_id,
            cursor_name,
            status,
            column_count,
            row_count,
        })
    }

    /// Check if cursor is open.
    pub fn is_open(&self) -> bool {
        self.status & cursor_status::CUR_OPEN != 0
    }

    /// Check if cursor is at end.
    pub fn is_at_end(&self) -> bool {
        self.status & cursor_status::CUR_ATEND != 0
    }
}

/// CURCLOSE token - close cursor response.
#[derive(Clone, Debug)]
pub struct CurClose {
    /// Token length.
    pub length: u16,
    /// Cursor ID.
    pub cursor_id: u32,
    /// Cursor name.
    pub cursor_name: String,
}

impl CurClose {
    /// Parse a CURCLOSE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<CurClose, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor ID (4 bytes)
        let cursor_id = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        Ok(CurClose { length, cursor_id, cursor_name })
    }
}

/// CURFETCH token - cursor fetch response.
#[derive(Clone, Debug)]
pub struct CurFetch {
    /// Token length.
    pub length: u16,
    /// Cursor ID.
    pub cursor_id: u32,
    /// Cursor name.
    pub cursor_name: String,
    /// Fetch type.
    pub fetch_type: u8,
    /// Row count.
    pub row_count: u32,
}

impl CurFetch {
    /// Parse a CURFETCH token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<CurFetch, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor ID (4 bytes)
        let cursor_id = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        // Fetch type (1 byte)
        let fetch_type = stream.read_u8_sync().map_err(SybaseParseError::Stream)?;

        // Row count (4 bytes)
        let row_count = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        Ok(CurFetch { length, cursor_id, cursor_name, fetch_type, row_count })
    }
}

/// CURDELETE token - cursor positioned delete response.
#[derive(Clone, Debug)]
pub struct CurDelete {
    /// Token length.
    pub length: u16,
    /// Cursor ID.
    pub cursor_id: u32,
    /// Cursor name.
    pub cursor_name: String,
}

impl CurDelete {
    /// Parse a CURDELETE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<CurDelete, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor ID (4 bytes)
        let cursor_id = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        Ok(CurDelete { length, cursor_id, cursor_name })
    }
}

/// CURUPDATE token - cursor positioned update response.
#[derive(Clone, Debug)]
pub struct CurUpdate {
    /// Token length.
    pub length: u16,
    /// Cursor ID.
    pub cursor_id: u32,
    /// Cursor name.
    pub cursor_name: String,
}

impl CurUpdate {
    /// Parse a CURUPDATE token after the token type byte has been read.
    pub fn parse_after_token_sync<'s>(stream: &'s SliceStream<'s>) -> Result<CurUpdate, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Length (2 bytes)
        let length = stream.read_u16_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor ID (4 bytes)
        let cursor_id = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Cursor name
        let name_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;
        let cursor_name = if name_len > 0 {
            let borrow = stream.peek(Some(name_len)).map_err(SybaseParseError::Stream)?;
            let n = String::from_utf8_lossy(&borrow[..name_len]).into_owned();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            n
        } else {
            String::new()
        };

        Ok(CurUpdate { length, cursor_id, cursor_name })
    }
}

/// Builder for cursor packets.
pub struct CursorBuilder {
    command: u8,
    cursor_name: String,
    options: u16,
    fetch_type: u8,
    row_count: u32,
    row_offset: i32,
    statement: Option<String>,
}

impl CursorBuilder {
    /// Create a cursor declare builder.
    pub fn declare(name: impl Into<String>, statement: impl Into<String>) -> Self {
        Self {
            command: cursor_cmd::CURSOR_DECLARE,
            cursor_name: name.into(),
            options: 0,
            fetch_type: 0,
            row_count: 0,
            row_offset: 0,
            statement: Some(statement.into()),
        }
    }

    /// Create a cursor open builder.
    pub fn open(name: impl Into<String>) -> Self {
        Self {
            command: cursor_cmd::CURSOR_OPEN,
            cursor_name: name.into(),
            options: 0,
            fetch_type: 0,
            row_count: 0,
            row_offset: 0,
            statement: None,
        }
    }

    /// Create a cursor fetch builder.
    pub fn fetch(name: impl Into<String>, fetch_type: u8) -> Self {
        Self {
            command: cursor_cmd::CURSOR_FETCH,
            cursor_name: name.into(),
            options: 0,
            fetch_type,
            row_count: 1,
            row_offset: 0,
            statement: None,
        }
    }

    /// Create a cursor close builder.
    pub fn close(name: impl Into<String>) -> Self {
        Self {
            command: cursor_cmd::CURSOR_CLOSE,
            cursor_name: name.into(),
            options: 0,
            fetch_type: 0,
            row_count: 0,
            row_offset: 0,
            statement: None,
        }
    }

    /// Create a cursor deallocate builder.
    pub fn deallocate(name: impl Into<String>) -> Self {
        Self {
            command: cursor_cmd::CURSOR_DEALLOC,
            cursor_name: name.into(),
            options: 0,
            fetch_type: 0,
            row_count: 0,
            row_offset: 0,
            statement: None,
        }
    }

    /// Set cursor options.
    pub fn with_options(mut self, options: u16) -> Self {
        self.options = options;
        self
    }

    /// Set number of rows to fetch.
    pub fn with_row_count(mut self, count: u32) -> Self {
        self.row_count = count;
        self
    }

    /// Set row offset for absolute/relative fetch.
    pub fn with_row_offset(mut self, offset: i32) -> Self {
        self.row_offset = offset;
        self
    }

    /// Build the cursor packet.
    pub fn build(self) -> Vec<u8> {
        let mut data = Vec::new();

        // Command type
        data.push(self.command);

        // Cursor name
        write_varchar(&mut data, self.cursor_name.as_bytes());

        // Options (for declare/open)
        if self.command == cursor_cmd::CURSOR_DECLARE || self.command == cursor_cmd::CURSOR_OPEN {
            write_u16_le(&mut data, self.options);
        }

        // Statement (for declare)
        if let Some(stmt) = self.statement {
            let stmt_bytes = stmt.as_bytes();
            write_u16_le(&mut data, stmt_bytes.len() as u16);
            data.extend_from_slice(stmt_bytes);
        }

        // Fetch type and row info (for fetch)
        if self.command == cursor_cmd::CURSOR_FETCH {
            data.push(self.fetch_type);
            write_u32_le(&mut data, self.row_count);
            if self.fetch_type == fetch_type::FETCH_ABSOLUTE || self.fetch_type == fetch_type::FETCH_RELATIVE {
                write_u32_le(&mut data, self.row_offset as u32);
            }
        }

        PacketBuilder::new(PacketType::Rpc).write_bytes(&data).build()
    }
}
