//! TTI function codes.
//!
//! These codes identify the operation being performed in a TTI message.
//! The function code is the first byte of the Data packet payload.
//!
//! ## Ambiguous Codes
//!
//! Some function codes have the same numeric value but different meanings
//! depending on context:
//!
//! - **0x05**: `Rollback` (request) or `Fetch` (request after cursor open)
//! - **0x06**: `OpenCursor` (request) or `RowData` (response)
//! - **0x08**: `CloseCursor` (request) or `EndOfData` (response)
//!
//! Use `from_u8_request()` or `from_u8_response()` for context-aware parsing.

/// TTI function code.
///
/// Note: Some function codes have the same numeric value but different meanings
/// depending on context (request vs response). Use `from_u8_request()` or
/// `from_u8_response()` for context-aware parsing, or `is_request()` and
/// `is_response()` to determine the context of an already-parsed code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FunctionCode {
    // Session and connection management
    /// Protocol negotiation (first message after connect).
    ProtocolNegotiation,
    /// Data type negotiation.
    DataTypeNegotiation,
    /// Transaction begin.
    TransactionBegin,
    /// Transaction commit.
    Commit,
    /// Transaction rollback.
    Rollback,
    /// Open cursor.
    OpenCursor,
    /// Close cursor.
    CloseCursor,
    /// User authentication (O5LOGON for older, O7LOGON for newer).
    Authentication,
    /// Version exchange.
    Version,

    // Query and execution operations
    /// Execute SQL statement.
    Execute,
    /// Fetch rows from cursor.
    Fetch,
    /// Describe columns (get metadata).
    Describe,
    /// Parse SQL statement.
    Parse,
    /// Execute and fetch combined.
    ExecuteAndFetch,

    // LOB operations (TNS v10+)
    /// LOB read operation.
    LobRead,
    /// LOB write operation.
    LobWrite,
    /// LOB get length.
    LobGetLength,
    /// LOB trim.
    LobTrim,
    /// LOB erase.
    LobErase,
    /// LOB open.
    LobOpen,
    /// LOB close.
    LobClose,
    /// LOB is open.
    LobIsOpen,
    /// LOB is temporary.
    LobIsTemp,
    /// LOB get chunk size.
    LobGetChunkSize,
    /// LOB create temporary.
    LobCreateTemp,
    /// LOB free temporary.
    LobFreeTemp,
    /// LOB copy.
    LobCopy,
    /// LOB append.
    LobAppend,
    /// LOB load from file.
    LobLoadFromFile,
    /// LOB get charset ID.
    LobGetCharsetId,

    // Session piggyback operations (TNS v11+)
    /// Session state piggyback.
    SessionStatePiggyback,
    /// DRCP release (TNS v11+).
    DrpcRelease,

    // Extended operations
    /// Batch execute.
    BatchExecute,
    /// Get server info.
    GetServerInfo,

    // Scrollable cursor operations (Oracle 8i+)
    /// Scrollable cursor fetch.
    ScrollFetch,

    // Statement cache operations
    /// Set statement tag for caching.
    SetStatementTag,
    /// Get statement by tag from cache.
    GetStatementByTag,

    // Response codes
    /// Success response.
    Success,
    /// Error response.
    Error,
    /// Warning response.
    Warning,
    /// Row data response.
    RowData,
    /// End of data response.
    EndOfData,

    /// Unknown function code.
    Unknown(u8),
}

impl FunctionCode {
    /// Create a FunctionCode from a raw byte.
    ///
    /// For ambiguous codes (0x05, 0x06, 0x08), this returns the request interpretation.
    /// Use `from_u8_response()` for response context parsing.
    pub fn from_u8(value: u8) -> Self {
        match value {
            0x01 => Self::ProtocolNegotiation,
            0x02 => Self::DataTypeNegotiation,
            0x03 => Self::TransactionBegin,
            0x04 => Self::Commit,
            0x05 => Self::Rollback,
            0x06 => Self::OpenCursor,
            0x08 => Self::CloseCursor,
            0x09 => Self::Authentication,
            0x0B => Self::Version,
            0x0E => Self::Execute,
            0x10 => Self::Describe,
            0x11 => Self::Parse,
            0x12 => Self::ExecuteAndFetch,
            0x60 => Self::LobRead,
            0x61 => Self::LobWrite,
            0x62 => Self::LobGetLength,
            0x63 => Self::LobTrim,
            0x64 => Self::LobErase,
            0x65 => Self::LobOpen,
            0x66 => Self::LobClose,
            0x67 => Self::LobIsOpen,
            0x68 => Self::LobIsTemp,
            0x69 => Self::LobGetChunkSize,
            0x6A => Self::LobCreateTemp,
            0x6D => Self::LobFreeTemp,
            0x6E => Self::LobCopy,
            0x6F => Self::LobAppend,
            0x73 => Self::LobLoadFromFile,
            0x74 => Self::LobGetCharsetId,
            0x6B => Self::SessionStatePiggyback,
            0x6C => Self::DrpcRelease,
            0x70 => Self::BatchExecute,
            0x76 => Self::GetServerInfo,
            0x77 => Self::ScrollFetch,
            0x78 => Self::SetStatementTag,
            0x79 => Self::GetStatementByTag,
            other => Self::Unknown(other),
        }
    }

    /// Create a FunctionCode from a raw byte in request context.
    ///
    /// This handles ambiguous codes correctly for client-to-server messages:
    /// - 0x05 -> Rollback (or Fetch if `is_fetch_context` is true)
    /// - 0x06 -> OpenCursor
    /// - 0x08 -> CloseCursor
    pub fn from_u8_request(value: u8, is_fetch_context: bool) -> Self {
        match value {
            0x05 if is_fetch_context => Self::Fetch,
            _ => Self::from_u8(value),
        }
    }

    /// Create a FunctionCode from a raw byte in response context.
    ///
    /// This handles ambiguous codes correctly for server-to-client messages:
    /// - 0x00 -> Success
    /// - 0x04 -> Error
    /// - 0x05 -> Warning
    /// - 0x06 -> RowData
    /// - 0x08 -> EndOfData
    pub fn from_u8_response(value: u8) -> Self {
        match value {
            0x00 => Self::Success,
            0x04 => Self::Error,
            0x05 => Self::Warning,
            0x06 => Self::RowData,
            0x08 => Self::EndOfData,
            _ => Self::from_u8(value),
        }
    }

    /// Convert to raw byte value.
    pub fn as_u8(&self) -> u8 {
        match self {
            Self::ProtocolNegotiation => 0x01,
            Self::DataTypeNegotiation => 0x02,
            Self::TransactionBegin => 0x03,
            Self::Commit => 0x04,
            Self::Rollback => 0x05,
            Self::OpenCursor => 0x06,
            Self::CloseCursor => 0x08,
            Self::Authentication => 0x09,
            Self::Version => 0x0B,
            Self::Execute => 0x0E,
            Self::Fetch => 0x05,
            Self::Describe => 0x10,
            Self::Parse => 0x11,
            Self::ExecuteAndFetch => 0x12,
            Self::LobRead => 0x60,
            Self::LobWrite => 0x61,
            Self::LobGetLength => 0x62,
            Self::LobTrim => 0x63,
            Self::LobErase => 0x64,
            Self::LobOpen => 0x65,
            Self::LobClose => 0x66,
            Self::LobIsOpen => 0x67,
            Self::LobIsTemp => 0x68,
            Self::LobGetChunkSize => 0x69,
            Self::LobCreateTemp => 0x6A,
            Self::LobFreeTemp => 0x6D,
            Self::LobCopy => 0x6E,
            Self::LobAppend => 0x6F,
            Self::LobLoadFromFile => 0x73,
            Self::LobGetCharsetId => 0x74,
            Self::SessionStatePiggyback => 0x6B,
            Self::DrpcRelease => 0x6C,
            Self::BatchExecute => 0x70,
            Self::GetServerInfo => 0x76,
            Self::ScrollFetch => 0x77,
            Self::SetStatementTag => 0x78,
            Self::GetStatementByTag => 0x79,
            Self::Success => 0x00,
            Self::Error => 0x04,
            Self::Warning => 0x05,
            Self::RowData => 0x06,
            Self::EndOfData => 0x08,
            Self::Unknown(v) => *v,
        }
    }

    /// Returns the human-readable name of this function.
    pub fn name(&self) -> &'static str {
        match self {
            Self::ProtocolNegotiation => "ProtocolNegotiation",
            Self::DataTypeNegotiation => "DataTypeNegotiation",
            Self::TransactionBegin => "TransactionBegin",
            Self::Commit => "Commit",
            Self::Rollback => "Rollback",
            Self::OpenCursor => "OpenCursor",
            Self::CloseCursor => "CloseCursor",
            Self::Authentication => "Authentication",
            Self::Version => "Version",
            Self::Execute => "Execute",
            Self::Fetch => "Fetch",
            Self::Describe => "Describe",
            Self::Parse => "Parse",
            Self::ExecuteAndFetch => "ExecuteAndFetch",
            Self::LobRead => "LobRead",
            Self::LobWrite => "LobWrite",
            Self::LobGetLength => "LobGetLength",
            Self::LobTrim => "LobTrim",
            Self::LobErase => "LobErase",
            Self::LobOpen => "LobOpen",
            Self::LobClose => "LobClose",
            Self::LobIsOpen => "LobIsOpen",
            Self::LobIsTemp => "LobIsTemp",
            Self::LobGetChunkSize => "LobGetChunkSize",
            Self::LobCreateTemp => "LobCreateTemp",
            Self::LobFreeTemp => "LobFreeTemp",
            Self::LobCopy => "LobCopy",
            Self::LobAppend => "LobAppend",
            Self::LobLoadFromFile => "LobLoadFromFile",
            Self::LobGetCharsetId => "LobGetCharsetId",
            Self::SessionStatePiggyback => "SessionStatePiggyback",
            Self::DrpcRelease => "DrpcRelease",
            Self::BatchExecute => "BatchExecute",
            Self::GetServerInfo => "GetServerInfo",
            Self::ScrollFetch => "ScrollFetch",
            Self::SetStatementTag => "SetStatementTag",
            Self::GetStatementByTag => "GetStatementByTag",
            Self::Success => "Success",
            Self::Error => "Error",
            Self::Warning => "Warning",
            Self::RowData => "RowData",
            Self::EndOfData => "EndOfData",
            Self::Unknown(_) => "Unknown",
        }
    }

    /// Check if this is a request function code.
    pub fn is_request(&self) -> bool {
        matches!(
            self,
            Self::ProtocolNegotiation
                | Self::DataTypeNegotiation
                | Self::TransactionBegin
                | Self::Commit
                | Self::Rollback
                | Self::OpenCursor
                | Self::CloseCursor
                | Self::Authentication
                | Self::Version
                | Self::Execute
                | Self::Describe
                | Self::Parse
                | Self::ExecuteAndFetch
                | Self::LobRead
                | Self::LobWrite
                | Self::LobGetLength
                | Self::LobTrim
                | Self::LobErase
                | Self::LobOpen
                | Self::LobClose
                | Self::LobIsOpen
                | Self::LobIsTemp
                | Self::LobGetChunkSize
                | Self::LobCreateTemp
                | Self::LobFreeTemp
                | Self::LobCopy
                | Self::LobAppend
                | Self::LobLoadFromFile
                | Self::LobGetCharsetId
                | Self::BatchExecute
                | Self::GetServerInfo
                | Self::ScrollFetch
                | Self::SetStatementTag
                | Self::GetStatementByTag
        )
    }

    /// Check if this is a response function code.
    pub fn is_response(&self) -> bool {
        matches!(self, Self::Success | Self::Error | Self::Warning | Self::RowData | Self::EndOfData)
    }

    /// Check if this is a LOB-related function code.
    pub fn is_lob_operation(&self) -> bool {
        matches!(
            self,
            Self::LobRead
                | Self::LobWrite
                | Self::LobGetLength
                | Self::LobTrim
                | Self::LobErase
                | Self::LobOpen
                | Self::LobClose
                | Self::LobIsOpen
                | Self::LobIsTemp
                | Self::LobGetChunkSize
                | Self::LobCreateTemp
                | Self::LobFreeTemp
                | Self::LobCopy
                | Self::LobAppend
                | Self::LobLoadFromFile
                | Self::LobGetCharsetId
        )
    }

    /// Check if this function requires TNS v11+.
    pub fn requires_v11(&self) -> bool {
        matches!(self, Self::SessionStatePiggyback | Self::DrpcRelease)
    }
}

impl From<u8> for FunctionCode {
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}

impl From<FunctionCode> for u8 {
    fn from(value: FunctionCode) -> Self {
        value.as_u8()
    }
}

/// Raw function code constants for pattern matching.
pub mod codes {
    pub const PROTOCOL_NEGOTIATION: u8 = 0x01;
    pub const DATA_TYPE_NEGOTIATION: u8 = 0x02;
    pub const TRANSACTION_BEGIN: u8 = 0x03;
    pub const COMMIT: u8 = 0x04;
    pub const ROLLBACK: u8 = 0x05;
    pub const OPEN_CURSOR: u8 = 0x06;
    pub const CLOSE_CURSOR: u8 = 0x08;
    pub const AUTHENTICATION: u8 = 0x09;
    pub const VERSION: u8 = 0x0B;
    pub const EXECUTE: u8 = 0x0E;
    pub const DESCRIBE: u8 = 0x10;
    pub const PARSE: u8 = 0x11;
    pub const EXECUTE_AND_FETCH: u8 = 0x12;
    pub const LOB_READ: u8 = 0x60;
    pub const LOB_WRITE: u8 = 0x61;
    pub const LOB_GET_LENGTH: u8 = 0x62;
    pub const LOB_TRIM: u8 = 0x63;
    pub const LOB_ERASE: u8 = 0x64;
    pub const LOB_OPEN: u8 = 0x65;
    pub const LOB_CLOSE: u8 = 0x66;
    pub const LOB_IS_OPEN: u8 = 0x67;
    pub const LOB_IS_TEMP: u8 = 0x68;
    pub const LOB_GET_CHUNK_SIZE: u8 = 0x69;
    pub const LOB_CREATE_TEMP: u8 = 0x6A;
    pub const LOB_FREE_TEMP: u8 = 0x6D;
    pub const LOB_COPY: u8 = 0x6E;
    pub const LOB_APPEND: u8 = 0x6F;
    pub const LOB_LOAD_FROM_FILE: u8 = 0x73;
    pub const LOB_GET_CHARSET_ID: u8 = 0x74;
    pub const SESSION_STATE_PIGGYBACK: u8 = 0x6B;
    pub const DRCP_RELEASE: u8 = 0x6C;
    pub const BATCH_EXECUTE: u8 = 0x70;
    pub const GET_SERVER_INFO: u8 = 0x76;
    pub const SCROLL_FETCH: u8 = 0x77;
    pub const SET_STATEMENT_TAG: u8 = 0x78;
    pub const GET_STATEMENT_BY_TAG: u8 = 0x79;
}
