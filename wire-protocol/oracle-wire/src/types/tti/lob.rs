//! LOB (Large Object) streaming operations.
//!
//! Oracle LOBs (CLOB, BLOB, NCLOB, BFILE) are handled differently from regular
//! data types. They use locators and streaming operations.

use super::function_codes::FunctionCode;

/// LOB operation type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LobOperation {
    /// Get LOB length.
    GetLength,
    /// Read LOB data.
    Read,
    /// Write LOB data.
    Write,
    /// Trim LOB to specified length.
    Trim,
    /// Erase portion of LOB.
    Erase,
    /// Create temporary LOB.
    CreateTemporary,
    /// Free temporary LOB.
    FreeTemporary,
    /// Open LOB for reading/writing.
    Open,
    /// Close LOB.
    Close,
    /// Check if LOB is open.
    IsOpen,
    /// Check if LOB is temporary.
    IsTemporary,
    /// Get chunk size.
    GetChunkSize,
    /// Copy LOB data.
    Copy,
    /// Append LOB data.
    Append,
    /// Load from file.
    LoadFromFile,
    /// Get character set ID (for CLOB).
    GetCharsetId,
}

impl LobOperation {
    /// Get the function code for this operation.
    pub const fn function_code(self) -> FunctionCode {
        match self {
            Self::GetLength => FunctionCode::LobGetLength,
            Self::Read => FunctionCode::LobRead,
            Self::Write => FunctionCode::LobWrite,
            Self::Trim => FunctionCode::LobTrim,
            Self::Erase => FunctionCode::LobErase,
            Self::CreateTemporary => FunctionCode::LobCreateTemp,
            Self::FreeTemporary => FunctionCode::LobFreeTemp,
            Self::Open => FunctionCode::LobOpen,
            Self::Close => FunctionCode::LobClose,
            Self::IsOpen => FunctionCode::LobIsOpen,
            Self::IsTemporary => FunctionCode::LobIsTemp,
            Self::GetChunkSize => FunctionCode::LobGetChunkSize,
            Self::Copy => FunctionCode::LobCopy,
            Self::Append => FunctionCode::LobAppend,
            Self::LoadFromFile => FunctionCode::LobLoadFromFile,
            Self::GetCharsetId => FunctionCode::LobGetCharsetId,
        }
    }
}

/// LOB type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LobType {
    /// Binary LOB.
    Blob,
    /// Character LOB.
    Clob,
    /// National character LOB.
    NClob,
    /// Binary file (external).
    Bfile,
}

impl LobType {
    /// Oracle's internal type code.
    pub const fn type_code(self) -> u8 {
        match self {
            Self::Blob => 0x71,  // 113
            Self::Clob => 0x70,  // 112
            Self::NClob => 0x70, // Same as CLOB, charset differs
            Self::Bfile => 0x72, // 114
        }
    }

    /// Check if this is a character LOB.
    pub const fn is_character(self) -> bool {
        matches!(self, Self::Clob | Self::NClob)
    }

    /// Check if this is external (BFILE).
    pub const fn is_external(self) -> bool {
        matches!(self, Self::Bfile)
    }
}

/// Open mode for LOB operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum LobOpenMode {
    /// Read-only access.
    #[default]
    ReadOnly,
    /// Read-write access.
    ReadWrite,
}

impl LobOpenMode {
    /// Oracle's internal code.
    pub const fn code(self) -> u8 {
        match self {
            Self::ReadOnly => 0x00,
            Self::ReadWrite => 0x01,
        }
    }
}

/// Request to read LOB data.
#[derive(Clone, Debug)]
pub struct LobReadRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Offset to start reading (1-based for Oracle).
    pub offset: u64,
    /// Amount to read (bytes for BLOB, characters for CLOB).
    pub amount: u32,
    /// Character set ID (for CLOB).
    pub charset_id: u16,
}

impl LobReadRequest {
    /// Create a new LOB read request.
    pub fn new(locator: Vec<u8>, offset: u64, amount: u32) -> Self {
        Self {
            locator,
            offset,
            amount,
            charset_id: 873, // AL32UTF8 default
        }
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 20);

        // Function code
        buf.push(LobOperation::Read.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Offset (8 bytes, big-endian)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Amount (4 bytes)
        buf.extend_from_slice(&self.amount.to_be_bytes());

        // Charset ID (2 bytes)
        buf.extend_from_slice(&self.charset_id.to_be_bytes());

        buf
    }
}

/// Response from a LOB read operation.
#[derive(Clone, Debug)]
pub struct LobReadResponse {
    /// Data read from the LOB.
    pub data: Vec<u8>,
    /// Amount actually read.
    pub amount_read: u32,
    /// Whether there is more data.
    pub has_more: bool,
}

impl LobReadResponse {
    /// Create an empty response.
    pub fn empty() -> Self {
        Self { data: Vec::new(), amount_read: 0, has_more: false }
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 5 {
            return Err(LobError::TooShort { expected: 5, actual: data.len() });
        }

        // Amount read (4 bytes)
        let amount_read = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        // Has more flag (1 byte)
        let has_more = data[4] != 0;

        // Remaining is data
        let lob_data = data[5..].to_vec();

        Ok(Self { data: lob_data, amount_read, has_more })
    }
}

/// Request to write LOB data.
#[derive(Clone, Debug)]
pub struct LobWriteRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Offset to start writing (1-based).
    pub offset: u64,
    /// Data to write.
    pub data: Vec<u8>,
    /// Character set ID (for CLOB).
    pub charset_id: u16,
}

impl LobWriteRequest {
    /// Create a new LOB write request.
    pub fn new(locator: Vec<u8>, offset: u64, data: Vec<u8>) -> Self {
        Self {
            locator,
            offset,
            data,
            charset_id: 873, // AL32UTF8
        }
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + self.data.len() + 20);

        // Function code
        buf.push(LobOperation::Write.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Offset (8 bytes)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Data length (4 bytes)
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());

        // Charset ID (2 bytes)
        buf.extend_from_slice(&self.charset_id.to_be_bytes());

        // Data
        buf.extend_from_slice(&self.data);

        buf
    }
}

/// Response from a LOB write operation.
#[derive(Clone, Debug)]
pub struct LobWriteResponse {
    /// Amount actually written.
    pub amount_written: u32,
}

impl LobWriteResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 4 {
            return Err(LobError::TooShort { expected: 4, actual: data.len() });
        }

        let amount_written = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        Ok(Self { amount_written })
    }
}

/// Request to get LOB length.
#[derive(Clone, Debug)]
pub struct LobGetLengthRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobGetLengthRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::GetLength.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Response from get LOB length.
#[derive(Clone, Debug)]
pub struct LobGetLengthResponse {
    /// LOB length (bytes for BLOB, characters for CLOB).
    pub length: u64,
}

impl LobGetLengthResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 8 {
            return Err(LobError::TooShort { expected: 8, actual: data.len() });
        }

        let length = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);

        Ok(Self { length })
    }
}

/// Request to create a temporary LOB.
#[derive(Clone, Debug)]
pub struct LobCreateTempRequest {
    /// LOB type.
    pub lob_type: LobType,
    /// Whether to cache the LOB.
    pub cache: bool,
    /// Duration (session or call).
    pub duration: TempLobDuration,
}

/// Duration for temporary LOB.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum TempLobDuration {
    /// LOB persists for the session.
    #[default]
    Session,
    /// LOB persists for the call only.
    Call,
}

impl TempLobDuration {
    /// Oracle's internal code.
    pub const fn code(self) -> u8 {
        match self {
            Self::Session => 0x0A, // DURATION_SESSION
            Self::Call => 0x0C,    // DURATION_CALL
        }
    }
}

impl LobCreateTempRequest {
    /// Create a new request.
    pub fn new(lob_type: LobType) -> Self {
        Self { lob_type, cache: true, duration: TempLobDuration::Session }
    }

    /// Set caching.
    pub fn with_cache(mut self, cache: bool) -> Self {
        self.cache = cache;
        self
    }

    /// Set duration.
    pub fn with_duration(mut self, duration: TempLobDuration) -> Self {
        self.duration = duration;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);

        // Function code
        buf.push(LobOperation::CreateTemporary.function_code().as_u8());

        // LOB type
        buf.push(self.lob_type.type_code());

        // Cache flag
        buf.push(if self.cache { 1 } else { 0 });

        // Duration
        buf.push(self.duration.code());

        buf
    }
}

/// Response from create temporary LOB.
#[derive(Clone, Debug)]
pub struct LobCreateTempResponse {
    /// Locator for the new temporary LOB.
    pub locator: Vec<u8>,
}

impl LobCreateTempResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.is_empty() {
            return Err(LobError::TooShort { expected: 1, actual: 0 });
        }

        let len = data[0] as usize;
        if data.len() < 1 + len {
            return Err(LobError::TooShort { expected: 1 + len, actual: data.len() });
        }

        Ok(Self { locator: data[1..1 + len].to_vec() })
    }
}

/// Request to free a temporary LOB.
#[derive(Clone, Debug)]
pub struct LobFreeTempRequest {
    /// LOB locator.
    pub locator: Vec<u8>,
}

impl LobFreeTempRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::FreeTemporary.function_code().as_u8());

        // Locator
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Request to trim a LOB to a specified length.
#[derive(Clone, Debug)]
pub struct LobTrimRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// New length (bytes for BLOB, characters for CLOB).
    pub new_length: u64,
}

impl LobTrimRequest {
    /// Create a new trim request.
    pub fn new(locator: Vec<u8>, new_length: u64) -> Self {
        Self { locator, new_length }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 12);

        // Function code
        buf.push(LobOperation::Trim.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // New length (8 bytes)
        buf.extend_from_slice(&self.new_length.to_be_bytes());

        buf
    }
}

/// Request to erase a portion of a LOB.
#[derive(Clone, Debug)]
pub struct LobEraseRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Offset to start erasing (1-based).
    pub offset: u64,
    /// Amount to erase (bytes for BLOB, characters for CLOB).
    pub amount: u64,
}

impl LobEraseRequest {
    /// Create a new erase request.
    pub fn new(locator: Vec<u8>, offset: u64, amount: u64) -> Self {
        Self { locator, offset, amount }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 20);

        // Function code
        buf.push(LobOperation::Erase.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Offset (8 bytes)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Amount (8 bytes)
        buf.extend_from_slice(&self.amount.to_be_bytes());

        buf
    }
}

/// Response from a LOB erase operation.
#[derive(Clone, Debug)]
pub struct LobEraseResponse {
    /// Amount actually erased.
    pub amount_erased: u64,
}

impl LobEraseResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 8 {
            return Err(LobError::TooShort { expected: 8, actual: data.len() });
        }

        let amount_erased = u64::from_be_bytes([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7]]);

        Ok(Self { amount_erased })
    }
}

/// Request to open a LOB for reading or writing.
#[derive(Clone, Debug)]
pub struct LobOpenRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Open mode.
    pub mode: LobOpenMode,
}

impl LobOpenRequest {
    /// Create a new open request.
    pub fn new(locator: Vec<u8>, mode: LobOpenMode) -> Self {
        Self { locator, mode }
    }

    /// Create a read-only open request.
    pub fn read_only(locator: Vec<u8>) -> Self {
        Self::new(locator, LobOpenMode::ReadOnly)
    }

    /// Create a read-write open request.
    pub fn read_write(locator: Vec<u8>) -> Self {
        Self::new(locator, LobOpenMode::ReadWrite)
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 4);

        // Function code
        buf.push(LobOperation::Open.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Mode
        buf.push(self.mode.code());

        buf
    }
}

/// Request to close a LOB.
#[derive(Clone, Debug)]
pub struct LobCloseRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobCloseRequest {
    /// Create a new close request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::Close.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Request to check if a LOB is open.
#[derive(Clone, Debug)]
pub struct LobIsOpenRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobIsOpenRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::IsOpen.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Response from LOB is-open check.
#[derive(Clone, Debug)]
pub struct LobIsOpenResponse {
    /// Whether the LOB is open.
    pub is_open: bool,
}

impl LobIsOpenResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.is_empty() {
            return Err(LobError::TooShort { expected: 1, actual: 0 });
        }

        Ok(Self { is_open: data[0] != 0 })
    }
}

/// Request to check if a LOB is temporary.
#[derive(Clone, Debug)]
pub struct LobIsTempRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobIsTempRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::IsTemporary.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Response from LOB is-temporary check.
#[derive(Clone, Debug)]
pub struct LobIsTempResponse {
    /// Whether the LOB is temporary.
    pub is_temporary: bool,
}

impl LobIsTempResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.is_empty() {
            return Err(LobError::TooShort { expected: 1, actual: 0 });
        }

        Ok(Self { is_temporary: data[0] != 0 })
    }
}

/// Request to get the chunk size of a LOB.
#[derive(Clone, Debug)]
pub struct LobGetChunkSizeRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobGetChunkSizeRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::GetChunkSize.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Response from get chunk size.
#[derive(Clone, Debug)]
pub struct LobGetChunkSizeResponse {
    /// Optimal chunk size for this LOB (in bytes).
    pub chunk_size: u32,
}

impl LobGetChunkSizeResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 4 {
            return Err(LobError::TooShort { expected: 4, actual: data.len() });
        }

        let chunk_size = u32::from_be_bytes([data[0], data[1], data[2], data[3]]);

        Ok(Self { chunk_size })
    }
}

/// Request to copy data from one LOB to another.
#[derive(Clone, Debug)]
pub struct LobCopyRequest {
    /// Source LOB locator.
    pub source_locator: Vec<u8>,
    /// Destination LOB locator.
    pub dest_locator: Vec<u8>,
    /// Source offset (1-based).
    pub source_offset: u64,
    /// Destination offset (1-based).
    pub dest_offset: u64,
    /// Amount to copy (bytes for BLOB, characters for CLOB).
    pub amount: u64,
}

impl LobCopyRequest {
    /// Create a new copy request.
    pub fn new(source_locator: Vec<u8>, dest_locator: Vec<u8>, source_offset: u64, dest_offset: u64, amount: u64) -> Self {
        Self {
            source_locator,
            dest_locator,
            source_offset,
            dest_offset,
            amount,
        }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.source_locator.len() + self.dest_locator.len() + 30);

        // Function code
        buf.push(LobOperation::Copy.function_code().as_u8());

        // Source locator length and data
        buf.push(self.source_locator.len() as u8);
        buf.extend_from_slice(&self.source_locator);

        // Dest locator length and data
        buf.push(self.dest_locator.len() as u8);
        buf.extend_from_slice(&self.dest_locator);

        // Source offset (8 bytes)
        buf.extend_from_slice(&self.source_offset.to_be_bytes());

        // Dest offset (8 bytes)
        buf.extend_from_slice(&self.dest_offset.to_be_bytes());

        // Amount (8 bytes)
        buf.extend_from_slice(&self.amount.to_be_bytes());

        buf
    }
}

/// Request to append one LOB to another.
#[derive(Clone, Debug)]
pub struct LobAppendRequest {
    /// Source LOB locator.
    pub source_locator: Vec<u8>,
    /// Destination LOB locator.
    pub dest_locator: Vec<u8>,
}

impl LobAppendRequest {
    /// Create a new append request.
    pub fn new(source_locator: Vec<u8>, dest_locator: Vec<u8>) -> Self {
        Self { source_locator, dest_locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.source_locator.len() + self.dest_locator.len() + 4);

        // Function code
        buf.push(LobOperation::Append.function_code().as_u8());

        // Source locator length and data
        buf.push(self.source_locator.len() as u8);
        buf.extend_from_slice(&self.source_locator);

        // Dest locator length and data
        buf.push(self.dest_locator.len() as u8);
        buf.extend_from_slice(&self.dest_locator);

        buf
    }
}

/// Request to load data from a BFILE into a LOB.
#[derive(Clone, Debug)]
pub struct LobLoadFromFileRequest {
    /// BFILE locator.
    pub bfile_locator: Vec<u8>,
    /// Destination LOB locator.
    pub dest_locator: Vec<u8>,
    /// BFILE offset (1-based).
    pub bfile_offset: u64,
    /// Destination offset (1-based).
    pub dest_offset: u64,
    /// Amount to load.
    pub amount: u64,
}

impl LobLoadFromFileRequest {
    /// Create a new load from file request.
    pub fn new(bfile_locator: Vec<u8>, dest_locator: Vec<u8>, bfile_offset: u64, dest_offset: u64, amount: u64) -> Self {
        Self {
            bfile_locator,
            dest_locator,
            bfile_offset,
            dest_offset,
            amount,
        }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.bfile_locator.len() + self.dest_locator.len() + 30);

        // Function code
        buf.push(LobOperation::LoadFromFile.function_code().as_u8());

        // BFILE locator length and data
        buf.push(self.bfile_locator.len() as u8);
        buf.extend_from_slice(&self.bfile_locator);

        // Dest locator length and data
        buf.push(self.dest_locator.len() as u8);
        buf.extend_from_slice(&self.dest_locator);

        // BFILE offset (8 bytes)
        buf.extend_from_slice(&self.bfile_offset.to_be_bytes());

        // Dest offset (8 bytes)
        buf.extend_from_slice(&self.dest_offset.to_be_bytes());

        // Amount (8 bytes)
        buf.extend_from_slice(&self.amount.to_be_bytes());

        buf
    }
}

/// Request to get the character set ID of a CLOB/NCLOB.
#[derive(Clone, Debug)]
pub struct LobGetCharsetIdRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
}

impl LobGetCharsetIdRequest {
    /// Create a new request.
    pub fn new(locator: Vec<u8>) -> Self {
        Self { locator }
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.locator.len() + 2);

        // Function code
        buf.push(LobOperation::GetCharsetId.function_code().as_u8());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        buf
    }
}

/// Response from get charset ID.
#[derive(Clone, Debug)]
pub struct LobGetCharsetIdResponse {
    /// Character set ID.
    pub charset_id: u16,
}

impl LobGetCharsetIdResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 2 {
            return Err(LobError::TooShort { expected: 2, actual: data.len() });
        }

        let charset_id = u16::from_be_bytes([data[0], data[1]]);

        Ok(Self { charset_id })
    }
}

/// A streaming LOB reader for chunked access.
#[derive(Clone, Debug)]
pub struct LobStream {
    /// LOB locator.
    locator: Vec<u8>,
    /// Current offset (1-based).
    offset: u64,
    /// Total length (if known).
    length: Option<u64>,
    /// Chunk size for reads.
    chunk_size: u32,
    /// Whether this is a character LOB.
    is_character: bool,
    /// Character set ID.
    charset_id: u16,
}

impl LobStream {
    /// Create a new LOB stream.
    pub fn new(locator: Vec<u8>, is_character: bool) -> Self {
        Self {
            locator,
            offset: 1, // Oracle is 1-based
            length: None,
            chunk_size: 32768, // Default 32KB chunks
            is_character,
            charset_id: 873, // AL32UTF8
        }
    }

    /// Set the chunk size.
    pub fn with_chunk_size(mut self, size: u32) -> Self {
        self.chunk_size = size;
        self
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Set the known length.
    pub fn with_length(mut self, length: u64) -> Self {
        self.length = Some(length);
        self
    }

    /// Check if this is a character LOB (CLOB/NCLOB).
    pub fn is_character(&self) -> bool {
        self.is_character
    }

    /// Get the current offset.
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Get the known length.
    pub fn length(&self) -> Option<u64> {
        self.length
    }

    /// Check if at end of LOB.
    pub fn is_eof(&self) -> bool {
        if let Some(len) = self.length { self.offset > len } else { false }
    }

    /// Get a request to read the next chunk.
    pub fn next_read_request(&self) -> LobReadRequest {
        LobReadRequest {
            locator: self.locator.clone(),
            offset: self.offset,
            amount: self.chunk_size,
            charset_id: self.charset_id,
        }
    }

    /// Process a read response and advance the stream.
    pub fn process_read(&mut self, response: &LobReadResponse) {
        self.offset += response.amount_read as u64;
    }

    /// Get a request to get the LOB length.
    pub fn length_request(&self) -> LobGetLengthRequest {
        LobGetLengthRequest { locator: self.locator.clone() }
    }

    /// Process a length response.
    pub fn process_length(&mut self, response: &LobGetLengthResponse) {
        self.length = Some(response.length);
    }

    /// Reset to beginning.
    pub fn reset(&mut self) {
        self.offset = 1;
    }

    /// Seek to a specific offset (1-based).
    pub fn seek(&mut self, offset: u64) {
        self.offset = offset.max(1);
    }
}

/// Error in LOB operations.
#[derive(Clone, Debug, thiserror::Error)]
pub enum LobError {
    #[error("data too short: expected {expected} bytes, got {actual}")]
    TooShort { expected: usize, actual: usize },
    #[error("invalid locator")]
    InvalidLocator,
    #[error("LOB is closed")]
    Closed,
    #[error("invalid offset: {0}")]
    InvalidOffset(u64),
    #[error("operation not supported for this LOB type")]
    UnsupportedOperation,
    #[error("piece sequence error: {0}")]
    PieceSequenceError(String),
    #[error("piecewise operation in progress")]
    PiecewiseInProgress,
    #[error("no piecewise operation in progress")]
    NoPiecewiseOperation,
}

// ============================================================================
// Piecewise LOB Operations
// ============================================================================
//
// Piecewise operations allow reading or writing very large LOBs in chunks
// without requiring the entire LOB to fit in memory. This is essential for
// LOBs larger than the maximum buffer size (typically 1GB).
//
// Piece types:
// - FIRST: Start of piecewise operation
// - NEXT: Continuation of piecewise operation
// - LAST: Final piece of piecewise operation
// - ONE: Complete operation in a single piece (not piecewise)

/// Piece type for piecewise LOB operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PieceType {
    /// Complete data in a single piece (not piecewise).
    One,
    /// First piece of a piecewise operation.
    First,
    /// Intermediate piece of a piecewise operation.
    Next,
    /// Final piece of a piecewise operation.
    Last,
}

impl PieceType {
    /// Get Oracle's internal code.
    pub fn code(&self) -> u8 {
        match self {
            Self::One => 0x00,
            Self::First => 0x01,
            Self::Next => 0x02,
            Self::Last => 0x03,
        }
    }

    /// Parse from Oracle's code.
    pub fn from_code(code: u8) -> Option<Self> {
        match code {
            0x00 => Some(Self::One),
            0x01 => Some(Self::First),
            0x02 => Some(Self::Next),
            0x03 => Some(Self::Last),
            _ => None,
        }
    }

    /// Check if this is the start of a piecewise operation.
    pub fn is_first(&self) -> bool {
        matches!(self, Self::First)
    }

    /// Check if this is a continuation.
    pub fn is_continuation(&self) -> bool {
        matches!(self, Self::Next | Self::Last)
    }

    /// Check if this is the final piece.
    pub fn is_last(&self) -> bool {
        matches!(self, Self::Last | Self::One)
    }

    /// Check if this is a piecewise operation (not ONE).
    pub fn is_piecewise(&self) -> bool {
        !matches!(self, Self::One)
    }
}

/// Piecewise read request.
///
/// Used for reading very large LOBs in chunks. The sequence is:
/// 1. Send FIRST piece request
/// 2. Receive data + continue indicator
/// 3. Send NEXT piece requests until done
/// 4. Receive LAST piece response
#[derive(Clone, Debug)]
pub struct LobPiecewiseReadRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Current piece type.
    pub piece_type: PieceType,
    /// Offset for FIRST piece (1-based).
    pub offset: u64,
    /// Amount to read in this piece.
    pub amount: u32,
    /// Character set ID (for CLOB).
    pub charset_id: u16,
    /// Polling context (returned from previous response for NEXT/LAST).
    pub poll_context: Option<Vec<u8>>,
}

impl LobPiecewiseReadRequest {
    /// Create the first piece of a piecewise read.
    pub fn first(locator: Vec<u8>, offset: u64, amount: u32) -> Self {
        Self {
            locator,
            piece_type: PieceType::First,
            offset,
            amount,
            charset_id: 873, // AL32UTF8
            poll_context: None,
        }
    }

    /// Create a continuation read request.
    pub fn next(locator: Vec<u8>, poll_context: Vec<u8>, amount: u32) -> Self {
        Self {
            locator,
            piece_type: PieceType::Next,
            offset: 0, // Not used for NEXT
            amount,
            charset_id: 873,
            poll_context: Some(poll_context),
        }
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let poll_len = self.poll_context.as_ref().map(|p| p.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(self.locator.len() + poll_len + 24);

        // Function code
        buf.push(FunctionCode::LobRead.as_u8());

        // Piece type
        buf.push(self.piece_type.code());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Offset (8 bytes, only meaningful for FIRST)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Amount (4 bytes)
        buf.extend_from_slice(&self.amount.to_be_bytes());

        // Charset ID (2 bytes)
        buf.extend_from_slice(&self.charset_id.to_be_bytes());

        // Poll context (if present)
        if let Some(ref ctx) = self.poll_context {
            buf.push(ctx.len() as u8);
            buf.extend_from_slice(ctx);
        } else {
            buf.push(0);
        }

        buf
    }
}

/// Piecewise read response.
#[derive(Clone, Debug)]
pub struct LobPiecewiseReadResponse {
    /// Data read in this piece.
    pub data: Vec<u8>,
    /// Amount actually read.
    pub amount_read: u32,
    /// Piece type of this response.
    pub piece_type: PieceType,
    /// Polling context for next request (None if done).
    pub poll_context: Option<Vec<u8>>,
    /// Whether more data is available.
    pub has_more: bool,
}

impl LobPiecewiseReadResponse {
    /// Check if the read is complete.
    pub fn is_complete(&self) -> bool {
        self.piece_type.is_last()
    }

    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 6 {
            return Err(LobError::TooShort { expected: 6, actual: data.len() });
        }

        // Piece type
        let piece_type = PieceType::from_code(data[0]).ok_or_else(|| LobError::PieceSequenceError("invalid piece type".to_string()))?;

        // Amount read (4 bytes)
        let amount_read = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);

        // Has more flag
        let has_more = data[5] != 0;

        // Poll context length
        let mut offset = 6;
        let poll_context = if offset < data.len() {
            let ctx_len = data[offset] as usize;
            offset += 1;
            if ctx_len > 0 && offset + ctx_len <= data.len() {
                let ctx = data[offset..offset + ctx_len].to_vec();
                offset += ctx_len;
                Some(ctx)
            } else {
                None
            }
        } else {
            None
        };

        // Remaining is data
        let lob_data = data[offset..].to_vec();

        Ok(Self {
            data: lob_data,
            amount_read,
            piece_type,
            poll_context,
            has_more,
        })
    }
}

/// Piecewise write request.
///
/// Used for writing very large LOBs in chunks. The sequence is:
/// 1. Send FIRST piece with data
/// 2. Receive write acknowledgment
/// 3. Send NEXT pieces with more data
/// 4. Send LAST piece to complete
#[derive(Clone, Debug)]
pub struct LobPiecewiseWriteRequest {
    /// LOB locator bytes.
    pub locator: Vec<u8>,
    /// Current piece type.
    pub piece_type: PieceType,
    /// Offset for FIRST piece (1-based).
    pub offset: u64,
    /// Data to write in this piece.
    pub data: Vec<u8>,
    /// Character set ID (for CLOB).
    pub charset_id: u16,
    /// Polling context (returned from previous response for NEXT/LAST).
    pub poll_context: Option<Vec<u8>>,
}

impl LobPiecewiseWriteRequest {
    /// Create the first piece of a piecewise write.
    pub fn first(locator: Vec<u8>, offset: u64, data: Vec<u8>) -> Self {
        Self {
            locator,
            piece_type: PieceType::First,
            offset,
            data,
            charset_id: 873, // AL32UTF8
            poll_context: None,
        }
    }

    /// Create a continuation write request.
    pub fn next(locator: Vec<u8>, poll_context: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            locator,
            piece_type: PieceType::Next,
            offset: 0, // Not used for NEXT
            data,
            charset_id: 873,
            poll_context: Some(poll_context),
        }
    }

    /// Create the final piece of a piecewise write.
    pub fn last(locator: Vec<u8>, poll_context: Vec<u8>, data: Vec<u8>) -> Self {
        Self {
            locator,
            piece_type: PieceType::Last,
            offset: 0,
            data,
            charset_id: 873,
            poll_context: Some(poll_context),
        }
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Encode to wire format.
    pub fn encode(&self) -> Vec<u8> {
        let poll_len = self.poll_context.as_ref().map(|p| p.len()).unwrap_or(0);
        let mut buf = Vec::with_capacity(self.locator.len() + self.data.len() + poll_len + 24);

        // Function code
        buf.push(FunctionCode::LobWrite.as_u8());

        // Piece type
        buf.push(self.piece_type.code());

        // Locator length and data
        buf.push(self.locator.len() as u8);
        buf.extend_from_slice(&self.locator);

        // Offset (8 bytes, only meaningful for FIRST)
        buf.extend_from_slice(&self.offset.to_be_bytes());

        // Data length (4 bytes)
        buf.extend_from_slice(&(self.data.len() as u32).to_be_bytes());

        // Charset ID (2 bytes)
        buf.extend_from_slice(&self.charset_id.to_be_bytes());

        // Poll context (if present)
        if let Some(ref ctx) = self.poll_context {
            buf.push(ctx.len() as u8);
            buf.extend_from_slice(ctx);
        } else {
            buf.push(0);
        }

        // Data
        buf.extend_from_slice(&self.data);

        buf
    }
}

/// Piecewise write response.
#[derive(Clone, Debug)]
pub struct LobPiecewiseWriteResponse {
    /// Amount actually written.
    pub amount_written: u32,
    /// Piece type of this response.
    pub piece_type: PieceType,
    /// Polling context for next request (None if done).
    pub poll_context: Option<Vec<u8>>,
    /// Whether the operation is complete.
    pub is_complete: bool,
}

impl LobPiecewiseWriteResponse {
    /// Parse from wire data.
    pub fn parse(data: &[u8]) -> Result<Self, LobError> {
        if data.len() < 6 {
            return Err(LobError::TooShort { expected: 6, actual: data.len() });
        }

        // Piece type
        let piece_type = PieceType::from_code(data[0]).ok_or_else(|| LobError::PieceSequenceError("invalid piece type".to_string()))?;

        // Amount written (4 bytes)
        let amount_written = u32::from_be_bytes([data[1], data[2], data[3], data[4]]);

        // Complete flag
        let is_complete = data[5] != 0;

        // Poll context
        let poll_context = if data.len() > 6 {
            let ctx_len = data[6] as usize;
            if ctx_len > 0 && data.len() >= 7 + ctx_len {
                Some(data[7..7 + ctx_len].to_vec())
            } else {
                None
            }
        } else {
            None
        };

        Ok(Self { amount_written, piece_type, poll_context, is_complete })
    }
}

/// State for tracking a piecewise LOB operation.
#[derive(Clone, Debug)]
pub struct LobPiecewiseState {
    /// LOB locator.
    pub locator: Vec<u8>,
    /// Whether this is a read or write operation.
    pub is_read: bool,
    /// Starting offset.
    pub start_offset: u64,
    /// Total bytes processed so far.
    pub bytes_processed: u64,
    /// Current polling context.
    pub poll_context: Option<Vec<u8>>,
    /// Whether the operation is complete.
    pub is_complete: bool,
    /// Number of pieces exchanged.
    pub piece_count: u32,
    /// Character set ID.
    pub charset_id: u16,
}

impl LobPiecewiseState {
    /// Create a new state for a piecewise read.
    pub fn new_read(locator: Vec<u8>, offset: u64) -> Self {
        Self {
            locator,
            is_read: true,
            start_offset: offset,
            bytes_processed: 0,
            poll_context: None,
            is_complete: false,
            piece_count: 0,
            charset_id: 873,
        }
    }

    /// Create a new state for a piecewise write.
    pub fn new_write(locator: Vec<u8>, offset: u64) -> Self {
        Self {
            locator,
            is_read: false,
            start_offset: offset,
            bytes_processed: 0,
            poll_context: None,
            is_complete: false,
            piece_count: 0,
            charset_id: 873,
        }
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.charset_id = charset_id;
        self
    }

    /// Get the next piece type to send.
    pub fn next_piece_type(&self) -> PieceType {
        if self.piece_count == 0 { PieceType::First } else { PieceType::Next }
    }

    /// Create the next read request.
    pub fn next_read_request(&self, amount: u32) -> LobPiecewiseReadRequest {
        if self.piece_count == 0 {
            LobPiecewiseReadRequest::first(self.locator.clone(), self.start_offset, amount).with_charset(self.charset_id)
        } else {
            LobPiecewiseReadRequest::next(self.locator.clone(), self.poll_context.clone().unwrap_or_default(), amount)
                .with_charset(self.charset_id)
        }
    }

    /// Create the next write request.
    pub fn next_write_request(&self, data: Vec<u8>, is_last: bool) -> LobPiecewiseWriteRequest {
        let mut req = if self.piece_count == 0 {
            LobPiecewiseWriteRequest::first(self.locator.clone(), self.start_offset, data)
        } else if is_last {
            LobPiecewiseWriteRequest::last(self.locator.clone(), self.poll_context.clone().unwrap_or_default(), data)
        } else {
            LobPiecewiseWriteRequest::next(self.locator.clone(), self.poll_context.clone().unwrap_or_default(), data)
        };
        req.charset_id = self.charset_id;
        req
    }

    /// Process a read response and update state.
    pub fn process_read_response(&mut self, response: &LobPiecewiseReadResponse) {
        self.bytes_processed += response.amount_read as u64;
        self.poll_context = response.poll_context.clone();
        self.is_complete = response.is_complete();
        self.piece_count += 1;
    }

    /// Process a write response and update state.
    pub fn process_write_response(&mut self, response: &LobPiecewiseWriteResponse) {
        self.bytes_processed += response.amount_written as u64;
        self.poll_context = response.poll_context.clone();
        self.is_complete = response.is_complete;
        self.piece_count += 1;
    }

    /// Get the current offset (for progress tracking).
    pub fn current_offset(&self) -> u64 {
        self.start_offset + self.bytes_processed
    }

    /// Reset the state for reuse.
    pub fn reset(&mut self) {
        self.bytes_processed = 0;
        self.poll_context = None;
        self.is_complete = false;
        self.piece_count = 0;
    }
}

/// Iterator-style piecewise reader.
///
/// Provides a convenient way to read a LOB in pieces.
#[derive(Clone, Debug)]
pub struct LobPiecewiseReader {
    /// Internal state.
    state: LobPiecewiseState,
    /// Chunk size for each read.
    chunk_size: u32,
    /// Total length of the LOB (if known).
    total_length: Option<u64>,
}

impl LobPiecewiseReader {
    /// Create a new piecewise reader.
    pub fn new(locator: Vec<u8>, offset: u64, chunk_size: u32) -> Self {
        Self {
            state: LobPiecewiseState::new_read(locator, offset),
            chunk_size,
            total_length: None,
        }
    }

    /// Set the total length (for progress tracking).
    pub fn with_total_length(mut self, length: u64) -> Self {
        self.total_length = Some(length);
        self
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.state.charset_id = charset_id;
        self
    }

    /// Check if reading is complete.
    pub fn is_complete(&self) -> bool {
        self.state.is_complete
    }

    /// Get bytes read so far.
    pub fn bytes_read(&self) -> u64 {
        self.state.bytes_processed
    }

    /// Get progress as a fraction (0.0 to 1.0).
    pub fn progress(&self) -> Option<f64> {
        self.total_length.map(|len| self.state.bytes_processed as f64 / len as f64)
    }

    /// Get the next read request.
    pub fn next_request(&self) -> Option<LobPiecewiseReadRequest> {
        if self.state.is_complete {
            None
        } else {
            Some(self.state.next_read_request(self.chunk_size))
        }
    }

    /// Process a read response.
    pub fn process_response(&mut self, response: &LobPiecewiseReadResponse) {
        self.state.process_read_response(response);
    }
}

/// Iterator-style piecewise writer.
///
/// Provides a convenient way to write a LOB in pieces.
#[derive(Clone, Debug)]
pub struct LobPiecewiseWriter {
    /// Internal state.
    state: LobPiecewiseState,
    /// Chunk size for each write.
    chunk_size: usize,
    /// Total length to write (if known).
    total_length: Option<u64>,
}

impl LobPiecewiseWriter {
    /// Create a new piecewise writer.
    pub fn new(locator: Vec<u8>, offset: u64, chunk_size: usize) -> Self {
        Self {
            state: LobPiecewiseState::new_write(locator, offset),
            chunk_size,
            total_length: None,
        }
    }

    /// Set the total length (for progress tracking).
    pub fn with_total_length(mut self, length: u64) -> Self {
        self.total_length = Some(length);
        self
    }

    /// Set the character set.
    pub fn with_charset(mut self, charset_id: u16) -> Self {
        self.state.charset_id = charset_id;
        self
    }

    /// Check if writing is complete.
    pub fn is_complete(&self) -> bool {
        self.state.is_complete
    }

    /// Get bytes written so far.
    pub fn bytes_written(&self) -> u64 {
        self.state.bytes_processed
    }

    /// Get progress as a fraction (0.0 to 1.0).
    pub fn progress(&self) -> Option<f64> {
        self.total_length.map(|len| self.state.bytes_processed as f64 / len as f64)
    }

    /// Create the next write request.
    ///
    /// The `data` should be the next chunk to write.
    /// `is_last` should be true if this is the final chunk.
    pub fn next_request(&self, data: Vec<u8>, is_last: bool) -> LobPiecewiseWriteRequest {
        self.state.next_write_request(data, is_last)
    }

    /// Process a write response.
    pub fn process_response(&mut self, response: &LobPiecewiseWriteResponse) {
        self.state.process_write_response(response);
    }

    /// Get the recommended chunk size.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

/// Utility for splitting data into chunks for piecewise writing.
pub fn split_into_chunks(data: &[u8], chunk_size: usize) -> Vec<&[u8]> {
    if data.is_empty() {
        return vec![];
    }

    let mut chunks = Vec::with_capacity(data.len().div_ceil(chunk_size));
    let mut offset = 0;

    while offset < data.len() {
        let end = (offset + chunk_size).min(data.len());
        chunks.push(&data[offset..end]);
        offset = end;
    }

    chunks
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lob_type() {
        assert!(LobType::Clob.is_character());
        assert!(LobType::NClob.is_character());
        assert!(!LobType::Blob.is_character());
        assert!(LobType::Bfile.is_external());
    }

    #[test]
    fn test_lob_read_request() {
        let locator = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let request = LobReadRequest::new(locator.clone(), 1, 1000);

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::LobRead.as_u8());
    }

    #[test]
    fn test_lob_write_request() {
        let locator = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let data = b"Hello, World!".to_vec();
        let request = LobWriteRequest::new(locator, 1, data);

        let encoded = request.encode();
        assert!(!encoded.is_empty());
        assert_eq!(encoded[0], FunctionCode::LobWrite.as_u8());
    }

    #[test]
    fn test_lob_create_temp() {
        let request = LobCreateTempRequest::new(LobType::Clob).with_cache(true).with_duration(TempLobDuration::Session);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobCreateTemp.as_u8());
        assert_eq!(encoded[1], LobType::Clob.type_code());
    }

    #[test]
    fn test_lob_stream() {
        let locator = vec![1, 2, 3, 4];
        let mut stream = LobStream::new(locator, true).with_chunk_size(8192).with_length(100000);

        assert_eq!(stream.offset(), 1);
        assert_eq!(stream.length(), Some(100000));
        assert!(!stream.is_eof());

        // Simulate reading
        let response = LobReadResponse { data: vec![0; 8192], amount_read: 8192, has_more: true };
        stream.process_read(&response);
        assert_eq!(stream.offset(), 8193);
    }

    #[test]
    fn test_lob_read_response_parse() {
        let data = [
            0, 0, 0, 100, // amount_read = 100
            1,   // has_more = true
            65, 66, 67, // data = "ABC"
        ];

        let response = LobReadResponse::parse(&data).unwrap();
        assert_eq!(response.amount_read, 100);
        assert!(response.has_more);
        assert_eq!(response.data, vec![65, 66, 67]);
    }

    #[test]
    fn test_lob_get_length_response_parse() {
        let data = [0, 0, 0, 0, 0, 0, 1, 0]; // length = 256

        let response = LobGetLengthResponse::parse(&data).unwrap();
        assert_eq!(response.length, 256);
    }

    #[test]
    fn test_lob_trim_request() {
        let locator = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let request = LobTrimRequest::new(locator, 1000);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobTrim.as_u8());
    }

    #[test]
    fn test_lob_erase_request() {
        let locator = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let request = LobEraseRequest::new(locator, 100, 500);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobErase.as_u8());
    }

    #[test]
    fn test_lob_erase_response_parse() {
        let data = [0, 0, 0, 0, 0, 0, 1, 244]; // amount_erased = 500

        let response = LobEraseResponse::parse(&data).unwrap();
        assert_eq!(response.amount_erased, 500);
    }

    #[test]
    fn test_lob_open_request() {
        let locator = vec![1, 2, 3, 4];
        let request = LobOpenRequest::read_only(locator.clone());

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobOpen.as_u8());

        let request = LobOpenRequest::read_write(locator);
        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobOpen.as_u8());
    }

    #[test]
    fn test_lob_close_request() {
        let locator = vec![1, 2, 3, 4];
        let request = LobCloseRequest::new(locator);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobClose.as_u8());
    }

    #[test]
    fn test_lob_is_open_request_and_response() {
        let locator = vec![1, 2, 3, 4];
        let request = LobIsOpenRequest::new(locator);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobIsOpen.as_u8());

        let response = LobIsOpenResponse::parse(&[1]).unwrap();
        assert!(response.is_open);

        let response = LobIsOpenResponse::parse(&[0]).unwrap();
        assert!(!response.is_open);
    }

    #[test]
    fn test_lob_is_temp_request_and_response() {
        let locator = vec![1, 2, 3, 4];
        let request = LobIsTempRequest::new(locator);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobIsTemp.as_u8());

        let response = LobIsTempResponse::parse(&[1]).unwrap();
        assert!(response.is_temporary);

        let response = LobIsTempResponse::parse(&[0]).unwrap();
        assert!(!response.is_temporary);
    }

    #[test]
    fn test_lob_get_chunk_size_request_and_response() {
        let locator = vec![1, 2, 3, 4];
        let request = LobGetChunkSizeRequest::new(locator);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobGetChunkSize.as_u8());

        let response = LobGetChunkSizeResponse::parse(&[0, 0, 128, 0]).unwrap();
        assert_eq!(response.chunk_size, 32768);
    }

    #[test]
    fn test_lob_copy_request() {
        let src = vec![1, 2, 3, 4];
        let dest = vec![5, 6, 7, 8];
        let request = LobCopyRequest::new(src, dest, 1, 1, 1000);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobCopy.as_u8());
    }

    #[test]
    fn test_lob_append_request() {
        let src = vec![1, 2, 3, 4];
        let dest = vec![5, 6, 7, 8];
        let request = LobAppendRequest::new(src, dest);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobAppend.as_u8());
    }

    #[test]
    fn test_lob_load_from_file_request() {
        let bfile = vec![1, 2, 3, 4];
        let dest = vec![5, 6, 7, 8];
        let request = LobLoadFromFileRequest::new(bfile, dest, 1, 1, 10000);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobLoadFromFile.as_u8());
    }

    #[test]
    fn test_lob_get_charset_id_request_and_response() {
        let locator = vec![1, 2, 3, 4];
        let request = LobGetCharsetIdRequest::new(locator);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobGetCharsetId.as_u8());

        // 873 = AL32UTF8
        let response = LobGetCharsetIdResponse::parse(&[3, 105]).unwrap();
        assert_eq!(response.charset_id, 873);
    }

    #[test]
    fn test_piece_type() {
        assert_eq!(PieceType::One.code(), 0x00);
        assert_eq!(PieceType::First.code(), 0x01);
        assert_eq!(PieceType::Next.code(), 0x02);
        assert_eq!(PieceType::Last.code(), 0x03);

        assert!(PieceType::First.is_first());
        assert!(!PieceType::Next.is_first());

        assert!(PieceType::Next.is_continuation());
        assert!(PieceType::Last.is_continuation());
        assert!(!PieceType::First.is_continuation());

        assert!(PieceType::Last.is_last());
        assert!(PieceType::One.is_last());
        assert!(!PieceType::Next.is_last());

        assert!(PieceType::First.is_piecewise());
        assert!(!PieceType::One.is_piecewise());
    }

    #[test]
    fn test_piecewise_read_request_first() {
        let locator = vec![1, 2, 3, 4];
        let request = LobPiecewiseReadRequest::first(locator, 1, 32768);

        let encoded = request.encode();
        assert_eq!(encoded[0], FunctionCode::LobRead.as_u8());
        assert_eq!(encoded[1], PieceType::First.code());
    }

    #[test]
    fn test_piecewise_read_request_next() {
        let locator = vec![1, 2, 3, 4];
        let poll_ctx = vec![10, 20, 30];
        let request = LobPiecewiseReadRequest::next(locator, poll_ctx, 32768);

        assert_eq!(request.piece_type, PieceType::Next);
        assert!(request.poll_context.is_some());
    }

    #[test]
    fn test_piecewise_write_request() {
        let locator = vec![1, 2, 3, 4];
        let data = b"Hello, World!".to_vec();

        let first = LobPiecewiseWriteRequest::first(locator.clone(), 1, data.clone());
        assert_eq!(first.piece_type, PieceType::First);

        let poll_ctx = vec![10, 20];
        let next = LobPiecewiseWriteRequest::next(locator.clone(), poll_ctx.clone(), data.clone());
        assert_eq!(next.piece_type, PieceType::Next);

        let last = LobPiecewiseWriteRequest::last(locator, poll_ctx, data);
        assert_eq!(last.piece_type, PieceType::Last);
    }

    #[test]
    fn test_piecewise_state_read() {
        let locator = vec![1, 2, 3, 4];
        let mut state = LobPiecewiseState::new_read(locator, 1);

        assert!(state.is_read);
        assert!(!state.is_complete);
        assert_eq!(state.next_piece_type(), PieceType::First);

        // Simulate first response
        let response = LobPiecewiseReadResponse {
            data: vec![0; 1000],
            amount_read: 1000,
            piece_type: PieceType::First,
            poll_context: Some(vec![1, 2, 3]),
            has_more: true,
        };
        state.process_read_response(&response);

        assert_eq!(state.bytes_processed, 1000);
        assert_eq!(state.next_piece_type(), PieceType::Next);
        assert!(!state.is_complete);
    }

    #[test]
    fn test_piecewise_state_write() {
        let locator = vec![1, 2, 3, 4];
        let mut state = LobPiecewiseState::new_write(locator, 1);

        assert!(!state.is_read);
        assert!(!state.is_complete);

        let req = state.next_write_request(b"chunk1".to_vec(), false);
        assert_eq!(req.piece_type, PieceType::First);

        // Simulate response
        let response = LobPiecewiseWriteResponse {
            amount_written: 6,
            piece_type: PieceType::First,
            poll_context: Some(vec![1, 2]),
            is_complete: false,
        };
        state.process_write_response(&response);

        assert_eq!(state.bytes_processed, 6);
        assert!(!state.is_complete);

        // Next request should be NEXT type
        let req2 = state.next_write_request(b"chunk2".to_vec(), false);
        assert_eq!(req2.piece_type, PieceType::Next);
    }

    #[test]
    fn test_piecewise_reader() {
        let locator = vec![1, 2, 3, 4];
        let mut reader = LobPiecewiseReader::new(locator, 1, 1024).with_total_length(5000);

        assert!(!reader.is_complete());
        assert_eq!(reader.bytes_read(), 0);
        assert!(reader.progress().is_some());

        // Get first request
        let req = reader.next_request().unwrap();
        assert_eq!(req.piece_type, PieceType::First);

        // Process response
        let response = LobPiecewiseReadResponse {
            data: vec![0; 1024],
            amount_read: 1024,
            piece_type: PieceType::First,
            poll_context: Some(vec![1]),
            has_more: true,
        };
        reader.process_response(&response);

        assert_eq!(reader.bytes_read(), 1024);
        assert!((reader.progress().unwrap() - 0.2048).abs() < 0.001);
    }

    #[test]
    fn test_piecewise_writer() {
        let locator = vec![1, 2, 3, 4];
        let mut writer = LobPiecewiseWriter::new(locator, 1, 1024).with_total_length(3000);

        assert!(!writer.is_complete());
        assert_eq!(writer.bytes_written(), 0);
        assert_eq!(writer.chunk_size(), 1024);

        // Create first write request
        let req = writer.next_request(vec![0; 1024], false);
        assert_eq!(req.piece_type, PieceType::First);

        // Process response
        let response = LobPiecewiseWriteResponse {
            amount_written: 1024,
            piece_type: PieceType::First,
            poll_context: Some(vec![1]),
            is_complete: false,
        };
        writer.process_response(&response);

        assert_eq!(writer.bytes_written(), 1024);
    }

    #[test]
    fn test_split_into_chunks() {
        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

        let chunks = split_into_chunks(&data, 3);
        assert_eq!(chunks.len(), 4);
        assert_eq!(chunks[0], &[1, 2, 3]);
        assert_eq!(chunks[1], &[4, 5, 6]);
        assert_eq!(chunks[2], &[7, 8, 9]);
        assert_eq!(chunks[3], &[10]);

        let chunks = split_into_chunks(&data, 5);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0], &[1, 2, 3, 4, 5]);
        assert_eq!(chunks[1], &[6, 7, 8, 9, 10]);

        let chunks = split_into_chunks(&data, 100);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0], &data[..]);

        let empty: Vec<u8> = vec![];
        let chunks = split_into_chunks(&empty, 10);
        assert!(chunks.is_empty());
    }
}
