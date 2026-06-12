//! MongoDB wire protocol reading helpers.
//!
//! Extension traits for reading MongoDB-specific data types.

use crate::error::MongoWireError;
use crate::{MAX_BSON_DOCUMENT_SIZE, MAX_BSON_STRING_SIZE};
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

fn checked_document_len(len: i32) -> Result<usize, MongoWireError> {
    if len < 0 {
        return Err(MongoWireError::InvalidBson("negative document length".into()));
    }
    if len < 5 {
        return Err(MongoWireError::InvalidBson("document too short".into()));
    }
    let len = len as usize;
    if len > MAX_BSON_DOCUMENT_SIZE {
        return Err(MongoWireError::DocumentTooLarge { length: len, max: MAX_BSON_DOCUMENT_SIZE });
    }
    Ok(len)
}

fn checked_string_len(len: i32) -> Result<usize, MongoWireError> {
    if len < 0 {
        return Err(MongoWireError::InvalidBson("negative string length".into()));
    }
    if len < 1 {
        return Err(MongoWireError::InvalidBson("string too short".into()));
    }
    let len = len as usize;
    if len > MAX_BSON_STRING_SIZE {
        return Err(MongoWireError::StringTooLarge { length: len, max: MAX_BSON_STRING_SIZE });
    }
    Ok(len)
}

fn checked_binary_len(len: i32) -> Result<usize, MongoWireError> {
    if len < 0 {
        return Err(MongoWireError::InvalidBson("negative binary length".into()));
    }
    let len = len as usize;
    if len > MAX_BSON_DOCUMENT_SIZE {
        return Err(MongoWireError::InvalidBson(
            format!("binary length {} exceeds maximum {}", len, MAX_BSON_DOCUMENT_SIZE).into(),
        ));
    }
    Ok(len)
}

/// Extension trait for synchronous MongoDB wire protocol reading.
pub trait MongoReadSyncExt: WireReadSync
where
    Self::ReadError: Into<MongoWireError>,
{
    /// Read a BSON document, returning the raw bytes including length prefix.
    ///
    /// # Validation
    /// - Document must be at least 5 bytes (length prefix + null terminator)
    /// - Document length must not exceed MAX_BSON_DOCUMENT_SIZE
    #[inline]
    fn read_bson_document_sync(&self) -> Result<Vec<u8>, MongoWireError> {
        let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
        let len = checked_document_len(len_i32)?;

        // Use conservative initial capacity to prevent allocation DoS attacks.
        let initial_capacity = len.min(64 * 1024);
        let mut doc = Vec::with_capacity(initial_capacity);
        doc.extend_from_slice(&len_i32.to_le_bytes());

        let remaining = self.read_bytes_sync(len - 4).map_err(Into::into)?;
        doc.extend_from_slice(&remaining);

        // BSON documents MUST end with a null terminator
        if doc.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }

        Ok(doc)
    }

    /// Read a length-prefixed BSON string (includes null terminator in length).
    #[inline]
    fn read_bson_string_sync(&self) -> Result<Vec<u8>, MongoWireError> {
        let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
        let len = checked_string_len(len_i32)?;

        let data = self.read_bytes_sync(len).map_err(Into::into)?;

        // BSON strings MUST end with a null terminator
        if data.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }

        // Return without the null terminator
        Ok(data[..data.len() - 1].to_vec())
    }

    /// Read a BSON binary value.
    #[inline]
    fn read_bson_binary_sync(&self) -> Result<(u8, Vec<u8>), MongoWireError> {
        let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
        let len = checked_binary_len(len_i32)?;

        let borrow = self.peek_exactly::<1>().map_err(Into::into)?;
        let subtype = borrow[0];
        self.accept_exactly(&borrow).map_err(Into::into)?;

        let data = self.read_bytes_sync(len).map_err(Into::into)?;
        Ok((subtype, data.to_vec()))
    }

    /// Read a BSON ObjectId (12 bytes).
    #[inline]
    fn read_bson_object_id_sync(&self) -> Result<[u8; 12], MongoWireError> {
        let borrow = self.peek_exactly::<12>().map_err(Into::into)?;
        let oid = *borrow;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(oid)
    }

    /// Read a BSON boolean.
    #[inline]
    fn read_bson_boolean_sync(&self) -> Result<bool, MongoWireError> {
        let borrow = self.peek_exactly::<1>().map_err(Into::into)?;
        let value = borrow[0] != 0;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON UTC datetime (i64 milliseconds since epoch).
    #[inline]
    fn read_bson_datetime_sync(&self) -> Result<i64, MongoWireError> {
        self.read_i64_le_sync().map_err(Into::into)
    }

    /// Read a BSON timestamp (u64: increment << 32 | seconds).
    #[inline]
    fn read_bson_timestamp_sync(&self) -> Result<u64, MongoWireError> {
        let borrow = self.peek_exactly::<8>().map_err(Into::into)?;
        let value = u64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON Decimal128 (16 bytes).
    #[inline]
    fn read_bson_decimal128_sync(&self) -> Result<[u8; 16], MongoWireError> {
        let borrow = self.peek_exactly::<16>().map_err(Into::into)?;
        let value = *borrow;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON double (f64).
    #[inline]
    fn read_bson_double_sync(&self) -> Result<f64, MongoWireError> {
        let borrow = self.peek_exactly::<8>().map_err(Into::into)?;
        let value = f64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Skip a BSON element based on its type tag.
    /// Returns the number of bytes skipped (not including the tag).
    fn skip_bson_element_sync(&self, element_type: u8) -> Result<usize, MongoWireError> {
        use crate::write::bson_type;

        let skipped = match element_type {
            bson_type::DOUBLE => {
                self.advance_by(8).map_err(Into::into)?;
                8
            }
            bson_type::STRING | bson_type::JAVASCRIPT | bson_type::SYMBOL => {
                let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
                let len = checked_string_len(len_i32)?;
                self.advance_by(len).map_err(Into::into)?;
                4 + len
            }
            bson_type::DOCUMENT | bson_type::ARRAY => {
                let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
                let len = checked_document_len(len_i32)?;
                self.advance_by(len - 4).map_err(Into::into)?; // length includes itself
                len
            }
            bson_type::BINARY => {
                let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
                let len = checked_binary_len(len_i32)?;
                self.advance_by(1 + len).map_err(Into::into)?; // subtype + data
                5 + len
            }
            bson_type::UNDEFINED | bson_type::NULL | bson_type::MIN_KEY | bson_type::MAX_KEY => 0,
            bson_type::OBJECT_ID => {
                self.advance_by(12).map_err(Into::into)?;
                12
            }
            bson_type::BOOLEAN => {
                self.advance_by(1).map_err(Into::into)?;
                1
            }
            bson_type::UTC_DATETIME | bson_type::TIMESTAMP | bson_type::INT64 => {
                self.advance_by(8).map_err(Into::into)?;
                8
            }
            bson_type::REGEX => {
                // Two cstrings: pattern and options
                let pattern = self.read_cstring_sync().map_err(Into::into)?;
                let p_len = match &pattern {
                    Ok(p) => p.len() + 1,
                    Err(p) => p.len(),
                };
                let options = self.read_cstring_sync().map_err(Into::into)?;
                let o_len = match &options {
                    Ok(o) => o.len() + 1,
                    Err(o) => o.len(),
                };
                p_len + o_len
            }
            bson_type::DB_POINTER => {
                let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
                let len = checked_string_len(len_i32)?;
                self.advance_by(len + 12).map_err(Into::into)?; // string + ObjectId
                4 + len + 12
            }
            bson_type::JAVASCRIPT_WITH_SCOPE => {
                let len_i32 = self.read_i32_le_sync().map_err(Into::into)?;
                let len = checked_document_len(len_i32)?;
                self.advance_by(len - 4).map_err(Into::into)?;
                len
            }
            bson_type::INT32 => {
                self.advance_by(4).map_err(Into::into)?;
                4
            }
            bson_type::DECIMAL128 => {
                self.advance_by(16).map_err(Into::into)?;
                16
            }
            _ => {
                return Err(MongoWireError::InvalidBson(format!("unknown BSON element type: 0x{:02X}", element_type).into()));
            }
        };

        Ok(skipped)
    }
}

impl<T> MongoReadSyncExt for T
where
    T: WireReadSync + ?Sized,
    T::ReadError: Into<MongoWireError>,
{
}

/// Extension trait for asynchronous MongoDB wire protocol reading.
pub trait MongoReadExt: WireRead
where
    Self::ReadError: Into<MongoWireError>,
{
    /// Read a BSON document asynchronously.
    async fn read_bson_document(&self) -> Result<Vec<u8>, MongoWireError> {
        let len_i32 = self.read_i32_le().await.map_err(Into::into)?;
        let len = checked_document_len(len_i32)?;

        // Use conservative initial capacity to prevent allocation DoS attacks.
        let initial_capacity = len.min(64 * 1024);
        let mut doc = Vec::with_capacity(initial_capacity);
        doc.extend_from_slice(&len_i32.to_le_bytes());

        let remaining = self.peek_read(Some(len - 4)).await.map_err(Into::into)?;
        doc.extend_from_slice(&remaining);
        self.accept(&remaining, None).map_err(Into::into)?;

        // BSON documents MUST end with a null terminator
        if doc.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }

        Ok(doc)
    }

    /// Read a length-prefixed BSON string asynchronously.
    async fn read_bson_string(&self) -> Result<Vec<u8>, MongoWireError> {
        let len_i32 = self.read_i32_le().await.map_err(Into::into)?;
        let len = checked_string_len(len_i32)?;

        let data = self.peek_read(Some(len)).await.map_err(Into::into)?;
        self.accept(&data, None).map_err(Into::into)?;

        // BSON strings MUST end with a null terminator
        if data.last() != Some(&0) {
            return Err(MongoWireError::MissingNullTerminator);
        }

        // Return without the null terminator
        Ok(data[..data.len() - 1].to_vec())
    }

    /// Read a BSON ObjectId asynchronously.
    async fn read_bson_object_id(&self) -> Result<[u8; 12], MongoWireError> {
        let borrow = self.peek_read_exactly::<12>().await.map_err(Into::into)?;
        let oid = *borrow;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(oid)
    }

    /// Read a BSON double asynchronously.
    async fn read_bson_double(&self) -> Result<f64, MongoWireError> {
        let borrow = self.peek_read_exactly::<8>().await.map_err(Into::into)?;
        let value = f64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON binary value asynchronously.
    async fn read_bson_binary(&self) -> Result<(u8, Vec<u8>), MongoWireError> {
        let len_i32 = self.read_i32_le().await.map_err(Into::into)?;
        let len = checked_binary_len(len_i32)?;

        let subtype_borrow = self.peek_read_exactly::<1>().await.map_err(Into::into)?;
        let subtype = subtype_borrow[0];
        self.accept_exactly(&subtype_borrow).map_err(Into::into)?;

        let data = self.peek_read(Some(len)).await.map_err(Into::into)?;
        let result = data.to_vec();
        self.accept(&data, None).map_err(Into::into)?;

        Ok((subtype, result))
    }

    /// Read a BSON boolean asynchronously.
    async fn read_bson_boolean(&self) -> Result<bool, MongoWireError> {
        let borrow = self.peek_read_exactly::<1>().await.map_err(Into::into)?;
        let value = borrow[0] != 0;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON UTC datetime asynchronously (i64 milliseconds since epoch).
    async fn read_bson_datetime(&self) -> Result<i64, MongoWireError> {
        self.read_i64_le().await.map_err(Into::into)
    }

    /// Read a BSON timestamp asynchronously (u64: increment << 32 | seconds).
    async fn read_bson_timestamp(&self) -> Result<u64, MongoWireError> {
        let borrow = self.peek_read_exactly::<8>().await.map_err(Into::into)?;
        let value = u64::from_le_bytes(*borrow);
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON Decimal128 asynchronously (16 bytes).
    async fn read_bson_decimal128(&self) -> Result<[u8; 16], MongoWireError> {
        let borrow = self.peek_read_exactly::<16>().await.map_err(Into::into)?;
        let value = *borrow;
        self.accept_exactly(&borrow).map_err(Into::into)?;
        Ok(value)
    }

    /// Read a BSON int32 asynchronously.
    async fn read_bson_int32(&self) -> Result<i32, MongoWireError> {
        self.read_i32_le().await.map_err(Into::into)
    }

    /// Read a BSON int64 asynchronously.
    async fn read_bson_int64(&self) -> Result<i64, MongoWireError> {
        self.read_i64_le().await.map_err(Into::into)
    }
}

impl<T> MongoReadExt for T
where
    T: WireRead + ?Sized,
    T::ReadError: Into<MongoWireError>,
{
}

// ============================================================================
// Message Buffer Validation
// ============================================================================

use crate::header::{MessageHeader, OpCode};
#[allow(deprecated)]
use crate::{MAX_MESSAGE_SIZE, OpCompressed, OpDelete, OpGetMore, OpInsert, OpKillCursors, OpMsg, OpQuery, OpReply, OpUpdate};

/// Result of validating a message buffer.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ValidationResult {
    /// Buffer contains a valid, complete message.
    Valid {
        /// Total message length including header.
        message_length: usize,
        /// The opcode of the message.
        opcode: Option<OpCode>,
    },
    /// Buffer is incomplete - need more bytes.
    Incomplete {
        /// Number of bytes needed for a complete message.
        needed: usize,
        /// Number of bytes currently available.
        available: usize,
    },
    /// Buffer contains invalid data.
    Invalid(ValidationError),
}

/// Validation errors.
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ValidationError {
    /// Message length is negative.
    NegativeLength,
    /// Message length is too small (< 16 bytes).
    LengthTooSmall,
    /// Message length exceeds maximum.
    LengthTooLarge,
    /// Unknown or invalid opcode.
    UnknownOpcode(i32),
}

impl ValidationResult {
    /// Returns true if the message is valid and complete.
    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid { .. })
    }

    /// Returns true if more bytes are needed.
    pub fn is_incomplete(&self) -> bool {
        matches!(self, Self::Incomplete { .. })
    }

    /// Returns true if the buffer contains invalid data.
    pub fn is_invalid(&self) -> bool {
        matches!(self, Self::Invalid(_))
    }
}

/// Validate a message buffer without fully parsing it.
///
/// This performs quick validation to check:
/// 1. Buffer has at least 16 bytes for the header
/// 2. Message length is valid (>= 16, <= MAX_MESSAGE_SIZE)
/// 3. Opcode is recognized
/// 4. Buffer contains the complete message
///
/// # Returns
/// - `Valid` if the buffer contains a complete, valid message
/// - `Incomplete` if more bytes are needed
/// - `Invalid` if the buffer contains invalid data
///
/// # Example
/// ```
/// use mongo_wire::{validate_message_buffer, ValidationResult};
///
/// let buffer = vec![0u8; 10]; // Too short
/// match validate_message_buffer(&buffer) {
///     ValidationResult::Incomplete { needed, available } => {
///         println!("Need {} bytes, have {}", needed, available);
///     }
///     _ => {}
/// }
/// ```
pub fn validate_message_buffer(buffer: &[u8]) -> ValidationResult {
    // Need at least 16 bytes for the header
    if buffer.len() < MessageHeader::SIZE {
        return ValidationResult::Incomplete { needed: MessageHeader::SIZE, available: buffer.len() };
    }

    // Read message length
    let message_length = i32::from_le_bytes([buffer[0], buffer[1], buffer[2], buffer[3]]);

    if message_length < 0 {
        return ValidationResult::Invalid(ValidationError::NegativeLength);
    }

    if message_length < MessageHeader::SIZE as i32 {
        return ValidationResult::Invalid(ValidationError::LengthTooSmall);
    }

    let message_length_usize = message_length as usize;
    if message_length_usize > MAX_MESSAGE_SIZE {
        return ValidationResult::Invalid(ValidationError::LengthTooLarge);
    }

    // Check if we have the complete message
    if buffer.len() < message_length_usize {
        return ValidationResult::Incomplete { needed: message_length_usize, available: buffer.len() };
    }

    // Validate opcode
    let opcode_raw = i32::from_le_bytes([buffer[12], buffer[13], buffer[14], buffer[15]]);
    let opcode = OpCode::from_i32(opcode_raw);

    if opcode.is_none() {
        return ValidationResult::Invalid(ValidationError::UnknownOpcode(opcode_raw));
    }

    ValidationResult::Valid { message_length: message_length_usize, opcode }
}

/// Check if a buffer contains a complete message.
///
/// This is a convenience function that returns `Some(length)` if the buffer
/// contains a complete message, or `None` if more bytes are needed or the
/// buffer is invalid.
pub fn message_complete(buffer: &[u8]) -> Option<usize> {
    match validate_message_buffer(buffer) {
        ValidationResult::Valid { message_length, .. } => Some(message_length),
        _ => None,
    }
}

// ============================================================================
// Message Framing Helpers
// ============================================================================

/// A complete MongoDB wire protocol message.
#[derive(Clone, Debug)]
#[allow(deprecated)]
pub enum WireMessage {
    /// OP_MSG message (modern format)
    Msg(OpMsg),
    /// OP_QUERY message (deprecated)
    Query(OpQuery),
    /// OP_REPLY message (deprecated)
    Reply(OpReply),
    /// OP_COMPRESSED message (contains a wrapped message)
    Compressed(OpCompressed),
    /// OP_INSERT message (deprecated)
    #[deprecated(since = "0.1.0", note = "OP_INSERT is deprecated; use insert command via OP_MSG")]
    Insert(OpInsert),
    /// OP_UPDATE message (deprecated)
    #[deprecated(since = "0.1.0", note = "OP_UPDATE is deprecated; use update command via OP_MSG")]
    Update(OpUpdate),
    /// OP_DELETE message (deprecated)
    #[deprecated(since = "0.1.0", note = "OP_DELETE is deprecated; use delete command via OP_MSG")]
    Delete(OpDelete),
    /// OP_GET_MORE message (deprecated)
    #[deprecated(since = "0.1.0", note = "OP_GET_MORE is deprecated; use getMore command via OP_MSG")]
    GetMore(OpGetMore),
    /// OP_KILL_CURSORS message (deprecated)
    #[deprecated(since = "0.1.0", note = "OP_KILL_CURSORS is deprecated; use killCursors command via OP_MSG")]
    KillCursors(OpKillCursors),
    /// Unknown or unsupported opcode
    Unknown { header: MessageHeader, body: Vec<u8> },
}

#[allow(deprecated)]
impl WireMessage {
    /// Read a complete message synchronously from a stream.
    ///
    /// This reads the 16-byte header first, then reads the message body
    /// based on the declared length, and parses it according to the opcode.
    pub fn read_sync<S: WireReadSync + ?Sized>(stream: &S) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        let header = MessageHeader::parse_sync(stream).map_err(Into::into)?;
        let body_length = header.body_length()?;

        match header.op_code() {
            Some(OpCode::Msg) => {
                let msg = OpMsg::parse_sync(stream, body_length)?;
                Ok(WireMessage::Msg(msg))
            }
            Some(OpCode::Query) => {
                let query = OpQuery::parse_sync(stream, body_length)?;
                Ok(WireMessage::Query(query))
            }
            Some(OpCode::Reply) => {
                let reply = OpReply::parse_sync(stream, body_length)?;
                Ok(WireMessage::Reply(reply))
            }
            Some(OpCode::Compressed) => {
                let compressed = OpCompressed::parse_sync(stream, &header, body_length)?;
                Ok(WireMessage::Compressed(compressed))
            }
            Some(OpCode::Insert) => {
                let insert = OpInsert::parse_sync(stream, body_length)?;
                Ok(WireMessage::Insert(insert))
            }
            Some(OpCode::Update) => {
                let update = OpUpdate::parse_sync(stream, body_length)?;
                Ok(WireMessage::Update(update))
            }
            Some(OpCode::Delete) => {
                let delete = OpDelete::parse_sync(stream, body_length)?;
                Ok(WireMessage::Delete(delete))
            }
            Some(OpCode::GetMore) => {
                let get_more = OpGetMore::parse_sync(stream, body_length)?;
                Ok(WireMessage::GetMore(get_more))
            }
            Some(OpCode::KillCursors) => {
                let kill_cursors = OpKillCursors::parse_sync(stream, body_length)?;
                Ok(WireMessage::KillCursors(kill_cursors))
            }
            _ => {
                // Unknown opcode - read body as raw bytes
                let body = stream.read_bytes_sync(body_length).map_err(Into::into)?.to_vec();
                Ok(WireMessage::Unknown { header, body })
            }
        }
    }

    /// Read a complete message asynchronously from a stream.
    pub async fn read<S: WireRead + ?Sized>(stream: &S) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        let header = MessageHeader::parse(stream).await.map_err(Into::into)?;
        let body_length = header.body_length()?;

        match header.op_code() {
            Some(OpCode::Msg) => {
                let msg = OpMsg::parse(stream, body_length).await?;
                Ok(WireMessage::Msg(msg))
            }
            Some(OpCode::Query) => {
                let query = OpQuery::parse(stream, body_length).await?;
                Ok(WireMessage::Query(query))
            }
            Some(OpCode::Reply) => {
                let reply = OpReply::parse(stream, body_length).await?;
                Ok(WireMessage::Reply(reply))
            }
            Some(OpCode::Compressed) => {
                let compressed = OpCompressed::parse(stream, &header, body_length).await?;
                Ok(WireMessage::Compressed(compressed))
            }
            Some(OpCode::Insert) => {
                let insert = OpInsert::parse(stream, body_length).await?;
                Ok(WireMessage::Insert(insert))
            }
            Some(OpCode::Update) => {
                let update = OpUpdate::parse(stream, body_length).await?;
                Ok(WireMessage::Update(update))
            }
            Some(OpCode::Delete) => {
                let delete = OpDelete::parse(stream, body_length).await?;
                Ok(WireMessage::Delete(delete))
            }
            Some(OpCode::GetMore) => {
                let get_more = OpGetMore::parse(stream, body_length).await?;
                Ok(WireMessage::GetMore(get_more))
            }
            Some(OpCode::KillCursors) => {
                let kill_cursors = OpKillCursors::parse(stream, body_length).await?;
                Ok(WireMessage::KillCursors(kill_cursors))
            }
            _ => {
                // Unknown opcode - read body as raw bytes
                let borrowed = stream.peek_read(Some(body_length)).await.map_err(Into::into)?;
                let body = borrowed.to_vec();
                stream.accept(&borrowed, None).map_err(Into::into)?;
                Ok(WireMessage::Unknown { header, body })
            }
        }
    }

    /// Get the message header.
    pub fn header(&self) -> Option<&MessageHeader> {
        match self {
            WireMessage::Unknown { header, .. } => Some(header),
            _ => None,
        }
    }

    /// Check if this is an OP_MSG.
    pub fn is_msg(&self) -> bool {
        matches!(self, WireMessage::Msg(_))
    }

    /// Check if this is an OP_QUERY.
    pub fn is_query(&self) -> bool {
        matches!(self, WireMessage::Query(_))
    }

    /// Check if this is an OP_REPLY.
    pub fn is_reply(&self) -> bool {
        matches!(self, WireMessage::Reply(_))
    }

    /// Check if this is an OP_COMPRESSED.
    pub fn is_compressed(&self) -> bool {
        matches!(self, WireMessage::Compressed(_))
    }

    /// Check if this is an OP_INSERT (deprecated).
    #[deprecated(since = "0.1.0", note = "OP_INSERT is deprecated")]
    pub fn is_insert(&self) -> bool {
        matches!(self, WireMessage::Insert(_))
    }

    /// Check if this is an OP_UPDATE (deprecated).
    #[deprecated(since = "0.1.0", note = "OP_UPDATE is deprecated")]
    pub fn is_update(&self) -> bool {
        matches!(self, WireMessage::Update(_))
    }

    /// Check if this is an OP_DELETE (deprecated).
    #[deprecated(since = "0.1.0", note = "OP_DELETE is deprecated")]
    pub fn is_delete(&self) -> bool {
        matches!(self, WireMessage::Delete(_))
    }

    /// Check if this is an OP_GET_MORE (deprecated).
    #[deprecated(since = "0.1.0", note = "OP_GET_MORE is deprecated")]
    pub fn is_get_more(&self) -> bool {
        matches!(self, WireMessage::GetMore(_))
    }

    /// Check if this is an OP_KILL_CURSORS (deprecated).
    #[deprecated(since = "0.1.0", note = "OP_KILL_CURSORS is deprecated")]
    pub fn is_kill_cursors(&self) -> bool {
        matches!(self, WireMessage::KillCursors(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_read_bson_document() {
        // Minimal BSON document: {}
        let data = [
            0x05, 0x00, 0x00, 0x00, // length = 5
            0x00, // terminator
        ];

        let stream = SliceStream::new(&data);
        let doc = stream.read_bson_document_sync().expect("");

        assert_eq!(doc, data);
    }

    #[test]
    fn test_read_bson_string() {
        // BSON string "hello"
        let data = [
            0x06, 0x00, 0x00, 0x00, // length = 6 (includes null)
            b'h', b'e', b'l', b'l', b'o', 0x00,
        ];

        let stream = SliceStream::new(&data);
        let s = stream.read_bson_string_sync().expect("");

        assert_eq!(s, b"hello");
    }

    #[test]
    fn test_read_bson_object_id() {
        let oid = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];
        let stream = SliceStream::new(&oid);

        let result = stream.read_bson_object_id_sync().expect("");
        assert_eq!(result, oid);
    }

    #[test]
    fn test_read_bson_boolean() {
        let stream = SliceStream::new(&[0x01]);
        assert!(stream.read_bson_boolean_sync().expect(""));

        let stream = SliceStream::new(&[0x00]);
        assert!(!stream.read_bson_boolean_sync().expect(""));
    }

    #[test]
    fn test_read_bson_double() {
        let value: f64 = 3.125;
        let bytes = value.to_le_bytes();
        let stream = SliceStream::new(&bytes);

        let result = stream.read_bson_double_sync().expect("");
        assert!((result - value).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_bson_datetime() {
        let timestamp: i64 = 1234567890123;
        let bytes = timestamp.to_le_bytes();
        let stream = SliceStream::new(&bytes);

        let result = stream.read_bson_datetime_sync().expect("");
        assert_eq!(result, timestamp);
    }
}
