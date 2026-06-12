//! MongoDB wire protocol encoding helpers.

use crate::OpCode;
use crate::op_compressed::CompressorId;
use std::io::{self, Write};
use std::sync::atomic::{AtomicI32, Ordering};

// ============================================================================
// Request ID Generator
// ============================================================================

/// Global atomic counter for generating unique request IDs.
static REQUEST_ID_COUNTER: AtomicI32 = AtomicI32::new(1);

/// Generate a unique request ID for MongoDB messages.
///
/// This uses an atomic counter to generate monotonically increasing IDs
/// that are unique within the process lifetime. The counter wraps around
/// at i32::MAX.
///
/// # Example
/// ```
/// use mongo_wire::next_request_id;
///
/// let id1 = next_request_id();
/// let id2 = next_request_id();
/// assert_ne!(id1, id2);
/// ```
#[inline]
pub fn next_request_id() -> i32 {
    REQUEST_ID_COUNTER.fetch_add(1, Ordering::Relaxed)
}

/// Reset the request ID counter (primarily for testing).
#[inline]
pub fn reset_request_id_counter(value: i32) {
    REQUEST_ID_COUNTER.store(value, Ordering::Relaxed);
}

/// Write a MongoDB message header.
///
/// Format: [length: i32][request_id: i32][response_to: i32][opcode: i32]
#[inline]
pub fn write_header(w: &mut impl Write, message_length: i32, request_id: i32, response_to: i32, opcode: OpCode) -> io::Result<()> {
    w.write_all(&message_length.to_le_bytes())?;
    w.write_all(&request_id.to_le_bytes())?;
    w.write_all(&response_to.to_le_bytes())?;
    w.write_all(&(opcode as i32).to_le_bytes())
}

/// Write a little-endian i32.
#[inline]
pub fn write_i32_le(w: &mut impl Write, value: i32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a little-endian u32.
#[inline]
pub fn write_u32_le(w: &mut impl Write, value: u32) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a little-endian i64.
#[inline]
pub fn write_i64_le(w: &mut impl Write, value: i64) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a little-endian u64.
#[inline]
pub fn write_u64_le(w: &mut impl Write, value: u64) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a little-endian f64.
#[inline]
pub fn write_f64_le(w: &mut impl Write, value: f64) -> io::Result<()> {
    w.write_all(&value.to_le_bytes())
}

/// Write a null-terminated C-string.
#[inline]
pub fn write_cstring(w: &mut impl Write, s: &[u8]) -> io::Result<()> {
    w.write_all(s)?;
    w.write_all(&[0])
}

/// Write raw bytes.
#[inline]
pub fn write_bytes(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(data)
}

// ============================================================================
// OP_MSG (opcode 2013) - Modern MongoDB protocol
// ============================================================================

/// OP_MSG flag bits.
pub mod msg_flags {
    pub const CHECKSUM_PRESENT: u32 = 1 << 0;
    pub const MORE_TO_COME: u32 = 1 << 1;
    pub const EXHAUST_ALLOWED: u32 = 1 << 16;
}

/// OP_MSG section kinds.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionKind {
    /// Body section (kind 0) - single BSON document
    Body = 0,
    /// Document sequence (kind 1) - multiple BSON documents
    DocumentSequence = 1,
}

/// Write OP_MSG flags.
#[inline]
pub fn write_msg_flags(w: &mut impl Write, flags: u32) -> io::Result<()> {
    w.write_all(&flags.to_le_bytes())
}

/// Write a section kind byte.
#[inline]
pub fn write_section_kind(w: &mut impl Write, kind: SectionKind) -> io::Result<()> {
    w.write_all(&[kind as u8])
}

/// Write a document sequence section header.
///
/// Format: [kind: u8][size: i32][identifier: cstring]
#[inline]
pub fn write_document_sequence_header(w: &mut impl Write, size: i32, identifier: &[u8]) -> io::Result<()> {
    write_section_kind(w, SectionKind::DocumentSequence)?;
    write_i32_le(w, size)?;
    write_cstring(w, identifier)
}

// ============================================================================
// OP_QUERY (opcode 2004) - Legacy protocol
// ============================================================================

/// OP_QUERY flag bits.
pub mod query_flags {
    pub const TAILABLE_CURSOR: u32 = 1 << 1;
    pub const SLAVE_OK: u32 = 1 << 2;
    pub const OPLOG_REPLAY: u32 = 1 << 3;
    pub const NO_CURSOR_TIMEOUT: u32 = 1 << 4;
    pub const AWAIT_DATA: u32 = 1 << 5;
    pub const EXHAUST: u32 = 1 << 6;
    pub const PARTIAL: u32 = 1 << 7;
}

/// Write OP_QUERY structure (after header).
///
/// Format: [flags: i32][collection: cstring][skip: i32][limit: i32][query: document]
#[inline]
pub fn write_query(w: &mut impl Write, flags: u32, collection: &[u8], skip: i32, limit: i32, query_doc: &[u8]) -> io::Result<()> {
    write_u32_le(w, flags)?;
    write_cstring(w, collection)?;
    write_i32_le(w, skip)?;
    write_i32_le(w, limit)?;
    w.write_all(query_doc)
}

// ============================================================================
// BSON encoding helpers
// ============================================================================

/// BSON element type tags.
pub mod bson_type {
    pub const DOUBLE: u8 = 0x01;
    pub const STRING: u8 = 0x02;
    pub const DOCUMENT: u8 = 0x03;
    pub const ARRAY: u8 = 0x04;
    pub const BINARY: u8 = 0x05;
    pub const UNDEFINED: u8 = 0x06; // Deprecated
    pub const OBJECT_ID: u8 = 0x07;
    pub const BOOLEAN: u8 = 0x08;
    pub const UTC_DATETIME: u8 = 0x09;
    pub const NULL: u8 = 0x0A;
    pub const REGEX: u8 = 0x0B;
    pub const DB_POINTER: u8 = 0x0C; // Deprecated
    pub const JAVASCRIPT: u8 = 0x0D;
    pub const SYMBOL: u8 = 0x0E; // Deprecated
    pub const JAVASCRIPT_WITH_SCOPE: u8 = 0x0F; // Deprecated
    pub const INT32: u8 = 0x10;
    pub const TIMESTAMP: u8 = 0x11;
    pub const INT64: u8 = 0x12;
    pub const DECIMAL128: u8 = 0x13;
    pub const MIN_KEY: u8 = 0xFF;
    pub const MAX_KEY: u8 = 0x7F;
}

/// Write a BSON element header (type + cstring key).
#[inline]
pub fn write_bson_element_header(w: &mut impl Write, element_type: u8, key: &[u8]) -> io::Result<()> {
    w.write_all(&[element_type])?;
    write_cstring(w, key)
}

/// Write a BSON string value (length-prefixed UTF-8 with null terminator).
#[inline]
pub fn write_bson_string(w: &mut impl Write, s: &[u8]) -> io::Result<()> {
    // Validate length to prevent integer overflow (s.len() + 1 must fit in i32)
    let len_with_null = s.len().checked_add(1).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "string too large"))?;
    if len_with_null > i32::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "string too large"));
    }
    write_i32_le(w, len_with_null as i32)?;
    w.write_all(s)?;
    w.write_all(&[0])
}

/// Write a BSON binary value.
#[inline]
pub fn write_bson_binary(w: &mut impl Write, subtype: u8, data: &[u8]) -> io::Result<()> {
    // Validate length to prevent integer overflow
    if data.len() > i32::MAX as usize {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "binary data too large"));
    }
    write_i32_le(w, data.len() as i32)?;
    w.write_all(&[subtype])?;
    w.write_all(data)
}

/// Write a BSON boolean value.
#[inline]
pub fn write_bson_boolean(w: &mut impl Write, value: bool) -> io::Result<()> {
    w.write_all(&[if value { 0x01 } else { 0x00 }])
}

/// Write a BSON ObjectId (12 bytes).
#[inline]
pub fn write_bson_object_id(w: &mut impl Write, oid: &[u8; 12]) -> io::Result<()> {
    w.write_all(oid)
}

/// Write BSON document terminator.
#[inline]
pub fn write_bson_document_end(w: &mut impl Write) -> io::Result<()> {
    w.write_all(&[0])
}

// ============================================================================
// BSON Document Builder
// ============================================================================

/// Builder for constructing BSON documents.
///
/// This provides a simple, type-safe way to build BSON documents without
/// manually managing byte offsets.
///
/// # Example
/// ```
/// use mongo_wire::BsonDocBuilder;
///
/// let doc = BsonDocBuilder::new()
///     .string("name", "Alice")
///     .int32("age", 30)
///     .bool("active", true)
///     .build();
///
/// // doc is now a valid BSON document as Vec<u8>
/// ```
pub struct BsonDocBuilder {
    buf: Vec<u8>,
}

impl BsonDocBuilder {
    /// Create a new BSON document builder.
    pub fn new() -> Self {
        let mut buf = Vec::with_capacity(64);
        // Reserve space for document length (4 bytes)
        buf.extend_from_slice(&[0u8; 4]);
        Self { buf }
    }

    /// Add a string field.
    pub fn string(mut self, key: &str, value: &str) -> Self {
        self.buf.push(bson_type::STRING);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0); // key null terminator
        let len = (value.len() + 1) as i32;
        self.buf.extend_from_slice(&len.to_le_bytes());
        self.buf.extend_from_slice(value.as_bytes());
        self.buf.push(0); // value null terminator
        self
    }

    /// Add a 32-bit integer field.
    pub fn int32(mut self, key: &str, value: i32) -> Self {
        self.buf.push(bson_type::INT32);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a 64-bit integer field.
    pub fn int64(mut self, key: &str, value: i64) -> Self {
        self.buf.push(bson_type::INT64);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a double (f64) field.
    pub fn double(mut self, key: &str, value: f64) -> Self {
        self.buf.push(bson_type::DOUBLE);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&value.to_le_bytes());
        self
    }

    /// Add a boolean field.
    pub fn bool(mut self, key: &str, value: bool) -> Self {
        self.buf.push(bson_type::BOOLEAN);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.push(if value { 1 } else { 0 });
        self
    }

    /// Add a null field.
    pub fn null(mut self, key: &str) -> Self {
        self.buf.push(bson_type::NULL);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self
    }

    /// Add an ObjectId field (12 bytes).
    pub fn object_id(mut self, key: &str, oid: &[u8; 12]) -> Self {
        self.buf.push(bson_type::OBJECT_ID);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(oid);
        self
    }

    /// Add a UTC datetime field (milliseconds since epoch).
    pub fn datetime(mut self, key: &str, millis: i64) -> Self {
        self.buf.push(bson_type::UTC_DATETIME);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&millis.to_le_bytes());
        self
    }

    /// Add a binary field.
    pub fn binary(mut self, key: &str, subtype: u8, data: &[u8]) -> Self {
        self.buf.push(bson_type::BINARY);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&(data.len() as i32).to_le_bytes());
        self.buf.push(subtype);
        self.buf.extend_from_slice(data);
        self
    }

    /// Add a nested document field.
    pub fn document(mut self, key: &str, doc: &[u8]) -> Self {
        self.buf.push(bson_type::DOCUMENT);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(doc);
        self
    }

    /// Add an array field (BSON arrays are documents with "0", "1", "2"... keys).
    pub fn array(mut self, key: &str, arr: &[u8]) -> Self {
        self.buf.push(bson_type::ARRAY);
        self.buf.extend_from_slice(key.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(arr);
        self
    }

    /// Add raw BSON element bytes (type + key + value already encoded).
    pub fn raw(mut self, element: &[u8]) -> Self {
        self.buf.extend_from_slice(element);
        self
    }

    /// Build the final BSON document.
    pub fn build(mut self) -> Vec<u8> {
        // Add document terminator
        self.buf.push(0);
        // Write document length at the start
        let len = self.buf.len() as i32;
        self.buf[0..4].copy_from_slice(&len.to_le_bytes());
        self.buf
    }
}

impl Default for BsonDocBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for BSON arrays.
///
/// Arrays in BSON are documents with sequential numeric string keys ("0", "1", etc.).
pub struct BsonArrayBuilder {
    inner: BsonDocBuilder,
    index: usize,
}

impl BsonArrayBuilder {
    /// Create a new BSON array builder.
    pub fn new() -> Self {
        Self { inner: BsonDocBuilder::new(), index: 0 }
    }

    /// Add a string element.
    pub fn string(self, value: &str) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.string(&key, value), index: self.index + 1 }
    }

    /// Add a 32-bit integer element.
    pub fn int32(self, value: i32) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.int32(&key, value), index: self.index + 1 }
    }

    /// Add a 64-bit integer element.
    pub fn int64(self, value: i64) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.int64(&key, value), index: self.index + 1 }
    }

    /// Add a double element.
    pub fn double(self, value: f64) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.double(&key, value), index: self.index + 1 }
    }

    /// Add a boolean element.
    pub fn bool(self, value: bool) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.bool(&key, value), index: self.index + 1 }
    }

    /// Add a null element.
    pub fn null(self) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.null(&key), index: self.index + 1 }
    }

    /// Add a nested document element.
    pub fn document(self, doc: &[u8]) -> Self {
        let key = self.index.to_string();
        Self { inner: self.inner.document(&key, doc), index: self.index + 1 }
    }

    /// Build the final BSON array.
    pub fn build(self) -> Vec<u8> {
        self.inner.build()
    }
}

impl Default for BsonArrayBuilder {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Builder for OP_MSG
// ============================================================================

/// Builder for constructing OP_MSG messages.
pub struct OpMsgBuilder {
    buf: Vec<u8>,
    request_id: i32,
    with_checksum: bool,
}

impl OpMsgBuilder {
    /// Create a new OP_MSG builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        // Write flags (0 for now, will be updated in build())
        buf.extend_from_slice(&0u32.to_le_bytes());
        Self { buf, request_id, with_checksum: false }
    }

    /// Enable checksum generation (CRC-32C).
    pub fn with_checksum(mut self) -> Self {
        self.with_checksum = true;
        self
    }

    /// Add a body section (kind 0) with a BSON document.
    pub fn body(mut self, document: &[u8]) -> Self {
        self.buf.push(SectionKind::Body as u8);
        self.buf.extend_from_slice(document);
        self
    }

    /// Add a document sequence section (kind 1).
    pub fn document_sequence(mut self, identifier: &str, documents: &[&[u8]]) -> Self {
        self.buf.push(SectionKind::DocumentSequence as u8);

        // Calculate section size
        let id_len = identifier.len() + 1; // +1 for null terminator
        let docs_len: usize = documents.iter().map(|d| d.len()).sum();
        let section_size = 4 + id_len + docs_len; // size + identifier + documents

        self.buf.extend_from_slice(&(section_size as i32).to_le_bytes());
        self.buf.extend_from_slice(identifier.as_bytes());
        self.buf.push(0); // null terminator

        for doc in documents {
            self.buf.extend_from_slice(doc);
        }

        self
    }

    /// Build the final message.
    pub fn build(mut self) -> Vec<u8> {
        // Update flags if checksum is enabled
        if self.with_checksum {
            self.buf[16..20].copy_from_slice(&msg_flags::CHECKSUM_PRESENT.to_le_bytes());
        }

        // Calculate message length (including checksum if enabled)
        let checksum_size = if self.with_checksum { 4 } else { 0 };
        let message_length = (self.buf.len() + checksum_size) as i32;

        // Write header at the beginning
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes()); // response_to = 0
        self.buf[12..16].copy_from_slice(&(OpCode::Msg as i32).to_le_bytes());

        // Append checksum if enabled
        if self.with_checksum {
            let checksum = crc32c::crc32c(&self.buf);
            self.buf.extend_from_slice(&checksum.to_le_bytes());
        }

        self.buf
    }
}

// ============================================================================
// Builder for OP_QUERY (deprecated but useful for compatibility)
// ============================================================================

/// Builder for constructing OP_QUERY messages.
pub struct OpQueryBuilder {
    buf: Vec<u8>,
    request_id: i32,
}

impl OpQueryBuilder {
    /// Create a new OP_QUERY builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id }
    }

    /// Build the query message.
    ///
    /// # Arguments
    /// * `flags` - Query flags (use query_flags constants)
    /// * `collection` - Full collection name (e.g., "db.collection")
    /// * `skip` - Number of documents to skip
    /// * `limit` - Number of documents to return (negative for no limit)
    /// * `query_doc` - The BSON query document
    pub fn build(mut self, flags: u32, collection: &str, skip: i32, limit: i32, query_doc: &[u8]) -> Vec<u8> {
        // Write query body
        self.buf.extend_from_slice(&flags.to_le_bytes());
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0); // null terminator
        self.buf.extend_from_slice(&skip.to_le_bytes());
        self.buf.extend_from_slice(&limit.to_le_bytes());
        self.buf.extend_from_slice(query_doc);

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Query as i32).to_le_bytes());

        self.buf
    }

    /// Build the query message with optional return fields selector.
    pub fn build_with_fields(
        mut self,
        flags: u32,
        collection: &str,
        skip: i32,
        limit: i32,
        query_doc: &[u8],
        fields_selector: &[u8],
    ) -> Vec<u8> {
        // Write query body
        self.buf.extend_from_slice(&flags.to_le_bytes());
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0);
        self.buf.extend_from_slice(&skip.to_le_bytes());
        self.buf.extend_from_slice(&limit.to_le_bytes());
        self.buf.extend_from_slice(query_doc);
        self.buf.extend_from_slice(fields_selector);

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Query as i32).to_le_bytes());

        self.buf
    }
}

// ============================================================================
// Builder for OP_REPLY (for server implementations)
// ============================================================================

/// Builder for constructing OP_REPLY messages.
pub struct OpReplyBuilder {
    buf: Vec<u8>,
    request_id: i32,
    response_to: i32,
}

impl OpReplyBuilder {
    /// Create a new OP_REPLY builder.
    pub fn new(request_id: i32, response_to: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id, response_to }
    }

    /// Build the reply message.
    ///
    /// # Arguments
    /// * `flags` - Response flags
    /// * `cursor_id` - Cursor ID for getMore operations
    /// * `starting_from` - Starting position in the cursor
    /// * `documents` - The response documents
    pub fn build(mut self, flags: u32, cursor_id: i64, starting_from: i32, documents: &[&[u8]]) -> Vec<u8> {
        // Write reply body
        self.buf.extend_from_slice(&flags.to_le_bytes());
        self.buf.extend_from_slice(&cursor_id.to_le_bytes());
        self.buf.extend_from_slice(&starting_from.to_le_bytes());
        self.buf.extend_from_slice(&(documents.len() as i32).to_le_bytes());

        for doc in documents {
            self.buf.extend_from_slice(doc);
        }

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&self.response_to.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Reply as i32).to_le_bytes());

        self.buf
    }
}

// ============================================================================
// Builder for OP_COMPRESSED
// ============================================================================

/// Builder for constructing OP_COMPRESSED messages.
///
/// This wraps an existing message (typically OP_MSG) with compression.
pub struct OpCompressedBuilder {
    request_id: i32,
    response_to: i32,
}

impl OpCompressedBuilder {
    /// Create a new OP_COMPRESSED builder.
    pub fn new(request_id: i32) -> Self {
        Self { request_id, response_to: 0 }
    }

    /// Set the response_to field (for reply messages).
    pub fn response_to(mut self, response_to: i32) -> Self {
        self.response_to = response_to;
        self
    }

    /// Compress and build the message using zlib compression.
    ///
    /// # Arguments
    /// * `original_message` - The complete original message (including header)
    ///
    /// # Returns
    /// The compressed message, or an error if compression fails.
    #[cfg(feature = "decompression")]
    pub fn build_zlib(self, original_message: &[u8]) -> io::Result<Vec<u8>> {
        self.build_with_compressor(original_message, CompressorId::Zlib)
    }

    /// Compress and build the message using snappy compression.
    #[cfg(all(feature = "decompression", feature = "snappy"))]
    pub fn build_snappy(self, original_message: &[u8]) -> io::Result<Vec<u8>> {
        self.build_with_compressor(original_message, CompressorId::Snappy)
    }

    /// Compress and build the message using zstd compression.
    #[cfg(feature = "decompression")]
    pub fn build_zstd(self, original_message: &[u8]) -> io::Result<Vec<u8>> {
        self.build_with_compressor(original_message, CompressorId::Zstd)
    }

    /// Build the message with a specific compressor.
    #[cfg(feature = "decompression")]
    fn build_with_compressor(self, original_message: &[u8], compressor: CompressorId) -> io::Result<Vec<u8>> {
        use crate::header::MessageHeader;

        if original_message.len() < MessageHeader::SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "message too short for header"));
        }

        // Extract original opcode from the message header
        let original_opcode = i32::from_le_bytes([
            original_message[12],
            original_message[13],
            original_message[14],
            original_message[15],
        ]);

        // The body to compress is everything after the header
        let body = &original_message[MessageHeader::SIZE..];
        let uncompressed_size = body.len() as i32;

        // Compress the body
        let compressed_data = match compressor {
            CompressorId::Zlib => {
                use flate2::Compression;
                use flate2::write::ZlibEncoder;

                let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
                encoder.write_all(body)?;
                encoder.finish()?
            }
            #[cfg(feature = "snappy")]
            CompressorId::Snappy => snap::raw::Encoder::new().compress_vec(body).map_err(io::Error::other)?,
            CompressorId::Zstd => zstd::encode_all(body, 0).map_err(io::Error::other)?,
            CompressorId::Noop => body.to_vec(),
            #[cfg(not(feature = "snappy"))]
            CompressorId::Snappy => {
                return Err(io::Error::new(io::ErrorKind::Unsupported, "snappy support not enabled"));
            }
        };

        // Build the compressed message
        // Header (16) + original_opcode (4) + uncompressed_size (4) + compressor_id (1) + compressed_data
        let body_length = 4 + 4 + 1 + compressed_data.len();
        let message_length = (MessageHeader::SIZE + body_length) as i32;

        let mut buf = Vec::with_capacity(message_length as usize);

        // Write header
        buf.extend_from_slice(&message_length.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.extend_from_slice(&self.response_to.to_le_bytes());
        buf.extend_from_slice(&(OpCode::Compressed as i32).to_le_bytes());

        // Write compressed message fields
        buf.extend_from_slice(&original_opcode.to_le_bytes());
        buf.extend_from_slice(&uncompressed_size.to_le_bytes());
        buf.push(compressor as u8);
        buf.extend_from_slice(&compressed_data);

        Ok(buf)
    }

    /// Build a no-op "compressed" message (no actual compression).
    ///
    /// Useful for testing or when compression overhead exceeds benefit.
    pub fn build_noop(self, original_message: &[u8]) -> io::Result<Vec<u8>> {
        use crate::header::MessageHeader;

        if original_message.len() < MessageHeader::SIZE {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "message too short for header"));
        }

        let original_opcode = i32::from_le_bytes([
            original_message[12],
            original_message[13],
            original_message[14],
            original_message[15],
        ]);

        let body = &original_message[MessageHeader::SIZE..];
        let uncompressed_size = body.len() as i32;

        let body_length = 4 + 4 + 1 + body.len();
        let message_length = (MessageHeader::SIZE + body_length) as i32;

        let mut buf = Vec::with_capacity(message_length as usize);

        buf.extend_from_slice(&message_length.to_le_bytes());
        buf.extend_from_slice(&self.request_id.to_le_bytes());
        buf.extend_from_slice(&self.response_to.to_le_bytes());
        buf.extend_from_slice(&(OpCode::Compressed as i32).to_le_bytes());

        buf.extend_from_slice(&original_opcode.to_le_bytes());
        buf.extend_from_slice(&uncompressed_size.to_le_bytes());
        buf.push(CompressorId::Noop as u8);
        buf.extend_from_slice(body);

        Ok(buf)
    }
}

// ============================================================================
// CRC-32C Checksum Utility
// ============================================================================

/// Compute CRC-32C checksum for MongoDB message checksums.
///
/// This is the checksum algorithm used by OP_MSG when the CHECKSUM_PRESENT
/// flag is set.
///
/// # Example
/// ```
/// use mongo_wire::compute_checksum;
///
/// let data = b"hello world";
/// let checksum = compute_checksum(data);
/// ```
#[inline]
pub fn compute_checksum(data: &[u8]) -> u32 {
    crc32c::crc32c(data)
}

// ============================================================================
// Deprecated Op Builders
// ============================================================================

/// Builder for constructing OP_INSERT messages (deprecated).
///
/// OP_INSERT is deprecated in MongoDB 3.6+. Use OP_MSG with insert command instead.
#[deprecated(since = "0.1.0", note = "OP_INSERT is deprecated; use OpMsgBuilder with insert command")]
pub struct OpInsertBuilder {
    buf: Vec<u8>,
    request_id: i32,
}

#[allow(deprecated)]
impl OpInsertBuilder {
    /// Create a new OP_INSERT builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id }
    }

    /// Build the insert message.
    ///
    /// # Arguments
    /// * `flags` - Insert flags (use op_insert::flags constants)
    /// * `collection` - Full collection name (e.g., "db.collection")
    /// * `documents` - The BSON documents to insert
    pub fn build(mut self, flags: u32, collection: &str, documents: &[&[u8]]) -> Vec<u8> {
        // Write flags
        self.buf.extend_from_slice(&flags.to_le_bytes());

        // Write collection name (null-terminated)
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0);

        // Write documents
        for doc in documents {
            self.buf.extend_from_slice(doc);
        }

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Insert as i32).to_le_bytes());

        self.buf
    }
}

/// Builder for constructing OP_UPDATE messages (deprecated).
///
/// OP_UPDATE is deprecated in MongoDB 3.6+. Use OP_MSG with update command instead.
#[deprecated(since = "0.1.0", note = "OP_UPDATE is deprecated; use OpMsgBuilder with update command")]
pub struct OpUpdateBuilder {
    buf: Vec<u8>,
    request_id: i32,
}

#[allow(deprecated)]
impl OpUpdateBuilder {
    /// Create a new OP_UPDATE builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id }
    }

    /// Build the update message.
    ///
    /// # Arguments
    /// * `collection` - Full collection name (e.g., "db.collection")
    /// * `flags` - Update flags (use op_update::flags constants)
    /// * `selector` - The BSON query selector document
    /// * `update` - The BSON update document
    pub fn build(mut self, collection: &str, flags: u32, selector: &[u8], update: &[u8]) -> Vec<u8> {
        // Write ZERO (reserved)
        self.buf.extend_from_slice(&0i32.to_le_bytes());

        // Write collection name (null-terminated)
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0);

        // Write flags
        self.buf.extend_from_slice(&flags.to_le_bytes());

        // Write selector and update documents
        self.buf.extend_from_slice(selector);
        self.buf.extend_from_slice(update);

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Update as i32).to_le_bytes());

        self.buf
    }
}

/// Builder for constructing OP_DELETE messages (deprecated).
///
/// OP_DELETE is deprecated in MongoDB 3.6+. Use OP_MSG with delete command instead.
#[deprecated(since = "0.1.0", note = "OP_DELETE is deprecated; use OpMsgBuilder with delete command")]
pub struct OpDeleteBuilder {
    buf: Vec<u8>,
    request_id: i32,
}

#[allow(deprecated)]
impl OpDeleteBuilder {
    /// Create a new OP_DELETE builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(256);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id }
    }

    /// Build the delete message.
    ///
    /// # Arguments
    /// * `collection` - Full collection name (e.g., "db.collection")
    /// * `flags` - Delete flags (use op_delete::flags constants)
    /// * `selector` - The BSON query selector document
    pub fn build(mut self, collection: &str, flags: u32, selector: &[u8]) -> Vec<u8> {
        // Write ZERO (reserved)
        self.buf.extend_from_slice(&0i32.to_le_bytes());

        // Write collection name (null-terminated)
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0);

        // Write flags
        self.buf.extend_from_slice(&flags.to_le_bytes());

        // Write selector document
        self.buf.extend_from_slice(selector);

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::Delete as i32).to_le_bytes());

        self.buf
    }
}

/// Builder for constructing OP_GET_MORE messages (deprecated).
///
/// OP_GET_MORE is deprecated in MongoDB 3.6+. Use OP_MSG with getMore command instead.
#[deprecated(since = "0.1.0", note = "OP_GET_MORE is deprecated; use OpMsgBuilder with getMore command")]
pub struct OpGetMoreBuilder {
    buf: Vec<u8>,
    request_id: i32,
}

#[allow(deprecated)]
impl OpGetMoreBuilder {
    /// Create a new OP_GET_MORE builder.
    pub fn new(request_id: i32) -> Self {
        let mut buf = Vec::with_capacity(64);
        // Reserve space for header (16 bytes)
        buf.extend_from_slice(&[0u8; 16]);
        Self { buf, request_id }
    }

    /// Build the getMore message.
    ///
    /// # Arguments
    /// * `collection` - Full collection name (e.g., "db.collection")
    /// * `number_to_return` - Number of documents to return
    /// * `cursor_id` - The cursor ID from a previous OP_REPLY
    pub fn build(mut self, collection: &str, number_to_return: i32, cursor_id: i64) -> Vec<u8> {
        // Write ZERO (reserved)
        self.buf.extend_from_slice(&0i32.to_le_bytes());

        // Write collection name (null-terminated)
        self.buf.extend_from_slice(collection.as_bytes());
        self.buf.push(0);

        // Write number to return
        self.buf.extend_from_slice(&number_to_return.to_le_bytes());

        // Write cursor ID
        self.buf.extend_from_slice(&cursor_id.to_le_bytes());

        let message_length = self.buf.len() as i32;

        // Write header
        self.buf[0..4].copy_from_slice(&message_length.to_le_bytes());
        self.buf[4..8].copy_from_slice(&self.request_id.to_le_bytes());
        self.buf[8..12].copy_from_slice(&0i32.to_le_bytes());
        self.buf[12..16].copy_from_slice(&(OpCode::GetMore as i32).to_le_bytes());

        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_header() {
        let mut buf = Vec::new();
        write_header(&mut buf, 100, 1, 0, OpCode::Msg).expect("");

        assert_eq!(buf.len(), 16);
        assert_eq!(&buf[0..4], &100i32.to_le_bytes());
        assert_eq!(&buf[4..8], &1i32.to_le_bytes());
        assert_eq!(&buf[8..12], &0i32.to_le_bytes());
        assert_eq!(&buf[12..16], &2013u32.to_le_bytes());
    }

    #[test]
    fn test_write_cstring() {
        let mut buf = Vec::new();
        write_cstring(&mut buf, b"test").expect("");
        assert_eq!(buf, b"test\0");
    }

    #[test]
    fn test_write_bson_string() {
        let mut buf = Vec::new();
        write_bson_string(&mut buf, b"hello").expect("");

        assert_eq!(&buf[0..4], &6i32.to_le_bytes()); // length = 5 + 1
        assert_eq!(&buf[4..9], b"hello");
        assert_eq!(buf[9], 0);
    }

    #[test]
    fn test_op_msg_builder() {
        // Simple BSON document: { "ping": 1 }
        let doc = [
            16, 0, 0, 0,    // document length
            0x10, // int32 type
            b'p', b'i', b'n', b'g', 0, // key "ping"
            1, 0, 0, 0, // value 1
            0, // document end
        ];

        let msg = OpMsgBuilder::new(42).body(&doc).build();

        // Check header
        let len = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(len, msg.len() as i32);

        let opcode = u32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, 2013);
    }

    #[test]
    fn test_write_bson_element() {
        let mut buf = Vec::new();
        write_bson_element_header(&mut buf, bson_type::INT32, b"count").expect("");
        write_i32_le(&mut buf, 42).expect("");

        assert_eq!(buf[0], bson_type::INT32);
        assert_eq!(&buf[1..6], b"count");
        assert_eq!(buf[6], 0); // null terminator
        assert_eq!(&buf[7..11], &42i32.to_le_bytes());
    }

    #[test]
    fn test_next_request_id() {
        let id1 = next_request_id();
        let id2 = next_request_id();
        let id3 = next_request_id();

        // IDs should be monotonically increasing
        assert!(id2 > id1);
        assert!(id3 > id2);
    }

    #[test]
    fn test_op_compressed_builder_noop() {
        // Build an original OP_MSG
        let doc = [
            5, 0, 0, 0, // document length (minimal empty doc)
            0, // document end
        ];

        let original = OpMsgBuilder::new(1).body(&doc).build();

        // Wrap it with OP_COMPRESSED (noop)
        let compressed = OpCompressedBuilder::new(2).build_noop(&original).expect("noop should work");

        // Verify header
        let message_length = i32::from_le_bytes([compressed[0], compressed[1], compressed[2], compressed[3]]);
        assert_eq!(message_length as usize, compressed.len());

        let opcode = i32::from_le_bytes([compressed[12], compressed[13], compressed[14], compressed[15]]);
        assert_eq!(opcode, OpCode::Compressed as i32);

        // Verify original opcode is OP_MSG
        let original_opcode = i32::from_le_bytes([compressed[16], compressed[17], compressed[18], compressed[19]]);
        assert_eq!(original_opcode, OpCode::Msg as i32);

        // Verify compressor ID is Noop (0)
        assert_eq!(compressed[24], CompressorId::Noop as u8);
    }

    #[test]
    fn test_compute_checksum() {
        let data = b"hello world";
        let checksum = compute_checksum(data);
        // CRC-32C of "hello world"
        assert_eq!(checksum, 0xc99465aa);
    }

    #[test]
    #[allow(deprecated)]
    fn test_op_insert_builder() {
        let doc = [5, 0, 0, 0, 0]; // minimal empty doc

        let msg = OpInsertBuilder::new(1).build(0, "test.collection", &[&doc]);

        // Check header
        let message_length = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(message_length as usize, msg.len());

        let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, OpCode::Insert as i32);

        // Check flags
        let flags = u32::from_le_bytes([msg[16], msg[17], msg[18], msg[19]]);
        assert_eq!(flags, 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_op_update_builder() {
        let selector = [5, 0, 0, 0, 0]; // minimal empty doc
        let update = [5, 0, 0, 0, 0]; // minimal empty doc

        let msg = OpUpdateBuilder::new(1).build("test.collection", 0, &selector, &update);

        // Check header
        let message_length = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(message_length as usize, msg.len());

        let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, OpCode::Update as i32);

        // Check ZERO reserved field
        let zero = i32::from_le_bytes([msg[16], msg[17], msg[18], msg[19]]);
        assert_eq!(zero, 0);
    }

    #[test]
    #[allow(deprecated)]
    fn test_op_delete_builder() {
        let selector = [5, 0, 0, 0, 0]; // minimal empty doc

        let msg = OpDeleteBuilder::new(1).build("test.collection", 0, &selector);

        // Check header
        let message_length = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(message_length as usize, msg.len());

        let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, OpCode::Delete as i32);
    }

    #[test]
    #[allow(deprecated)]
    fn test_op_get_more_builder() {
        let msg = OpGetMoreBuilder::new(1).build("test.collection", 100, 12345);

        // Check header
        let message_length = i32::from_le_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(message_length as usize, msg.len());

        let opcode = i32::from_le_bytes([msg[12], msg[13], msg[14], msg[15]]);
        assert_eq!(opcode, OpCode::GetMore as i32);

        // Check ZERO reserved field
        let zero = i32::from_le_bytes([msg[16], msg[17], msg[18], msg[19]]);
        assert_eq!(zero, 0);
    }
}
