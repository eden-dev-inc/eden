//! TEXT/IMAGE large object handling.
//!
//! TDS supports large object (LOB) types TEXT, IMAGE, and UNITEXT.
//! These types use text pointers (TEXTPTR) for streaming and updates.

use crate::error::SybaseWireError;
use crate::parse::SybaseParseError;
use crate::sybase_ext::SybaseReadSync;
use crate::types::packet::PacketType;
use crate::write::{PacketBuilder, write_u32_le};
use wire_stream::{SliceReadError, SliceStream, WireReadSync};

/// Text pointer size in bytes.
pub const TEXTPTR_SIZE: usize = 16;

/// Timestamp size in bytes.
pub const TIMESTAMP_SIZE: usize = 8;

/// Text pointer for TEXT/IMAGE columns.
///
/// A text pointer is a 16-byte value that identifies a TEXT/IMAGE value
/// in the database. It's used for streaming reads and updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextPtr {
    /// The 16-byte text pointer value.
    pub value: [u8; TEXTPTR_SIZE],
}

impl TextPtr {
    /// Create a new text pointer from bytes.
    pub fn new(value: [u8; TEXTPTR_SIZE]) -> Self {
        Self { value }
    }

    /// Check if this is a null text pointer (all zeros).
    pub fn is_null(&self) -> bool {
        self.value.iter().all(|&b| b == 0)
    }

    /// Parse a text pointer from the stream.
    pub fn parse_sync<'s>(stream: &'s SliceStream<'s>) -> Result<TextPtr, SybaseParseError<SliceReadError, SybaseWireError>> {
        let borrow = stream.peek(Some(TEXTPTR_SIZE)).map_err(SybaseParseError::Stream)?;
        let mut value = [0u8; TEXTPTR_SIZE];
        value.copy_from_slice(&borrow[..TEXTPTR_SIZE]);
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
        Ok(TextPtr { value })
    }
}

/// Timestamp for TEXT/IMAGE columns.
///
/// Used for optimistic concurrency control on LOB updates.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TextTimestamp {
    /// The 8-byte timestamp value.
    pub value: [u8; TIMESTAMP_SIZE],
}

impl TextTimestamp {
    /// Create a new timestamp from bytes.
    pub fn new(value: [u8; TIMESTAMP_SIZE]) -> Self {
        Self { value }
    }

    /// Parse a timestamp from the stream.
    pub fn parse_sync<'s>(stream: &'s SliceStream<'s>) -> Result<TextTimestamp, SybaseParseError<SliceReadError, SybaseWireError>> {
        let borrow = stream.peek(Some(TIMESTAMP_SIZE)).map_err(SybaseParseError::Stream)?;
        let mut value = [0u8; TIMESTAMP_SIZE];
        value.copy_from_slice(&borrow[..TIMESTAMP_SIZE]);
        stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
        Ok(TextTimestamp { value })
    }
}

/// TEXT/IMAGE column value with metadata.
#[derive(Clone, Debug)]
pub struct TextValue {
    /// Text pointer for this value.
    pub textptr: TextPtr,
    /// Timestamp for concurrency control.
    pub timestamp: TextTimestamp,
    /// The actual data (may be partial for streaming).
    pub data: Vec<u8>,
    /// Total length of the value (may be larger than data.len() for streaming).
    pub total_length: u32,
}

impl TextValue {
    /// Parse a TEXT/IMAGE value from the stream.
    ///
    /// This reads the text pointer, timestamp, and data.
    pub fn parse_sync<'s>(stream: &'s SliceStream<'s>) -> Result<Option<TextValue>, SybaseParseError<SliceReadError, SybaseWireError>> {
        // Text pointer length (1 byte)
        let textptr_len = stream.read_u8_sync().map_err(SybaseParseError::Stream)? as usize;

        if textptr_len == 0 {
            // NULL value
            return Ok(None);
        }

        if textptr_len != TEXTPTR_SIZE {
            return Err(SybaseParseError::Parse(SybaseWireError::InvalidPacketLength {
                declared: TEXTPTR_SIZE as u16,
                actual: textptr_len,
            }));
        }

        // Text pointer
        let textptr = TextPtr::parse_sync(stream)?;

        // Timestamp
        let timestamp = TextTimestamp::parse_sync(stream)?;

        // Data length (4 bytes)
        let total_length = stream.read_u32_le_sync().map_err(SybaseParseError::Stream)?;

        // Data
        let data = if total_length > 0 {
            let borrow = stream.peek(Some(total_length as usize)).map_err(SybaseParseError::Stream)?;
            let d = borrow[..total_length as usize].to_vec();
            stream.accept(&borrow, None).map_err(SybaseParseError::Stream)?;
            d
        } else {
            Vec::new()
        };

        Ok(Some(TextValue { textptr, timestamp, data, total_length }))
    }

    /// Check if this is a null value.
    pub fn is_null(&self) -> bool {
        self.textptr.is_null()
    }

    /// Get the data as a string (for TEXT columns).
    pub fn as_str(&self) -> Option<&str> {
        std::str::from_utf8(&self.data).ok()
    }

    /// Get the data as a lossy string (for TEXT columns).
    pub fn as_string_lossy(&self) -> String {
        String::from_utf8_lossy(&self.data).into_owned()
    }
}

/// Builder for WRITETEXT operations.
///
/// WRITETEXT is used to update TEXT/IMAGE columns.
pub struct WriteTextBuilder {
    textptr: TextPtr,
    timestamp: TextTimestamp,
    data: Vec<u8>,
    with_log: bool,
}

impl WriteTextBuilder {
    /// Create a new WRITETEXT builder.
    pub fn new(textptr: TextPtr, timestamp: TextTimestamp) -> Self {
        Self { textptr, timestamp, data: Vec::new(), with_log: true }
    }

    /// Set the data to write.
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.data = data.into();
        self
    }

    /// Set whether to log the operation (default: true).
    pub fn with_log(mut self, with_log: bool) -> Self {
        self.with_log = with_log;
        self
    }

    /// Build the WRITETEXT packet.
    pub fn build(self) -> Vec<u8> {
        let mut payload = Vec::new();

        // Text pointer
        payload.extend_from_slice(&self.textptr.value);

        // Timestamp
        payload.extend_from_slice(&self.timestamp.value);

        // Data length
        write_u32_le(&mut payload, self.data.len() as u32);

        // Data
        payload.extend_from_slice(&self.data);

        PacketBuilder::new(PacketType::Query5).write_bytes(&payload).build()
    }
}

/// Builder for UPDATETEXT operations.
///
/// UPDATETEXT is used to partially update TEXT/IMAGE columns.
pub struct UpdateTextBuilder {
    textptr: TextPtr,
    timestamp: TextTimestamp,
    insert_offset: u32,
    delete_length: u32,
    data: Vec<u8>,
    with_log: bool,
}

impl UpdateTextBuilder {
    /// Create a new UPDATETEXT builder.
    pub fn new(textptr: TextPtr, timestamp: TextTimestamp) -> Self {
        Self {
            textptr,
            timestamp,
            insert_offset: 0,
            delete_length: 0,
            data: Vec::new(),
            with_log: true,
        }
    }

    /// Set the offset where to insert/replace data.
    pub fn insert_offset(mut self, offset: u32) -> Self {
        self.insert_offset = offset;
        self
    }

    /// Set the length of data to delete at the offset.
    pub fn delete_length(mut self, length: u32) -> Self {
        self.delete_length = length;
        self
    }

    /// Set the data to insert.
    pub fn data(mut self, data: impl Into<Vec<u8>>) -> Self {
        self.data = data.into();
        self
    }

    /// Set whether to log the operation (default: true).
    pub fn with_log(mut self, with_log: bool) -> Self {
        self.with_log = with_log;
        self
    }

    /// Build the UPDATETEXT packet.
    pub fn build(self) -> Vec<u8> {
        let mut payload = Vec::new();

        // Text pointer
        payload.extend_from_slice(&self.textptr.value);

        // Timestamp
        payload.extend_from_slice(&self.timestamp.value);

        // Insert offset
        write_u32_le(&mut payload, self.insert_offset);

        // Delete length
        write_u32_le(&mut payload, self.delete_length);

        // Data length
        write_u32_le(&mut payload, self.data.len() as u32);

        // Data
        payload.extend_from_slice(&self.data);

        PacketBuilder::new(PacketType::Query5).write_bytes(&payload).build()
    }
}

/// Builder for READTEXT operations.
///
/// READTEXT is used to read portions of TEXT/IMAGE columns.
pub struct ReadTextBuilder {
    textptr: TextPtr,
    offset: u32,
    size: u32,
}

impl ReadTextBuilder {
    /// Create a new READTEXT builder.
    pub fn new(textptr: TextPtr) -> Self {
        Self { textptr, offset: 0, size: 0 }
    }

    /// Set the offset to start reading from.
    pub fn offset(mut self, offset: u32) -> Self {
        self.offset = offset;
        self
    }

    /// Set the number of bytes to read.
    pub fn size(mut self, size: u32) -> Self {
        self.size = size;
        self
    }

    /// Build the READTEXT packet.
    pub fn build(self) -> Vec<u8> {
        let mut payload = Vec::new();

        // Text pointer
        payload.extend_from_slice(&self.textptr.value);

        // Offset
        write_u32_le(&mut payload, self.offset);

        // Size
        write_u32_le(&mut payload, self.size);

        PacketBuilder::new(PacketType::Query5).write_bytes(&payload).build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_textptr_null() {
        let ptr = TextPtr::new([0u8; 16]);
        assert!(ptr.is_null());

        let mut value = [0u8; 16];
        value[0] = 1;
        let ptr = TextPtr::new(value);
        assert!(!ptr.is_null());
    }

    #[test]
    fn test_writetext_builder() {
        let textptr = TextPtr::new([1u8; 16]);
        let timestamp = TextTimestamp::new([2u8; 8]);

        let packet = WriteTextBuilder::new(textptr, timestamp).data(b"Hello, World!".to_vec()).with_log(true).build();

        assert!(!packet.is_empty());
    }

    #[test]
    fn test_updatetext_builder() {
        let textptr = TextPtr::new([1u8; 16]);
        let timestamp = TextTimestamp::new([2u8; 8]);

        let packet = UpdateTextBuilder::new(textptr, timestamp).insert_offset(10).delete_length(5).data(b"new text".to_vec()).build();

        assert!(!packet.is_empty());
    }

    #[test]
    fn test_readtext_builder() {
        let textptr = TextPtr::new([1u8; 16]);

        let packet = ReadTextBuilder::new(textptr).offset(0).size(1024).build();

        assert!(!packet.is_empty());
    }
}
