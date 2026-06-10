//! Encoding helpers for building PostgreSQL wire protocol messages.

use std::io::{self, Write};

/// Extension trait for writing PostgreSQL protocol values.
pub trait PgWrite: Write {
    /// Write a single byte.
    #[inline]
    fn write_u8(&mut self, value: u8) -> io::Result<()> {
        self.write_all(&[value])
    }

    /// Write a signed byte.
    #[inline]
    fn write_i8(&mut self, value: i8) -> io::Result<()> {
        self.write_all(&[value as u8])
    }

    /// Write a 2-byte big-endian i16.
    #[inline]
    fn write_i16_be(&mut self, value: i16) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write a 2-byte big-endian u16.
    #[inline]
    fn write_u16_be(&mut self, value: u16) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write a 4-byte big-endian i32.
    #[inline]
    fn write_i32_be(&mut self, value: i32) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write a 4-byte big-endian u32.
    #[inline]
    fn write_u32_be(&mut self, value: u32) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write an 8-byte big-endian i64.
    #[inline]
    fn write_i64_be(&mut self, value: i64) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write an 8-byte big-endian u64.
    #[inline]
    fn write_u64_be(&mut self, value: u64) -> io::Result<()> {
        self.write_all(&value.to_be_bytes())
    }

    /// Write a NUL-terminated string.
    #[inline]
    fn write_cstring(&mut self, value: &[u8]) -> io::Result<()> {
        self.write_all(value)?;
        self.write_all(&[0])
    }

    /// Write a NUL-terminated UTF-8 string.
    #[inline]
    fn write_cstring_str(&mut self, value: &str) -> io::Result<()> {
        self.write_cstring(value.as_bytes())
    }
}

impl<W: Write + ?Sized> PgWrite for W {}

/// Builder for constructing PostgreSQL messages.
///
/// Handles the message format: [type: u8][length: i32][payload]
#[derive(Debug, Default)]
pub struct MessageBuilder {
    buffer: Vec<u8>,
}

impl MessageBuilder {
    /// Create a new message builder.
    pub fn new() -> Self {
        Self { buffer: Vec::new() }
    }

    /// Create a new message builder with the specified capacity.
    pub fn with_capacity(capacity: usize) -> Self {
        Self { buffer: Vec::with_capacity(capacity) }
    }

    /// Start a new message with the given type byte.
    ///
    /// Reserves space for the type byte and length field.
    pub fn begin(&mut self, msg_type: u8) -> &mut Self {
        self.buffer.clear();
        self.buffer.push(msg_type);
        // Reserve space for length (4 bytes)
        self.buffer.extend_from_slice(&[0, 0, 0, 0]);
        self
    }

    /// Start a new startup message (no type byte).
    ///
    /// Used for StartupMessage, SSLRequest, and CancelRequest.
    pub fn begin_startup(&mut self) -> &mut Self {
        self.buffer.clear();
        // Reserve space for length (4 bytes)
        self.buffer.extend_from_slice(&[0, 0, 0, 0]);
        self
    }

    /// Write raw bytes to the message.
    pub fn write_bytes(&mut self, data: &[u8]) -> &mut Self {
        self.buffer.extend_from_slice(data);
        self
    }

    /// Write a single byte.
    pub fn write_u8(&mut self, value: u8) -> &mut Self {
        self.buffer.push(value);
        self
    }

    /// Write a 2-byte big-endian i16.
    pub fn write_i16_be(&mut self, value: i16) -> &mut Self {
        self.buffer.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Write a 2-byte big-endian u16.
    pub fn write_u16_be(&mut self, value: u16) -> &mut Self {
        self.buffer.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Write a 4-byte big-endian i32.
    pub fn write_i32_be(&mut self, value: i32) -> &mut Self {
        self.buffer.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Write a 4-byte big-endian u32.
    pub fn write_u32_be(&mut self, value: u32) -> &mut Self {
        self.buffer.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Write an 8-byte big-endian i64.
    pub fn write_i64_be(&mut self, value: i64) -> &mut Self {
        self.buffer.extend_from_slice(&value.to_be_bytes());
        self
    }

    /// Write a NUL-terminated string.
    pub fn write_cstring(&mut self, value: &[u8]) -> &mut Self {
        self.buffer.extend_from_slice(value);
        self.buffer.push(0);
        self
    }

    /// Write a NUL-terminated UTF-8 string.
    pub fn write_cstring_str(&mut self, value: &str) -> &mut Self {
        self.write_cstring(value.as_bytes())
    }

    /// Finish the message and return the buffer.
    ///
    /// Updates the length field to reflect the actual message size.
    /// For regular messages, length includes itself (4 bytes) but not the type byte.
    pub fn finish(&mut self) -> &[u8] {
        let has_type_byte = !self.buffer.is_empty() && self.buffer.len() > 4;
        if has_type_byte && self.buffer[0] != 0 {
            // Regular message: length starts at byte 1
            let length = (self.buffer.len() - 1) as i32;
            self.buffer[1..5].copy_from_slice(&length.to_be_bytes());
        } else if self.buffer.len() >= 4 {
            // Startup message: length is at the beginning
            let length = self.buffer.len() as i32;
            self.buffer[0..4].copy_from_slice(&length.to_be_bytes());
        }
        &self.buffer
    }

    /// Finish and take ownership of the buffer.
    pub fn finish_owned(&mut self) -> Vec<u8> {
        let _ = self.finish();
        std::mem::take(&mut self.buffer)
    }

    /// Get a reference to the current buffer contents.
    pub fn as_bytes(&self) -> &[u8] {
        &self.buffer
    }

    /// Get the current length of the buffer.
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear the buffer.
    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_builder_regular() {
        let mut builder = MessageBuilder::new();
        builder.begin(b'Q').write_cstring_str("SELECT 1");
        let msg = builder.finish();

        // Type byte
        assert_eq!(msg[0], b'Q');
        // Length: 4 (length field) + 8 ("SELECT 1") + 1 (NUL) = 13
        let length = i32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]);
        assert_eq!(length, 13);
        // Query string
        assert_eq!(&msg[5..13], b"SELECT 1");
        // NUL terminator
        assert_eq!(msg[13], 0);
    }

    #[test]
    fn test_message_builder_startup() {
        let mut builder = MessageBuilder::new();
        builder
            .begin_startup()
            .write_i32_be(196608) // Protocol version 3.0
            .write_cstring_str("user")
            .write_cstring_str("postgres")
            .write_u8(0); // Final NUL
        let msg = builder.finish();

        // Length: 4 (length) + 4 (version) + 5 (user) + 9 (postgres) + 1 (NUL) = 23
        let length = i32::from_be_bytes([msg[0], msg[1], msg[2], msg[3]]);
        assert_eq!(length, 23);
    }
}
