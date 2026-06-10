//! MySQL packet compression support.
//!
//! MySQL supports packet compression using zlib when the CLIENT_COMPRESS
//! capability is negotiated. Compressed packets have a different header format.

use crate::limits::Limits;
use crate::mysql_ext::MysqlReadSync;
use crate::parse::MysqlParseError;
use wire_stream::WireReadSync;

/// Compressed packet header.
///
/// When compression is enabled, packets have a 7-byte header:
/// - 3 bytes: compressed payload length (little-endian)
/// - 1 byte: compressed sequence ID
/// - 3 bytes: uncompressed payload length (0 if payload is not compressed)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompressedPacketHeader {
    /// Length of the compressed payload.
    pub compressed_length: u32,
    /// Sequence ID for compressed packets (separate from regular sequence).
    pub compressed_sequence_id: u8,
    /// Length of the uncompressed payload (0 if not actually compressed).
    pub uncompressed_length: u32,
}

impl CompressedPacketHeader {
    /// Header size in bytes.
    pub const SIZE: usize = 7;

    /// Create a new compressed packet header.
    pub fn new(compressed_length: u32, sequence_id: u8, uncompressed_length: u32) -> Self {
        Self {
            compressed_length,
            compressed_sequence_id: sequence_id,
            uncompressed_length,
        }
    }

    /// Check if the payload is actually compressed.
    ///
    /// If uncompressed_length is 0, the payload is sent uncompressed.
    pub fn is_compressed(&self) -> bool {
        self.uncompressed_length > 0
    }

    /// Get the actual payload size after decompression.
    pub fn payload_size(&self) -> u32 {
        if self.is_compressed() {
            self.uncompressed_length
        } else {
            self.compressed_length
        }
    }

    /// Encode the header to bytes.
    pub fn to_bytes(&self) -> [u8; 7] {
        let mut bytes = [0u8; 7];
        bytes[0] = self.compressed_length as u8;
        bytes[1] = (self.compressed_length >> 8) as u8;
        bytes[2] = (self.compressed_length >> 16) as u8;
        bytes[3] = self.compressed_sequence_id;
        bytes[4] = self.uncompressed_length as u8;
        bytes[5] = (self.uncompressed_length >> 8) as u8;
        bytes[6] = (self.uncompressed_length >> 16) as u8;
        bytes
    }

    /// Parse a compressed packet header from bytes.
    pub fn from_bytes(bytes: &[u8; 7]) -> Self {
        let compressed_length = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0]);
        let compressed_sequence_id = bytes[3];
        let uncompressed_length = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], 0]);

        Self {
            compressed_length,
            compressed_sequence_id,
            uncompressed_length,
        }
    }
}

#[derive(Clone, Debug, thiserror::Error)]
pub enum CompressionError {
    #[error("compressed payload too large: {0} bytes")]
    PayloadTooLarge(usize),
    #[error("decompression failed: {0}")]
    DecompressionFailed(String),
    #[error("compression failed: {0}")]
    CompressionFailed(String),
    #[error("invalid compressed header")]
    InvalidHeader,
}

/// Compression context for a MySQL connection.
#[derive(Debug)]
pub struct CompressionContext {
    /// Whether compression is enabled.
    enabled: bool,
    /// Current compressed sequence ID.
    sequence_id: u8,
    /// Minimum payload size to compress (smaller payloads sent uncompressed).
    min_compress_length: usize,
    /// Limits for validation.
    limits: Limits,
}

impl Default for CompressionContext {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionContext {
    /// Create a new compression context (disabled by default).
    pub fn new() -> Self {
        Self {
            enabled: false,
            sequence_id: 0,
            min_compress_length: 50, // MySQL default
            limits: Limits::default(),
        }
    }

    /// Enable compression.
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable compression.
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Check if compression is enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Set the minimum length for compression.
    pub fn set_min_compress_length(&mut self, len: usize) {
        self.min_compress_length = len;
    }

    /// Get the next sequence ID and increment.
    pub fn next_sequence_id(&mut self) -> u8 {
        let id = self.sequence_id;
        self.sequence_id = self.sequence_id.wrapping_add(1);
        id
    }

    /// Reset the sequence ID.
    pub fn reset_sequence(&mut self) {
        self.sequence_id = 0;
    }

    /// Parse a compressed packet header from a stream.
    pub fn parse_header_sync<S: WireReadSync + ?Sized>(
        &self,
        stream: &S,
    ) -> Result<CompressedPacketHeader, MysqlParseError<S::ReadError, CompressionError>> {
        let compressed_length = stream.read_u24_le_sync().map_err(MysqlParseError::Stream)?;
        let sequence_id = stream.read_u8_sync().map_err(MysqlParseError::Stream)?;
        let uncompressed_length = stream.read_u24_le_sync().map_err(MysqlParseError::Stream)?;

        // Validate sizes
        if compressed_length as usize > self.limits.max_packet_size {
            return Err(MysqlParseError::Parse(CompressionError::PayloadTooLarge(compressed_length as usize)));
        }

        Ok(CompressedPacketHeader {
            compressed_length,
            compressed_sequence_id: sequence_id,
            uncompressed_length,
        })
    }

    /// Compress data if it meets the minimum length threshold.
    ///
    /// Returns (compressed_data, uncompressed_length) where uncompressed_length
    /// is 0 if the data was not compressed.
    #[cfg(feature = "compression")]
    pub fn compress(&self, data: &[u8]) -> Result<(Vec<u8>, u32), CompressionError> {
        use flate2::Compression;
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        if data.len() < self.min_compress_length {
            // Don't compress small payloads
            return Ok((data.to_vec(), 0));
        }

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

        let compressed = encoder.finish().map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

        // Only use compressed version if it's actually smaller
        if compressed.len() < data.len() {
            Ok((compressed, data.len() as u32))
        } else {
            Ok((data.to_vec(), 0))
        }
    }

    /// Compress data (stub when compression feature is disabled).
    #[cfg(not(feature = "compression"))]
    pub fn compress(&self, data: &[u8]) -> Result<(Vec<u8>, u32), CompressionError> {
        // Without compression support, always send uncompressed
        Ok((data.to_vec(), 0))
    }

    /// Decompress data.
    #[cfg(feature = "compression")]
    pub fn decompress(&self, data: &[u8], uncompressed_length: u32) -> Result<Vec<u8>, CompressionError> {
        use flate2::read::ZlibDecoder;
        use std::io::Read;

        if uncompressed_length == 0 {
            // Data is not compressed
            return Ok(data.to_vec());
        }

        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::with_capacity(uncompressed_length as usize);

        decoder.read_to_end(&mut decompressed).map_err(|e| CompressionError::DecompressionFailed(e.to_string()))?;

        if decompressed.len() != uncompressed_length as usize {
            return Err(CompressionError::DecompressionFailed(format!(
                "expected {} bytes, got {}",
                uncompressed_length,
                decompressed.len()
            )));
        }

        Ok(decompressed)
    }

    /// Decompress data (stub when compression feature is disabled).
    #[cfg(not(feature = "compression"))]
    pub fn decompress(&self, data: &[u8], uncompressed_length: u32) -> Result<Vec<u8>, CompressionError> {
        if uncompressed_length == 0 {
            Ok(data.to_vec())
        } else {
            Err(CompressionError::DecompressionFailed("compression support not enabled".to_string()))
        }
    }

    /// Build a compressed packet.
    pub fn build_packet(&mut self, payload: &[u8]) -> Result<Vec<u8>, CompressionError> {
        let (compressed_payload, uncompressed_length) = self.compress(payload)?;
        let sequence_id = self.next_sequence_id();

        let header = CompressedPacketHeader::new(compressed_payload.len() as u32, sequence_id, uncompressed_length);

        let mut packet = Vec::with_capacity(7 + compressed_payload.len());
        packet.extend_from_slice(&header.to_bytes());
        packet.extend_from_slice(&compressed_payload);

        Ok(packet)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compressed_header_roundtrip() {
        let header = CompressedPacketHeader::new(1000, 5, 2000);
        let bytes = header.to_bytes();
        let parsed = CompressedPacketHeader::from_bytes(&bytes);

        assert_eq!(parsed.compressed_length, 1000);
        assert_eq!(parsed.compressed_sequence_id, 5);
        assert_eq!(parsed.uncompressed_length, 2000);
    }

    #[test]
    fn test_is_compressed() {
        let compressed = CompressedPacketHeader::new(100, 0, 200);
        assert!(compressed.is_compressed());
        assert_eq!(compressed.payload_size(), 200);

        let uncompressed = CompressedPacketHeader::new(100, 0, 0);
        assert!(!uncompressed.is_compressed());
        assert_eq!(uncompressed.payload_size(), 100);
    }

    #[test]
    fn test_compression_context() {
        let mut ctx = CompressionContext::new();
        assert!(!ctx.is_enabled());

        ctx.enable();
        assert!(ctx.is_enabled());

        assert_eq!(ctx.next_sequence_id(), 0);
        assert_eq!(ctx.next_sequence_id(), 1);

        ctx.reset_sequence();
        assert_eq!(ctx.next_sequence_id(), 0);
    }

    #[test]
    fn test_compress_small_payload() {
        let ctx = CompressionContext::new();
        let data = b"small";

        let (compressed, uncompressed_len) = ctx.compress(data).unwrap();
        assert_eq!(uncompressed_len, 0); // Not compressed (too small)
        assert_eq!(compressed, data);
    }
}
