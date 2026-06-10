//! Oracle TNS data compression.
//!
//! Oracle supports optional compression for TNS packets to reduce bandwidth.
//! This is negotiated during connection establishment via the compression capability.
//!
//! Oracle uses zlib (deflate) compression for packet data.
//!
//! ## Compression Format
//!
//! Compressed data packets have the following format:
//! - First 2 bytes: uncompressed length (big-endian)
//! - Remaining bytes: zlib-compressed data
//!
//! ## Usage
//!
//! ```rust,ignore
//! use oracle_wire::types::tti::compression::{compress, decompress};
//!
//! // Compress data before sending
//! let compressed = compress(&original_data)?;
//!
//! // Decompress received data
//! let decompressed = decompress(&compressed_data)?;
//! ```

use flate2::Compression;
use flate2::read::{ZlibDecoder, ZlibEncoder};
use std::io::Read;

/// Compression error.
#[derive(Clone, Debug, thiserror::Error)]
pub enum CompressionError {
    /// Invalid compressed data.
    #[error("invalid compressed data: {0}")]
    InvalidData(String),
    /// Decompressed size exceeds maximum.
    #[error("decompressed size {actual} exceeds maximum {max}")]
    SizeExceeded { actual: usize, max: usize },
    /// Compression failed.
    #[error("compression failed: {0}")]
    CompressionFailed(String),
    /// Data too short for header.
    #[error("data too short: expected at least {expected} bytes, got {actual}")]
    DataTooShort { expected: usize, actual: usize },
}

/// Maximum decompressed size to prevent memory exhaustion (64MB).
pub const MAX_DECOMPRESSED_SIZE: usize = 64 * 1024 * 1024;

/// Minimum data size worth compressing.
pub const MIN_COMPRESS_SIZE: usize = 64;

/// Compression level for Oracle TNS (default/balanced).
pub const COMPRESSION_LEVEL: u32 = 6;

/// Compress data using zlib (deflate).
///
/// Returns the compressed data prefixed with the original length.
///
/// # Arguments
/// * `data` - Data to compress
///
/// # Returns
/// Compressed data with 2-byte length prefix
pub fn compress(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    // Create output buffer with length prefix
    let mut result = Vec::with_capacity(data.len());

    // Prepend uncompressed length as 2 bytes (big-endian)
    let len = data.len();
    if len > u16::MAX as usize {
        return Err(CompressionError::InvalidData("data too large for 2-byte length prefix".to_string()));
    }
    result.push((len >> 8) as u8);
    result.push(len as u8);

    // Compress the data
    let mut encoder = ZlibEncoder::new(data, Compression::new(COMPRESSION_LEVEL));
    encoder.read_to_end(&mut result).map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    Ok(result)
}

/// Compress data only if it results in a smaller size.
///
/// Returns `None` if compression doesn't reduce size significantly.
///
/// # Arguments
/// * `data` - Data to compress
/// * `min_savings_percent` - Minimum percentage savings required (0-100)
pub fn compress_if_beneficial(data: &[u8], min_savings_percent: u8) -> Option<Vec<u8>> {
    if data.len() < MIN_COMPRESS_SIZE {
        return None;
    }

    match compress(data) {
        Ok(compressed) => {
            let savings = data.len().saturating_sub(compressed.len());
            let savings_percent = (savings * 100) / data.len();
            if savings_percent >= min_savings_percent as usize {
                Some(compressed)
            } else {
                None
            }
        }
        Err(_) => None,
    }
}

/// Decompress zlib-compressed data.
///
/// Expects data with 2-byte length prefix indicating uncompressed size.
///
/// # Arguments
/// * `data` - Compressed data with length prefix
///
/// # Returns
/// Decompressed data
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    if data.len() < 2 {
        return Err(CompressionError::DataTooShort { expected: 2, actual: data.len() });
    }

    // Read uncompressed length from prefix
    let expected_len = ((data[0] as usize) << 8) | (data[1] as usize);

    if expected_len > MAX_DECOMPRESSED_SIZE {
        return Err(CompressionError::SizeExceeded { actual: expected_len, max: MAX_DECOMPRESSED_SIZE });
    }

    // Decompress the data
    let compressed = &data[2..];
    let mut decoder = ZlibDecoder::new(compressed);
    let mut result = Vec::with_capacity(expected_len);

    decoder.read_to_end(&mut result).map_err(|e| CompressionError::InvalidData(e.to_string()))?;

    if result.len() != expected_len {
        return Err(CompressionError::InvalidData(format!(
            "decompressed size {} doesn't match expected {}",
            result.len(),
            expected_len
        )));
    }

    Ok(result)
}

/// Decompress zlib-compressed data without length prefix.
///
/// Use this for raw zlib streams without Oracle's length prefix.
///
/// # Arguments
/// * `data` - Raw zlib-compressed data
/// * `max_size` - Maximum decompressed size allowed
pub fn decompress_raw(data: &[u8], max_size: usize) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut decoder = ZlibDecoder::new(data);
    let mut result = Vec::new();

    // Read in chunks to enforce size limit
    let mut buffer = [0u8; 8192];
    loop {
        match decoder.read(&mut buffer) {
            Ok(0) => break,
            Ok(n) => {
                if result.len() + n > max_size {
                    return Err(CompressionError::SizeExceeded { actual: result.len() + n, max: max_size });
                }
                result.extend_from_slice(&buffer[..n]);
            }
            Err(e) => return Err(CompressionError::InvalidData(e.to_string())),
        }
    }

    Ok(result)
}

/// Compress data without length prefix (raw zlib).
///
/// Use this for raw zlib streams without Oracle's length prefix.
pub fn compress_raw(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    if data.is_empty() {
        return Ok(Vec::new());
    }

    let mut encoder = ZlibEncoder::new(data, Compression::new(COMPRESSION_LEVEL));
    let mut result = Vec::with_capacity(data.len());

    encoder.read_to_end(&mut result).map_err(|e| CompressionError::CompressionFailed(e.to_string()))?;

    Ok(result)
}

/// Check if data appears to be zlib-compressed.
///
/// Checks for zlib header magic bytes.
pub fn is_zlib_compressed(data: &[u8]) -> bool {
    if data.len() < 2 {
        return false;
    }

    // Check for zlib header: CMF and FLG bytes
    // CMF = 0x78 for default compression
    // Common combinations: 0x78 0x01, 0x78 0x5E, 0x78 0x9C, 0x78 0xDA
    let cmf = data[0];
    let flg = data[1];

    // CMF should be 0x78 (deflate with 32K window)
    if cmf != 0x78 {
        return false;
    }

    // Check that CMF*256 + FLG is a multiple of 31
    let check = (cmf as u16) * 256 + (flg as u16);
    check.is_multiple_of(31)
}

/// Compression context for tracking compression statistics.
#[derive(Clone, Debug, Default)]
pub struct CompressionStats {
    /// Total bytes before compression.
    pub bytes_in: u64,
    /// Total bytes after compression.
    pub bytes_out: u64,
    /// Number of packets compressed.
    pub packets_compressed: u64,
    /// Number of packets decompressed.
    pub packets_decompressed: u64,
}

impl CompressionStats {
    /// Create a new compression stats tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a compression operation.
    pub fn record_compression(&mut self, original_size: usize, compressed_size: usize) {
        self.bytes_in += original_size as u64;
        self.bytes_out += compressed_size as u64;
        self.packets_compressed += 1;
    }

    /// Record a decompression operation.
    pub fn record_decompression(&mut self, compressed_size: usize, decompressed_size: usize) {
        self.bytes_in += compressed_size as u64;
        self.bytes_out += decompressed_size as u64;
        self.packets_decompressed += 1;
    }

    /// Get compression ratio (0.0 to 1.0, lower is better).
    pub fn compression_ratio(&self) -> f64 {
        if self.bytes_in == 0 {
            1.0
        } else {
            self.bytes_out as f64 / self.bytes_in as f64
        }
    }

    /// Get space savings percentage (0-100, higher is better).
    pub fn savings_percent(&self) -> f64 {
        (1.0 - self.compression_ratio()) * 100.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compress_decompress_roundtrip() {
        let original = b"Hello, Oracle! This is a test of compression functionality.";

        let compressed = compress(original).expect("compress");
        let decompressed = decompress(&compressed).expect("decompress");

        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_compress_decompress_empty() {
        let original: &[u8] = b"";

        let compressed = compress(original).expect("compress");
        assert!(compressed.is_empty());

        let decompressed = decompress(&compressed).expect("decompress");
        assert!(decompressed.is_empty());
    }

    #[test]
    fn test_compress_decompress_large() {
        // Create a large, compressible data set
        let original: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();

        let compressed = compress(&original).expect("compress");
        // Compression should reduce size for repetitive data
        assert!(compressed.len() < original.len());

        let decompressed = decompress(&compressed).expect("decompress");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_compress_raw_decompress_raw() {
        let original = b"Test data for raw compression without length prefix.";

        let compressed = compress_raw(original).expect("compress");
        let decompressed = decompress_raw(&compressed, 1024).expect("decompress");

        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_invalid_data() {
        // Invalid zlib data
        let result = decompress(&[0x00, 0x10, 0xFF, 0xFF, 0xFF]);
        assert!(result.is_err());
    }

    #[test]
    fn test_decompress_too_short() {
        let result = decompress(&[0x00]);
        assert!(matches!(result, Err(CompressionError::DataTooShort { .. })));
    }

    #[test]
    fn test_decompress_size_exceeded() {
        // Create data that claims to decompress to > MAX_DECOMPRESSED_SIZE
        let mut data = vec![0xFF, 0xFF]; // Claims 65535 bytes
        data.extend(compress_raw(b"small").expect("compress"));

        // This should fail because expected size doesn't match
        // (we only compressed "small" which is 5 bytes)
        let result = decompress(&data);
        assert!(result.is_err());
    }

    #[test]
    fn test_is_zlib_compressed() {
        // Valid zlib headers
        assert!(is_zlib_compressed(&[0x78, 0x01])); // No compression
        assert!(is_zlib_compressed(&[0x78, 0x5E])); // Fast compression
        assert!(is_zlib_compressed(&[0x78, 0x9C])); // Default compression
        assert!(is_zlib_compressed(&[0x78, 0xDA])); // Best compression

        // Invalid headers
        assert!(!is_zlib_compressed(&[0x00, 0x00]));
        assert!(!is_zlib_compressed(&[0x78])); // Too short
        assert!(!is_zlib_compressed(&[])); // Empty
    }

    #[test]
    fn test_compress_if_beneficial() {
        // Highly compressible data (repeated bytes)
        let compressible: Vec<u8> = vec![0xAA; 1000];
        let result = compress_if_beneficial(&compressible, 50);
        assert!(result.is_some());
        assert!(result.expect("should compress").len() < compressible.len() / 2);

        // Incompressible data (random-like)
        let incompressible: Vec<u8> = (0..1000).map(|i| (i * 7 + 13) as u8).collect();
        let result = compress_if_beneficial(&incompressible, 50);
        // May or may not compress depending on the data
        if let Some(compressed) = result {
            // If it compressed, it should meet the savings threshold
            let savings = incompressible.len().saturating_sub(compressed.len());
            let savings_percent = (savings * 100) / incompressible.len();
            assert!(savings_percent >= 50);
        }

        // Data too small
        let small = b"hi";
        assert!(compress_if_beneficial(small, 10).is_none());
    }

    #[test]
    fn test_compression_stats() {
        let mut stats = CompressionStats::new();

        stats.record_compression(1000, 400);
        stats.record_compression(2000, 800);

        assert_eq!(stats.bytes_in, 3000);
        assert_eq!(stats.bytes_out, 1200);
        assert_eq!(stats.packets_compressed, 2);
        assert!((stats.compression_ratio() - 0.4).abs() < 0.001);
        assert!((stats.savings_percent() - 60.0).abs() < 0.1);
    }

    #[test]
    fn test_decompress_raw_size_limit() {
        // Create compressible data
        let original: Vec<u8> = vec![0xAA; 1000];
        let compressed = compress_raw(&original).expect("compress");

        // Should succeed with sufficient limit
        let result = decompress_raw(&compressed, 2000);
        assert!(result.is_ok());

        // Should fail with insufficient limit
        let result = decompress_raw(&compressed, 500);
        assert!(matches!(result, Err(CompressionError::SizeExceeded { .. })));
    }
}
