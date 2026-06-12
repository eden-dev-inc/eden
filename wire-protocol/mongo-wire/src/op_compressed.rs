//! OP_COMPRESSED message parsing with CVE-2025-14847 mitigation.
//!
//! OP_COMPRESSED (opcode 2012) wraps other MongoDB messages with compression.
//! This implementation includes validation to prevent CVE-2025-14847, which
//! exploits mismatched length fields in zlib compression headers to read
//! uninitialized memory.
//!
//! ## Message Structure
//!
//! ```text
//! MsgHeader header {
//!     int32   messageLength;  // Total message size
//!     int32   requestID;
//!     int32   responseTo;
//!     int32   opCode = 2012;  // OP_COMPRESSED
//! }
//! int32       originalOpcode;      // Opcode of wrapped message
//! int32       uncompressedSize;    // Size after decompression
//! uint8       compressorId;        // Compression algorithm
//! char[]      compressedMessage;   // Compressed data
//! ```
//!
//! ## CVE-2025-14847 Mitigation
//!
//! The vulnerability occurs when the declared `uncompressedSize` doesn't match
//! the actual size needed for decompression, causing MongoDB to read uninitialized
//! heap memory. This parser validates:
//!
//! 1. Message length consistency (header.messageLength == actual bytes)
//! 2. Compressed data length matches calculated size
//! 3. Uncompressed size is within reasonable bounds
//! 4. For zlib: compression header lengths are consistent

use crate::error::MongoWireError;
use crate::header::{MessageHeader, OpCode};
use wire_stream::{WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

/// Maximum allowed uncompressed size (256 MB).
/// This prevents memory exhaustion attacks.
pub const MAX_UNCOMPRESSED_SIZE: i32 = 256 * 1024 * 1024;

/// Compression algorithm identifiers.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum CompressorId {
    /// No compression (0) - shouldn't be used but might appear
    Noop = 0,
    /// Snappy compression (1)
    Snappy = 1,
    /// Zlib compression (2) - VULNERABLE TO CVE-2025-14847
    Zlib = 2,
    /// Zstandard compression (3)
    Zstd = 3,
}

impl CompressorId {
    /// Parse compressor ID from byte.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Self::Noop),
            1 => Some(Self::Snappy),
            2 => Some(Self::Zlib),
            3 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Returns true if this compressor is vulnerable to CVE-2025-14847.
    pub fn is_vulnerable(&self) -> bool {
        matches!(self, Self::Zlib)
    }
}

/// Parsed OP_COMPRESSED message with validation.
#[derive(Clone, Debug)]
pub struct OpCompressed {
    /// The opcode of the wrapped (uncompressed) message.
    pub original_opcode: i32,
    /// Declared size after decompression.
    pub uncompressed_size: i32,
    /// Compression algorithm used.
    pub compressor_id: CompressorId,
    /// The compressed message data.
    pub compressed_data: Vec<u8>,
}

impl OpCompressed {
    /// Size of the OP_COMPRESSED header (excluding standard message header).
    /// original_opcode (4) + uncompressed_size (4) + compressor_id (1) = 9 bytes
    pub const HEADER_SIZE: usize = 9;

    /// Parse an OP_COMPRESSED message with CVE-2025-14847 validation.
    ///
    /// # Security Checks
    ///
    /// 1. Validates message length consistency
    /// 2. Validates uncompressed size is reasonable
    /// 3. For zlib compression, validates compression header integrity
    /// 4. Ensures compressed data length matches expectations
    #[inline]
    pub fn parse_sync<S: WireReadSync + ?Sized>(stream: &S, header: &MessageHeader, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        // Minimum body: original_opcode (4) + uncompressed_size (4) + compressor_id (1) = 9 bytes
        if body_length < Self::HEADER_SIZE {
            return Err(MongoWireError::message_too_short(Self::HEADER_SIZE, body_length));
        }

        // Read compression metadata
        let original_opcode = stream.read_i32_le_sync().map_err(Into::into)?;
        let uncompressed_size = stream.read_i32_le_sync().map_err(Into::into)?;
        let compressor_byte = {
            let byte = stream.peek_exactly::<1>().map_err(Into::into)?;
            stream.accept_exactly(&byte).map_err(Into::into)?;
            byte[0]
        };

        let compressor_id = CompressorId::from_u8(compressor_byte)
            .ok_or_else(|| MongoWireError::InvalidBson(format!("unknown compressor: {}", compressor_byte).into()))?;

        // CVE-2025-14847 Check #1: Validate uncompressed size is reasonable
        if uncompressed_size < 0 {
            return Err(MongoWireError::InvalidMessageLength(uncompressed_size));
        }
        if uncompressed_size > MAX_UNCOMPRESSED_SIZE {
            return Err(MongoWireError::InvalidBson(
                format!("uncompressed size {} exceeds maximum {}", uncompressed_size, MAX_UNCOMPRESSED_SIZE).into(),
            ));
        }

        // Calculate expected compressed data length
        let compressed_data_len = body_length - Self::HEADER_SIZE;

        // CVE-2025-14847 Check #2: Validate message length consistency
        let expected_message_length = MessageHeader::SIZE + body_length;
        if header.message_length as usize != expected_message_length {
            return Err(MongoWireError::InvalidMessageLength(header.message_length));
        }

        // Read compressed data
        let compressed_data = stream.read_bytes_sync(compressed_data_len).map_err(Into::into)?.to_vec();

        // CVE-2025-14847 Check #3: For zlib, validate compression header
        if compressor_id.is_vulnerable() {
            Self::validate_zlib_header(&compressed_data, uncompressed_size as usize)?;
        }

        Ok(Self {
            original_opcode,
            uncompressed_size,
            compressor_id,
            compressed_data,
        })
    }

    /// Parse an OP_COMPRESSED message asynchronously with CVE-2025-14847 validation.
    pub async fn parse<S: WireRead + ?Sized>(stream: &S, header: &MessageHeader, body_length: usize) -> Result<Self, MongoWireError>
    where
        S::ReadError: Into<MongoWireError>,
    {
        if body_length < Self::HEADER_SIZE {
            return Err(MongoWireError::message_too_short(Self::HEADER_SIZE, body_length));
        }

        // Read compression metadata
        let original_opcode = stream.read_i32_le().await.map_err(Into::into)?;
        let uncompressed_size = stream.read_i32_le().await.map_err(Into::into)?;
        let compressor_byte = {
            let byte = stream.peek_read_exactly::<1>().await.map_err(Into::into)?;
            stream.accept_exactly(&byte).map_err(Into::into)?;
            byte[0]
        };

        let compressor_id = CompressorId::from_u8(compressor_byte)
            .ok_or_else(|| MongoWireError::InvalidBson(format!("unknown compressor: {}", compressor_byte).into()))?;

        // CVE-2025-14847 Check #1: Validate uncompressed size is reasonable
        if uncompressed_size < 0 {
            return Err(MongoWireError::InvalidMessageLength(uncompressed_size));
        }
        if uncompressed_size > MAX_UNCOMPRESSED_SIZE {
            return Err(MongoWireError::InvalidBson(
                format!("uncompressed size {} exceeds maximum {}", uncompressed_size, MAX_UNCOMPRESSED_SIZE).into(),
            ));
        }

        // Calculate expected compressed data length
        let compressed_data_len = body_length - Self::HEADER_SIZE;

        // CVE-2025-14847 Check #2: Validate message length consistency
        let expected_message_length = MessageHeader::SIZE + body_length;
        if header.message_length as usize != expected_message_length {
            return Err(MongoWireError::InvalidMessageLength(header.message_length));
        }

        // Read compressed data - peek, copy, then advance
        let borrowed = stream.peek_read(Some(compressed_data_len)).await.map_err(Into::into)?;
        let compressed_data = borrowed.to_vec();
        stream.accept(&borrowed, None).map_err(Into::into)?;

        // CVE-2025-14847 Check #3: For zlib, validate compression header
        if compressor_id.is_vulnerable() {
            Self::validate_zlib_header(&compressed_data, uncompressed_size as usize)?;
        }

        Ok(Self {
            original_opcode,
            uncompressed_size,
            compressor_id,
            compressed_data,
        })
    }

    /// Validate zlib compression header for CVE-2025-14847.
    ///
    /// Zlib format:
    /// - 2 byte header (CMF + FLG)
    /// - Compressed blocks
    /// - Optional 4 byte Adler32 checksum
    ///
    /// The vulnerability occurs when length fields in the zlib stream
    /// don't match the actual data, causing reads of uninitialized memory.
    fn validate_zlib_header(data: &[u8], expected_uncompressed: usize) -> Result<(), MongoWireError> {
        if data.len() < 2 {
            return Err(MongoWireError::InvalidBson("zlib data too short for header".into()));
        }

        let cmf = data[0];
        let flg = data[1];

        // Validate CMF (Compression Method and Flags)
        let cm = cmf & 0x0F; // Compression method (lower 4 bits)
        let cinfo = (cmf >> 4) & 0x0F; // Compression info (upper 4 bits)

        // CM must be 8 (deflate)
        if cm != 8 {
            return Err(MongoWireError::InvalidBson(format!("invalid zlib compression method: {}", cm).into()));
        }

        // CINFO must be <= 7 (window size)
        if cinfo > 7 {
            return Err(MongoWireError::InvalidBson(format!("invalid zlib window size: {}", cinfo).into()));
        }

        // Validate FLG (Flags)
        // FCHECK: CMF*256 + FLG must be multiple of 31
        let fcheck = ((cmf as u16) * 256 + (flg as u16)) % 31;
        if fcheck != 0 {
            return Err(MongoWireError::InvalidBson("invalid zlib header checksum".into()));
        }

        // Additional safety: compressed data shouldn't be excessively larger than expected.
        // Use a zlib compressBound-style allowance that scales with size.
        let max_allowed = expected_uncompressed
            .saturating_add(expected_uncompressed >> 12)
            .saturating_add(expected_uncompressed >> 14)
            .saturating_add(expected_uncompressed >> 25)
            .saturating_add(13);
        if data.len() > max_allowed {
            return Err(MongoWireError::InvalidBson(
                format!("suspicious: compressed size {} exceeds allowed bound {}", data.len(), max_allowed).into(),
            ));
        }

        Ok(())
    }

    /// Get the wrapped message's opcode.
    pub fn original_op_code(&self) -> Option<OpCode> {
        OpCode::from_i32(self.original_opcode)
    }

    /// Returns true if this uses a vulnerable compressor.
    pub fn is_vulnerable_compression(&self) -> bool {
        self.compressor_id.is_vulnerable()
    }

    /// Decompress the message data with streaming size limits.
    ///
    /// # Security Features
    ///
    /// This method uses streaming decompression with incremental size checking
    /// to prevent zip bomb attacks. It will abort decompression early if the
    /// output exceeds the declared uncompressed size.
    ///
    /// # Safety Checks
    ///
    /// 1. Validates declared size is within MAX_UNCOMPRESSED_SIZE before starting
    /// 2. Checks decompressed bytes incrementally during decompression
    /// 3. Aborts immediately if output exceeds declared size (prevents zip bombs)
    /// 4. Verifies final size matches declared size exactly
    #[cfg(feature = "decompression")]
    pub fn decompress(&self) -> Result<Vec<u8>, MongoWireError> {
        use flate2::bufread::ZlibDecoder;
        use std::io::Read;

        let max_size = self.uncompressed_size as usize;

        // Pre-validation: ensure declared size is reasonable
        if max_size > MAX_UNCOMPRESSED_SIZE as usize {
            return Err(MongoWireError::DecompressionSizeExceeded { actual: max_size, limit: MAX_UNCOMPRESSED_SIZE as usize });
        }

        // Start with conservative allocation (min of 1MB or declared size)
        let initial_capacity = max_size.min(1024 * 1024);
        let mut decompressed = Vec::with_capacity(initial_capacity);

        match self.compressor_id {
            CompressorId::Zlib => {
                let mut decoder = ZlibDecoder::new(&self.compressed_data[..]);
                let mut buffer = [0u8; 8192];
                let mut total_read = 0usize;

                loop {
                    let bytes_read = decoder
                        .read(&mut buffer)
                        .map_err(|e| MongoWireError::InvalidBson(format!("zlib decompression failed: {}", e).into()))?;

                    if bytes_read == 0 {
                        break;
                    }

                    // Check BEFORE extending to prevent zip bombs
                    total_read = total_read
                        .checked_add(bytes_read)
                        .ok_or_else(|| MongoWireError::InvalidBson("decompression size overflow".into()))?;

                    if total_read > max_size {
                        return Err(MongoWireError::DecompressionSizeExceeded { actual: total_read, limit: max_size });
                    }

                    decompressed.extend_from_slice(&buffer[..bytes_read]);
                }
            }
            CompressorId::Snappy => {
                #[cfg(feature = "snappy")]
                {
                    let expected_len = snap::raw::decompress_len(&self.compressed_data)
                        .map_err(|e| MongoWireError::InvalidBson(format!("snappy decompression failed: {}", e).into()))?;
                    if expected_len > max_size {
                        return Err(MongoWireError::DecompressionSizeExceeded { actual: expected_len, limit: max_size });
                    }
                    if expected_len != max_size {
                        return Err(MongoWireError::InvalidBson(
                            format!("snappy uncompressed size mismatch: got {}, expected {}", expected_len, max_size).into(),
                        ));
                    }

                    // Snappy doesn't support streaming, but we can pre-check size.
                    decompressed = snap::raw::Decoder::new()
                        .decompress_vec(&self.compressed_data)
                        .map_err(|e| MongoWireError::InvalidBson(format!("snappy decompression failed: {}", e).into()))?;
                }
                #[cfg(not(feature = "snappy"))]
                {
                    return Err(MongoWireError::InvalidBson("snappy support not enabled".into()));
                }
            }
            CompressorId::Zstd => {
                // Use streaming zstd with size limit
                let mut decoder = zstd::stream::Decoder::new(&self.compressed_data[..])
                    .map_err(|e| MongoWireError::InvalidBson(format!("zstd decoder init failed: {}", e).into()))?;

                let mut buffer = [0u8; 8192];
                let mut total_read = 0usize;

                loop {
                    use std::io::Read;
                    let bytes_read = decoder
                        .read(&mut buffer)
                        .map_err(|e| MongoWireError::InvalidBson(format!("zstd decompression failed: {}", e).into()))?;

                    if bytes_read == 0 {
                        break;
                    }

                    total_read = total_read
                        .checked_add(bytes_read)
                        .ok_or_else(|| MongoWireError::InvalidBson("decompression size overflow".into()))?;

                    if total_read > max_size {
                        return Err(MongoWireError::DecompressionSizeExceeded { actual: total_read, limit: max_size });
                    }

                    decompressed.extend_from_slice(&buffer[..bytes_read]);
                }
            }
            CompressorId::Noop => {
                decompressed = self.compressed_data.clone();
            }
        }

        // Final validation: decompressed size should match declared size exactly
        if decompressed.len() != max_size {
            return Err(MongoWireError::InvalidBson(
                format!("decompressed size mismatch: got {}, expected {}", decompressed.len(), max_size).into(),
            ));
        }

        Ok(decompressed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wire_stream::SliceStream;

    #[test]
    fn test_parse_compressed_message() {
        // Simulate a compressed message structure
        let original_opcode = OpCode::Msg as i32;
        let uncompressed_size = 100i32;
        let compressor = CompressorId::Snappy as u8;
        let compressed_data = b"fake_compressed_data";

        let mut data = Vec::new();
        data.extend_from_slice(&original_opcode.to_le_bytes());
        data.extend_from_slice(&uncompressed_size.to_le_bytes());
        data.push(compressor);
        data.extend_from_slice(compressed_data);

        let header = MessageHeader {
            message_length: (MessageHeader::SIZE + data.len()) as i32,
            request_id: 1,
            response_to: 0,
            op_code: OpCode::Compressed as i32,
        };

        let stream = SliceStream::new(&data);
        let msg = OpCompressed::parse_sync(&stream, &header, data.len()).expect("Should parse valid compressed message");

        assert_eq!(msg.original_opcode, OpCode::Msg as i32);
        assert_eq!(msg.uncompressed_size, 100);
        assert_eq!(msg.compressor_id, CompressorId::Snappy);
        assert_eq!(msg.compressed_data, compressed_data);
    }

    #[test]
    fn test_reject_invalid_message_length() {
        let mut data = Vec::new();
        data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
        data.extend_from_slice(&100i32.to_le_bytes());
        data.push(CompressorId::Snappy as u8);
        data.extend_from_slice(b"data");

        // Create header with WRONG length (CVE-2025-14847 scenario)
        let header = MessageHeader {
            message_length: 999, // Mismatched!
            request_id: 1,
            response_to: 0,
            op_code: OpCode::Compressed as i32,
        };

        let stream = SliceStream::new(&data);
        let result = OpCompressed::parse_sync(&stream, &header, data.len());

        assert!(result.is_err());
        match result {
            Err(MongoWireError::InvalidMessageLength(_)) => (),
            _ => panic!("Expected InvalidMessageLength error"),
        }
    }

    #[test]
    fn test_reject_excessive_uncompressed_size() {
        let mut data = Vec::new();
        data.extend_from_slice(&(OpCode::Msg as i32).to_le_bytes());
        data.extend_from_slice(&(MAX_UNCOMPRESSED_SIZE + 1).to_le_bytes()); // Too large!
        data.push(CompressorId::Snappy as u8);
        data.extend_from_slice(b"data");

        let header = MessageHeader {
            message_length: (MessageHeader::SIZE + data.len()) as i32,
            request_id: 1,
            response_to: 0,
            op_code: OpCode::Compressed as i32,
        };

        let stream = SliceStream::new(&data);
        let result = OpCompressed::parse_sync(&stream, &header, data.len());

        assert!(result.is_err());
    }

    #[test]
    fn test_validate_zlib_header_valid() {
        // Valid zlib header: CMF=0x78 (deflate, 32K window), FLG=0x9C
        // 0x78 * 256 + 0x9C = 30876, which is divisible by 31
        let data = vec![0x78, 0x9C, 0x00, 0x00, 0x00];

        let result = OpCompressed::validate_zlib_header(&data, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_zlib_header_invalid_checksum() {
        // Invalid zlib header: bad FCHECK
        let data = vec![0x78, 0x00, 0x00]; // FCHECK would fail

        let result = OpCompressed::validate_zlib_header(&data, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_zlib_header_invalid_method() {
        // Invalid compression method (not 8)
        let data = vec![0x79, 0x9C, 0x00]; // CM = 9

        let result = OpCompressed::validate_zlib_header(&data, 100);
        assert!(result.is_err());
    }

    #[test]
    fn test_compressor_vulnerability_check() {
        assert!(CompressorId::Zlib.is_vulnerable());
        assert!(!CompressorId::Snappy.is_vulnerable());
        assert!(!CompressorId::Zstd.is_vulnerable());
        assert!(!CompressorId::Noop.is_vulnerable());
    }
}
