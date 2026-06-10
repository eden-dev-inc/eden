//! LZ4 compression support for ClickHouse native protocol.
//!
//! ClickHouse uses LZ4 compression with CityHash128 checksums for data blocks.

use crate::MAX_DECOMPRESSED_SIZE;
use crate::error::ClickhouseWireError;
use crate::native::read::ClickhouseReadSyncExt;
use crate::native::write::ClickhouseWriteExt;
use std::io::{self, Write};
use wire_stream::{WireRead, WireReadSync};

/// Compression method identifiers.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
#[repr(u8)]
pub enum CompressionMethod {
    /// No compression (but checksum is still used).
    None = 0x02,
    /// LZ4 compression (default).
    Lz4 = 0x82,
    /// LZ4HC (high compression).
    Lz4Hc = 0x92,
    /// ZSTD compression.
    Zstd = 0x90,
}

impl CompressionMethod {
    /// Convert from u8.
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0x02 => Some(Self::None),
            0x82 => Some(Self::Lz4),
            0x92 => Some(Self::Lz4Hc),
            0x90 => Some(Self::Zstd),
            _ => None,
        }
    }

    /// Check if this method involves actual compression.
    pub fn is_compressed(&self) -> bool {
        !matches!(self, Self::None)
    }
}

/// Size of the compression header (method + sizes).
pub const COMPRESSION_HEADER_SIZE: usize = 9;

/// Compressed block structure.
///
/// Format:
/// - 16 bytes: CityHash128 checksum (covering method + sizes + data)
/// - 1 byte: compression method
/// - 4 bytes: compressed size (LE u32, includes header)
/// - 4 bytes: decompressed size (LE u32)
/// - N bytes: compressed data
#[derive(Clone, Debug)]
pub struct CompressedBlock {
    /// CityHash128 checksum of the compressed data (including header).
    pub checksum: u128,
    /// Compression method.
    pub method: CompressionMethod,
    /// Size of compressed data (including 9-byte header).
    pub compressed_size: u32,
    /// Size of decompressed data.
    pub decompressed_size: u32,
    /// Compressed data payload.
    pub data: Vec<u8>,
}

impl CompressedBlock {
    /// Parse a compressed block from a synchronous stream.
    pub fn parse_sync<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireReadSync + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        // Read 128-bit checksum
        let checksum = stream.read_u128_le_ch_sync()?;

        // Read compression method
        let method_byte = stream.read_u8_ch_sync()?;
        let method = CompressionMethod::from_u8(method_byte).ok_or(ClickhouseWireError::UnsupportedCompression(method_byte))?;

        // Read sizes
        let compressed_size = stream.read_u32_le_ch_sync()?;
        let decompressed_size = stream.read_u32_le_ch_sync()?;

        // Validate sizes
        if decompressed_size as usize > MAX_DECOMPRESSED_SIZE {
            return Err(ClickhouseWireError::DecompressionSizeExceeded {
                actual: decompressed_size as usize,
                limit: MAX_DECOMPRESSED_SIZE,
            });
        }

        // Calculate payload size (compressed_size includes the 9-byte header)
        let payload_size = (compressed_size as usize)
            .checked_sub(COMPRESSION_HEADER_SIZE)
            .ok_or_else(|| ClickhouseWireError::InvalidBlock(format!("compressed_size {} too small for header", compressed_size)))?;

        // Read compressed data
        let data = stream.read_bytes_ch_sync(payload_size)?;

        Ok(Self { checksum, method, compressed_size, decompressed_size, data })
    }

    /// Parse a compressed block asynchronously.
    pub async fn parse<S>(stream: &S) -> Result<Self, ClickhouseWireError>
    where
        S: WireRead + ?Sized,
        S::ReadError: Into<ClickhouseWireError>,
    {
        use crate::native::read::ClickhouseReadExt;

        let checksum = stream.read_u128_le_ch().await?;

        let method_byte = stream.read_u8_ch().await?;
        let method = CompressionMethod::from_u8(method_byte).ok_or(ClickhouseWireError::UnsupportedCompression(method_byte))?;

        let compressed_size = stream.read_u32_le_ch().await?;
        let decompressed_size = stream.read_u32_le_ch().await?;

        if decompressed_size as usize > MAX_DECOMPRESSED_SIZE {
            return Err(ClickhouseWireError::DecompressionSizeExceeded {
                actual: decompressed_size as usize,
                limit: MAX_DECOMPRESSED_SIZE,
            });
        }

        let payload_size = (compressed_size as usize)
            .checked_sub(COMPRESSION_HEADER_SIZE)
            .ok_or_else(|| ClickhouseWireError::InvalidBlock(format!("compressed_size {} too small for header", compressed_size)))?;

        let borrow = stream.peek_read(Some(payload_size)).await.map_err(Into::into)?;
        let data = borrow.to_vec();
        stream.accept(&borrow, None).map_err(Into::into)?;

        Ok(Self { checksum, method, compressed_size, decompressed_size, data })
    }

    /// Rebuild the header bytes for checksum verification.
    fn build_header(&self) -> [u8; COMPRESSION_HEADER_SIZE] {
        let mut header = [0u8; COMPRESSION_HEADER_SIZE];
        header[0] = self.method as u8;
        header[1..5].copy_from_slice(&self.compressed_size.to_le_bytes());
        header[5..9].copy_from_slice(&self.decompressed_size.to_le_bytes());
        header
    }

    /// Validate the checksum using CityHash128.
    pub fn validate_checksum(&self) -> Result<(), ClickhouseWireError> {
        let header = self.build_header();
        let mut buffer = Vec::with_capacity(COMPRESSION_HEADER_SIZE + self.data.len());
        buffer.extend_from_slice(&header);
        buffer.extend_from_slice(&self.data);

        let actual = cityhash_rs::cityhash_102_128(&buffer);

        if actual != self.checksum {
            return Err(ClickhouseWireError::ChecksumMismatch { expected: self.checksum, actual });
        }

        Ok(())
    }

    /// Decompress the data.
    #[cfg(feature = "lz4")]
    pub fn decompress(&self) -> Result<Vec<u8>, ClickhouseWireError> {
        self.validate_checksum()?;

        match self.method {
            CompressionMethod::None => Ok(self.data.clone()),
            CompressionMethod::Lz4 | CompressionMethod::Lz4Hc => {
                let mut decompressed = vec![0u8; self.decompressed_size as usize];
                lz4_flex::decompress_into(&self.data, &mut decompressed)
                    .map_err(|e| ClickhouseWireError::DecompressionFailed(e.to_string()))?;
                Ok(decompressed)
            }
            CompressionMethod::Zstd => Err(ClickhouseWireError::UnsupportedCompression(self.method as u8)),
        }
    }

    /// Create a compressed block from raw data.
    #[cfg(feature = "lz4")]
    pub fn compress(data: &[u8], method: CompressionMethod) -> Result<Self, ClickhouseWireError> {
        let (compressed_data, decompressed_size) = match method {
            CompressionMethod::None => (data.to_vec(), data.len() as u32),
            CompressionMethod::Lz4 | CompressionMethod::Lz4Hc => {
                // Use raw compression (ClickHouse doesn't use lz4_flex's size prefix)
                let max_size = lz4_flex::block::get_maximum_output_size(data.len());
                let mut compressed = vec![0u8; max_size];
                let actual_size =
                    lz4_flex::compress_into(data, &mut compressed).map_err(|e| ClickhouseWireError::DecompressionFailed(e.to_string()))?;
                compressed.truncate(actual_size);
                (compressed, data.len() as u32)
            }
            CompressionMethod::Zstd => {
                return Err(ClickhouseWireError::UnsupportedCompression(method as u8));
            }
        };

        let compressed_size = (COMPRESSION_HEADER_SIZE + compressed_data.len()) as u32;

        // Build header for checksum
        let mut header = [0u8; COMPRESSION_HEADER_SIZE];
        header[0] = method as u8;
        header[1..5].copy_from_slice(&compressed_size.to_le_bytes());
        header[5..9].copy_from_slice(&decompressed_size.to_le_bytes());

        // Calculate checksum over header + data
        let mut buffer = Vec::with_capacity(COMPRESSION_HEADER_SIZE + compressed_data.len());
        buffer.extend_from_slice(&header);
        buffer.extend_from_slice(&compressed_data);
        let checksum = cityhash_rs::cityhash_102_128(&buffer);

        Ok(Self {
            checksum,
            method,
            compressed_size,
            decompressed_size,
            data: compressed_data,
        })
    }

    /// Encode the compressed block to a writer.
    pub fn encode<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_u128_le_ch(self.checksum)?;
        w.write_u8_ch(self.method as u8)?;
        w.write_u32_le_ch(self.compressed_size)?;
        w.write_u32_le_ch(self.decompressed_size)?;
        w.write_all(&self.data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_method_from_u8() {
        assert_eq!(CompressionMethod::from_u8(0x02), Some(CompressionMethod::None));
        assert_eq!(CompressionMethod::from_u8(0x82), Some(CompressionMethod::Lz4));
        assert_eq!(CompressionMethod::from_u8(0x92), Some(CompressionMethod::Lz4Hc));
        assert_eq!(CompressionMethod::from_u8(0x90), Some(CompressionMethod::Zstd));
        assert_eq!(CompressionMethod::from_u8(0xFF), None);
    }

    #[test]
    fn test_compression_method_is_compressed() {
        assert!(!CompressionMethod::None.is_compressed());
        assert!(CompressionMethod::Lz4.is_compressed());
        assert!(CompressionMethod::Lz4Hc.is_compressed());
        assert!(CompressionMethod::Zstd.is_compressed());
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_compress_decompress_roundtrip() {
        let original = b"Hello, ClickHouse! This is a test message for compression.";

        let compressed = CompressedBlock::compress(original, CompressionMethod::Lz4).unwrap();
        let decompressed = compressed.decompress().unwrap();

        assert_eq!(decompressed, original);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_compress_decompress_none() {
        let original = b"No compression test";

        let compressed = CompressedBlock::compress(original, CompressionMethod::None).unwrap();
        assert_eq!(compressed.data, original);

        let decompressed = compressed.decompress().unwrap();
        assert_eq!(decompressed, original);
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn test_compressed_block_encode_parse() {
        use wire_stream::SliceStream;

        let original = b"Test data for encode/parse roundtrip";
        let compressed = CompressedBlock::compress(original, CompressionMethod::Lz4).unwrap();

        let mut buf = Vec::new();
        compressed.encode(&mut buf).unwrap();

        let stream = SliceStream::new(&buf);
        let parsed = CompressedBlock::parse_sync(&stream).unwrap();

        assert_eq!(parsed.checksum, compressed.checksum);
        assert_eq!(parsed.method, compressed.method);
        assert_eq!(parsed.compressed_size, compressed.compressed_size);
        assert_eq!(parsed.decompressed_size, compressed.decompressed_size);
        assert_eq!(parsed.data, compressed.data);

        let decompressed = parsed.decompress().unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_checksum_validation() {
        let block = CompressedBlock {
            checksum: 0, // Wrong checksum
            method: CompressionMethod::None,
            compressed_size: 9 + 4, // header + data
            decompressed_size: 4,
            data: vec![1, 2, 3, 4],
        };

        let result = block.validate_checksum();
        assert!(matches!(result, Err(ClickhouseWireError::ChecksumMismatch { .. })));
    }
}
