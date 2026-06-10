#![cfg_attr(test, allow(clippy::unwrap_used))]
// This crate is internal to Eden and not intended for external use.
//
// We allow async fn in traits here. The alternative would be desugaring:
//   async fn foo(&self) -> T
// into:
//   fn foo(&self) -> impl Future<Output = T> + Send
//
// The issue: `async fn` in traits returns an opaque future whose Send-ness
// depends on the implementation. Rust can't guarantee Send across all impls,
// so the future is not Send by default. This means you can't do:
//   tokio::spawn(trait_object.async_method())  // won't compile without Send
//
// We accept this because:
// - Internal crate with controlled execution context
// - Our usage doesn't require spawning these futures across threads
//
// If someone tries to use these futures in a Send context (e.g., tokio::spawn),
// they'll get a compile-time error - not a runtime failure. The compiler will
// catch the misuse. If that happens, refactor to the desugared form with + Send.
#![allow(async_fn_in_trait)]

//! MySQL wire protocol streaming parser.
//!
//! This crate provides a streaming parser for the MySQL client-server protocol,
//! built on top of the `wire-stream` primitives.
//!
//! # Protocol Overview
//!
//! MySQL uses a packet-based protocol where each packet has:
//! - 3-byte payload length (little-endian)
//! - 1-byte sequence ID
//! - Payload data (up to 16MB - 1)
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//! - **MySQL 5.x and 8.x support**: Handle both protocol versions
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use mysql_wire::{MysqlParseSync, types::packet::MysqlPacketHeader};
//!
//! let data = &[/* MySQL packet bytes */];
//! let stream = SliceStream::new(data);
//!
//! let header = MysqlPacketHeader::parse_sync(&stream).unwrap();
//! ```

pub mod builder;
pub mod capabilities;
pub mod charset;
pub mod compression;
pub mod error;
pub mod limits;
pub mod parse;
pub mod pipeline;
pub mod types;
pub mod write;

#[cfg(test)]
mod tests;

pub use builder::{MysqlEncode, ResultSetBuilder, TextResultSetBuilder};
pub use capabilities::CapabilityFlags;
pub use charset::{CharsetInfo, charset_by_id, charset_by_name};
pub use compression::{CompressedPacketHeader, CompressionContext, CompressionError};
pub use error::{ColumnFlags, IncorrectPacketType, InvalidLength, MysqlWireError, ServerStatusFlags};
pub use limits::{LimitExceeded, Limits};
pub use parse::{MysqlBuilder, MysqlConstruct, MysqlConstructError, MysqlParse, MysqlParseError, MysqlParseSync};
pub use pipeline::{AsyncPipeline, AsyncPipelineExt, MysqlSlice, Pipeline, PipelineError, PipelineExt};
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};
pub use write::*;

// Type aliases for MySQL-specific extensions
pub use crate::mysql_ext::{MysqlRead, MysqlReadExt, MysqlReadSync, MysqlReadSyncExt};

/// MySQL-specific extension traits that add protocol-aware methods.
pub mod mysql_ext {
    use crate::error::{IncorrectPacketType, InvalidLength};
    use wire_stream::{WireRead, WireReadSync};

    /// MySQL-specific synchronous reading trait.
    ///
    /// Adds MySQL protocol-aware methods on top of `WireReadSync`.
    /// MySQL uses little-endian byte order for numeric values.
    pub trait MysqlReadSync: WireReadSync {
        /// Read a single byte.
        #[inline]
        fn read_u8_sync(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_exactly::<1>()?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 2-byte little-endian u16.
        #[inline]
        fn read_u16_le_sync(&self) -> Result<u16, Self::ReadError> {
            let borrow = self.peek_exactly::<2>()?;
            let value = u16::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 3-byte little-endian u24 (as u32).
        ///
        /// MySQL uses 3-byte integers for packet payload lengths.
        #[inline]
        fn read_u24_le_sync(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_exactly::<3>()?;
            let value = u32::from_le_bytes([borrow[0], borrow[1], borrow[2], 0]);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 4-byte little-endian u32.
        #[inline]
        fn read_u32_le_sync(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_exactly::<4>()?;
            let value = u32::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 6-byte little-endian u48 (as u64).
        ///
        /// MySQL uses 6-byte integers for some timestamp fields.
        #[inline]
        fn read_u48_le_sync(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_exactly::<6>()?;
            let value = u64::from_le_bytes([borrow[0], borrow[1], borrow[2], borrow[3], borrow[4], borrow[5], 0, 0]);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read an 8-byte little-endian u64.
        #[inline]
        fn read_u64_le_sync(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_exactly::<8>()?;
            let value = u64::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a length-encoded integer.
        ///
        /// MySQL's length-encoded integer format:
        /// - 0x00-0xFA: 1-byte value
        /// - 0xFB: NULL indicator (returns u64::MAX)
        /// - 0xFC: 2-byte value follows
        /// - 0xFD: 3-byte value follows
        /// - 0xFE: 8-byte value follows
        /// - 0xFF: Reserved for ERR packet header
        #[inline]
        fn read_lenenc_int_sync(&self) -> Result<Result<u64, InvalidLength>, Self::ReadError> {
            let first = self.read_u8_sync()?;
            match first {
                0..=0xFA => Ok(Ok(first as u64)),
                0xFB => Ok(Ok(u64::MAX)), // NULL indicator
                0xFC => Ok(Ok(self.read_u16_le_sync()? as u64)),
                0xFD => Ok(Ok(self.read_u24_le_sync()? as u64)),
                0xFE => Ok(Ok(self.read_u64_le_sync()?)),
                0xFF => Ok(Err(InvalidLength::Reserved)), // Reserved for ERR packet
            }
        }

        /// Read MySQL packet header (4 bytes: 3-byte length + 1-byte sequence).
        ///
        /// Returns (payload_length, sequence_id).
        #[inline]
        fn read_packet_header_sync(&self) -> Result<(u32, u8), Self::ReadError> {
            let payload_length = self.read_u24_le_sync()?;
            let sequence_id = self.read_u8_sync()?;
            Ok((payload_length, sequence_id))
        }

        /// Check for expected packet type byte and consume it.
        #[inline]
        fn expect_packet_type_sync(&self, expected: u8) -> Result<Result<(), IncorrectPacketType>, Self::ReadError> {
            let borrow = self.peek_exactly::<1>()?;
            let encountered = borrow[0];

            if encountered != expected {
                Ok(Err(IncorrectPacketType { encountered, expected }))
            } else {
                self.accept_exactly(&borrow)?;
                Ok(Ok(()))
            }
        }

        /// Read a NUL-terminated string.
        ///
        /// Returns the bytes before the NUL terminator, consuming the NUL.
        fn read_cstring_sync(&self) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::new();
            loop {
                let byte = self.read_u8_sync()?;
                if byte == 0 {
                    break;
                }
                result.push(byte);
            }
            Ok(result)
        }

        /// Read a length-encoded string.
        ///
        /// First reads the length as a length-encoded integer, then reads that many bytes.
        fn read_lenenc_string_sync(&self) -> Result<Result<Vec<u8>, InvalidLength>, Self::ReadError> {
            let len = match self.read_lenenc_int_sync()? {
                Ok(len) => len,
                Err(e) => return Ok(Err(e)),
            };

            if len == u64::MAX {
                // NULL
                return Ok(Ok(Vec::new()));
            }

            let mut result = Vec::with_capacity(len.min(8192) as usize);
            for _ in 0..len {
                result.push(self.read_u8_sync()?);
            }
            Ok(Ok(result))
        }

        /// Read exactly `len` bytes.
        fn read_bytes_sync(&self, len: usize) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::with_capacity(len.min(8192));
            for _ in 0..len {
                result.push(self.read_u8_sync()?);
            }
            Ok(result)
        }
    }

    impl<T: WireReadSync + ?Sized> MysqlReadSync for T {}

    /// MySQL-specific asynchronous reading trait.
    pub trait MysqlRead: WireRead + MysqlReadSync {
        /// Read a single byte (async version).
        async fn read_u8(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_read_exactly::<1>().await?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 2-byte little-endian u16 (async version).
        async fn read_u16_le(&self) -> Result<u16, Self::ReadError> {
            let borrow = self.peek_read_exactly::<2>().await?;
            let value = u16::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 3-byte little-endian u24 (async version).
        async fn read_u24_le(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_read_exactly::<3>().await?;
            let value = u32::from_le_bytes([borrow[0], borrow[1], borrow[2], 0]);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 4-byte little-endian u32 (async version).
        async fn read_u32_le(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_read_exactly::<4>().await?;
            let value = u32::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 6-byte little-endian u48 (async version).
        async fn read_u48_le(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<6>().await?;
            let value = u64::from_le_bytes([borrow[0], borrow[1], borrow[2], borrow[3], borrow[4], borrow[5], 0, 0]);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read an 8-byte little-endian u64 (async version).
        async fn read_u64_le(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<8>().await?;
            let value = u64::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a length-encoded integer (async version).
        async fn read_lenenc_int(&self) -> Result<Result<u64, InvalidLength>, Self::ReadError> {
            let first = self.read_u8().await?;
            match first {
                0..=0xFA => Ok(Ok(first as u64)),
                0xFB => Ok(Ok(u64::MAX)),
                0xFC => Ok(Ok(self.read_u16_le().await? as u64)),
                0xFD => Ok(Ok(self.read_u24_le().await? as u64)),
                0xFE => Ok(Ok(self.read_u64_le().await?)),
                0xFF => Ok(Err(InvalidLength::Reserved)),
            }
        }

        /// Read MySQL packet header (async version).
        async fn read_packet_header(&self) -> Result<(u32, u8), Self::ReadError> {
            let payload_length = self.read_u24_le().await?;
            let sequence_id = self.read_u8().await?;
            Ok((payload_length, sequence_id))
        }

        /// Read bytes (async version).
        async fn read_bytes(&self, len: usize) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::with_capacity(len.min(8192));
            for _ in 0..len {
                result.push(self.read_u8().await?);
            }
            Ok(result)
        }

        /// Check for expected packet type byte (async version).
        async fn expect_packet_type(&self, expected: u8) -> Result<Result<(), IncorrectPacketType>, Self::ReadError> {
            let borrow = self.peek_read_exactly::<1>().await?;
            let encountered = borrow[0];

            if encountered != expected {
                Ok(Err(IncorrectPacketType { encountered, expected }))
            } else {
                self.accept_exactly(&borrow)?;
                Ok(Ok(()))
            }
        }

        /// Read a NUL-terminated string (async version).
        async fn read_cstring(&self) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::new();
            loop {
                let byte = self.read_u8().await?;
                if byte == 0 {
                    break;
                }
                result.push(byte);
            }
            Ok(result)
        }

        /// Read a length-encoded string (async version).
        async fn read_lenenc_string(&self) -> Result<Result<Vec<u8>, InvalidLength>, Self::ReadError> {
            let len = match self.read_lenenc_int().await? {
                Ok(len) => len,
                Err(e) => return Ok(Err(e)),
            };

            if len == u64::MAX {
                return Ok(Ok(Vec::new()));
            }

            let mut result = Vec::with_capacity(len.min(8192) as usize);
            for _ in 0..len {
                result.push(self.read_u8().await?);
            }
            Ok(Ok(result))
        }
    }

    impl<T: WireRead + ?Sized> MysqlRead for T {}

    /// Extension trait re-export for convenience.
    pub trait MysqlReadExt: MysqlRead {}
    pub trait MysqlReadSyncExt: MysqlReadSync {}

    impl<T: MysqlRead + ?Sized> MysqlReadExt for T {}
    impl<T: MysqlReadSync + ?Sized> MysqlReadSyncExt for T {}
}
