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

//! Sybase TDS (Tabular Data Stream) wire protocol streaming parser.
//!
//! This crate provides a streaming parser for Sybase's TDS wire protocol,
//! built on top of the `wire-stream` primitives.
//!
//! # Protocol Versions
//!
//! Sybase TDS has evolved through several versions:
//!
//! - **TDS 4.2**: Original Sybase protocol
//! - **TDS 5.0**: Sybase-specific enhancements (most common for Sybase ASE)
//! - **TDS 7.0+**: Microsoft SQL Server divergence (not covered here)
//!
//! This crate focuses on Sybase-specific TDS (4.2 and 5.0).
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//!
//! # Packet Structure
//!
//! TDS packets have an 8-byte header:
//! - 1 byte: packet type
//! - 1 byte: status (last packet flag, etc.)
//! - 2 bytes: length (big-endian, includes header)
//! - 2 bytes: SPID/channel
//! - 1 byte: packet number
//! - 1 byte: window (unused, typically 0)
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use sybase_wire::{SybaseParseSync, types::packet::TdsPacket};
//!
//! let data = &[/* TDS packet bytes */];
//! let stream = SliceStream::new(data);
//!
//! let packet = TdsPacket::parse_sync(&stream).unwrap();
//! ```

pub mod error;
pub mod limits;
pub mod parse;
pub mod types;
pub mod write;

#[cfg(test)]
mod tests;

pub use error::{IncorrectPacketType, InvalidLength, SybaseWireError};
pub use limits::{LimitExceeded, Limits};
pub use parse::{SybaseConstruct, SybaseConstructError, SybaseParse, SybaseParseError, SybaseParseSync};
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};
pub use write::*;

// Type aliases for Sybase-specific extensions
pub use crate::sybase_ext::{SybaseRead, SybaseReadExt, SybaseReadSync, SybaseReadSyncExt};

/// Sybase-specific extension traits that add protocol-aware methods.
pub mod sybase_ext {
    use crate::error::IncorrectPacketType;
    use wire_stream::{WireRead, WireReadSync};

    type TdsHeader = (u8, u8, u16, u16, u8, u8);

    /// TDS header size in bytes.
    pub const TDS_HEADER_SIZE: usize = 8;

    /// Sybase-specific synchronous reading trait.
    ///
    /// Adds Sybase TDS protocol-aware methods on top of `WireReadSync`.
    /// TDS uses big-endian byte order for header fields.
    pub trait SybaseReadSync: WireReadSync {
        /// Read a single byte.
        #[inline]
        fn read_u8_sync(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_exactly::<1>()?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 2-byte big-endian u16.
        #[inline]
        fn read_u16_be_sync(&self) -> Result<u16, Self::ReadError> {
            let borrow = self.peek_exactly::<2>()?;
            let value = u16::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 2-byte little-endian u16.
        /// TDS uses little-endian for data values within packets.
        #[inline]
        fn read_u16_le_sync(&self) -> Result<u16, Self::ReadError> {
            let borrow = self.peek_exactly::<2>()?;
            let value = u16::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 4-byte big-endian u32.
        #[inline]
        fn read_u32_be_sync(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_exactly::<4>()?;
            let value = u32::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 4-byte little-endian u32.
        /// TDS uses little-endian for data values within packets.
        #[inline]
        fn read_u32_le_sync(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_exactly::<4>()?;
            let value = u32::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 8-byte big-endian u64.
        #[inline]
        fn read_u64_be_sync(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_exactly::<8>()?;
            let value = u64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 8-byte little-endian u64.
        #[inline]
        fn read_u64_le_sync(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_exactly::<8>()?;
            let value = u64::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Check for expected TDS packet type byte and consume it.
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

        /// Read TDS packet header (8 bytes).
        /// Returns (packet_type, status, length, spid, packet_number, window).
        #[inline]
        fn read_tds_header_sync(&self) -> Result<TdsHeader, Self::ReadError> {
            let packet_type = self.read_u8_sync()?;
            let status = self.read_u8_sync()?;
            let length = self.read_u16_be_sync()?;
            let spid = self.read_u16_be_sync()?;
            let packet_number = self.read_u8_sync()?;
            let window = self.read_u8_sync()?;
            Ok((packet_type, status, length, spid, packet_number, window))
        }

        /// Read a length-prefixed string (1-byte length prefix).
        #[inline]
        fn read_varchar_sync(&self) -> Result<Vec<u8>, Self::ReadError> {
            let len = self.read_u8_sync()? as usize;
            if len == 0 {
                return Ok(Vec::new());
            }
            let borrow = self.peek(Some(len))?;
            let data = borrow[..len].to_vec();
            self.accept(&borrow, None)?;
            Ok(data)
        }

        /// Read a length-prefixed string (2-byte length prefix, little-endian).
        #[inline]
        fn read_longvarchar_sync(&self) -> Result<Vec<u8>, Self::ReadError> {
            let len = self.read_u16_le_sync()? as usize;
            if len == 0 {
                return Ok(Vec::new());
            }
            let borrow = self.peek(Some(len))?;
            let data = borrow[..len].to_vec();
            self.accept(&borrow, None)?;
            Ok(data)
        }
    }

    impl<T: WireReadSync + ?Sized> SybaseReadSync for T {}

    /// Sybase-specific asynchronous reading trait.
    pub trait SybaseRead: WireRead + SybaseReadSync {
        /// Read a single byte (async version).
        async fn read_u8(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_read_exactly::<1>().await?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 2-byte big-endian u16 (async version).
        async fn read_u16_be(&self) -> Result<u16, Self::ReadError> {
            let borrow = self.peek_read_exactly::<2>().await?;
            let value = u16::from_be_bytes(*borrow);
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

        /// Read a 4-byte big-endian u32 (async version).
        async fn read_u32_be(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_read_exactly::<4>().await?;
            let value = u32::from_be_bytes(*borrow);
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

        /// Read a 8-byte big-endian u64 (async version).
        async fn read_u64_be(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<8>().await?;
            let value = u64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a 8-byte little-endian u64 (async version).
        async fn read_u64_le(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<8>().await?;
            let value = u64::from_le_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read bytes (async version).
        async fn read_bytes(&self, len: usize) -> Result<Self::ReadBorrow<'_>, Self::ReadError> {
            let borrow = self.peek_read(Some(len)).await?;
            self.accept(&borrow, None)?;
            Ok(borrow)
        }

        /// Check for expected TDS packet type byte and consume it (async version).
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

        /// Read TDS packet header (async version).
        async fn read_tds_header(&self) -> Result<TdsHeader, Self::ReadError> {
            let packet_type = self.read_u8().await?;
            let status = self.read_u8().await?;
            let length = self.read_u16_be().await?;
            let spid = self.read_u16_be().await?;
            let packet_number = self.read_u8().await?;
            let window = self.read_u8().await?;
            Ok((packet_type, status, length, spid, packet_number, window))
        }

        /// Read a length-prefixed string (1-byte length prefix, async version).
        async fn read_varchar(&self) -> Result<Vec<u8>, Self::ReadError> {
            let len = self.read_u8().await? as usize;
            if len == 0 {
                return Ok(Vec::new());
            }
            let borrow = self.peek_read(Some(len)).await?;
            let data = borrow[..len].to_vec();
            self.accept(&borrow, None)?;
            Ok(data)
        }

        /// Read a length-prefixed string (2-byte length prefix, little-endian, async version).
        async fn read_longvarchar(&self) -> Result<Vec<u8>, Self::ReadError> {
            let len = self.read_u16_le().await? as usize;
            if len == 0 {
                return Ok(Vec::new());
            }
            let borrow = self.peek_read(Some(len)).await?;
            let data = borrow[..len].to_vec();
            self.accept(&borrow, None)?;
            Ok(data)
        }
    }

    impl<T: WireRead + ?Sized> SybaseRead for T {}

    /// Extension trait re-export for convenience.
    pub trait SybaseReadExt: SybaseRead {}
    pub trait SybaseReadSyncExt: SybaseReadSync {}

    impl<T: SybaseRead + ?Sized> SybaseReadExt for T {}
    impl<T: SybaseReadSync + ?Sized> SybaseReadSyncExt for T {}
}
