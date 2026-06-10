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

//! Oracle TNS (Transparent Network Substrate) wire protocol streaming parser.
//!
//! This crate provides a streaming parser for Oracle's TNS wire protocol,
//! built on top of the `wire-stream` primitives.
//!
//! # Protocol Versions
//!
//! Oracle TNS has evolved through several versions:
//!
//! - **TNS v8**: Oracle 8i era protocol
//! - **TNS v9**: Oracle 9i with improved security
//! - **TNS v10**: Oracle 10g with connection pooling enhancements
//! - **TNS v11**: Oracle 11g with session multiplexing
//! - **TNS v12**: Oracle 12c with multitenant architecture support
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//! - **Multi-version support**: Handle different TNS protocol versions
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use oracle_wire::{OracleParseSync, types::packet::TnsPacket};
//!
//! let data = &[/* TNS packet bytes */];
//! let stream = SliceStream::new(data);
//!
//! let packet = TnsPacket::parse_sync(&stream).unwrap();
//! ```

pub mod checksum;
pub mod error;
pub mod fragment;
pub mod limits;
pub mod parse;
pub mod types;
pub mod write;

#[cfg(test)]
mod tests;

pub use error::{IncorrectPacketType, InvalidLength, OracleWireError};
pub use limits::{LimitExceeded, Limits};
pub use parse::{OracleConstruct, OracleConstructError, OracleParse, OracleParseError, OracleParseSync};
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};
pub use write::*;

// Type aliases for Oracle-specific extensions
pub use crate::oracle_ext::{OracleRead, OracleReadExt, OracleReadSync, OracleReadSyncExt};

/// Oracle-specific extension traits that add protocol-aware methods.
pub mod oracle_ext {
    use crate::error::IncorrectPacketType;
    use wire_stream::{WireRead, WireReadSync};

    /// Oracle-specific synchronous reading trait.
    ///
    /// Adds Oracle TNS protocol-aware methods on top of `WireReadSync`.
    /// TNS uses big-endian byte order for numeric values.
    pub trait OracleReadSync: WireReadSync {
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

        /// Read a 4-byte big-endian u32.
        #[inline]
        fn read_u32_be_sync(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_exactly::<4>()?;
            let value = u32::from_be_bytes(*borrow);
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

        /// Check for expected TNS packet type byte and consume it.
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

        /// Read TNS packet header (8 bytes for standard TNS).
        /// Returns (packet_length, packet_checksum, packet_type, reserved_byte, header_checksum).
        #[inline]
        fn read_tns_header_sync(&self) -> Result<(u16, u16, u8, u8, u16), Self::ReadError> {
            let packet_length = self.read_u16_be_sync()?;
            let packet_checksum = self.read_u16_be_sync()?;
            let packet_type = self.read_u8_sync()?;
            let reserved = self.read_u8_sync()?;
            let header_checksum = self.read_u16_be_sync()?;
            Ok((packet_length, packet_checksum, packet_type, reserved, header_checksum))
        }
    }

    impl<T: WireReadSync + ?Sized> OracleReadSync for T {}

    /// Oracle-specific asynchronous reading trait.
    pub trait OracleRead: WireRead + OracleReadSync {
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

        /// Read a 4-byte big-endian u32 (async version).
        async fn read_u32_be(&self) -> Result<u32, Self::ReadError> {
            let borrow = self.peek_read_exactly::<4>().await?;
            let value = u32::from_be_bytes(*borrow);
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

        /// Read bytes (async version).
        async fn read_bytes(&self, len: usize) -> Result<Self::ReadBorrow<'_>, Self::ReadError> {
            let borrow = self.peek_read(Some(len)).await?;
            self.accept(&borrow, None)?;
            Ok(borrow)
        }

        /// Check for expected TNS packet type byte and consume it (async version).
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

        /// Read TNS packet header (async version).
        async fn read_tns_header(&self) -> Result<(u16, u16, u8, u8, u16), Self::ReadError> {
            let packet_length = self.read_u16_be().await?;
            let packet_checksum = self.read_u16_be().await?;
            let packet_type = self.read_u8().await?;
            let reserved = self.read_u8().await?;
            let header_checksum = self.read_u16_be().await?;
            Ok((packet_length, packet_checksum, packet_type, reserved, header_checksum))
        }
    }

    impl<T: WireRead + ?Sized> OracleRead for T {}

    /// Extension trait re-export for convenience.
    pub trait OracleReadExt: OracleRead {}
    pub trait OracleReadSyncExt: OracleReadSync {}

    impl<T: OracleRead + ?Sized> OracleReadExt for T {}
    impl<T: OracleReadSync + ?Sized> OracleReadSyncExt for T {}
}
