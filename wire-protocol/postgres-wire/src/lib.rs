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

//! PostgreSQL wire protocol streaming parser.
//!
//! This crate provides a streaming parser for the PostgreSQL Frontend/Backend Protocol,
//! built on top of the `wire-stream` primitives.
//!
//! # Protocol Overview
//!
//! PostgreSQL uses a message-based protocol where each message has:
//! - 1-byte message type identifier (except for startup messages)
//! - 4-byte message length (big-endian, includes the length field itself)
//! - Message payload
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//! - **Full protocol support**: Simple query, extended query, and COPY protocols
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use postgres_wire::{PgReadSync, types::ReadyForQuery};
//!
//! let data = &[/* PostgreSQL message bytes */];
//! let stream = SliceStream::new(data);
//!
//! // Read message type and length
//! let msg_type = stream.read_u8_sync().unwrap();
//! let length = stream.read_i32_be_sync().unwrap();
//! ```

pub mod error;
pub mod extensions;
pub mod frontend;
pub mod limits;
pub mod parse;
pub mod sql;
pub mod stmt_cache;
pub mod types;
pub mod write;

#[cfg(feature = "scram")]
pub mod scram;

#[cfg(test)]
mod tests;

pub use error::{IncorrectMessageType, PgWireError};
pub use limits::{LimitExceeded, Limits};
pub use parse::{PgParse, PgParseError, PgParseSync};
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};

// Type aliases for PostgreSQL-specific extensions
pub use crate::pg_ext::{PgRead, PgReadExt, PgReadSync, PgReadSyncExt};

/// PostgreSQL-specific extension traits that add protocol-aware methods.
///
/// PostgreSQL uses **big-endian** (network byte order) for all numeric values.
pub mod pg_ext {
    use crate::error::IncorrectMessageType;
    use wire_stream::{WireRead, WireReadSync};

    /// PostgreSQL-specific synchronous reading trait.
    ///
    /// Adds PostgreSQL protocol-aware methods on top of `WireReadSync`.
    /// PostgreSQL uses big-endian (network) byte order for numeric values.
    pub trait PgReadSync: WireReadSync {
        /// Read a single byte.
        #[inline]
        fn read_u8_sync(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_exactly::<1>()?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a signed byte.
        #[inline]
        fn read_i8_sync(&self) -> Result<i8, Self::ReadError> {
            Ok(self.read_u8_sync()? as i8)
        }

        /// Read a 2-byte big-endian i16.
        #[inline]
        fn read_i16_be_sync(&self) -> Result<i16, Self::ReadError> {
            let borrow = self.peek_exactly::<2>()?;
            let value = i16::from_be_bytes(*borrow);
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

        /// Read a 4-byte big-endian i32.
        ///
        /// PostgreSQL uses i32 for message lengths.
        #[inline]
        fn read_i32_be_sync(&self) -> Result<i32, Self::ReadError> {
            let borrow = self.peek_exactly::<4>()?;
            let value = i32::from_be_bytes(*borrow);
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

        /// Read an 8-byte big-endian i64.
        #[inline]
        fn read_i64_be_sync(&self) -> Result<i64, Self::ReadError> {
            let borrow = self.peek_exactly::<8>()?;
            let value = i64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read an 8-byte big-endian u64.
        #[inline]
        fn read_u64_be_sync(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_exactly::<8>()?;
            let value = u64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read PostgreSQL message header (1-byte type + 4-byte length).
        ///
        /// Returns (message_type, payload_length).
        /// The length field in PostgreSQL includes itself (4 bytes) but NOT the type byte.
        /// This function returns the payload length (length - 4).
        ///
        /// Note: Callers should validate that payload_length >= 0 before using it.
        /// A negative payload length indicates a malformed message.
        #[inline]
        fn read_message_header_sync(&self) -> Result<(u8, i32), Self::ReadError> {
            let msg_type = self.read_u8_sync()?;
            let length = self.read_i32_be_sync()?;
            // Length includes itself (4 bytes) but not the type byte
            let payload_length = length.saturating_sub(4);
            Ok((msg_type, payload_length))
        }

        /// Check for expected message type byte and consume it.
        #[inline]
        fn expect_message_type_sync(&self, expected: u8) -> Result<Result<(), IncorrectMessageType>, Self::ReadError> {
            let borrow = self.peek_exactly::<1>()?;
            let encountered = borrow[0];

            if encountered != expected {
                Ok(Err(IncorrectMessageType { encountered, expected }))
            } else {
                self.accept_exactly(&borrow)?;
                Ok(Ok(()))
            }
        }

        /// Read a NUL-terminated string (C-string).
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

        /// Read exactly `len` bytes.
        fn read_bytes_sync(&self, len: usize) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::with_capacity(len.min(8192));
            for _ in 0..len {
                result.push(self.read_u8_sync()?);
            }
            Ok(result)
        }

        /// Skip exactly `len` bytes.
        fn skip_bytes_sync(&self, len: usize) -> Result<(), Self::ReadError> {
            for _ in 0..len {
                self.read_u8_sync()?;
            }
            Ok(())
        }
    }

    impl<T: WireReadSync + ?Sized> PgReadSync for T {}

    /// PostgreSQL-specific asynchronous reading trait.
    pub trait PgRead: WireRead + PgReadSync {
        /// Read a single byte (async version).
        async fn read_u8(&self) -> Result<u8, Self::ReadError> {
            let borrow = self.peek_read_exactly::<1>().await?;
            let value = borrow[0];
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read a signed byte (async version).
        async fn read_i8(&self) -> Result<i8, Self::ReadError> {
            Ok(self.read_u8().await? as i8)
        }

        /// Read a 2-byte big-endian i16 (async version).
        async fn read_i16_be(&self) -> Result<i16, Self::ReadError> {
            let borrow = self.peek_read_exactly::<2>().await?;
            let value = i16::from_be_bytes(*borrow);
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

        /// Read a 4-byte big-endian i32 (async version).
        async fn read_i32_be(&self) -> Result<i32, Self::ReadError> {
            let borrow = self.peek_read_exactly::<4>().await?;
            let value = i32::from_be_bytes(*borrow);
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

        /// Read an 8-byte big-endian i64 (async version).
        async fn read_i64_be(&self) -> Result<i64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<8>().await?;
            let value = i64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read an 8-byte big-endian u64 (async version).
        async fn read_u64_be(&self) -> Result<u64, Self::ReadError> {
            let borrow = self.peek_read_exactly::<8>().await?;
            let value = u64::from_be_bytes(*borrow);
            self.accept_exactly(&borrow)?;
            Ok(value)
        }

        /// Read PostgreSQL message header (async version).
        async fn read_message_header(&self) -> Result<(u8, i32), Self::ReadError> {
            let msg_type = self.read_u8().await?;
            let length = self.read_i32_be().await?;
            let payload_length = length.saturating_sub(4);
            Ok((msg_type, payload_length))
        }

        /// Check for expected message type byte (async version).
        async fn expect_message_type(&self, expected: u8) -> Result<Result<(), IncorrectMessageType>, Self::ReadError> {
            let borrow = self.peek_read_exactly::<1>().await?;
            let encountered = borrow[0];

            if encountered != expected {
                Ok(Err(IncorrectMessageType { encountered, expected }))
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

        /// Read exactly `len` bytes (async version).
        async fn read_bytes(&self, len: usize) -> Result<Vec<u8>, Self::ReadError> {
            let mut result = Vec::with_capacity(len.min(8192));
            for _ in 0..len {
                result.push(self.read_u8().await?);
            }
            Ok(result)
        }

        /// Skip exactly `len` bytes (async version).
        async fn skip_bytes(&self, len: usize) -> Result<(), Self::ReadError> {
            for _ in 0..len {
                self.read_u8().await?;
            }
            Ok(())
        }
    }

    impl<T: WireRead + ?Sized> PgRead for T {}

    /// Extension trait re-export for convenience.
    pub trait PgReadExt: PgRead {}
    pub trait PgReadSyncExt: PgReadSync {}

    impl<T: PgRead + ?Sized> PgReadExt for T {}
    impl<T: PgReadSync + ?Sized> PgReadSyncExt for T {}
}
