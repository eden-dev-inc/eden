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

//! RESP (Redis Serialization Protocol) streaming parser.
//!
//! This crate provides a streaming parser for RESP2 and RESP3 protocols,
//! built on top of the `wire-stream` primitives.
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//! - **RESP2 and RESP3 support**: Full support for all RESP types
//!
//! # Example
//!
//! ```rust
//! use wire_stream::SliceStream;
//! use resp_wire::{RespParse, RespParseSync, types::simple_string::SimpleString};
//!
//! let data = b"+OK\r\n";
//! let stream = SliceStream::new(data);
//!
//! let mut reader = SimpleString::parse_sync(&stream).unwrap();
//! let content = reader.next_sync().unwrap().unwrap();
//! assert_eq!(&*content, b"OK");
//! ```

pub mod builder;
pub mod error;
pub mod limits;
pub mod parse;
pub mod pipeline;
#[cfg(test)]
pub mod tests;
pub mod types;
pub mod write;

pub use builder::*;
pub use error::{IncorrectTag, InvalidLength};
pub use parse::{RespConstruct, RespConstructError, RespParse, RespParseError, RespParseSync};
pub use pipeline::{Pipeline, PipelineError, PipelineExt, RespSlice};
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};
pub use write::*;

// Type aliases for RESP-specific extensions
pub use crate::resp_ext::{RespRead, RespReadExt, RespReadSync, RespReadSyncExt};

/// RESP-specific extension traits that add protocol-aware methods.
mod resp_ext {
    use crate::error::{IncorrectTag, InvalidLength};
    use wire_stream::{WireRead, WireReadExt as WireReadExtTrait, WireReadSync, WireReadSyncExt};

    /// RESP-specific synchronous reading trait.
    ///
    /// Adds RESP protocol-aware methods on top of `WireReadSync`.
    pub trait RespReadSync: WireReadSync {
        /// Check for expected RESP tag byte and consume it.
        #[inline]
        fn expect_tag_sync(&self, expected: u8) -> Result<Result<(), IncorrectTag>, Self::ReadError> {
            match self.expect_byte_sync(expected)? {
                Ok(()) => Ok(Ok(())),
                Err(encountered) => Ok(Err(IncorrectTag { encountered, expected })),
            }
        }

        /// Parse a RESP length value followed by CRLF (sync version).
        #[inline]
        fn expect_length_sync(&self) -> Result<Result<usize, InvalidLength>, Self::ReadError> {
            // Max length digits (20 for usize::MAX) + 2 for CRLF
            self.read_to_crlf_sync(Some(22)).map(|result| {
                if let Ok(line) = result {
                    // Fast path: manual parsing without UTF-8 conversion
                    let mut value: usize = 0;
                    for &b in line.iter() {
                        let digit = b.wrapping_sub(b'0');
                        if digit > 9 {
                            return Err(InvalidLength::NonNumeric);
                        }
                        value = value.checked_mul(10).and_then(|v| v.checked_add(digit as usize)).ok_or(InvalidLength::TooLarge)?;
                    }
                    Ok(value)
                } else {
                    Err(InvalidLength::TooLarge)
                }
            })
        }

        /// Parse a signed RESP integer value followed by CRLF (sync version).
        #[inline]
        fn expect_integer_sync(&self) -> Result<Result<i128, InvalidLength>, Self::ReadError> {
            self.read_to_crlf_sync(Some(41)).map(|result| {
                if let Ok(line) = result {
                    let s = std::str::from_utf8(&line).map_err(InvalidLength::InvalidUtf8)?;
                    let n = s.parse::<i128>().map_err(InvalidLength::ParseIntError)?;
                    Ok(n)
                } else {
                    Err(InvalidLength::TooLarge)
                }
            })
        }
    }

    impl<T: WireReadSync + ?Sized> RespReadSync for T {}

    /// RESP-specific asynchronous reading trait.
    pub trait RespRead: WireRead + RespReadSync {
        /// Check for expected RESP tag byte and consume it (async version).
        async fn expect_tag(&self, expected: u8) -> Result<Result<(), IncorrectTag>, Self::ReadError> {
            match self.expect_byte(expected).await? {
                Ok(()) => Ok(Ok(())),
                Err(encountered) => Ok(Err(IncorrectTag { encountered, expected })),
            }
        }

        /// Parse a RESP length value followed by CRLF (async version).
        async fn expect_length(&self) -> Result<Result<usize, InvalidLength>, Self::ReadError> {
            self.read_to_crlf(Some(22)).await.map(|result| {
                if let Ok(line) = result {
                    let mut value: usize = 0;
                    for &b in line.iter() {
                        let digit = b.wrapping_sub(b'0');
                        if digit > 9 {
                            return Err(InvalidLength::NonNumeric);
                        }
                        value = value.checked_mul(10).and_then(|v| v.checked_add(digit as usize)).ok_or(InvalidLength::TooLarge)?;
                    }
                    Ok(value)
                } else {
                    Err(InvalidLength::TooLarge)
                }
            })
        }
    }

    impl<T: WireRead + ?Sized> RespRead for T {}

    /// Extension trait re-export for convenience.
    pub trait RespReadExt: RespRead {}
    pub trait RespReadSyncExt: RespReadSync {}

    impl<T: RespRead + ?Sized> RespReadExt for T {}
    impl<T: RespReadSync + ?Sized> RespReadSyncExt for T {}
}
