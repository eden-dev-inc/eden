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

//! Protocol-agnostic streaming primitives for zero-copy wire protocol parsing.
//!
//! This crate provides the foundational abstractions for building streaming parsers
//! for binary and text protocols. It is designed to enable:
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers
//! - **Incremental processing**: Parse and act on data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//!
//! # Core Abstractions
//!
//! - [`WireReadSync`] - Synchronous streaming read trait for complete buffers
//! - [`WireRead`] - Async extension for streaming I/O
//! - [`SliceStream`] - Zero-overhead implementation for `&[u8]`
//!
//! # Example
//!
//! ```rust
//! use wire_stream::{SliceStream, WireReadSync, WireReadSyncExt};
//!
//! let data = b"Hello\r\nWorld\r\n";
//! let stream = SliceStream::new(data);
//!
//! // Peek at data without consuming
//! let peeked = stream.peek(Some(5)).unwrap();
//! assert_eq!(&*peeked, b"Hello");
//!
//! // Read until CRLF
//! let line = stream.read_to_crlf_sync(None).unwrap().unwrap();
//! assert_eq!(&*line, b"Hello");
//! ```

mod borrow_tracker;
mod read;
mod slice_stream;

pub use borrow_tracker::{BorrowTracker, PositionBorrow, SpanBorrow};
pub use read::*;
pub use slice_stream::{SliceBorrow, SliceBorrowConst, SliceCursor, SliceReadError, SliceStream};

#[cfg(test)]
mod proptest;
#[cfg(test)]
mod tests;
