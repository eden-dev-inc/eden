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

//! MongoDB Wire Protocol streaming parser.
//!
//! This crate provides a streaming parser for the MongoDB Wire Protocol,
//! built on top of the `wire-stream` primitives.
//!
//! # Features
//!
//! - **Zero-copy header parsing**: Parse message headers without copying
//! - **Streaming message bodies**: Process large documents incrementally
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming I/O
//! - **CVE-2025-14847 Mitigation**: Validates compressed message integrity
//!
//! # MongoDB Wire Protocol Overview
//!
//! MongoDB uses a binary protocol with:
//! - 16-byte message header (length, request ID, response ID, opcode)
//! - Length-prefixed message body
//! - BSON documents for data encoding
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use mongo_wire::{WireMessage, OpCode};
//!
//! let data: &[u8] = /* wire message bytes */;
//! let stream = SliceStream::new(data);
//!
//! let header = WireMessage::parse_header_sync(&stream)?;
//! println!("OpCode: {:?}, Length: {}", header.op_code(), header.message_length());
//! ```

pub mod error;
pub mod header;
pub mod op_compressed;
pub mod op_delete;
pub mod op_get_more;
pub mod op_insert;
pub mod op_kill_cursors;
pub mod op_msg;
pub mod op_msg_ref;
pub mod op_query;
pub mod op_reply;
pub mod op_update;
pub mod read;
#[cfg(test)]
pub mod tests;
pub mod write;

/// Maximum MongoDB message size (48MB, same as MongoDB default).
pub const MAX_MESSAGE_SIZE: usize = 48 * 1024 * 1024;

/// Maximum BSON document size (16MB, MongoDB limit).
pub const MAX_BSON_DOCUMENT_SIZE: usize = 16 * 1024 * 1024;

/// Maximum BSON string size (16MB).
pub const MAX_BSON_STRING_SIZE: usize = 16 * 1024 * 1024;

/// Maximum documents in a single response.
pub const MAX_DOCUMENTS_PER_MESSAGE: usize = 100_000;

pub use read::*;
pub use wire_stream::{BorrowTracker, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync, WireReadSyncExt};
pub use write::*;

pub use error::MongoWireError;
pub use header::{MessageHeader, OpCode};
pub use op_compressed::{CompressorId, MAX_UNCOMPRESSED_SIZE, OpCompressed};
#[allow(deprecated)]
pub use op_delete::OpDelete;
#[allow(deprecated)]
pub use op_get_more::OpGetMore;
#[allow(deprecated)]
pub use op_insert::OpInsert;
#[allow(deprecated)]
pub use op_kill_cursors::{OpKillCursors, OpKillCursorsBuilder};
pub use op_msg::{OpMsg, OpMsgSection};
pub use op_msg_ref::{DocumentIterator, DocumentSequence, OpMsgRef, OpMsgSectionRef};
pub use op_query::OpQuery;
pub use op_reply::OpReply;
#[allow(deprecated)]
pub use op_update::OpUpdate;
