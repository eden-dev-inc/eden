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

//! ClickHouse Wire Protocol streaming parser.
//!
//! This crate provides streaming parsers for ClickHouse protocols:
//! - Native binary protocol (TCP port 9000)
//! - HTTP API (port 8123)
//!
//! # Features
//!
//! - **Zero-copy parsing**: Borrow directly from input buffers where possible
//! - **Streaming support**: Parse data as it arrives
//! - **Dual sync/async APIs**: Use sync for complete buffers, async for streaming
//! - **LZ4 compression**: Full support for compressed data blocks
//!
//! # Example
//!
//! ```rust,ignore
//! use wire_stream::SliceStream;
//! use clickhouse_wire::native::{ServerPacketType, server::ServerHello};
//! use clickhouse_wire::ClickhouseReadSyncExt;
//!
//! let data: &[u8] = /* wire message bytes */;
//! let stream = SliceStream::new(data);
//!
//! let packet_type = stream.read_varuint_sync()?;
//! match ServerPacketType::from_u64(packet_type) {
//!     Some(ServerPacketType::Hello) => {
//!         let hello = ServerHello::parse_sync(&stream, DBMS_TCP_PROTOCOL_VERSION)?;
//!         println!("Connected to: {}", hello.server_name);
//!     }
//!     // ...
//! }
//! ```

pub mod error;
pub mod native;

pub mod http;

#[cfg(test)]
mod tests;

// ============================================================================
// Constants
// ============================================================================

/// Maximum string size (1 GB, ClickHouse limit).
pub const MAX_STRING_SIZE: usize = 1 << 30;

/// Maximum VarUInt encoding length (9 bytes for u64).
pub const VARINT_MAX_BYTES: usize = 9;

/// ClickHouse TCP protocol version.
pub const DBMS_TCP_PROTOCOL_VERSION: u64 = 54448;

/// Maximum decompressed block size (256 MB).
pub const MAX_DECOMPRESSED_SIZE: usize = 256 * 1024 * 1024;

/// Maximum block size (1 MB default).
pub const MAX_BLOCK_SIZE: usize = 1024 * 1024;

// ============================================================================
// Re-exports
// ============================================================================

pub use error::ClickhouseWireError;
pub use native::packet::{ClientPacketType, ServerPacketType};
pub use native::read::{ClickhouseReadExt, ClickhouseReadSyncExt};
pub use native::write::ClickhouseWriteExt;

// Re-export wire-stream types
pub use wire_stream::{
    BorrowTracker, SliceBorrow, SliceBorrowConst, SliceCursor, SliceReadError, SliceStream, WireRead, WireReadExt, WireReadSync,
    WireReadSyncExt,
};
