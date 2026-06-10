//! ClickHouse native binary protocol (TCP port 9000).
//!
//! This module provides parsing and serialization for ClickHouse's native
//! wire protocol used for direct TCP connections.

pub mod block;
pub mod block_info;
pub mod client;
pub mod client_info;
pub mod column;
pub mod compression;
pub mod packet;
pub mod read;
pub mod server;
pub mod settings;
pub mod varint;
pub mod write;

// Re-export commonly used items
pub use block::Block;
pub use block_info::BlockInfo;
pub use client_info::ClientInfo;
pub use compression::CompressionMethod;
pub use packet::{ClientPacketType, ServerPacketType};
pub use read::{ClickhouseReadExt, ClickhouseReadSyncExt};
pub use settings::Settings;
pub use write::ClickhouseWriteExt;
