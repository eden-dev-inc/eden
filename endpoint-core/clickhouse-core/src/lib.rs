#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # ClickHouse Endpoint Core
//!
//! ClickHouse driver integration using the official `clickhouse` client with `deadpool` pooling.
//! Also provides native protocol (TCP port 9000) support for low-latency and proxy scenarios.
//!
//! ## Usage
//!
//! ```ignore
//! use clickhouse_core::config::ClickhouseConfig;
//! use clickhouse_core::connection::{ClickhouseCredentials, ClickhouseTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = ClickhouseConfig {
//!     target: ClickhouseTarget {
//!         url: "http://localhost:8123".to_string(),
//!         database: Some("default".to_string()),
//!         ..Default::default()
//!     },
//!     read_credentials: Some(ClickhouseCredentials {
//!         user: Some("default".to_string()),
//!         password: Some("".to_string()),
//!     }),
//!     write_credentials: Some(ClickhouseCredentials {
//!         user: Some("default".to_string()),
//!         password: Some("".to_string()),
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Optimized for analytics (OLAP) workloads with columnar storage.

pub mod codec;
pub mod config;
pub mod connection;
// TODO: revisit when telemetry is added to this module
// #[named] is applied for future function_name!() use in telemetry spans.
#[allow(unused_macros)]
pub mod native_client;

use deadpool::unmanaged::Pool;

/// Type alias for ClickHouse async client pool (read operations via HTTP).
pub type ClickhouseAsync = Pool<clickhouse::Client>;

/// Type alias for ClickHouse client pool (write operations via HTTP).
pub type ClickhouseTx = Pool<clickhouse::Client>;

/// Type alias for ClickHouse native protocol client pool.
pub type ClickhouseNativePool = Pool<native_client::ClickhouseNativeClient>;

// Re-exports
pub use codec::{ClickhouseBuffer, ClickhouseStream};
pub use native_client::{ClickhouseNativeClient, ClickhouseResponse};
