#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Snowflake Endpoint Core
//!
//! Snowflake client integration using reqwest with `deadpool` pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use snowflake_core::config::SnowflakeConfig;
//! use snowflake_core::connection::{SnowflakeCredentials, SnowflakeTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = SnowflakeConfig {
//!     target: SnowflakeTarget {
//!         account: "xy12345.us-east-1".to_string(),
//!         warehouse: Some("COMPUTE_WH".to_string()),
//!         database: Some("MY_DB".to_string()),
//!         ..Default::default()
//!     },
//!     read_credentials: Some(SnowflakeCredentials {
//!         user: "MY_USER".to_string(),
//!         private_key: Some("...".to_string()),
//!         ..Default::default()
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Optimized for cloud data warehouse workloads with SQL API access.

pub mod client;
pub mod config;
pub mod connection;

use client::SnowflakeClient;
use deadpool::unmanaged::Pool;
use std::sync::Arc;

/// Type alias for Snowflake async client pool (read operations).
pub type SnowflakeAsync = Pool<Arc<SnowflakeClient>>;

/// Type alias for Snowflake client pool (write operations).
pub type SnowflakeTx = Pool<Arc<SnowflakeClient>>;
