#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Microsoft SQL Server Endpoint Core
//!
//! MS SQL Server driver integration using `tiberius` with `deadpool` pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use mssql_core::config::MssqlConfig;
//! use mssql_core::connection::{MssqlAuth, MssqlCredentials, MssqlTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MssqlConfig {
//!     database: "mydb".to_string(),
//!     target: MssqlTarget {
//!         url: "server=tcp:localhost,1433".to_string(),
//!     },
//!     read_credentials: Some(MssqlCredentials {
//!         auth: MssqlAuth {
//!             username: "sa".to_string(),
//!             password: "Password123!".to_string(),
//!         },
//!     }),
//!     write_credentials: Some(MssqlCredentials {
//!         auth: MssqlAuth {
//!             username: "sa".to_string(),
//!             password: "Password123!".to_string(),
//!         },
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports SQL Server and Windows authentication.

pub mod auth;
pub mod comm;
pub mod config;
pub mod connection;

use comm::MssqlClient;
use deadpool::unmanaged::Pool;

/// Type alias for MSSQL async client pool (read operations).
pub type MssqlAsync = Pool<MssqlClient>;

/// Type alias for MSSQL client pool (write operations).
pub type MssqlTx = Pool<MssqlClient>;
