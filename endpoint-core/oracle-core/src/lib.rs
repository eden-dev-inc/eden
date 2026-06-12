#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Oracle Database Endpoint Core
//!
//! Oracle Database driver integration using `oracle` crate with `bb8` pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use oracle_core::config::OracleConfig;
//! use oracle_core::connection::{OracleAuth, OracleCredentials, OracleTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = OracleConfig {
//!     target: OracleTarget {
//!         url: "//localhost:1521/ORCL".to_string(),
//!     },
//!     read_credentials: Some(OracleCredentials {
//!         auth: OracleAuth {
//!             username: "system".to_string(),
//!             password: "oracle".to_string(),
//!             privilege: None,
//!         },
//!     }),
//!     write_credentials: Some(OracleCredentials {
//!         auth: OracleAuth {
//!             username: "system".to_string(),
//!             password: "oracle".to_string(),
//!             privilege: None,
//!         },
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports username/password, Oracle Wallet, and Kerberos authentication.

pub mod auth;
pub mod comm;
pub mod config;
pub mod connection;

use bb8::Pool;
use bb8_oracle::OracleConnectionManager;
use error::EpError;

/// Type alias for Oracle async connection pool (read operations).
pub type OracleAsync = Pool<OracleConnectionManager>;

/// Type alias for Oracle connection pool (write operations).
pub type OracleTx = Pool<OracleConnectionManager>;

#[allow(dead_code)] // Test/development function
async fn a(sync: OracleAsync) -> Result<(), EpError> {
    let client = sync.get().await.map_err(EpError::connect)?;
    client.clear_object_type_cache().map_err(|e| EpError::request(e.to_string()))?;
    Ok(())
}
