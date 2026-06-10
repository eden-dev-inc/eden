#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # PostgreSQL Endpoint Core
//!
//! PostgreSQL raw wire protocol integration with deadpool connection pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use postgres_core::config::PostgresConfig;
//! use postgres_core::connection::{PostgresCredentials, PostgresTarget, SslMode};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PostgresConfig {
//!     target: PostgresTarget {
//!         host: "localhost".to_string(),
//!         port: 5432,
//!         database: Some("mydb".to_string()),
//!         sslmode: Some(SslMode::Require),
//!     }),
//!     read_credentials: Some(PostgresCredentials {
//!         username: "user".to_string(),
//!         password: Some("pass".to_string()),
//!     }),
//!     write_credentials: Some(PostgresCredentials {
//!         username: "user".to_string(),
//!         password: Some("pass".to_string()),
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports TLS/SSL with modes: Disable, Prefer, Require.

pub mod client;
pub mod codec;
pub mod config;
pub mod connection;
pub mod pool;
pub mod typed;
pub mod url;

pub use config::*;
pub use connection::*;
pub use typed::{
    PgSimpleRow, StatementResult, check_for_error, extract_command_complete_count, parse_simple_query_response,
    parse_simple_query_statements,
};

// Raw wire protocol pool
use deadpool::managed::Pool;
use pool::PgConnectionManager;

/// Raw wire protocol connection pool for PostgreSQL.
pub type PgRawPool = Pool<PgConnectionManager>;

/// Type alias for PostgreSQL async connection pool (read operations).
pub type PostgresAsync = PgRawPool;

/// Type alias for PostgreSQL transaction pool (write operations).
pub type PostgresTx = PgRawPool;
