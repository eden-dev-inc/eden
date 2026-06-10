#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # MySQL Endpoint Core
//!
//! MySQL/MariaDB driver integration using `mysql_async` with native connection pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use mysql_core::config::MysqlConfig;
//! use mysql_core::connection::{MysqlCredentials, MysqlTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MysqlConfig {
//!     target: MysqlTarget {
//!         host: "localhost".to_string(),
//!         port: Some(3306),
//!         database: Some("mydb".to_string()),
//!     }),
//!     read_credentials: Some(MysqlCredentials {
//!         username: "user".to_string(),
//!         password: Some("password".to_string()),
//!     }),
//!     write_credentials: Some(MysqlCredentials {
//!         username: "user".to_string(),
//!         password: Some("password".to_string()),
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```

pub mod comm;
pub mod config;
pub mod connection;

/// Type alias for MySQL async connection pool (read operations).
pub type MysqlAsync = mysql_async::Pool;

/// Type alias for MySQL connection pool (write operations).
pub type MysqlTx = mysql_async::Pool;
