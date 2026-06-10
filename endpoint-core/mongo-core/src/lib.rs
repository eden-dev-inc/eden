#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # MongoDB Endpoint Core
//!
//! MongoDB driver integration using the official `mongodb` driver with `deadpool` pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use mongo_core::config::MongoConfig;
//! use mongo_core::connection::{MongoCredentials, MongoTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = MongoConfig {
//!     auth: None,
//!     target: MongoTarget {
//!         url: "mongodb://localhost:27017/mydb".to_string(),
//!     },
//!     read_credentials: Some(MongoCredentials { auth: None }),
//!     write_credentials: Some(MongoCredentials { auth: None }),
//!     content: Default::default(),
//!     accept: Default::default(),
//!     api_key: String::new(),
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports multiple auth mechanisms (SCRAM-SHA-256, X.509, AWS, OIDC) and MongoDB Atlas.

pub mod auth;
pub mod config;
pub mod connection;

use deadpool::unmanaged::Pool;
use mongodb::{Client, ClientSession};

pub use config::*;
pub use connection::*;

/// Type alias for MongoDB async client pool.
pub type MongoAsync = Pool<Client>;

/// Type alias for MongoDB client sessions (transaction support).
pub type MongoTx = ClientSession;
