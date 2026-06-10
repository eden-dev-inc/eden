#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # HTTP Endpoint Core
//!
//! HTTP/HTTPS endpoint support for treating REST APIs as queryable "databases".
//!
//! ## Usage
//!
//! ```ignore
//! use http_core::config::HttpConfig;
//! use http_core::connection::{HttpCredentials, HttpTarget};
//! use std::collections::HashMap;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut headers = HashMap::new();
//! headers.insert("Authorization".to_string(), "Bearer token".to_string());
//!
//! let config = HttpConfig {
//!     target: HttpTarget {
//!         url: "https://api.example.com".to_string(),
//!     },
//!     read_credentials: Some(HttpCredentials {
//!         headers: Some(headers.clone()),
//!     }),
//!     write_credentials: Some(HttpCredentials {
//!         headers: Some(headers),
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports GET, POST, PUT, DELETE methods with custom headers and JSON bodies.

pub mod comm;
pub mod config;
pub mod connection;

use comm::HttpClient;
use deadpool::unmanaged::Pool;

/// Type alias for HTTP async client pool (read operations).
pub type HttpAsync = Pool<HttpClient>;

/// Type alias for HTTP client pool (write operations).
pub type HttpTx = Pool<HttpClient>;
