#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Pinecone Endpoint Core
//!
//! Pinecone vector database integration for similarity search and embeddings storage.
//!
//! ## Usage
//!
//! ```ignore
//! use pinecone_core::config::PineconeConfig;
//! use pinecone_core::connection::{PineconeCredentials, PineconeTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PineconeConfig {
//!     target: PineconeTarget {
//!         url: "https://my-index-xxxx.svc.us-west1-gcp.pinecone.io".to_string(),
//!     }),
//!     read_credentials: Some(PineconeCredentials {
//!         token: "your-api-key".to_string(),
//!     }),
//!     write_credentials: Some(PineconeCredentials {
//!         token: "your-api-key".to_string(),
//!     }),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports vector upsert, query (k-NN search), fetch, delete, and metadata updates.

pub mod comm;
pub mod config;
pub mod connection;

use comm::PineconeClient;
use deadpool::unmanaged::Pool;

/// Type alias for Pinecone async client pool (read operations).
pub type PineconeAsync = Pool<PineconeClient>;

/// Type alias for Pinecone client pool (write operations).
pub type PineconeTx = Pool<PineconeClient>;
