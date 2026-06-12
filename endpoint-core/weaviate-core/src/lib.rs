pub mod comm;
pub mod config;
pub mod connection;

use comm::WeaviateClient;
use deadpool::unmanaged::Pool;

/// Type alias for Weaviate async client pool (read operations).
pub type WeaviateAsync = Pool<WeaviateClient>;

/// Type alias for Weaviate client pool (write operations).
pub type WeaviateTx = Pool<WeaviateClient>;
