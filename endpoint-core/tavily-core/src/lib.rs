#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::TavilyClient;
use deadpool::unmanaged::Pool;

/// Type alias for Tavily async client pool (read operations).
pub type TavilyAsync = Pool<TavilyClient>;

/// Type alias for Tavily client pool (write operations).
pub type TavilyTx = Pool<TavilyClient>;
