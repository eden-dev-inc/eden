#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::PosthogClient;
use deadpool::unmanaged::Pool;

/// Type alias for PostHog async client pool (read operations).
pub type PosthogAsync = Pool<PosthogClient>;

/// Type alias for PostHog client pool (write operations).
pub type PosthogTx = Pool<PosthogClient>;
