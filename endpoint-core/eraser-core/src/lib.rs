#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::EraserClient;
use deadpool::unmanaged::Pool;

/// Type alias for Eraser async client pool (read operations).
pub type EraserAsync = Pool<EraserClient>;

/// Type alias for Eraser client pool (write operations).
pub type EraserTx = Pool<EraserClient>;
