#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::GoogleWorkspaceClient;
use deadpool::unmanaged::Pool;

/// Type alias for Google Workspace async client pool (read operations).
pub type GoogleWorkspaceAsync = Pool<GoogleWorkspaceClient>;

/// Type alias for Google Workspace client pool (write operations).
pub type GoogleWorkspaceTx = Pool<GoogleWorkspaceClient>;
