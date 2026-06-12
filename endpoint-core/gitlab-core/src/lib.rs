#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::GitlabClient;
use deadpool::unmanaged::Pool;

/// Type alias for GitLab async client pool (read operations).
pub type GitlabAsync = Pool<GitlabClient>;

/// Type alias for GitLab client pool (write operations).
pub type GitlabTx = Pool<GitlabClient>;
