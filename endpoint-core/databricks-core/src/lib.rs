#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod client;
pub mod config;
pub mod connection;

use client::DatabricksClient;
use deadpool::unmanaged::Pool;

pub type DatabricksAsync = Pool<DatabricksClient>;

pub type DatabricksTx = Pool<DatabricksClient>;
