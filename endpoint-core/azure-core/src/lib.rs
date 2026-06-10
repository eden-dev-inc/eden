#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::AzureClient;
use deadpool::unmanaged::Pool;

pub type AzureAsync = Pool<AzureClient>;

pub type AzureTx = Pool<AzureClient>;
