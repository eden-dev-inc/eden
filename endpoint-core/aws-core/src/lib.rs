#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod comm;
pub mod config;
pub mod connection;

use comm::AwsClient;
use deadpool::unmanaged::Pool;

pub type AwsAsync = Pool<AwsClient>;

pub type AwsTx = Pool<AwsClient>;
