#![cfg_attr(test, allow(clippy::unwrap_used))]

#[cfg(feature = "sdk")]
pub mod comm;
pub mod config;
pub mod connection;

#[cfg(feature = "sdk")]
use comm::DatadogClient;
#[cfg(feature = "sdk")]
use deadpool::unmanaged::Pool;

#[cfg(feature = "sdk")]
pub type DatadogAsync = Pool<DatadogClient>;

#[cfg(feature = "sdk")]
pub type DatadogTx = Pool<DatadogClient>;
