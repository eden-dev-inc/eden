#![cfg_attr(test, allow(clippy::unwrap_used))]
pub use endpoint_types::*;

pub mod api;
pub mod command;
pub mod ep;
pub mod metadata;
pub mod output;
// TODO: revisit once run_transaction_generic stubs are implemented with telemetry.
// #[named] is applied for future function_name!() use in telemetry spans.
#[allow(unused_macros)]
pub mod protocol;
pub mod redis_like;
pub mod request;
pub mod serde;

#[cfg(test)]
mod test_utils;

pub use serde::RedisOperation;
