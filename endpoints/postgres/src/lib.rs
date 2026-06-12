#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::field_reassign_with_default, clippy::approx_constant, clippy::manual_range_contains)
)]
pub use endpoint_types::*;

pub mod api;
pub mod catalog;
pub mod ep;
pub mod metadata;
pub mod output;
pub mod request;
pub mod serde;

#[cfg(test)]
mod test_utils;

#[cfg(test)]
mod integration_tests;

pub mod protocol;

pub use serde::PostgresOperation;
