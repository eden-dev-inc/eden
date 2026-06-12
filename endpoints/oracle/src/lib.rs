#![cfg_attr(
    test,
    allow(clippy::unwrap_used, clippy::field_reassign_with_default, clippy::approx_constant, clippy::manual_range_contains)
)]
pub use endpoint_types::*;

pub mod api;
pub mod ep;
pub mod metadata;
pub mod request;
pub mod serde;

pub use serde::OracleOperation;
