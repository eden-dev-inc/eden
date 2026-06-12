#![cfg_attr(test, allow(clippy::unwrap_used))]
//! Endpoint runtime dispatch.
//!
//! This crate owns the shared endpoint runtime facade used by the HTTP
//! service, gateway, and migration crates. Provider features gate which
//! endpoint implementations are registered into the runtime router.

pub mod comp;
pub mod servers;
#[cfg(test)]
pub mod test_utils;

pub use comp::MyEngineService;
