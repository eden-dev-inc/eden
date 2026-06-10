#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Node Management
//!
//! Eden node coordination and endpoint management.
//!
//! Provides address book, endpoint tracking, and node implementations.

mod address_book;
mod endpoints;
mod implementations;

pub use address_book::*;
pub use endpoints::*;
pub use implementations::*;
