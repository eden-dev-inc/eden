#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Database Abstractions
//!
//! Low-level database connection and configuration types.
//!
//! Defines [`DBKind`] enum and [`DB`] trait for database operations.

mod config;
mod db;

pub use config::{ConnectionParameters, DBKind};
pub use db::DB;
