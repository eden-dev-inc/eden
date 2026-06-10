#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # JSON Utilities
//!
//! JSON manipulation and transformation utilities for Eve.
//!
//! ## Overview
//!
//! Provides specialized functions for working with `serde_json::Value`:
//! - Extracting nested values
//! - Filtering by conditions
//! - Flattening/unflattening structures
//! - Mathematical operations on JSON data
//!
//! ## Modules
//!
//! - [`extract`] - Extract values from nested JSON structures
//! - [`filter`] - Filter JSON objects/arrays by conditions
//! - [`flatten`] - Convert nested JSON to flat key-value pairs
//! - [`unflatten`] - Reconstruct nested JSON from flat format
//! - [`map`] - Transform JSON values with mapping functions
//! - [`math`] - Perform arithmetic operations on JSON numbers
//! - [`parse`] - Parse JSON strings and validate structure
//! - [`reduce`] - Aggregate JSON array values

pub mod extract;
pub mod filter;
pub mod flatten;
pub mod map;
pub mod math;
pub mod parse;
pub mod reduce;
pub mod unflatten;
