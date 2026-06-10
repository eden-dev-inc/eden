//! ClickHouse analytics row types, table constants, and DDL for Eden.
//!
//! This crate owns all row structs and DDL that define the ClickHouse
//! analytics schema.  It has no runtime dependencies (no tokio, dashmap,
//! etc.) so that `poll-clickhouse` can depend on it without pulling in
//! the full wire-analysis pipeline.

pub mod connection_metrics;
pub mod ddl;
pub mod discovery;
pub mod events;
pub mod infrastructure;
#[cfg(feature = "llm")]
pub mod llm;
pub mod poll;
pub mod telemetry;
pub mod wire;

pub use ch_push::insert_batch;
