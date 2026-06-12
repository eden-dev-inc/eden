//! STC module layout conventions (ClickHouse):
//! - `<domain>.rs`: data models, request definitions, sync entrypoint, public helpers.
//! - `<domain>/core_sync.rs`: mandatory/cheap metadata collection.
//! - `<domain>/detailed_sync.rs`: conditional/expensive detailed collection.
//! - `<domain>/parsers.rs`: row-to-model parsing logic.

pub(crate) mod activity;
pub(crate) mod cluster;
pub(crate) mod connections;
pub(crate) mod database;
pub(crate) mod dictionaries;
pub(crate) mod merges;
pub(crate) mod mutations;
pub(crate) mod parts;
pub(crate) mod queries;
pub(crate) mod replication;
pub(crate) mod settings;
pub(crate) mod storage;
pub(crate) mod tables;
mod utils;
pub(crate) mod zookeeper;
