use crate::api::lib::query::QueryInput;
use crate::metadata::stc::common::{bytes_to_gb, bytes_to_mb, ratio_percentage, status_by_count, status_by_flags};
use crate::metadata::stc::utils::{RowExt, map_rows, run_single_row};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use oracle_client::Row;
use oracle_core::OracleAsync;
use std::collections::HashMap;
use std::time::Duration;
use telemetry::TelemetryWrapper;

mod collection;
mod collector;
mod metrics;
mod models;

pub use models::*;
