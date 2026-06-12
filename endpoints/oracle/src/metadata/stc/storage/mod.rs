use crate::api::lib::query::QueryInput;
use crate::metadata::stc::common::{bytes_to_gb, ratio_percentage, status_by_count, status_by_flags, status_by_high_threshold};
use crate::metadata::stc::utils::{RowExt, map_rows, run_named_query};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use oracle_client::Row;
use oracle_core::OracleAsync;
use std::collections::HashMap;
use std::time::Duration;
use telemetry::TelemetryWrapper;

mod collection;
mod collector;
mod methods;
mod models;

pub use models::*;
