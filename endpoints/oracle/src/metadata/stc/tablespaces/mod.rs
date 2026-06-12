use crate::api::lib::query::QueryInput;
use crate::metadata::stc::common::{bytes_to_gb, ratio_percentage, status_by_flags, status_by_low_threshold};
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
mod methods;
mod models;

pub use models::*;
