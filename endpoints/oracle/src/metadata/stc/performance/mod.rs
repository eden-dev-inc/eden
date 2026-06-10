use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{RowExt, run_named_query};
use chrono::Utc;
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use oracle_client::Row;
use oracle_core::OracleAsync;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use telemetry::TelemetryWrapper;

mod collection;
mod collector;
mod methods;
mod models;

pub use models::*;
